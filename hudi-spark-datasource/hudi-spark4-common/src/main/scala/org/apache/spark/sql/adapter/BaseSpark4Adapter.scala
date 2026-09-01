/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.adapter

import org.apache.hudi.{AvroConversionUtils, DefaultSource, HoodieFileScanRDD, HoodieSchemaConversionUtils}
import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.spark.internal.ReflectUtil
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, DataFrameUtil, Dataset, Encoder, ExpressionColumnNodeWrapper, HoodieUnsafeUtils, HoodieUTF8StringFactory, Spark4DataFrameUtil, Spark4HoodieUnsafeUtils, Spark4HoodieUTF8StringFactory, SparkSession, SQLContext}
import org.apache.spark.sql.FileFormatUtilsForFileGroupReader.applyFiltersToPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, CreateNamedStruct, Expression, GetStructField, If, InterpretedPredicate, IsNull, Literal, Predicate, SpecializedGetters, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.classic.ColumnConversions
import org.apache.spark.sql.execution.{PartitionedFileUtil, QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReader, SparkOrcReaderBase}
import org.apache.spark.sql.execution.datasources.parquet.{HoodieFormatTrait, ParquetFilters, SparkShreddingUtils}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructField, StructType, VariantType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{BiConsumer, Consumer}

import scala.collection.JavaConverters._

/**
 * Base implementation of [[SparkAdapter]] for Spark 4.x branch
 */
abstract class BaseSpark4Adapter extends SparkAdapter with Logging {

  JsonUtils.registerModules()

  private val cache = new ConcurrentHashMap[ZoneId, DateFormatter](1)

  override def getDateFormatter(tz: TimeZone): DateFormatter = {
    cache.computeIfAbsent(tz.toZoneId, zoneId => ReflectUtil.getDateFormatter(zoneId))
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  /**
   * Checks whether [[LogicalPlan]] refers to Hudi table, and if it's the case extracts
   * corresponding [[CatalogTable]]
   */
  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    EliminateSubqueryAliases(plan) match {
      // First, we need to weed out unresolved plans
      case plan if !plan.resolved => None
      // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
      //       on top of it
      case PhysicalOperation(_, _, LogicalRelation(_, _, Some(table), _, _)) if isHoodieTable(table) => Some(table)
      case _ => None
    }
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new HoodieFileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  override def createOrcFileReader(vectorized: Boolean,
                                   sqlConf: SQLConf,
                                   options: Map[String, String],
                                   hadoopConf: Configuration,
                                   dataSchema: StructType): SparkColumnarFileReader = {
    SparkOrcReaderBase.build(vectorized, sqlConf, options, hadoopConf, dataSchema,
      (capacity, memoryMode) => new OrcColumnarBatchReader(capacity, memoryMode))
  }

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: HoodieSchema,
                              parameters: java.util.Map[String, String]): BaseRelation = {
    val dataSchema = Option(schema).map(HoodieSchemaConversionUtils.convertHoodieSchemaToStructType).orNull
    DefaultSource.createRelation(sqlContext, metaClient, dataSchema, parameters.asScala.toMap)
  }

  override def convertStorageLevelToString(level: StorageLevel): String

  override def translateFilter(predicate: Expression,
                               supportNestedPredicatePushdown: Boolean = false): Option[Filter] = {
    DataSourceStrategy.translateFilter(predicate, supportNestedPredicatePushdown)
  }

  override def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch = {
    new ColumnarBatch(vectors, numRows)
  }

  override def sqlExecutionWithNewExecutionId[T](sparkSession: SparkSession,
                                                 queryExecution: QueryExecution,
                                                 name: Option[String])(body: => T): T = {
      SQLExecution.withNewExecutionId(queryExecution, name)(body)
  }

  def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit

  override def getUTF8StringFactory: HoodieUTF8StringFactory = Spark4HoodieUTF8StringFactory

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils = HoodieSpark4PartitionedFileUtils

  override def splitFiles(sparkSession: SparkSession,
                          partitionDirectory: PartitionDirectory,
                          isSplitable: Boolean,
                          maxSplitSize: Long): Seq[PartitionedFile] = {
    partitionDirectory.files.flatMap(file =>
      PartitionedFileUtil.splitFiles(file, file.getPath, isSplitable, maxSplitSize, partitionDirectory.values)
    )
  }

  override def createColumnFromExpression(expression: Expression): Column = {
    new Column(ExpressionColumnNodeWrapper.apply(expression))
  }

  override def getExpressionFromColumn(column: Column): Expression = ColumnConversions.expression(column)

  override def getUnsafeUtils: HoodieUnsafeUtils = Spark4HoodieUnsafeUtils

  override def getDataFrameUtil: DataFrameUtil = Spark4DataFrameUtil

  override def internalCreateDataFrame(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType, isStreaming: Boolean = false): DataFrame = {
    spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession].internalCreateDataFrame(rdd, schema, isStreaming)
  }

  def createStreamingDataFrame(sqlContext: SQLContext, relation: HadoopFsRelation, requiredSchema: StructType): DataFrame = {
    val logicalRelation = LogicalRelation(relation, isStreaming = true)
    val resolvedSchema = logicalRelation.resolve(requiredSchema, sqlContext.sparkSession.sessionState.analyzer.resolver)
    org.apache.spark.sql.classic.Dataset.ofRows(sqlContext.sparkSession.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
      applyFiltersToPlan(logicalRelation, requiredSchema, resolvedSchema,
        relation.fileFormat.asInstanceOf[HoodieFormatTrait].getRequiredFilters))
  }

  override def createParquetFilters(schema: MessageType, storageConf: StorageConfiguration[_], sqlConf: SQLConf): ParquetFilters = {
    new ParquetFilters(
      schema,
      storageConf.getBoolean(SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key, sqlConf.parquetFilterPushDownDate),
      storageConf.getBoolean(SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key, sqlConf.parquetFilterPushDownTimestamp),
      storageConf.getBoolean(SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key, sqlConf.parquetFilterPushDownDecimal),
      storageConf.getBoolean(SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED.key, sqlConf.parquetFilterPushDownStringPredicate),
      storageConf.getInt(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key, sqlConf.parquetFilterPushDownInFilterThreshold),
      storageConf.getBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis),
      getRebaseSpec("CORRECTED"))
  }

  override def getVariantDataType: Option[DataType] = {
    Some(VariantType)
  }

  override def isDataTypeEqualForPhysicalSchema(requiredType: DataType, fileType: DataType): Option[Boolean] = {
    /**
     * Checks if a StructType is the physical representation of VariantType in Parquet.
     * VariantType is stored in Parquet as a struct with binary fields: "metadata" and "value".
     * Supports both unshredded (2 fields) and shredded (3 fields with "typed_value") layouts.
     */
    // TODO(voon) parquet-1.16: replace this name/arity shape heuristic with a VariantLogicalTypeAnnotation check once all supported parquet versions are >= 1.16.
    def isVariantPhysicalSchema(structType: StructType): Boolean = {
      val fieldMap = structType.fields.map(f => (f.name, f.dataType)).toMap
      val hasRequiredFields = fieldMap.contains(HoodieSchema.Variant.VARIANT_VALUE_FIELD) &&
        fieldMap.contains(HoodieSchema.Variant.VARIANT_METADATA_FIELD) &&
        fieldMap(HoodieSchema.Variant.VARIANT_VALUE_FIELD) == BinaryType &&
        fieldMap(HoodieSchema.Variant.VARIANT_METADATA_FIELD) == BinaryType
      val isUnshredded = structType.fields.length == 2
      val isShredded = structType.fields.length == 3 &&
        fieldMap.contains(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD)
      hasRequiredFields && (isUnshredded || isShredded)
    }

    // Handle VariantType comparisons
    (requiredType, fileType) match {
      case (_: VariantType, s: StructType) if isVariantPhysicalSchema(s) => Some(true)
      case (s: StructType, _: VariantType) if isVariantPhysicalSchema(s) => Some(true)
      // Spark 4.1's PushVariantIntoScan rewrites a `v: VariantType` column into a
      // pushed-down projection struct (each child carries `VariantMetadata`). When the file
      // stores `v` as a real Variant, the projection struct is NOT a type change — parquet-mr
      // reads the variant natively and projects per-row using the field metadata. Treat the
      // pair as compatible so Hudi's schema-change machinery doesn't rewrite the requested
      // schema back to `VariantType` (which would lose the projection metadata).
      case (s: StructType, _: VariantType) if isVariantProjectionStruct(s) => Some(true)
      case (_: VariantType, s: StructType) if isVariantProjectionStruct(s) => Some(true)
      case _ => None // Not a VariantType comparison, use default logic
    }
  }

  override def isVariantType(dataType: DataType): Boolean = {
    dataType.isInstanceOf[VariantType]
  }

  override def createVariantValueWriter(
    dataType: DataType,
    writeValue: Consumer[Array[Byte]],
    writeMetadata: Consumer[Array[Byte]]
  ): BiConsumer[SpecializedGetters, Integer] = {
    if (!isVariantType(dataType)) {
      throw new IllegalArgumentException(s"Expected VariantType but got $dataType")
    }

    (row: SpecializedGetters, ordinal: Integer) => {
      val variant = row.getVariant(ordinal)
      writeMetadata.accept(variant.getMetadata)
      writeValue.accept(variant.getValue)
    }
  }

  override def convertVariantFieldToParquetType(
    dataType: DataType,
    fieldName: String,
    fieldSchema: HoodieSchema,
    repetition: Repetition
  ): Type = {
    if (!isVariantType(dataType)) {
      throw new IllegalArgumentException(s"Expected VariantType but got $dataType")
    }

    // Determine if this is a shredded variant
    val isShredded = fieldSchema match {
      case variant: HoodieSchema.Variant => variant.isShredded
      case _ => false
    }

    // For shredded variants, the value field is OPTIONAL (nullable)
    // For unshredded variants, the value field is REQUIRED
    val valueRepetition = if (isShredded) Repetition.OPTIONAL else Repetition.REQUIRED

    // VariantType is always stored in Parquet as a struct with separate value and metadata binary fields.
    // This matches how the HoodieRowParquetWriteSupport writes variant data.
    // Note: We intentionally omit 'typed_value' for shredded variants as this writer only accesses raw binary blobs.
    // The variant LogicalTypeAnnotation is applied via applyVariantLogicalType, Spark 4.0 (parquet 1.15.2)
    // is a no-op since the annotation only exists in parquet 1.16.0+; Spark 4.1 overrides to apply it.
    val builder = Types.buildGroup(repetition)
      .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Repetition.REQUIRED).named(HoodieSchema.Variant.VARIANT_METADATA_FIELD))
      .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, valueRepetition).named(HoodieSchema.Variant.VARIANT_VALUE_FIELD))
    applyVariantLogicalType(builder).named(fieldName)
  }

  // TODO(#18935) drop-spark4.0: when all remaining 4.x adapters are parquet 1.16+, apply variantType() in this base and delete the no-op override plus the Spark4_1Adapter override.
  override def applyVariantLogicalType(builder: Types.GroupBuilder[GroupType]): Types.GroupBuilder[GroupType] = builder

  override def isVariantShreddingStruct(structType: StructType): Boolean = {
    SparkShreddingUtils.isVariantShreddingStruct(structType)
  }

  override def generateVariantWriteShreddingSchema(dataType: DataType, isTopLevel: Boolean, isObjectField: Boolean): StructType = {
    SparkShreddingUtils.addWriteShreddingMetadata(
      SparkShreddingUtils.variantShreddingSchema(dataType, isTopLevel, isObjectField))
  }

  /**
   * Shared implementation behind [[SparkAdapter#buildFullVariantReadSchema]] for the 4.x
   * adapters whose parquet reader can reconstruct shredded variants (4.1+, SPARK-54410).
   * Top-level fields only, matching the whole-variant request PushVariantIntoScan makes for a
   * root attribute; a variant below the top level is left native, and that reader rebuilds it
   * at any depth from a native VariantType request (#19775). Spark 4.0 keeps the default None:
   * the write-side methods above have no version gate, so it does write shredded files, but
   * its reader cannot rebuild them and the projection shape would not help.
   */
  protected final def rewriteTopLevelVariantsForFullRead(schema: StructType): Option[StructType] = {
    var rewritten = false
    val fields = schema.fields.map { f =>
      if (isVariantType(f.dataType)) {
        rewritten = true
        // Mirrors RequestedVariantField.fullVariant in PushVariantIntoScan: whole-variant
        // access is a single child "0" at path "$" with failOnError and UTC.
        f.copy(dataType = StructType(Array(StructField("0", VariantType,
          metadata = VariantMetadata("$", failOnError = true, timeZoneId = "UTC").toMetadata))))
      } else {
        f
      }
    }
    if (rewritten) Some(StructType(fields)) else None
  }

  /**
   * Shared implementation behind [[SparkAdapter#buildVariantProjector]] for the 4.x adapters
   * whose planner rewrites variants into projection structs (4.1+).
   *
   * Recurses into struct members, mirroring PushVariantIntoScan's `VariantInRelation.rewriteType`:
   * a variant is rewritten at the root of the relation output or below a STRUCT path, while
   * arrays and maps keep their native VariantType, so nothing under a collection is projected
   * here either. Before #19775 this walked top-level fields only, and a projection struct sitting
   * one struct member down was left holding a raw variant that the plan then read as its
   * projected children.
   */
  protected final def buildVariantProjectorForStructPaths(
      sparkDataSchema: StructType,
      sparkRequiredSchema: StructType): Option[InternalRow => InternalRow] = {
    // Quick check: does any required field carry a variant projection struct, at any depth?
    if (!sparkRequiredSchema.fields.exists(f => containsVariantProjection(f.dataType))) {
      None
    } else {
      // Surface mismatched schemas with both field lists rather than Spark's bare
      // IllegalArgumentException from fieldIndex. `path` is the dotted field path of `name`.
      def lookupDataField(dataStruct: StructType, requiredStruct: StructType,
                          name: String, path: String): (Int, StructField) = {
        val idx = dataStruct.getFieldIndex(name).getOrElse(
          throw new IllegalStateException(
            s"Required field '$path' is absent from sparkDataSchema; " +
              s"required=${requiredStruct.fieldNames.mkString("[", ",", "]")}, " +
              s"data=${dataStruct.fieldNames.mkString("[", ",", "]")}"))
        (idx, dataStruct.fields(idx))
      }

      // `ref` reads the data-schema value of type `dataType`; the result has type `requiredType`.
      def projectionExpr(ref: Expression, dataType: DataType, requiredType: DataType,
                         path: String): Expression = requiredType match {
        case projectedStruct: StructType if VariantMetadata.isVariantStruct(projectedStruct) =>
          require(isVariantType(dataType),
            s"Expected VariantType for field '$path' in data schema, got $dataType")
          val childExprs: Seq[Expression] = projectedStruct.fields.toSeq.flatMap { child =>
            val vm = VariantMetadata.fromMetadata(child.metadata)
            val pathLit = Literal(UTF8String.fromString(vm.path), StringType)
            val variantGet: Expression =
              VariantGet(ref, pathLit, child.dataType, vm.failOnError, Option(vm.timeZoneId))
            Seq(Literal(UTF8String.fromString(child.name), StringType), variantGet)
          }
          val projected = CreateNamedStruct(childExprs)
          // A null variant has to come out as a NULL struct, not a struct of nulls: CreateNamedStruct
          // is never null, the parquet paths leave the field null, and PushVariantIntoScan rewrites
          // IsNull(v) / IsNotNull(v) onto this struct directly.
          If(IsNull(ref), Literal(null, projected.dataType), projected)
        case requiredStruct: StructType =>
          dataType match {
            // Rebuild the struct member by member only when something below it is projected;
            // otherwise the reference is already in the required shape and is cheaper untouched.
            case dataStruct: StructType if containsVariantProjection(requiredStruct) =>
              val childExprs: Seq[Expression] = requiredStruct.fields.toSeq.flatMap { rf =>
                val childPath = s"$path.${rf.name}"
                val (childIdx, childField) = lookupDataField(dataStruct, requiredStruct, rf.name, childPath)
                val childRef = GetStructField(ref, childIdx, Some(rf.name))
                Seq(Literal(UTF8String.fromString(rf.name), StringType),
                  projectionExpr(childRef, childField.dataType, rf.dataType, childPath))
              }
              val rebuilt = CreateNamedStruct(childExprs)
              // CreateNamedStruct is never null, so a null struct would come back as a struct of
              // nulls without this guard.
              If(IsNull(ref), Literal(null, rebuilt.dataType), rebuilt)
            case _ => ref
          }
        case _ => ref
      }

      val exprs: Array[Expression] = sparkRequiredSchema.fields.map { rf =>
        val (dataIdx, dataField) = lookupDataField(sparkDataSchema, sparkRequiredSchema, rf.name, rf.name)
        val ref: Expression = BoundReference(dataIdx, dataField.dataType, dataField.nullable)
        projectionExpr(ref, dataField.dataType, rf.dataType, rf.name)
      }

      val projection = UnsafeProjection.create(exprs.toIndexedSeq, DataTypeUtils.toAttributes(sparkDataSchema))
      Some(row => projection(row))
    }
  }

  override def createShreddedVariantWriter(
    shreddedStructType: StructType,
    writeStruct: Consumer[InternalRow]
  ): BiConsumer[SpecializedGetters, Integer] = {
    val variantShreddingSchema = SparkShreddingUtils.buildVariantSchema(shreddedStructType)

    (row: SpecializedGetters, ordinal: Integer) => {
      val variantVal = row.getVariant(ordinal)
      val variant = new Variant(variantVal.getValue, variantVal.getMetadata)
      val shreddedValues = SparkShreddingUtils.castShredded(variant, variantShreddingSchema)
      writeStruct.accept(shreddedValues)
    }
  }

  override def extractVariantBinary(row: SpecializedGetters, ordinal: Int): VariantShreddingSchemaInferrer.VariantSample = {
    if (row.isNullAt(ordinal)) {
      null
    } else {
      val variantVal = row.getVariant(ordinal)
      // Defensive copies: Spark iterators reuse row instances and VariantVal exposes its backing arrays.
      new VariantShreddingSchemaInferrer.VariantSample(variantVal.getValue.clone(), variantVal.getMetadata.clone())
    }
  }
}
