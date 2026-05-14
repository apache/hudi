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

import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, Spark41HoodieFileScanRDD, Spark41HoodiePartitionCDCFileGroupMapping, Spark41HoodiePartitionFileSliceMapping}
import org.apache.hudi.client.model.{HoodieInternalRow, Spark41HoodieInternalRow}
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, Types}
import org.apache.spark.SparkEnv
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, CreateNamedStruct, Expression, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{METADATA_COL_ATTR_KEY, RebaseDateTime}
import org.apache.spark.sql.connector.catalog.{V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.lance.SparkLanceReaderBase
import org.apache.spark.sql.execution.datasources.orc.Spark41OrcReader
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark41LegacyHoodieParquetFileFormat, Spark41ParquetReader}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.hudi.{HoodieMemoryStream, SparkAdapter}
import org.apache.spark.sql.hudi.analysis.TableValuedFunctions
import org.apache.spark.sql.hudi.blob.{BatchedBlobReaderStrategy, ScalarFunctions}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.parser.{HoodieExtendedParserInterface, HoodieSpark4_1ExtendedSqlParser}
import org.apache.spark.sql.types.{DataType, DataTypes, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatchRow
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Implementation of [[SparkAdapter]] for Spark 4.1.x branch
 */
class Spark4_1Adapter extends BaseSpark4Adapter {

  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    super.resolveHoodieTable(plan).orElse {
      EliminateSubqueryAliases(plan) match {
        // First, we need to weed out unresolved plans
        case plan if !plan.resolved => None
        // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
        //       on top of it
        case PhysicalOperation(_, _, DataSourceV2Relation(v2: V2TableWithV1Fallback, _, _, _, _, _)) if isHoodieTable(v2) =>
          Some(v2.v1Table)
        case ResolvedTable(_, _, V1Table(v1Table), _) if isHoodieTable(v1Table) =>
          Some(v1Table)
        case _ => None
      }
    }
  }

  def isHoodieTable(v2Table: V2TableWithV1Fallback): Boolean = {
    v2Table.getClass.getName.contains("HoodieInternalV2Table")
  }

  override def isColumnarBatchRow(r: InternalRow): Boolean = r.isInstanceOf[ColumnarBatchRow]

  def createCatalystMetadataForMetaField: Metadata =
    new MetadataBuilder()
      .putBoolean(METADATA_COL_ATTR_KEY, value = true)
      .build()

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark41CatalystExpressionUtils

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark41CatalystPlanUtils

  override def getSchemaUtils: HoodieSchemaUtils = HoodieSpark41SchemaUtils

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils = HoodieSpark41PartitionedFileUtils

  override def newParseException(command: Option[String],
                                 exception: AnalysisException,
                                 start: Origin,
                                 stop: Origin): ParseException = {
    new ParseException(command, start, exception.getErrorClass, exception.getMessageParameters.asScala.toMap)
  }



  override def createAvroSerializer(rootCatalystType: DataType, rootType: HoodieSchema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark4_1AvroSerializer(rootCatalystType, rootType.toAvroSchema, nullable)

  override def createAvroDeserializer(rootType: HoodieSchema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark4_1AvroDeserializer(rootType.toAvroSchema, rootCatalystType)

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    new HoodieSpark4_1ExtendedSqlParser(spark, delegate)

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark41LegacyHoodieParquetFileFormat(appendPartitionValues))
  }

  override def createInternalRow(metaFields: Array[UTF8String],
                                 sourceRow: InternalRow,
                                 sourceContainsMetaFields: Boolean): HoodieInternalRow = {
    new Spark41HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
  }

  override def createPartitionCDCFileGroupMapping(partitionValues: InternalRow,
                                                  fileSplits: List[HoodieCDCFileSplit]): HoodiePartitionCDCFileGroupMapping = {
    new Spark41HoodiePartitionCDCFileGroupMapping(partitionValues, fileSplits)
  }

  override def createPartitionFileSliceMapping(values: InternalRow,
                                               slices: Map[String, FileSlice]): HoodiePartitionFileSliceMapping = {
    new Spark41HoodiePartitionFileSliceMapping(values, slices)
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark41HoodieFileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  override def extractDeleteCondition(deleteFromTable: Command): Expression = {
    deleteFromTable.asInstanceOf[DeleteFromTable].condition
  }

  override def injectTableFunctions(extensions: SparkSessionExtensions): Unit = {
    TableValuedFunctions.funcs.foreach(extensions.injectTableFunction)
  }

  override def injectScalarFunctions(extensions: SparkSessionExtensions): Unit = {
    ScalarFunctions.funcs.foreach(extensions.injectFunction)
  }

  override def injectPlannerStrategies(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      BatchedBlobReaderStrategy(session)
    }
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  override def convertStorageLevelToString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
    case DISK_ONLY_3 => "DISK_ONLY_3"
    case MEMORY_ONLY => "MEMORY_ONLY"
    case MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case OFF_HEAP => "OFF_HEAP"
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $level")
  }

  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return parquet file reader
   */
  override def createParquetFileReader(vectorized: Boolean,
                                       sqlConf: SQLConf,
                                       options: Map[String, String],
                                       hadoopConf: Configuration): SparkColumnarFileReader = {
    Spark41ParquetReader.build(vectorized, sqlConf, options, hadoopConf)
  }

  /**
   * Get ORC file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @param dataSchema the data schema of the ORC file
   * @return ORC file reader
   */
  override def createOrcFileReader(vectorized: Boolean,
                                   sqlConf: SQLConf,
                                   options: Map[String, String],
                                   hadoopConf: Configuration,
                                   dataSchema: StructType): SparkColumnarFileReader = {
    Spark41OrcReader.build(vectorized, sqlConf, options, hadoopConf, dataSchema)
  }

  override def createLanceFileReader(vectorized: Boolean,
                                     sqlConf: SQLConf,
                                     options: Map[String, String],
                                     hadoopConf: Configuration): Option[SparkColumnarFileReader] = {
    Some(new SparkLanceReaderBase(vectorized))
  }

  override def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit = {
    jssc.sc.stop(exitCode)
  }

  override def getDateTimeRebaseMode(): LegacyBehaviorPolicy.Value = {
    // See Spark3_5Adapter.getDateTimeRebaseMode for the rationale. In Spark 4.1
    // the ConfigEntry returns LegacyBehaviorPolicy.Value directly, so the
    // SparkConf string is parsed via withName before the orElse chain.
    val fromSqlConf = Option(SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE, null))
    val fromSparkConf = Option(SparkEnv.get)
      .flatMap(env => Option(env.conf.get(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key, null)))
      .map(LegacyBehaviorPolicy.withName)
    fromSqlConf.orElse(fromSparkConf)
      .getOrElse(SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE))
  }

  override def isLegacyBehaviorPolicy(value: Object): Boolean = {
    value == LegacyBehaviorPolicy.LEGACY
  }

  override def isTimestampNTZType(dataType: DataType): Boolean = {
    dataType == DataTypes.TimestampNTZType
  }

  override def getRebaseSpec(policy: String): RebaseDateTime.RebaseSpec = {
    RebaseDateTime.RebaseSpec(LegacyBehaviorPolicy.withName(policy))
  }

  override def isVariantProjectionStruct(structType: StructType): Boolean = {
    VariantMetadata.isVariantStruct(structType)
  }

  override def buildVariantProjector(sparkDataSchema: StructType,
                                     sparkRequiredSchema: StructType): Option[InternalRow => InternalRow] = {
    // Quick check: any required field a variant projection struct?
    if (!sparkRequiredSchema.fields.exists(f => VariantMetadata.isVariantStruct(f.dataType))) {
      None
    } else {
      // Surface mismatched schemas with both field lists rather than Spark's bare
      // IllegalArgumentException from fieldIndex.
      def lookupDataField(name: String): (Int, StructField) = {
        val idx = sparkDataSchema.getFieldIndex(name).getOrElse(
          throw new IllegalStateException(
            s"Required field '$name' is absent from sparkDataSchema; " +
              s"required=${sparkRequiredSchema.fieldNames.mkString("[", ",", "]")}, " +
              s"data=${sparkDataSchema.fieldNames.mkString("[", ",", "]")}"))
        (idx, sparkDataSchema.fields(idx))
      }
      val exprs: Array[Expression] = sparkRequiredSchema.fields.map { rf =>
        rf.dataType match {
          case projectedStruct: StructType if VariantMetadata.isVariantStruct(projectedStruct) =>
            val (dataIdx, dataField) = lookupDataField(rf.name)
            require(isVariantType(dataField.dataType),
              s"Expected VariantType for field '${rf.name}' in data schema, got ${dataField.dataType}")
            val variantRef: Expression = BoundReference(dataIdx, dataField.dataType, dataField.nullable)
            val childExprs: Seq[Expression] = projectedStruct.fields.toSeq.flatMap { child =>
              val vm = VariantMetadata.fromMetadata(child.metadata)
              val pathLit = Literal(UTF8String.fromString(vm.path), DataTypes.StringType)
              val tz: Option[String] = Option(vm.timeZoneId)
              val variantGet: Expression = VariantGet(variantRef, pathLit, child.dataType, vm.failOnError, tz)
              Seq(Literal(UTF8String.fromString(child.name), DataTypes.StringType), variantGet)
            }
            CreateNamedStruct(childExprs)
          case _ =>
            val (dataIdx, dataField) = lookupDataField(rf.name)
            BoundReference(dataIdx, dataField.dataType, dataField.nullable)
        }
      }

      val projection = UnsafeProjection.create(exprs.toIndexedSeq, DataTypeUtils.toAttributes(sparkDataSchema))
      Some(row => projection(row))
    }
  }

  // Apply LogicalTypeAnnotation.variantType((byte) 1) to the variant group, matching parquet 1.16+'s
  // SparkToParquetSchemaConverter convention.
  override protected def applyVariantLogicalType(builder: Types.GroupBuilder[GroupType]): Types.GroupBuilder[GroupType] = {
    builder.as(LogicalTypeAnnotation.variantType(1.toByte))
  }

  override def createMemoryStream[T: Encoder](id: Int, sparkSession: SparkSession): HoodieMemoryStream[T] = {
    // In Spark 4.1, MemoryStream is in org.apache.spark.sql.execution.streaming.runtime package
    // and takes SparkSession directly instead of SQLContext
    val memoryStream = new MemoryStream[T](id, sparkSession)
    new HoodieMemoryStream[T] {
      override def addData(data: TraversableOnce[T]): Unit = memoryStream.addData(data)
      override def toDS(): Dataset[T] = memoryStream.toDS()
    }
  }
}
