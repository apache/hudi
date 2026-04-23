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

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.{DataSourceReadOptions, HoodieBaseRelation, HoodieFileIndex, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.TimelineLayout
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.config.HoodieBootstrapConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.stats.HoodieColumnRangeMetadata
import org.apache.hudi.util.SparkConfigUtils

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.v2.HoodieScanBuilder.{CountStarColumn, CountStarNoColumn, CountStarWithColumn, NoCountStar}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Scan builder for DSv2 CoW snapshot reads.
 */
class HoodieScanBuilder(spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableSchema: StructType,
                        options: Map[String, String]) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with PartialLimitPushDown
  with SupportsPushDownAggregates
  with SparkAdapterSupport {

  private val log = LoggerFactory.getLogger(getClass)

  private var requiredSchema: StructType = tableSchema
  private var _pushedFilters: Array[Filter] = Array.empty
  private var partitionFilterExprs: Seq[Expression] = Seq.empty
  private var dataFilterExprs: Seq[Expression] = Seq.empty
  private var hasPostScanFilters: Boolean = false

  private var pushedLimit: Option[Int] = None
  private var pushedAggregation: Option[Aggregation] = None
  private var aggregateResult: Option[Array[InternalRow]] = None

  private lazy val fileIndex = HoodieFileIndex(spark, metaClient, Some(tableSchema), options,
    includeLogFiles = false, shouldEmbedFileSlices = false)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, postScan) = filters.partition { f =>
      HoodieCatalystExpressionUtils.convertToCatalystExpression(f, tableSchema).isDefined
    }

    val expressions = pushed.flatMap(f =>
      HoodieCatalystExpressionUtils.convertToCatalystExpression(f, tableSchema))

    val (partFilters, datFilters) = HoodieCatalystExpressionUtils
      .splitPartitionAndDataPredicates(spark, expressions, fileIndex.partitionSchema.fieldNames)

    // Mirror DSv1's MergeOnReadSnapshotRelation.collectFileSplits: convert partition filters
    // for TimestampBasedKeyGenerator before pruning. HoodieFileIndex only applies this
    // conversion internally when shouldEmbedFileSlices=true; this scan builder sets it
    // false, so without an explicit conversion `dt = '2024-01-01'` would be matched against
    // formatted path values like "2024/01/01" and prune away the matching partition.
    partitionFilterExprs = HoodieFileIndex
      .convertFilterForTimestampKeyGenerator(metaClient, partFilters).toSeq
    dataFilterExprs = datFilters.toSeq

    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    // Split pushed filters into:
    //   - pushedDataFilters (references only data columns): forwarded to Parquet for
    //     row-group pruning AND returned so Spark re-applies them row-wise (Parquet stats
    //     only prune row-groups).
    //   - partitionReferencingFilters (references at least one partition column, including
    //     partition-only and mixed filters): must NOT be forwarded to Parquet because the
    //     partition column may be absent from the base file (drop_partition_columns) or,
    //     when present, may hold a value that disagrees with the path-derived value —
    //     e.g. for TimestampBased/Custom key generators where path = "2024/01/01" but the
    //     stored column value is "2024-01-01 12:00:00". Row-group stats would then prune
    //     row-groups containing matching rows. They MUST be returned so Spark re-applies
    //     them row-wise; partition pruning narrows the file set but does not evaluate
    //     predicates against actual column values.
    val (pushedDataFilters, partitionReferencingFilters) = pushed.partition(f =>
      f.references.nonEmpty && f.references.forall(r => !partFieldNames.contains(r)))

    _pushedFilters = pushedDataFilters
    hasPostScanFilters = postScan.nonEmpty || partitionReferencingFilters.nonEmpty

    postScan ++ partitionReferencingFilters ++ pushedDataFilters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushLimit(limit: Int): Boolean = {
    // Spark 3.3's planner does not honor SupportsPushDownLimit.isPartiallyPushed — any
    // successful pushdown is treated as COMPLETE and the outer LocalLimit is dropped.
    // HoodieBatchScan enforces the limit per input partition, so LIMIT N across multiple
    // base files could over-return. Refuse pushdown on 3.3; on 3.4+, PartialLimitPushDown
    // keeps the outer LocalLimit in place.
    // Otherwise the pushdown is only safe when no filter is re-applied above the scan:
    // _pushedFilters are returned to Spark for row-wise re-evaluation (Parquet only
    // prunes row-groups via them), and hasPostScanFilters covers filters we couldn't
    // convert. Capping rows in the reader before either runs would drop later matching
    // rows — e.g. WHERE id > 3 LIMIT 1 could stop at a row with id = 1. Spark pushes
    // filters before limits, so both flags are already populated here.
    if (!HoodieSparkUtils.gteqSpark3_4) {
      false
    } else if (_pushedFilters.nonEmpty || hasPostScanFilters) {
      false
    } else {
      pushedLimit = Some(limit)
      true
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (aggregation.groupByExpressions().nonEmpty) {
      false
    } else if (!MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(metaClient)) {
      false
    } else {
      val funcs = aggregation.aggregateExpressions()
      val allSupported = funcs.forall {
        case _: CountStar => true
        case c: Count => !c.isDistinct
        // Parquet/Hudi column stats exclude NaN from min/max, but Spark SQL
        // treats NaN as greater than any non-NaN value. Stats can't distinguish
        // "all NaN" from "all null" (both surface as a null min/max), so skip
        // MIN/MAX pushdown on float/double columns and let Spark compute from
        // the scan.
        case m: Min => !isFloatingPointColumn(m.column())
        case m: Max => !isFloatingPointColumn(m.column())
        case _ => false
      }
      if (!allSupported) {
        false
      } else {
        tryComputeAggregates(aggregation) match {
          case Some(rows) =>
            pushedAggregation = Some(aggregation)
            aggregateResult = Some(rows)
            true
          case None => false
        }
      }
    }
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    pushedAggregation.contains(aggregation) && dataFilterExprs.isEmpty && !hasPostScanFilters
  }

  override def build(): Scan = {
    aggregateResult match {
      case Some(rows) =>
        val outputSchema = buildAggregateOutputSchema(pushedAggregation.get)
        new HoodieLocalScan(outputSchema, rows)
      case None =>
        buildSnapshotScan()
    }
  }

  private def buildSnapshotScan(): Scan = {
    // Invariant established by HoodieV2ReadSupport.isSupportedByDSv2:
    // COW snapshot or MOR read_optimized only, Parquet only, single base-file format.
    val queryType = SparkConfigUtils.getStringWithAltKeys(options, DataSourceReadOptions.QUERY_TYPE)
    require(metaClient.getTableType == COPY_ON_WRITE ||
              queryType == DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
            "HoodieScanBuilder supports COW snapshot or read_optimized only; " +
              s"got tableType=${metaClient.getTableType}, queryType=$queryType")

    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    // Mirror DSv1's HoodieBaseRelation.shouldExtractPartitionValuesFromPartitionPath: only
    // strip partition columns from the file schema and supply them from the parsed path
    // when the table was written with drop_partition_columns, the user explicitly opted
    // in via EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH, or the bootstrap fast-read
    // path is requested. Otherwise partition columns are persisted in the base files and
    // path-derived values would be wrong for timestamp/custom key generators or encoded
    // partition paths. (Bootstrap is currently rejected by HoodieV2ReadSupport, but the
    // gate is kept identical for parity.)
    val shouldOmitPartitionColumns =
      metaClient.getTableConfig.shouldDropPartitionColumns && partFieldNames.nonEmpty
    val shouldExtractPartitionValueFromPath =
      options.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    val shouldUseBootstrapFastRead =
      options.getOrElse(HoodieBootstrapConfig.DATA_QUERIES_ONLY.key, "false").toBoolean
    val extractPartitionValuesFromPartitionPath =
      shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath || shouldUseBootstrapFastRead

    // When partition values are extracted from the path, the partition column types must come
    // from the file index. TimestampBasedKeyGenerator (and similar) parse non-DATE_STRING
    // partition path values as UTF8String/StringType, so fileIndex.partitionSchema is the only
    // place that reflects what HoodieFileIndex puts into HoodieInputPartition.partitionValues.
    // Building requiredPartitionSchema from requiredSchema would advertise the original
    // Long/Timestamp type and feed string path values into the parquet partition reader,
    // producing wrong values or runtime errors. If the file-index type differs from the
    // requiredSchema type, reject — DSv2 cannot reconcile that mismatch without also rewriting
    // HoodieSparkV2Table.schema(), which is out of scope here.
    val fullPartSchema = fileIndex.partitionSchema
    val (requiredDataSchema, requiredPartitionSchema) = if (extractPartitionValuesFromPartitionPath) {
      val partFieldsByName = fullPartSchema.fields.map(f => f.name -> f).toMap
      val partitionFieldsInRequired = requiredSchema.fields
        .filter(f => partFieldNames.contains(f.name))
        .map { f =>
          val fileIdxField = partFieldsByName(f.name)
          if (fileIdxField.dataType != f.dataType) {
            throw new UnsupportedOperationException(
              s"DSv2 read with extractPartitionValuesFromPartitionPath=true does not support " +
                s"partition column '${f.name}' whose file-index type ${fileIdxField.dataType.simpleString} " +
                s"differs from the table-schema type ${f.dataType.simpleString} (typical for " +
                s"TimestampBasedKeyGenerator). Disable extractPartitionValuesFromPartitionPath, " +
                s"unset hoodie.datasource.write.drop.partition.columns, or use the DSv1 reader.")
          }
          fileIdxField
        }
      (StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name))),
        StructType(partitionFieldsInRequired))
    } else {
      (requiredSchema, StructType(Nil))
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    val internalSchemaOpt = fetchInternalSchema()
    embedInternalSchema(hadoopConf, internalSchemaOpt)
    val tableAvroSchemaOpt = fetchTableAvroSchema()

    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)

    val partitions = fileSlicesPerPartition.flatMap { case (partitionOpt, fileSlices) =>
      fileSlices.filter(_.getBaseFile.isPresent).map { fs =>
        val baseFilePath = fs.getBaseFile.get().getPath
        val baseFileLength = fs.getBaseFile.get().getFileSize
        val allPartValues = partitionOpt.map(_.getValues).getOrElse(Array.empty[AnyRef])

        val partValues = if (requiredPartitionSchema.isEmpty) {
          Array.empty[AnyRef]
        } else {
          requiredPartitionSchema.fieldNames.map { name =>
            val idx = fullPartSchema.fieldIndex(name)
            allPartValues(idx)
          }
        }

        HoodieInputPartition(0, baseFilePath, baseFileLength, partValues)
      }
    }.zipWithIndex.map { case (p, i) => p.copy(index = i) }.toArray[InputPartition]

    new HoodieBatchScan(
      requiredSchema,
      partitions,
      broadcastReader,
      broadcastConf,
      requiredDataSchema,
      requiredPartitionSchema,
      internalSchemaOpt,
      _pushedFilters,
      pushedLimit,
      tableAvroSchemaOpt)
  }

  private def fetchTableAvroSchema(): HOption[HoodieSchema] = {
    try {
      val resolver = new TableSchemaResolver(metaClient)
      // Mirror DSv1's HoodieHadoopFsRelationFactory and HoodieFileGroupReaderBasedFileFormat:
      // resolve the Avro schema as of a time-travel instant when supplied so the Parquet
      // SchemaRepair sees the column types that match the snapshot being read. Without this,
      // SparkXXParquetReader cannot repair logical timestamp annotations and timestamp-millis
      // columns surface as their physical long value.
      val schema = options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
        .map(HoodieSqlCommonUtils.formatQueryInstant) match {
          case Some(ts) => resolver.getTableSchema(ts)
          case None     => resolver.getTableSchema
        }
      HOption.ofNullable(schema)
    } catch {
      case e: Exception =>
        log.warn("Failed to fetch table Avro schema for SchemaRepair", e)
        HOption.empty[HoodieSchema]()
    }
  }

  private def fetchInternalSchema(): HOption[InternalSchema] = {
    if (!HoodieBaseRelation.isSchemaEvolutionEnabledOnRead(options, spark)) {
      HOption.empty[InternalSchema]()
    } else {
      try {
        val resolver = new TableSchemaResolver(metaClient)
        // Mirror DSv1 (HoodieBaseRelation): when a time-travel instant is supplied, fetch
        // the internal schema as of that instant so schema-evolved column IDs/types match
        // the snapshot being read. Otherwise fall back to the latest internal schema.
        options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
          .map(HoodieSqlCommonUtils.formatQueryInstant) match {
            case Some(ts) => resolver.getTableInternalSchemaFromCommitMetadata(ts)
            case None     => resolver.getTableInternalSchemaFromCommitMetadata
          }
      } catch {
        case e: Exception =>
          log.warn("Failed to fetch internal schema from commit metadata", e)
          HOption.empty[InternalSchema]()
      }
    }
  }

  private def embedInternalSchema(conf: org.apache.hadoop.conf.Configuration,
                                  internalSchemaOpt: HOption[InternalSchema]): Unit = {
    if (internalSchemaOpt.isPresent) {
      val internalSchema = internalSchemaOpt.get
      val instantFileNameGenerator = TimelineLayout.fromVersion(metaClient.getTimelineLayoutVersion)
        .getInstantFileNameGenerator
      val validCommits = metaClient.getActiveTimeline.getInstants.iterator.asScala
        .map(instant => instantFileNameGenerator.getFileName(instant)).mkString(",")
      conf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchema))
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, metaClient.getBasePath.toString)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
  }

  private def tryComputeAggregates(aggregation: Aggregation): Option[Array[InternalRow]] = {
    try {
      val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)

      if (dataFilterExprs.nonEmpty || hasPostScanFilters) {
        None
      } else {
        // Collect base files as (partitionPath, fileName) pairs
        val baseFiles = fileSlicesPerPartition.flatMap { case (partOpt, slices) =>
          slices.filter(_.getBaseFile.isPresent).map { fs =>
            val partPath = partOpt.map(_.getPath).getOrElse("")
            val fileName = fs.getBaseFile.get().getFileName
            (partPath, fileName)
          }
        }

        if (baseFiles.isEmpty) {
          Some(Array(buildEmptyAggregateRow(aggregation)))
        } else {
          computeAggregatesFromStats(aggregation, baseFiles)
        }
      }
    } catch {
      case e: Exception =>
        log.debug("Aggregate pushdown computation failed, falling back to scan", e)
        None
    }
  }

  private def computeAggregatesFromStats(
      aggregation: Aggregation,
      baseFiles: Seq[(String, String)]): Option[Array[InternalRow]] = {
    val aggFuncs = aggregation.aggregateExpressions()

    // Determine which columns need stats; None if unsupported function found
    val referencedColumnsOpt = aggFuncs.foldLeft(Option(Seq.empty[String])) { (accOpt, func) =>
      accOpt.flatMap { acc =>
        func match {
          case _: CountStar => Some(acc)
          case c: Count => extractColumnName(c.column()).map(name => acc :+ name)
          case m: Min => extractColumnName(m.column()).map(name => acc :+ name)
          case m: Max => extractColumnName(m.column()).map(name => acc :+ name)
          case _ => None
        }
      }
    }

    referencedColumnsOpt.flatMap { referencedColumns =>
      val distinctColumns = referencedColumns.distinct

      // For CountStar, prefer a user-defined record key field that's present in the
      // user schema: it's an original column from the table's first commit, so its
      // column stats are complete even after schema evolution. Fall back to the
      // first column for keyless tables or when the record key is meta-field-only.
      val countStarColumn: CountStarColumn = if (aggFuncs.exists(_.isInstanceOf[CountStar])) {
        val schemaNames = tableSchema.fields.map(_.name).toSet
        val recordKeyFields = metaClient.getTableConfig.getRecordKeyFields.orElse(Array.empty[String])
        recordKeyFields.find(schemaNames.contains)
          .orElse(tableSchema.fields.headOption.map(_.name)) match {
            case Some(name) => CountStarWithColumn(name)
            case None       => CountStarNoColumn
          }
      } else {
        NoCountStar
      }

      countStarColumn match {
        case CountStarNoColumn => None
        case resolved =>
          val countStarColOpt: Option[String] = resolved match {
            case CountStarWithColumn(name) => Some(name)
            case _                         => None
          }
          val allColumns = (distinctColumns ++ countStarColOpt.toSeq).distinct
          if (allColumns.isEmpty) {
            None
          } else {
            queryAndComputeAggregates(aggFuncs, allColumns, countStarColOpt, baseFiles)
          }
      }
    }
  }

  private def queryAndComputeAggregates(
      aggFuncs: Array[org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc],
      allColumns: Seq[String],
      countStarColumn: Option[String],
      baseFiles: Seq[(String, String)]): Option[Array[InternalRow]] = {
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
    val engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf)
    val tableMetadata = new HoodieBackedTableMetadata(
      engineContext, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)

    try {
      val filePairs = baseFiles.map { case (part, file) => Pair.of(part, file) }.asJava

      // Query stats for all needed columns; None if any column has incomplete stats
      val columnStatsOpt = allColumns.foldLeft(
        Option(Map.empty[String, Seq[HoodieColumnRangeMetadata[Comparable[_]]]])
      ) { (accOpt, col) =>
        accOpt.flatMap { acc =>
          val statsMap = tableMetadata.getColumnStats(filePairs, col)
          if (statsMap.size() != baseFiles.size) {
            None
          } else {
            val allFileStats = statsMap.values().asScala
              .map(cs => HoodieColumnRangeMetadata.fromColumnStats(cs)).toSeq
            Some(acc + (col -> allFileStats))
          }
        }
      }

      columnStatsOpt.flatMap { columnStats =>
        // Compute each aggregate value as Option; None means unsupported
        val valuesOpt: Array[Option[Any]] = aggFuncs.map {
          case _: CountStar =>
            val stats = columnStats(countStarColumn.get)
            Some(stats.map(s => s.getValueCount).sum: Any)

          case c: Count =>
            extractColumnName(c.column()).map { colName =>
              val stats = columnStats(colName)
              stats.map(s => s.getValueCount - s.getNullCount).sum: Any
            }

          case m: Min =>
            for {
              colName <- extractColumnName(m.column())
              colType <- tableSchema.fields.find(_.name == colName).map(_.dataType)
              result <- computeMinMax(columnStats(colName), colType, isMin = true)
            } yield result

          case m: Max =>
            for {
              colName <- extractColumnName(m.column())
              colType <- tableSchema.fields.find(_.name == colName).map(_.dataType)
              result <- computeMinMax(columnStats(colName), colType, isMin = false)
            } yield result

          case _ => None
        }

        if (valuesOpt.forall(_.isDefined)) {
          Some(Array(new GenericInternalRow(valuesOpt.map(_.get))))
        } else {
          None
        }
      }
    } finally {
      tableMetadata.close()
    }
  }

  private def computeMinMax(
      allFileStats: Seq[HoodieColumnRangeMetadata[Comparable[_]]],
      colType: DataType,
      isMin: Boolean): Option[Any] = {
    val rawValues = allFileStats.map(s => if (isMin) s.getMinValue else s.getMaxValue).filter(_ != null)
    if (rawValues.isEmpty) {
      Some(null)
    } else {
      val sparkValues = rawValues.flatMap(v => convertToSparkValue(v, colType))
      if (sparkValues.size != rawValues.size) {
        None
      } else {
        colType match {
          case ByteType =>
            val vs = sparkValues.map(_.asInstanceOf[Byte])
            Some(if (isMin) vs.min else vs.max)
          case ShortType =>
            val vs = sparkValues.map(_.asInstanceOf[Short])
            Some(if (isMin) vs.min else vs.max)
          case IntegerType =>
            val vs = sparkValues.map(_.asInstanceOf[Int])
            Some(if (isMin) vs.min else vs.max)
          case LongType =>
            val vs = sparkValues.map(_.asInstanceOf[Long])
            Some(if (isMin) vs.min else vs.max)
          case FloatType =>
            val vs = sparkValues.map(_.asInstanceOf[Float]).filterNot(_.isNaN)
            if (vs.isEmpty) None else Some(if (isMin) vs.min else vs.max)
          case DoubleType =>
            val vs = sparkValues.map(_.asInstanceOf[Double]).filterNot(_.isNaN)
            if (vs.isEmpty) None else Some(if (isMin) vs.min else vs.max)
          case StringType =>
            val vs = sparkValues.map(_.asInstanceOf[UTF8String])
            Some(vs.reduce((a, b) => if ((isMin && a.compareTo(b) <= 0) || (!isMin && a.compareTo(b) >= 0)) a else b))
          case _ => None
        }
      }
    }
  }

  private def buildEmptyAggregateRow(aggregation: Aggregation): InternalRow = {
    val values = aggregation.aggregateExpressions().map {
      case _: CountStar => 0L: Any
      case _: Count => 0L: Any
      case _ => null: Any
    }
    new GenericInternalRow(values)
  }

  private def buildAggregateOutputSchema(aggregation: Aggregation): StructType = {
    val fields = aggregation.aggregateExpressions().zipWithIndex.map { case (func, i) =>
      func match {
        case _: CountStar =>
          StructField(s"count(*)", LongType, nullable = false)
        case c: Count =>
          val colName = extractColumnName(c.column()).getOrElse(s"col$i")
          StructField(s"count($colName)", LongType, nullable = false)
        case m: Min =>
          val colName = extractColumnName(m.column()).getOrElse(s"col$i")
          val colType = tableSchema.fields.find(_.name == colName).map(_.dataType).getOrElse(LongType)
          StructField(s"min($colName)", colType, nullable = true)
        case m: Max =>
          val colName = extractColumnName(m.column()).getOrElse(s"col$i")
          val colType = tableSchema.fields.find(_.name == colName).map(_.dataType).getOrElse(LongType)
          StructField(s"max($colName)", colType, nullable = true)
        case _ =>
          StructField(s"agg$i", LongType, nullable = true)
      }
    }
    StructType(fields)
  }

  private def extractColumnName(
      expr: org.apache.spark.sql.connector.expressions.Expression): Option[String] = {
    expr match {
      case ref: NamedReference =>
        val names = ref.fieldNames()
        if (names.length == 1) Some(names.head) else None
      case _ => None
    }
  }

  private def isFloatingPointColumn(
      expr: org.apache.spark.sql.connector.expressions.Expression): Boolean = {
    extractColumnName(expr)
      .flatMap(name => tableSchema.fields.find(_.name == name).map(_.dataType))
      .exists {
        case FloatType | DoubleType => true
        case _ => false
      }
  }

  private def convertToSparkValue(value: Any, dataType: DataType): Option[Any] = {
    if (value == null) {
      Some(null)
    } else try {
      dataType match {
        case BooleanType => Some(value.asInstanceOf[Boolean])
        case ByteType => Some(value.asInstanceOf[Number].byteValue())
        case ShortType => Some(value.asInstanceOf[Number].shortValue())
        case IntegerType => Some(value.asInstanceOf[Number].intValue())
        case LongType => Some(value.asInstanceOf[Number].longValue())
        case FloatType => Some(value.asInstanceOf[Number].floatValue())
        case DoubleType => Some(value.asInstanceOf[Number].doubleValue())
        case StringType => Some(UTF8String.fromString(value.toString))
        case _ => None
      }
    } catch {
      case _: Exception => None
    }
  }
}

private[v2] object HoodieScanBuilder {
  sealed trait CountStarColumn
  case object NoCountStar extends CountStarColumn
  case object CountStarNoColumn extends CountStarColumn
  final case class CountStarWithColumn(name: String) extends CountStarColumn
}
