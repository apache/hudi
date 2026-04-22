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

import org.apache.hudi.{DataSourceReadOptions, HoodieBaseRelation, HoodieFileIndex, SparkAdapterSupport}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.TimelineLayout
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.Pair
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

    partitionFilterExprs = partFilters.toSeq
    dataFilterExprs = datFilters.toSeq

    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    // Partition filters are fully handled by partition pruning in filterFileSlices — the
    // partition column is not in the base file, so forwarding them to the Parquet reader
    // would filter all rows as null. Only pass data filters to the reader. Return those
    // data filters to Spark so it re-applies them row-wise (Parquet only does row-group
    // level pruning via these filters, not precise filtering).
    val pushedDataFilters = pushed.filterNot(f => f.references.forall(partFieldNames.contains))

    _pushedFilters = pushedDataFilters
    hasPostScanFilters = postScan.nonEmpty

    postScan ++ pushedDataFilters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
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
    val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name)))
    val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames.contains(f.name)))

    val hadoopConf = spark.sessionState.newHadoopConf()
    val internalSchemaOpt = fetchInternalSchema()
    embedInternalSchema(hadoopConf, internalSchemaOpt)

    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val fullPartSchema = fileIndex.partitionSchema
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
      pushedLimit)
  }

  private def fetchInternalSchema(): HOption[InternalSchema] = {
    if (!HoodieBaseRelation.isSchemaEvolutionEnabledOnRead(options, spark)) {
      HOption.empty[InternalSchema]()
    } else {
      try {
        new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata
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
      val countStarColumnOpt: Option[Option[String]] = if (aggFuncs.exists(_.isInstanceOf[CountStar])) {
        val schemaNames = tableSchema.fields.map(_.name).toSet
        val recordKeyFields = metaClient.getTableConfig.getRecordKeyFields.orElse(Array.empty[String])
        recordKeyFields.find(schemaNames.contains)
          .orElse(tableSchema.fields.headOption.map(_.name))
          .map(Some(_))
      } else {
        Some(None)
      }

      countStarColumnOpt.flatMap { countStarColumn =>
        val allColumns = (distinctColumns ++ countStarColumn.toSeq).distinct
        if (allColumns.isEmpty) {
          None
        } else {
          queryAndComputeAggregates(aggFuncs, allColumns, countStarColumn, baseFiles)
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
