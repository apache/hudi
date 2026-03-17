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

import org.apache.hudi.{HoodieFileIndex, SparkAdapterSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.stats.HoodieColumnRangeMetadata

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
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
  with SupportsPushDownLimit
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

  private lazy val fileIndex = HoodieFileIndex(spark, metaClient, None, options,
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

    _pushedFilters = pushed
    hasPostScanFilters = postScan.nonEmpty

    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    val dataFilterArr = pushed.filterNot(f => f.references.forall(partFieldNames.contains))
    postScan ++ dataFilterArr
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def isPartiallyPushed(): Boolean = true

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
        case _: Min => true
        case _: Max => true
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
    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name)))
    val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames.contains(f.name)))

    val hadoopConf = spark.sessionState.newHadoopConf()
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
      _pushedFilters,
      pushedLimit)
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

      // For CountStar, use the first table column to get total row count
      val countStarColumnOpt: Option[Option[String]] = if (aggFuncs.exists(_.isInstanceOf[CountStar])) {
        if (tableSchema.nonEmpty) Some(Some(tableSchema.fields.head.name)) else None
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
            Some(stats.map(s => s.getValueCount + s.getNullCount).sum: Any)

          case c: Count =>
            extractColumnName(c.column()).map { colName =>
              val stats = columnStats(colName)
              stats.map(_.getValueCount).sum: Any
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
            val vs = sparkValues.map(_.asInstanceOf[Float])
            Some(if (isMin) vs.min else vs.max)
          case DoubleType =>
            val vs = sparkValues.map(_.asInstanceOf[Double])
            Some(if (isMin) vs.min else vs.max)
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
