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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.avro.model._
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.timeline.{HoodieDefaultTimeline, HoodieInstant}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.{Function, Supplier}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.{JavaConversions, mutable}
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter, seqAsJavaListConverter}

/**
 * Calculate the degree of overlap between column stats.
 * The overlap represents the extent to which the min-max ranges cover each other.
 * By referring to the overlap, we can visually demonstrate the degree of data skipping
 * for different columns under the current table's data layout.
 * The calculation is performed at the partition level (assuming that data skipping is based on partition pruning).
 *
 * For example, consider three files: a.parquet, b.parquet, and c.parquet.
 * Taking an integer-type column 'id' as an example, the range (min-max) for 'a' is 1–5,
 * for 'b' is 3–7, and for 'c' is 7–8. This results in their values overlapping on the coordinate axis as follows:
 * Value Range: 1 2 3 4 5 6 7 8
 * a.parquet:   [-------]
 * b.parquet:      [--------]
 * c.parquet:               [-]
 * Thus, there will be overlap within the ranges 3–5 and 7.
 * If the filter conditions for 'id' during data skipping include these values,
 * multiple files will be filtered out. For a simpler case, if it's an equality query,
 * 2 files will be filtered within these ranges, and no more than one file will be filtered in other cases (possibly outside of the range).
 *
 * Additionally, calculating the degree of overlap based solely on the maximum values
 * may not provide sufficient information. Therefore, we sample and calculate the overlap degree
 * for all values involved in the min-max range. We also compute the degree of overlap
 * at different percentiles and tally the count of these values.An example of a result is as follows:
 * |Partition path |Field name |Average overlap  |Maximum file overlap |Total file number |50% overlap        |75% overlap        |95% overlap        |99% overlap        |Total value number |
 * ----------------------------------------------------------------------
 * |path           |c8         |1.33             |2                   |2                |1                 |1                 |1                 |1                 |3                  |

 */
class ShowColumnStatsOverlapProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partition", DataTypes.StringType),
    ProcedureParameter.optional(2, "targetColumns", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("Partition path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("Field name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("Average overlap", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("Maximum file overlap", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("Total file number", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("50% overlap", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("75% overlap", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("95% overlap", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("99% overlap", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("Total value number", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val partitions = getArgValueOrDefault(args, PARAMETERS(1)).getOrElse("").toString
    val partitionsSeq = partitions.split(",").filter(_.nonEmpty).toSeq

    val targetColumnsSeq = getTargetColumnsSeq(args)
    val basePath = getBasePath(table)
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val schema = getSchema(metaClient)
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, schema, metadataConfig, metaClient)
    val fsView = buildFileSystemView(table)
    val engineCtx = new HoodieSparkEngineContext(jsc)
    val metaTable = HoodieTableMetadata.create(engineCtx, metadataConfig, basePath)
    val allFileSlices = getAllFileSlices(partitionsSeq, metaTable, fsView)
    val fileSlicesSizeByPartition = allFileSlices.groupBy(_.getPartitionPath).mapValues(_.size)

    val allFileNamesMap = getAllFileNamesMap(allFileSlices)
    val colStatsRecords = getColStatsRecords(targetColumnsSeq, columnStatsIndex, schema)

    val pointList = getPointList(colStatsRecords, allFileNamesMap, schema)

    // Group points by column name
    val groupedPoints = pointList.groupBy(p => (p.partitionPath, p.columnName))

    val rows = new util.ArrayList[Row]
    addStatisticsToRows(groupedPoints, fileSlicesSizeByPartition, rows)

    // The returned results are sorted by column name and average value
    rows.toList.sortBy(row => (row.getString(1), row.getDouble(2)))
  }

  def getTargetColumnsSeq(args: ProcedureArgs): Seq[String] = {
    val targetColumns = getArgValueOrDefault(args, PARAMETERS(2)).getOrElse("").toString
    if (targetColumns != "") {
      targetColumns.split(",").toSeq
    } else {
      Seq.empty[String]
    }
  }

  def getSchema(metaClient: HoodieTableMetaClient): StructType = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
  }


  def getAllFileSlices(partitionsSeq: Seq[String], metaTable: HoodieTableMetadata, fsView: HoodieTableFileSystemView): Set[FileSlice] = {
    if (partitionsSeq.isEmpty) {
      getFileSlices(metaTable.getAllPartitionPaths, fsView)
    } else {
      val filteredPartitions = metaTable.getAllPartitionPaths.asScala
        .filter(partition => partitionsSeq.exists(prefix => partition.startsWith(prefix)))
        .toList
        .asJava
      getFileSlices(filteredPartitions, fsView)
    }
  }

  def getFileSlices(partitionPaths: util.List[String], fsView: HoodieTableFileSystemView): Set[FileSlice] = {
    partitionPaths
      .asScala
      .flatMap(path => fsView.getLatestFileSlices(path).iterator().asScala)
      .toSet
  }

  def getAllFileNamesMap(allFileSlices: Set[FileSlice]): Map[String, String] = {
    allFileSlices.map { fileSlice =>
      val fileName = fileSlice.getBaseFile.get().getFileName
      val partitionPath = fileSlice.getPartitionPath
      fileName -> partitionPath
    }.toMap
  }

  def getColStatsRecords(targetColumnsSeq: Seq[String], columnStatsIndex: ColumnStatsIndexSupport, schema: StructType): HoodieData[HoodieMetadataColumnStats] = {
    if (!targetColumnsSeq.isEmpty) {
      columnStatsIndex.loadColumnStatsIndexRecords(targetColumnsSeq, false)
    } else {
      columnStatsIndex.loadColumnStatsIndexRecords(
        schema.fields.filter(field => !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name)).map(_.name).toSeq,
        false
      )
    }
  }

  def getPointList(colStatsRecords: HoodieData[HoodieMetadataColumnStats], allFileNamesMap: Map[String, String], schema: StructType): List[ColumnStatsPoint] = {
    colStatsRecords.collectAsList().asScala
      .filter(c => allFileNamesMap.keySet.contains(c.getFileName))
      .flatMap(c =>
        (getColumnStatsValue(c.getMinValue), getColumnStatsValue(c.getMaxValue)) match {
          case (Some(minValue), Some(maxValue)) =>
            Seq(
              new ColumnStatsPoint(allFileNamesMap.get(c.getFileName).getOrElse(c.getColumnName), c.getColumnName, minValue, "min", schema(c.getColumnName).dataType.typeName),
              new ColumnStatsPoint(allFileNamesMap.get(c.getFileName).getOrElse(c.getColumnName), c.getColumnName, maxValue, "max", schema(c.getColumnName).dataType.typeName)
            )
          case _ => Seq.empty
        }
      )
      .toList
  }

  /**
   * Adds statistical information to the result rows.
   *
   * @param groupedPoints             Data points grouped by partition path and column name
   * @param fileSlicesSizeByPartition Calculated number of file slices by partition path
   * @param rows                      List of rows storing the results
   */
  def addStatisticsToRows(groupedPoints: Map[(String, String), List[ColumnStatsPoint]],
                          fileSlicesSizeByPartition: Map[String, Int],
                          rows: util.ArrayList[Row]): Unit = {
    groupedPoints.map { case ((partitionPath, columnName), points) =>
      val sortedPoints = points.sorted
      var maxCount, currentCount = 0
      val valueToCountMap: mutable.ListMap[String, Int] = mutable.ListMap.empty[String, Int]

      sortedPoints.foreach { point =>
        if (point.pType == "min") {
          currentCount += 1
          maxCount = Math.max(maxCount, currentCount)
          valueToCountMap(point.value) = currentCount
        } else {
          if (!valueToCountMap.contains(point.value)) {
            valueToCountMap(point.value) = currentCount
          }
          currentCount -= 1
        }
      }

      val averageCount =
        if (valueToCountMap.nonEmpty) valueToCountMap.values.sum.toDouble / valueToCountMap.size
        else 0
      val sortedCounts = valueToCountMap.values.toList.sorted

      rows.add(Row(
        partitionPath,
        columnName,
        averageCount,
        maxCount,
        fileSlicesSizeByPartition.get(partitionPath),
        calculatePercentile(sortedCounts, 50),
        calculatePercentile(sortedCounts, 75),
        calculatePercentile(sortedCounts, 95),
        calculatePercentile(sortedCounts, 99),
        sortedCounts.size
      ))
    }
  }

  def calculatePercentile(values: List[Int], percentile: Double): Int = {
    val index = (percentile / 100.0 * (values.size - 1)).toInt
    values(index)
  }

  def buildFileSystemView(table: Option[Any]): HoodieTableFileSystemView = {
    val basePath = getBasePath(table)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val fs = metaClient.getFs
    val globPath = s"$basePath/*/*/*"
    val statuses = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(globPath))

    val timeline = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()

    val maxInstant = metaClient.createNewInstantTime()
    val instants = timeline.getInstants.iterator().asScala.filter(_.getTimestamp < maxInstant)

    val details = new Function[HoodieInstant, org.apache.hudi.common.util.Option[Array[Byte]]]
      with java.io.Serializable {
      override def apply(instant: HoodieInstant): HOption[Array[Byte]] = {
        metaClient.getActiveTimeline.getInstantDetails(instant)
      }
    }

    val filteredTimeline = new HoodieDefaultTimeline(
      new java.util.ArrayList[HoodieInstant](JavaConversions.asJavaCollection(instants.toList)).stream(), details)

    new HoodieTableFileSystemView(metaClient, filteredTimeline, statuses.toArray(new Array[FileStatus](statuses.size)))
  }

  def getColumnStatsValue(stats_value: Any): Option[String] = {
    if (stats_value == null) {
      logInfo("invalid value " + stats_value)
      None
    } else {
      stats_value match {
        case _: IntWrapper |
             _: BooleanWrapper |
             _: DateWrapper |
             _: DoubleWrapper |
             _: FloatWrapper |
             _: LongWrapper |
             _: StringWrapper |
             _: TimeMicrosWrapper |
             _: TimestampMicrosWrapper =>
          Some(String.valueOf(stats_value.asInstanceOf[IndexedRecord].get(0)))
        case _ => throw new HoodieException(s"Unsupported type: ${stats_value.getClass.getSimpleName}")
      }
    }
  }

  override def build: Procedure = new ShowColumnStatsOverlapProcedure()
}

/**
 * Defines a class called ColumnStatsPoint, which includes the partition path, column name, value, operation type, and schema type.
 * This class is used in algorithms for calculating overlap.
 * It contains comparison functions for comparing two ColumnStatsPoint objects.
 *
 * @param partitionPath The partition path
 * @param columnName    The name of the column
 * @param value         The point value
 * @param operationType The type of operation, either "min" or "max"
 * @param schemaType    The schema type, such as "string", "int", etc.
 */
class ColumnStatsPoint(val partitionPath: String, val columnName: String, val value: String, val pType: String, val schemaType: String) extends Ordered[ColumnStatsPoint] with Logging {

  override def compare(that: ColumnStatsPoint): Int = {
    val valueComparison = compareValue(this.value, that.value, schemaType)
    if (valueComparison != 0) {
      valueComparison
    } else {
      if (this.pType == "min" && that.pType == "max") -1
      else if (this.pType == "max" && that.pType == "min") 1
      else 0
    }
  }

  def compareValue(o1: Any, o2: Any, oType: String): Int = {
    oType match {
      case "string" | "boolean" =>
        Ordering[String].compare(o1.toString, o2.toString)
      case "integer" | "date" =>
        Ordering[Int].compare(o1.toString.toInt, o2.toString.toInt)
      case "double" =>
        Ordering[Double].compare(o1.toString.toDouble, o2.toString.toDouble)
      case "float" =>
        Ordering[Float].compare(o1.toString.toFloat, o2.toString.toFloat)
      case "long" | "timestamp" =>
        Ordering[Long].compare(o1.toString.toLong, o2.toString.toLong)
      case "short" =>
        Ordering[Short].compare(o1.toString.toShort, o2.toString.toShort)
      case "byte" =>
        Ordering[Byte].compare(o1.toString.toByte, o2.toString.toByte)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported type: $oType")
    }
  }
}

object ShowColumnStatsOverlapProcedure {
  val NAME = "show_metadata_column_stats_overlap"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowColumnStatsOverlapProcedure()
  }
}

