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

import org.apache.hudi.avro.model._
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.timeline.{HoodieDefaultTimeline, HoodieInstant}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.storage.StoragePathInfo
import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport}

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.{Function, Supplier}

import scala.collection.JavaConverters._
import scala.collection.mutable

class ShowMetadataTableColumnStatsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partition", DataTypes.StringType),
    ProcedureParameter.optional(2, "targetColumns", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("file_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("column_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("min_value", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("max_value", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("null_number", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val partitions = getArgValueOrDefault(args, PARAMETERS(1)).getOrElse("").toString
    val partitionsSeq = partitions.split(",").filter(_.nonEmpty).toSeq

    val targetColumns = getArgValueOrDefault(args, PARAMETERS(2)).getOrElse("").toString
    val targetColumnsSeq = targetColumns.split(",").toSeq
    val basePath = getBasePath(table)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .enable(true)
      .build
    val metaClient = createMetaClient(jsc, basePath)
    val schemaUtil = new TableSchemaResolver(metaClient)
    val schema = AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, schema, metadataConfig, metaClient)
    val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = columnStatsIndex.loadColumnStatsIndexRecords(targetColumnsSeq, shouldReadInMemory = false)
    val fsView = buildFileSystemView(table)
    val allFileSlices: Set[FileSlice] = {
      if (partitionsSeq.isEmpty) {
        val engineCtx = new HoodieSparkEngineContext(jsc)
        val metaTable = HoodieTableMetadata.create(engineCtx, metaClient.getStorage, metadataConfig, basePath)
        metaTable.getAllPartitionPaths
          .asScala
          .flatMap(path => fsView.getLatestFileSlices(path).iterator().asScala)
          .toSet
      } else {
        partitionsSeq
          .flatMap(partition => fsView.getLatestFileSlices(partition).iterator().asScala)
          .toSet
      }
    }

    val allFileNames: Set[String] = allFileSlices.map(_.getBaseFile.get().getFileName)

    val rows = mutable.ListBuffer[Row]()
    colStatsRecords.collectAsList().asScala
      .filter(c => allFileNames.contains(c.getFileName))
      .foreach { c =>
        rows += Row(c.getFileName, c.getColumnName, getColumnStatsValue(c.getMinValue),
          getColumnStatsValue(c.getMaxValue), c.getNullCount.longValue())
      }

    rows.toList.sortBy(r => r.getString(1))
  }

  private def getColumnStatsValue(stats_value: Any): String = {
    stats_value match {
      case null => "null"
      case _: IntWrapper |
           _: BooleanWrapper |
           _: DateWrapper |
           _: DoubleWrapper |
           _: FloatWrapper |
           _: LongWrapper |
           _: StringWrapper |
           _: TimeMicrosWrapper |
           _: TimestampMicrosWrapper =>
        String.valueOf(stats_value.asInstanceOf[IndexedRecord].get(0))
      case _: BytesWrapper =>
        val bytes_value = stats_value.asInstanceOf[BytesWrapper].getValue
        util.Arrays.toString(bytes_value.array())
      case _: DecimalWrapper =>
        val decimal_value = stats_value.asInstanceOf[DecimalWrapper].getValue
        util.Arrays.toString(decimal_value.array())
      case _ =>
        throw new HoodieException(s"Unsupported type: ${stats_value.getClass.getSimpleName}")
    }
  }

  def buildFileSystemView(table: Option[Any]): HoodieTableFileSystemView = {
    val basePath = getBasePath(table)
    val metaClient = createMetaClient(jsc, basePath)

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
      new java.util.ArrayList[HoodieInstant](instants.toList.asJava).stream(), details)

    new HoodieTableFileSystemView(metaClient, filteredTimeline, new java.util.ArrayList[StoragePathInfo])
  }

  override def build: Procedure = new ShowMetadataTableColumnStatsProcedure()
}

object ShowMetadataTableColumnStatsProcedure {
  val NAME = "show_metadata_table_column_stats"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTableColumnStatsProcedure()
  }
}

