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
import org.apache.hudi.avro.model.{BooleanWrapper, BytesWrapper, DateWrapper, DecimalWrapper, DoubleWrapper, FloatWrapper, HoodieMetadataColumnStats, IntWrapper, LongWrapper, StringWrapper, TimeMicrosWrapper, TimestampMicrosWrapper}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier


class ShowMetadataTableColumnStatsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "targetColumns", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("file_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("column_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("min_value", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("max_value", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("null_num", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val targetColumns = getArgValueOrDefault(args, PARAMETERS(1)).getOrElse("").toString
    val targetColumnsSeq = targetColumns.split(",").toSeq
    val basePath = getBasePath(table)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .enable(true)
      .build
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val schemaUtil = new TableSchemaResolver(metaClient)
    var schema = AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, schema, metadataConfig, metaClient)
    val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = columnStatsIndex.loadColumnStatsIndexRecords(targetColumnsSeq, false)

    val rows = new util.ArrayList[Row]
    colStatsRecords.collectAsList()
      .stream()
      .forEach(c => {
        rows.add(Row(c.getFileName, c.getColumnName, getColumnStatsValue(c.getMinValue), getColumnStatsValue(c.getMaxValue), c.getNullCount.longValue()))
      })
    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  def getColumnStatsValue(stats_value: Any): String = {
    stats_value match {
      case _: IntWrapper |
           _: BooleanWrapper |
           _: BytesWrapper |
           _: DateWrapper |
           _: DecimalWrapper |
           _: DoubleWrapper |
           _: FloatWrapper |
           _: LongWrapper |
           _: StringWrapper |
           _: TimeMicrosWrapper |
           _: TimestampMicrosWrapper =>
        String.valueOf(stats_value.asInstanceOf[IndexedRecord].get(0))
      case _ => throw new Exception("Unsupported type.")
    }
  }

  override def build: Procedure = new ShowMetadataTableColumnStatsProcedure()
}

object ShowMetadataTableColumnStatsProcedure {
  val NAME = "show_metadata_table_column_stats"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTableColumnStatsProcedure()
  }
}

