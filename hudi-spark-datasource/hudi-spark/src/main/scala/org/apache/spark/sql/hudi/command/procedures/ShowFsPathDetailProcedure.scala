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

import org.apache.hadoop.fs.{ContentSummary, FileStatus, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.text.DecimalFormat
import java.util.function.Supplier

class ShowFsPathDetailProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "is_sub", DataTypes.BooleanType, false),
    ProcedureParameter.optional(2, "sort", DataTypes.BooleanType, true)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("path_num", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("file_num", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("storage_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("storage_size(unit)", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("storage_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("space_consumed", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("quota", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("space_quota", DataTypes.LongType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val srcPath = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val isSub = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    val sort = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]

    val path: Path = new Path(srcPath)
    val fs = FSUtils.getFs(path, jsc.hadoopConfiguration())
    val status: Array[FileStatus] = if (isSub) fs.listStatus(path) else fs.globStatus(path)
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]()

    if (status.nonEmpty) {
      for (i <- status.indices) {
        val summary: ContentSummary = fs.getContentSummary(status(i).getPath)
        val storagePath: String = status(i).getPath.toString
        rows.add(Row(summary.getDirectoryCount, summary.getFileCount, summary.getLength,
          getFileSize(summary.getLength), storagePath, summary.getQuota, summary.getSpaceConsumed,
          summary.getSpaceQuota))
      }
    }

    val df = spark.sqlContext.createDataFrame(rows, OUTPUT_TYPE)
    if (sort) {
      df.orderBy(df("storage_size").desc).collect()
    } else {
      df.orderBy(df("file_num").desc).collect()
    }
  }

  def getFileSize(size: Long): String = {
    val GB = 1024 * 1024 * 1024
    val MB = 1024 * 1024
    val KB = 1024
    val df: DecimalFormat = new DecimalFormat("0.00")

    val resultSize = if (size / GB >= 1) {
      df.format(size / GB.toFloat) + "GB"
    } else if (size / MB >= 1) {
      df.format(size / MB.toFloat) + "MB"
    } else if (size / KB >= 1) {
      df.format(size / KB.toFloat) + "KB"
    } else {
      size + "B"
    }

    resultSize
  }

  override def build: Procedure = new ShowFsPathDetailProcedure()
}

object ShowFsPathDetailProcedure {
  val NAME = "show_fs_path_detail"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowFsPathDetailProcedure()
  }
}



