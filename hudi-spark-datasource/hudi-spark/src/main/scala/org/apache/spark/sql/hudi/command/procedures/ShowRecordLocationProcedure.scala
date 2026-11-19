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

import java.util.function.Supplier

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieKey
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ShowRecordLocationProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "partition_path", DataTypes.StringType),
    ProcedureParameter.required(2, "record_key", DataTypes.StringType),
    ProcedureParameter.required(3, "num_buckets", DataTypes.IntegerType),
    ProcedureParameter.required(4, "index_key_fields", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("record_key", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("bucket_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_path", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val partitionPath = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val recordKey = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val numBuckets = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Int]
    val indexKeyFieldStr = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val basePath = getBasePath(tableName)
    val partitionFullPath = new Path(basePath, partitionPath)
    val fs = HadoopFSUtils.getFs(partitionFullPath, jsc.hadoopConfiguration())
    val rows = new java.util.ArrayList[Row]

    Try {
      val indexKeyFields = if (indexKeyFieldStr.contains(",")) indexKeyFieldStr.split(",") else Array(indexKeyFieldStr)
      val hoodieKey = new HoodieKey(recordKey, partitionPath)
      val bucketId = BucketIdentifier.getBucketId(hoodieKey, indexKeyFields.toList.asJava, numBuckets)
      val files = fetchBucketIdFile(fs, partitionFullPath, bucketId)
      if (files.length > 0) {
        for (f <- files) {
          rows.add(Row(recordKey, String.valueOf(bucketId), f.getPath.toString))
        }
      }
    } match {
      case Success(_) =>
        rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
      case Failure(e) =>
        throw new HoodieException(s"Show record location failed!", e)
    }
  }

  private def fetchBucketIdFile(fs: FileSystem, filePath: Path, bucketId: Int): Array[FileStatus] = {
    val regex = "(?=" + String.valueOf(bucketId) + ")"
    val regexFilter = new PathFilter {
      private val pattern = regex.r

      override def accept(path: Path): Boolean = {
        val fileId = FSUtils.getFileId(path.getName)
        pattern.findFirstIn(fileId).isDefined
      }
    }
    fs.listStatus(filePath, regexFilter)
  }

  override def build: Procedure = new ShowRecordLocationProcedure()
}

object ShowRecordLocationProcedure {
  val NAME = "show_record_location"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowRecordLocationProcedure()
  }
}
