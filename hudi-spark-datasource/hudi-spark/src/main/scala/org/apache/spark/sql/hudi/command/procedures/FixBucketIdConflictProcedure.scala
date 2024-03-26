/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters._

/**
 * Can use the procedure when bucket index partition occur conflict due to multiple write
 */
class FixBucketIdConflictProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "bucket_id", DataTypes.StringType),
    ProcedureParameter.required(2, "partition_path", DataTypes.StringType),
    ProcedureParameter.optional(3, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("fix_bucket_path_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val bucketId = getArgValueOrDefault(args, PARAMETERS(1))
    val partitionPath = getArgValueOrDefault(args, PARAMETERS(2))
    val path = getArgValueOrDefault(args, PARAMETERS(3))

    var result = false

    val basePath: String = getBasePath(tableName, path)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val instants = metaClient.getActiveTimeline.getCommitsTimeline().filterInflightsAndRequested().getInstants.asScala.toList

    val statuses = metaClient.getFs.listStatus(new Path(partitionPath.get.asInstanceOf[String]))
    for (status <- statuses) {
      if (judgeDeleteConflictBucketFile(status, bucketId.get.asInstanceOf[String], instants)) {
        metaClient.getFs.delete(status.getPath, false)
        result = true
      }
    }

    Seq(Row(result))
  }

  /**
   * Judge whether need delete bucket data file according two conditions:
   * 1. file start with the bucket id.
   * 2. file is uncompleted instant.
   */
  def judgeDeleteConflictBucketFile(fileStatus: FileStatus, bucketId: String, filterInstants: Seq[HoodieInstant]): Boolean = {
    for (i <- filterInstants) {
      val fileName = fileStatus.getPath.getName
      // FileName starts with bucket id and contains instant timestamp
      if (fileStatus.isFile && BucketIdentifier.bucketIdFromFileId(fileName) == bucketId && fileName.contains(i.getTimestamp)) {
        true
      }
    }
    false
  }

  override def build: Procedure = new FixBucketIdConflictProcedure()

}

object FixBucketIdConflictProcedure {
  val NAME: String = "fix_bucket_path"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): FixBucketIdConflictProcedure = new FixBucketIdConflictProcedure()
  }
}
