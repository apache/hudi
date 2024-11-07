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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.storage.StoragePath

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class RepairAddpartitionmetaProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "dry_run", DataTypes.BooleanType, true)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_is_present", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val dryRun = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    val tablePath = getBasePath(tableName)

    val metaClient = createMetaClient(jsc, tablePath)

    val latestCommit: String = metaClient.getActiveTimeline.getCommitAndReplaceTimeline.lastInstant.get.requestedTime
    val partitionPaths: util.List[String] = FSUtils.getAllPartitionFoldersThreeLevelsDown(metaClient.getStorage, tablePath);
    val basePath: StoragePath = new StoragePath(tablePath)

    val rows = new util.ArrayList[Row](partitionPaths.size)
    for (partition <- partitionPaths.asScala) {
      val partitionPath: StoragePath = FSUtils.constructAbsolutePath(basePath, partition)
      var isPresent = "Yes"
      var action = "None"
      if (!HoodiePartitionMetadata.hasPartitionMetadata(metaClient.getStorage, partitionPath)) {
        isPresent = "No"
        if (!dryRun) {
          val partitionMetadata: HoodiePartitionMetadata = new HoodiePartitionMetadata(
            metaClient.getStorage, latestCommit, basePath, partitionPath, metaClient.getTableConfig.getPartitionMetafileFormat)
          partitionMetadata.trySave()
          action = "Repaired"
        }
      }
      rows.add(Row(partition, isPresent, action))
    }

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new RepairAddpartitionmetaProcedure()
}

object RepairAddpartitionmetaProcedure {
  val NAME = "repair_add_partition_meta"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RepairAddpartitionmetaProcedure()
  }
}
