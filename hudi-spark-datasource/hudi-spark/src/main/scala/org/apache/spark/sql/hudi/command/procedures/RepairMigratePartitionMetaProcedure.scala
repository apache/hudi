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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.Option
import org.apache.hudi.exception.HoodieIOException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.io.IOException
import java.util
import java.util.Properties
import java.util.function.{Consumer, Supplier}
import scala.collection.JavaConversions._

class RepairMigratePartitionMetaProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "dry_run", DataTypes.BooleanType, true)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("text_metafile_present", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("base_metafile_present", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val dryRun = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    val tablePath = getBasePath(tableName)

    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(tablePath).build

    val engineContext: HoodieLocalEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf)
    val partitionPaths: util.List[String] = FSUtils.getAllPartitionPaths(engineContext, tablePath, false, false)
    val basePath: Path = new Path(tablePath)

    val rows = new util.ArrayList[Row](partitionPaths.size)
    for (partitionPath <- partitionPaths) {
      val partition: Path = FSUtils.getPartitionPath(tablePath, partitionPath)
      val textFormatFile: Option[Path] = HoodiePartitionMetadata.textFormatMetaPathIfExists(metaClient.getFs, partition)
      val baseFormatFile: Option[Path] = HoodiePartitionMetadata.baseFormatMetaPathIfExists(metaClient.getFs, partition)
      val latestCommit: String = metaClient.getActiveTimeline.getCommitTimeline.lastInstant.get.getTimestamp
      var action = if (textFormatFile.isPresent) "MIGRATE" else "NONE"
      if (!dryRun) {
        if (!baseFormatFile.isPresent) {
          val partitionMetadata: HoodiePartitionMetadata = new HoodiePartitionMetadata(metaClient.getFs, latestCommit,
            basePath, partition, Option.of(metaClient.getTableConfig.getBaseFileFormat))
          partitionMetadata.trySave(0)
        }
        // delete it, in case we failed midway last time.
        textFormatFile.ifPresent(
          new Consumer[Path] {
            override def accept(p: Path): Unit = {
              try metaClient.getFs.delete(p, false)
              catch {
                case e: IOException =>
                  throw new HoodieIOException(e.getMessage, e)
              }
            }
          })
        action = "MIGRATED"
      }
      rows.add(Row(partitionPath, String.valueOf(textFormatFile.isPresent),
        String.valueOf(baseFormatFile.isPresent), action))
    }
    val props: Properties = new Properties
    props.setProperty(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key, "true")
    HoodieTableConfig.update(metaClient.getFs, new Path(metaClient.getMetaPath), props)

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new RepairMigratePartitionMetaProcedure()
}

object RepairMigratePartitionMetaProcedure {
  val NAME = "repair_migrate_partition_meta"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RepairMigratePartitionMetaProcedure()
  }
}
