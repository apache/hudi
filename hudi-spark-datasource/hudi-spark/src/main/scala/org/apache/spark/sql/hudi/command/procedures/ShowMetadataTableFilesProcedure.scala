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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

class ShowMetadataTableFilesProcedure() extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "partition", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("file_path", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val partition = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val basePath = getBasePath(table)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metaReader = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getHadoopConf),
      config, basePath, "/tmp")
    if (!metaReader.enabled){
      throw new HoodieException(s"Metadata Table not enabled/initialized.")
    }

    val timer = new HoodieTimer().startTimer
    val statuses = metaReader.getAllFilesInPartition(new Path(basePath, partition))
    logDebug("Took " + timer.endTimer + " ms")

    val rows = new util.ArrayList[Row]
    statuses.toStream.sortBy(p => p.getPath.getName).foreach((f: FileStatus) => {
        rows.add(Row(f.getPath.getName))
    })
    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowMetadataTableFilesProcedure()
}

object ShowMetadataTableFilesProcedure {
  val NAME = "show_metadata_table_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTableFilesProcedure()
  }
}
