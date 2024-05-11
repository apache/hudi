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

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Properties
import java.util.function.Supplier

import scala.collection.JavaConverters._

class RepairOverwriteHoodiePropsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "new_props_file_path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("property", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("old_value", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("new_value", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  def loadNewProps(filePath: String, props: Properties):Unit = {
    val fs = HadoopFSUtils.getFs(filePath, spark.sessionState.newHadoopConf())
    val fis = fs.open(new Path(filePath))
    props.load(fis)

    fis.close()
  }

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val overwriteFilePath = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val tablePath = getBasePath(tableName)

    val metaClient = createMetaClient(jsc, tablePath)

    var newProps = new Properties
    loadNewProps(overwriteFilePath, newProps)
    val oldProps = metaClient.getTableConfig.propsMap
    HoodieTableConfig.create(metaClient.getStorage, metaClient.getMetaPath, newProps)
    // reload new props as checksum would have been added
    newProps = HoodieTableMetaClient.reload(metaClient).getTableConfig.getProps

    val allPropKeys = new util.TreeSet[String]
    allPropKeys.addAll(newProps.keySet.stream.iterator().asScala.map(key => key.toString).toList.asJava)
    allPropKeys.addAll(oldProps.keySet)

    val rows = new util.ArrayList[Row](allPropKeys.size)
    for (propKey <- allPropKeys.asScala) {
      rows.add(Row(propKey, oldProps.getOrDefault(propKey, "null"),
        newProps.getOrDefault(propKey, "null").toString))
    }

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new RepairOverwriteHoodiePropsProcedure()
}

object RepairOverwriteHoodiePropsProcedure {
  val NAME = "repair_overwrite_hoodie_props"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RepairOverwriteHoodiePropsProcedure()
  }
}
