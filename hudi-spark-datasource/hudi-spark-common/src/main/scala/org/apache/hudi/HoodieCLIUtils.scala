/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.avro.model.HoodieClusteringGroup
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.withSparkConf

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter}

object HoodieCLIUtils {

  def createHoodieClientFromPath(sparkSession: SparkSession,
                                 basePath: String,
                                 conf: Map[String, String]): SparkRDDWriteClient[_] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val schemaStr = schemaUtil.getTableAvroSchemaWithoutMetadataFields.toString
    val finalParameters = HoodieWriterUtils.parametersWithWriteDefaults(
      withSparkConf(sparkSession, Map.empty)(
        conf + (DataSourceWriteOptions.TABLE_TYPE.key() -> metaClient.getTableType.name()))
    )

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    DataSourceUtils.createHoodieClient(jsc, schemaStr, basePath,
      metaClient.getTableConfig.getTableName, finalParameters.asJava)
  }

  def extractPartitions(clusteringGroups: Seq[HoodieClusteringGroup]): String = {
    var partitionPaths: Seq[String] = Seq.empty
    clusteringGroups.foreach(g =>
      g.getSlices.asScala.foreach(slice =>
        partitionPaths = partitionPaths :+ slice.getPartitionPath
      )
    )

    partitionPaths.sorted.mkString(",")
  }
}
