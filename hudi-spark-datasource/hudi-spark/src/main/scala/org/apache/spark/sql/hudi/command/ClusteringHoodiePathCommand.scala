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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ClusteringUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

case class ClusteringHoodiePathCommand(path: String,
  orderByColumns: Seq[String], timestamp: Option[Long]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()
    val client = HoodieSqlUtils.createHoodieClientFromPath(sparkSession, path,
      Map(
        HoodieClusteringConfig.CLUSTERING_SORT_COLUMNS_PROPERTY.key() -> orderByColumns.mkString(",")
      )
    )

    // Get all of the pending clustering.
    val pendingClustering = ClusteringUtils.getAllPendingClusteringPlans(metaClient)
      .iterator().asScala.map(_.getLeft.getTimestamp).toSeq.sortBy(f => f)

    val clusteringInstants = if (timestamp.isEmpty) {
      // If there is no pending clustering, schedule to generate one.
      if (pendingClustering.isEmpty) {
        val instantTime = HoodieActiveTimeline.createNewInstantTime
        if(client.scheduleClusteringAtInstant(instantTime, HOption.empty())) {
          Seq(instantTime)
        } else {
          Seq.empty[String]
        }
      } else { // If exist pending cluster, cluster all of the pending plan.
        pendingClustering
      }
    } else { // Clustering the specified instant.
      if (pendingClustering.contains(timestamp.get.toString)) {
        Seq(timestamp.get.toString)
      } else {
        throw new IllegalArgumentException(s"Clustering instant: ${timestamp.get} is not found in $path," +
          s" Available pending clustering instants are: ${pendingClustering.mkString(",")}.")
      }
    }
    logInfo(s"Clustering instants are: ${clusteringInstants.mkString(",")}.")
    val startTs = System.currentTimeMillis()
    clusteringInstants.foreach(client.cluster(_, true))
    logInfo(s"Finish clustering all the instants: ${clusteringInstants.mkString(",")}," +
      s" time send: ${System.currentTimeMillis() - startTs}ms.")
    Seq.empty[Row]
  }
}
