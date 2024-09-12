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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.junit.jupiter.api.Assertions

class TestRunRollbackInflightTableServiceProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call run_rollback_inflight_tableservice Procedure for clustering") {
    withTempDir {tmp => {
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
           | partitioned by(ts)
           | location '$basePath'
     """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")
      spark.sql(s"call run_clustering(table => '$tableName', op => 'execute')")

      // delete clustering commit file
      val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)).build()
      val clusteringInstant = metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.get(0)
      metaClient.getActiveTimeline.deleteInstantFileIfExists(clusteringInstant)

      val clusteringInstantTime = clusteringInstant.getTimestamp

      spark.sql(s"call run_rollback_inflight_tableservice(table => '$tableName', pending_instant => '$clusteringInstantTime')")
      Assertions.assertTrue(!metaClient.reloadActiveTimeline().getInstants
        .contains(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringInstantTime)))
      Assertions.assertTrue(metaClient.reloadActiveTimeline().getInstants
        .contains(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, clusteringInstantTime)))
    }}
  }

  test("Test Call run_rollback_inflight_tableservice Procedure for compaction") {
    withTempDir {
      tmp => {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
         """.stripMargin)
        spark.sql("set hoodie.parquet.max.file.size = 10000")
        // disable automatic inline compaction
        spark.sql("set hoodie.compact.inline=false")
        spark.sql("set hoodie.compact.schedule.inline=false")

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")

        // delete compaction commit file
        val metaClient = HoodieTableMetaClient.builder
          .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)).setBasePath(basePath).build
        val compactionInstant: HoodieInstant = metaClient.getActiveTimeline.getReverseOrderedInstants.findFirst().get()

        metaClient.getActiveTimeline.deleteInstantFileIfExists(compactionInstant)
        val compactionInstantTime = compactionInstant.getTimestamp

        spark.sql(s"call run_rollback_inflight_tableservice(table => '$tableName', pending_instant => '$compactionInstantTime', delete_request_instant_file => true)")
        Assertions.assertTrue(!metaClient.reloadActiveTimeline().getInstants
          .contains(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime)))
        Assertions.assertTrue(!metaClient.reloadActiveTimeline().getInstants
          .contains(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime)))
      }
    }
  }


}
