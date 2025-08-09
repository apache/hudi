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
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

class TestDropPartitionProcedure extends HoodieSparkProcedureTestBase {

  test("Case1: Test Call drop_partition Procedure For Multiple Partitions: '*' stands for all partitions in leaf partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string,
           |  month string,
           |  day string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | preCombineField = 'ts'
           |)
           |partitioned by(year, month, day)
           |location '$tablePath'
           |
     """.stripMargin)

      insertData(tableName)

      spark.sql(s"""call drop_partition(table => '$tableName', partition => 'year=2019/month=08/*')""")

      val metaClient = getTableMetaClient(tablePath)
      val replaceCommitInstant = metaClient.getActiveTimeline.getWriteTimeline
        .getCompletedReplaceTimeline.getReverseOrderedInstants.findFirst()
        .get()

      val partitions = metaClient.getActiveTimeline.readReplaceCommitMetadata(replaceCommitInstant)
        .getPartitionToReplaceFileIds
        .keySet()

      assertEquals(3, partitions.size())
      assertTrue(partitions.asScala.forall(_.startsWith("year=2019/month=08")))

      // clean
      spark.sql(s"""call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)""")
      val result = spark.sql(s"""select * from $tableName""").collect()
      assertEquals(1, result.length)
    }
  }

  test("Case2: Test Call drop_partition Procedure For Multiple Partitions: '*' stands for all partitions in middle partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string,
           |  month string,
           |  day string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | preCombineField = 'ts'
           |)
           |partitioned by(year, month, day)
           |location '$tablePath'
           |
     """.stripMargin)

      insertData(tableName)

      spark.sql(s"""call drop_partition(table => '$tableName', partition => 'year=2019/*')""")

      val metaClient = getTableMetaClient(tablePath)
      val replaceCommitInstant = metaClient.getActiveTimeline.getWriteTimeline
        .getCompletedReplaceTimeline.getReverseOrderedInstants.findFirst()
        .get()

      val partitions = metaClient.getActiveTimeline.readReplaceCommitMetadata(replaceCommitInstant)
        .getPartitionToReplaceFileIds
        .keySet()

      assertEquals(4, partitions.size())

      // clean
      spark.sql(s"""call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)""")
      val result = spark.sql(s"""select * from $tableName""").collect()
      assertEquals(0, result.length)
    }
  }

  test("Case3: Test Call drop_partition Procedure For Multiple Partitions: provide partition list") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string,
           |  month string,
           |  day string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | preCombineField = 'ts'
           |)
           |partitioned by(year, month, day)
           |location '$tablePath'
           |
     """.stripMargin)

      insertData(tableName)

      spark.sql(s"""call drop_partition(table => '$tableName', partition => 'year=2019/month=08/day=31,year=2019/month=08/day=30')""")

      val metaClient = getTableMetaClient(tablePath)
      val replaceCommitInstant = metaClient.getActiveTimeline.getWriteTimeline
        .getCompletedReplaceTimeline.getReverseOrderedInstants.findFirst()
        .get()

      val partitions = metaClient.getActiveTimeline.readReplaceCommitMetadata(replaceCommitInstant)
        .getPartitionToReplaceFileIds
        .keySet()

      assertEquals(2, partitions.size())
      assertTrue(partitions.asScala.forall(_.startsWith("year=2019/month=08")))

      // clean
      spark.sql(s"""call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)""")
      val result = spark.sql(s"""select * from $tableName""").collect()
      assertEquals(2, result.length)
    }
  }

  test("Case4: Test Call drop_partition Procedure For Single Partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string,
           |  month string,
           |  day string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | preCombineField = 'ts'
           |)
           |partitioned by(year, month, day)
           |location '$tablePath'
           |
     """.stripMargin)

      insertData(tableName)

      spark.sql(s"""call drop_partition(table => '$tableName', partition => 'year=2019/month=08/day=31')""")

      val metaClient = getTableMetaClient(tablePath)
      val replaceCommitInstant = metaClient.getActiveTimeline.getWriteTimeline
        .getCompletedReplaceTimeline.getReverseOrderedInstants.findFirst()
        .get()

      val partitions = metaClient.getActiveTimeline.readReplaceCommitMetadata(replaceCommitInstant)
        .getPartitionToReplaceFileIds
        .keySet()

      assertEquals(1, partitions.size())
      assertTrue(partitions.asScala.forall(_.equals("year=2019/month=08/day=31")))

      // clean
      spark.sql(s"""call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)""")
      val result = spark.sql(s"""select * from $tableName""").collect()
      assertEquals(3, result.length)
    }
  }

  private def insertData(tableName: String): Unit = {
    withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
      spark.sql(s"""insert into $tableName values (1, 'n1', 1, 1, '2019', '08', '31')""")
      spark.sql(s"""insert into $tableName values (2, 'n2', 2, 2, '2019', '08', '30')""")
      spark.sql(s"""insert into $tableName values (3, 'n3', 3, 3, '2019', '08', '29')""")
      spark.sql(s"""insert into $tableName values (4, 'n4', 4, 4, '2019', '07', '31')""")
    }
  }

  private def getTableMetaClient(tablePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
      .build()
  }
}
