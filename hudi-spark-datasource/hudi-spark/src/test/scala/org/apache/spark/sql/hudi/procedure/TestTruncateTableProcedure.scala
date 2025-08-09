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

import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

class TestTruncateTableProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call truncate_table Procedure：truncate table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      //Step1: create table and insert data
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
     """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10.0, 1000L")
      spark.sql(s"insert into $tableName select 2, 'a2', 20.0, 1500L")
      spark.sql(s"insert into $tableName select 3, 'a3', 30.0, 2000L")
      spark.sql(s"insert into $tableName select 4, 'a4', 40.0, 2500L")

      //Step2: call truncate_table procedure
      spark.sql(s"""call truncate_table(table => '$tableName')""")

      val fs = new Path(tablePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val files = fs.listStatus(new Path(tablePath))

      //Step3: check number of directories under tablePath, only .hoodie
      assertTrue(files.size == 1)
    }
  }

  test("Test Call truncate_table Procedure：truncate given partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      //Step1: create table and insert data
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
      //Step2: call truncate_table procedure, truncate given partitions:year=2019/month=08/day=31,30,29
      spark.sql(s"""call truncate_table(table => '$tableName', partitions => 'year=2019/month=08/day=31,year=2019/month=08/day=30,year=2019/month=08/day=29')""")
      val metaClient = getTableMetaClient(tablePath)
      val replaceCommitInstant = metaClient.getActiveTimeline.getWriteTimeline
        .getCompletedReplaceTimeline.getReverseOrderedInstants.findFirst()
        .get()
      val partitions = metaClient.getActiveTimeline.readReplaceCommitMetadata(replaceCommitInstant)
        .getPartitionToReplaceFileIds
        .keySet()
      //Step3: check number of truncated partitions and location startWith
      assertEquals(3, partitions.size())
      assertTrue(partitions.asScala.forall(_.startsWith("year=2019/month=08")))
      // clean
      //Step4: call clean and check result: left only 1 record
      spark.sql(s"""call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)""")
      val result = spark.sql(s"""select * from $tableName""").collect()
      assertEquals(1, result.length)
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
