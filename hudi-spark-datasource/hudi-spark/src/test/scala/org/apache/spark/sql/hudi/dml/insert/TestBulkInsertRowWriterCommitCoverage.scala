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

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Focused coverage for the row-writer bulk-insert commit executors in
 * {@code org.apache.hudi.commit}: [[org.apache.hudi.commit.BaseDatasetBulkInsertCommitActionExecutor]]
 * and [[org.apache.hudi.commit.DatasetBulkInsertOverwriteCommitActionExecutor]].
 *
 * All tests set {@code hoodie.spark.sql.insert.into.operation = bulk_insert} and keep the default
 * row-writer path enabled, so INSERT / INSERT OVERWRITE flow through the Dataset-based executors.
 */
class TestBulkInsertRowWriterCommitCoverage extends HoodieSparkSqlTestBase {

  test("Test row-writer bulk_insert into partitioned table") {
    withSQLConf(
      "hoodie.spark.sql.insert.into.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      Seq("cow", "mor").foreach { tableType =>
        withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  dt string
               |) using hudi
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
               | tblproperties (type = '$tableType', primaryKey = 'id')
             """.stripMargin)

          spark.sql(
            s"""insert into $tableName values
               | (1, 'a1', 10.0, '2024-01-01'),
               | (2, 'a2', 20.0, '2024-01-01'),
               | (3, 'a3', 30.0, '2024-01-02')
             """.stripMargin)

          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2024-01-01"),
            Seq(2, "a2", 20.0, "2024-01-01"),
            Seq(3, "a3", 30.0, "2024-01-02")
          )
        }
      }
    }
  }

  test("Test row-writer insert overwrite with dynamic partitions") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  dt string
             |) using hudi
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | tblproperties (type = 'cow', primaryKey = 'id')
           """.stripMargin)

        spark.sql(
          s"""insert into $tableName values
             | (1, 'a1', '2024-01-01'),
             | (2, 'a2', '2024-01-02')
           """.stripMargin)

        // Dynamic insert overwrite: only the partitions present in the incoming data
        // (2024-01-01) are replaced; 2024-01-02 must survive.
        spark.sql(
          s"""insert overwrite table $tableName partition (dt)
             | select 1 as id, 'a1_new' as name, '2024-01-01' as dt union all
             | select 3 as id, 'a3' as name, '2024-01-01' as dt
           """.stripMargin)

        checkAnswer(s"select id, name, dt from $tableName order by id")(
          Seq(1, "a1_new", "2024-01-01"),
          Seq(2, "a2", "2024-01-02"),
          Seq(3, "a3", "2024-01-01")
        )
      }
    }
  }

  test("Test row-writer insert overwrite with static partition") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  dt string
             |) using hudi
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | tblproperties (type = 'cow', primaryKey = 'id')
           """.stripMargin)

        spark.sql(
          s"""insert into $tableName values
             | (1, 'a1', '2024-01-01'),
             | (2, 'a2', '2024-01-01'),
             | (3, 'a3', '2024-01-02')
           """.stripMargin)

        // Static partition spec -> STATIC_OVERWRITE_PARTITION_PATHS drives the static
        // branch of getPartitionToReplacedFileIds. Only 2024-01-01 is replaced.
        spark.sql(
          s"""insert overwrite table $tableName partition (dt = '2024-01-01')
             | select 9 as id, 'a9' as name
           """.stripMargin)

        checkAnswer(s"select id, name, dt from $tableName order by id")(
          Seq(3, "a3", "2024-01-02"),
          Seq(9, "a9", "2024-01-01")
        )
      }
    }
  }

  test("Test row-writer insert overwrite rejected when overlapping pending clustering") {
    withSQLConf(
      "hoodie.spark.sql.insert.into.operation" -> "bulk_insert",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.schedule.inline" -> "false") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  dt string
             |) using hudi
             | partitioned by (dt)
             | location '$basePath'
             | tblproperties (type = 'cow', primaryKey = 'id')
           """.stripMargin)

        // Two separate bulk-insert commits create two file groups in the same partition,
        // guaranteeing the size-based planner produces a non-empty clustering plan.
        spark.sql(s"insert into $tableName values (1, 'a1', '2024-01-01')")
        spark.sql(s"insert into $tableName values (2, 'a2', '2024-01-01')")

        // Schedule (but do not run) a clustering plan so the file groups in 2024-01-01
        // are in pending clustering.
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
        try {
          assert(client.scheduleClustering(HOption.empty()).isPresent,
            "expected a pending clustering plan to be scheduled")
        } finally {
          client.close()
        }

        // An insert overwrite that targets the partition under pending clustering must be
        // rejected by the default SparkRejectUpdateStrategy before any write materializes.
        checkExceptionContain(new Runnable {
          override def run(): Unit = spark.sql(
            s"""insert overwrite table $tableName partition (dt)
               | select 1 as id, 'a1_new' as name, '2024-01-01' as dt
             """.stripMargin)
        })("Not allowed to update the clustering file group")

        // The rejection happens before commit, so the original data is intact.
        checkAnswer(s"select id, name, dt from $tableName order by id")(
          Seq(1, "a1", "2024-01-01"),
          Seq(2, "a2", "2024-01-01")
        )
      }
    }
  }
}
