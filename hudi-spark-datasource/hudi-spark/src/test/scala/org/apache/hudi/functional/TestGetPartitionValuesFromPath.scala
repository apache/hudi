/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestGetPartitionValuesFromPath extends HoodieSparkSqlTestBase {

  Seq(true, false).foreach { hiveStylePartitioning =>
    Seq(true, false).foreach {readFromPath =>
      test(s"Get partition values from path: $readFromPath, isHivePartitioning: $hiveStylePartitioning") {
        withSQLConf("hoodie.datasource.read.extract.partition.values.from.path" -> readFromPath.toString,
          HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
          withTable(generateTableName) { tableName =>
            spark.sql(
              s"""
                 |create table $tableName (
                 | id int,
                 | name string,
                 | region string,
                 | dt date
                 |) using hudi
                 |tblproperties (
                 | primaryKey = 'id',
                 | type='mor',
                 | hoodie.datasource.write.hive_style_partitioning='$hiveStylePartitioning')
                 |partitioned by (region, dt)""".stripMargin)
            spark.sql(s"insert into $tableName partition (region='reg1', dt='2023-08-01') select 1, 'name1'")

            checkAnswer(s"select id, name, region, cast(dt as string) from $tableName")(
              Seq(1, "name1", "reg1", "2023-08-01")
            )
          }
        }
      }
    }
  }

  test("Test get partition values from path when upsert and bulk_insert MOR table") {
    withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
      withTable(generateTableName) { tableName =>
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | ts bigint,
             | region string,
             | dt date
             |) using hudi
             |tblproperties (
             | primaryKey = 'id',
             | type = 'mor',
             | orderingFields = 'ts',
             | hoodie.datasource.write.drop.partition.columns = 'true'
             |)
             |partitioned by (region, dt)""".stripMargin)

        spark.sql(s"insert into $tableName partition (region='reg1', dt='2023-10-01') select 1, 'name1', 1000")
        checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName")(
          Seq(1, "name1", 1000, "reg1", "2023-10-01")
        )

        withSQLConf("hoodie.datasource.write.operation" -> "upsert") {
          spark.sql(s"insert into $tableName partition (region='reg1', dt='2023-10-01') select 1, 'name11', 1000")
          checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName")(
            Seq(1, "name11", 1000, "reg1", "2023-10-01")
          )
        }

        withSQLConf("hoodie.datasource.write.operation" -> "bulk_insert") {
          spark.sql(s"insert into $tableName partition (region='reg1', dt='2023-10-01') select 1, 'name111', 1000")
          checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName")(
            Seq(1, "name11", 1000, "reg1", "2023-10-01"), Seq(1, "name111", 1000, "reg1", "2023-10-01")
          )
        }
      }
    }
  }
}
