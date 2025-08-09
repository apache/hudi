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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.DataSourceWriteOptions.{ORDERING_FIELDS, PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestRepairTable extends HoodieSparkSqlTestBase {

  test("Test msck repair non-partitioned table") {
    Seq("true", "false").foreach { hiveStylePartitionEnable =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  dt string,
             |  hh string
             | ) using hudi
             | location '$basePath'
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.hive_style_partitioning = '$hiveStylePartitionEnable'
             | )
        """.stripMargin)

        checkExceptionContain(s"msck repair table $tableName")(
          s"Operation not allowed")
      }
    }
  }

  test("Test msck repair partitioned table") {
    Seq("true", "false").foreach { hiveStylePartitionEnable =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  dt string,
             |  hh string
             | ) using hudi
             | partitioned by (dt, hh)
             | location '$basePath'
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.hive_style_partitioning = '$hiveStylePartitionEnable'
             | )
        """.stripMargin)
        val table = spark.sessionState.sqlParser.parseTableIdentifier(tableName)

        import spark.implicits._
        val df = Seq((1, "a1", 1000L, "2022-10-06", "11"), (2, "a2", 1001L, "2022-10-06", "12"))
          .toDF("id", "name", "ts", "dt", "hh")
        df.write.format("hudi")
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt,hh")
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key, hiveStylePartitionEnable)
          .mode(SaveMode.Append)
          .save(basePath)

        assertResult(Seq())(spark.sessionState.catalog.listPartitionNames(table))
        withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
          spark.sql(s"msck repair table $tableName")
        }
        assertResult(Seq("dt=2022-10-06/hh=11", "dt=2022-10-06/hh=12"))(
          spark.sessionState.catalog.listPartitionNames(table))
      }
    }
  }

  test("Test msck repair external partitioned table") {
    Seq("true", "false").foreach { hiveStylePartitionEnable =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "a1", 1000, "2022-10-06", "11"), (2, "a2", 1001, "2022-10-06", "12"))
          .toDF("id", "name", "ts", "dt", "hh")
        df.write.format("hudi")
          .option(TBL_NAME.key(), tableName)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt,hh")
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key, hiveStylePartitionEnable)
          .mode(SaveMode.Append)
          .save(basePath)

        spark.sql(
          s"""
             | create table $tableName
             | using hudi
             | location '$basePath'
        """.stripMargin)
        val table = spark.sessionState.sqlParser.parseTableIdentifier(tableName)

        assertResult(Seq())(spark.sessionState.catalog.listPartitionNames(table))
        withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
          spark.sql(s"msck repair table $tableName")
        }
        assertResult(Seq("dt=2022-10-06/hh=11", "dt=2022-10-06/hh=12"))(
          spark.sessionState.catalog.listPartitionNames(table))
      }
    }
  }

  test("Test msck repair partitioned table [add/drop/sync] partitions") {
    Seq("true", "false").foreach { hiveStylePartitionEnable =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  dt string
             | ) using hudi
             | partitioned by (dt)
             | location '$basePath'
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.hive_style_partitioning = '$hiveStylePartitionEnable'
             | )
        """.stripMargin)
        val table = spark.sessionState.sqlParser.parseTableIdentifier(tableName)

        // test msck repair table add partitions
        import spark.implicits._
        val df1 = Seq((1, "a1", 1000L, "2022-10-06")).toDF("id", "name", "ts", "dt")
        df1.write.format("hudi")
          .option(TBL_NAME.key(), tableName)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key, hiveStylePartitionEnable)
          .mode(SaveMode.Append)
          .save(basePath)

        assertResult(Seq())(spark.sessionState.catalog.listPartitionNames(table))
        spark.sql(s"msck repair table $tableName add partitions")
        assertResult(Seq("dt=2022-10-06"))(spark.sessionState.catalog.listPartitionNames(table))

        // test msck repair table drop partitions
        val df2 = Seq((2, "a2", 1001L, "2022-10-07")).toDF("id", "name", "ts", "dt")
        df2.write.format("hudi")
          .option(TBL_NAME.key(), tableName)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key, hiveStylePartitionEnable)
          .mode(SaveMode.Overwrite)
          .save(basePath)

        assertResult(Seq("dt=2022-10-06"))(spark.sessionState.catalog.listPartitionNames(table))
        spark.sql(s"msck repair table $tableName drop partitions")
        assertResult(Seq())(spark.sessionState.catalog.listPartitionNames(table))

        // test msck repair table sync partitions
        spark.sql(s"msck repair table $tableName sync partitions")
        assertResult(Seq("dt=2022-10-07"))(spark.sessionState.catalog.listPartitionNames(table))
      }
    }
  }
}
