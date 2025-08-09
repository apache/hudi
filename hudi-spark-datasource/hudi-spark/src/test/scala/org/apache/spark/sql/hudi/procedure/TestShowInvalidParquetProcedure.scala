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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path

import java.util.{Date, UUID}

class TestShowInvalidParquetProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call show_invalid_parquet Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val customParallelism = 3
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | partitioned by (ts)
           | location '$basePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_invalid_parquet(limit => 10)""")(
        s"Argument: path is required")

      val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
      val invalidPath1 = new Path(basePath, "ts=1000/1.parquet")
      val out1 = fs.create(invalidPath1)
      out1.write(1)
      out1.close()

      val invalidPath2 = new Path(basePath, "ts=1500/2.parquet")
      val out2 = fs.create(invalidPath2)
      out2.write(1)
      out2.close()

      // collect result for table
      var result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism)""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, limit => 1)""".stripMargin).collect()
      assertResult(1) {
        result.length
      }
    }
  }

  test("Test Call show_invalid_parquet Procedure And Delete") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val customParallelism = 3
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | partitioned by (ts)
           | location '$basePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
      val invalidPath1 = new Path(basePath, "ts=1000/1.parquet")
      val out1 = fs.create(invalidPath1)
      out1.write(1)
      out1.close()

      val invalidPath2 = new Path(basePath, "ts=1500/2.parquet")
      val out2 = fs.create(invalidPath2)
      out2.write(1)
      out2.close()

      // collect result for table
      val result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, needDelete => true)""".stripMargin).collect()
      assertResult(0) {
        result.length
      }
    }
  }

  test("Test Call show_invalid_parquet Procedure, Specify Partitions and/or Instants.") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val customParallelism = 3
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
           | partitioned by (year, month, day)
           | location '$basePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1001, '2022', '08', '30'")
        spark.sql(s"insert into $tableName select 2, 'a2', 20, 1002, '2022', '08', '31'")
        spark.sql(s"insert into $tableName select 3, 'a3', 10, 1003, '2022', '07', '03'")
        spark.sql(s"insert into $tableName select 4, 'a4', 20, 1004, '2022', '07', '04'")
      }

      // Check required fields
      checkExceptionContain(s"""call show_invalid_parquet(limit => 10)""")(
        s"Argument: path is required")

      val TEST_WRITE_TOKEN = "1-0-1"
      val instantTime = HoodieInstantTimeGenerator.formatDate(new Date)
      val fileName1 = UUID.randomUUID.toString
      val fullFileName1 = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName1, HoodieFileFormat.PARQUET.getFileExtension)
      val fileName2 = UUID.randomUUID.toString
      val fullFileName2 = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName2, HoodieFileFormat.PARQUET.getFileExtension)
      val fileName3 = UUID.randomUUID.toString
      val fullFileName3 = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName3, HoodieFileFormat.PARQUET.getFileExtension)
      val fileName4 = UUID.randomUUID.toString
      val fullFileName4 = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName4, HoodieFileFormat.PARQUET.getFileExtension)

      val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
      val invalidPath1 = new Path(basePath, "year=2022/month=08/day=30/" + fullFileName1)
      val out1 = fs.create(invalidPath1)
      out1.write(1)
      out1.close()

      val invalidPath2 = new Path(basePath, "year=2022/month=08/day=31/" + fullFileName2)
      val out2 = fs.create(invalidPath2)
      out2.write(1)
      out2.close()

      val invalidPath3 = new Path(basePath, "year=2022/month=07/day=03/" + fullFileName3)
      val out3 = fs.create(invalidPath3)
      out3.write(1)
      out3.close()

      val invalidPath4 = new Path(basePath, "year=2022/month=07/day=04/" + fullFileName4)
      val out4 = fs.create(invalidPath4)
      out4.write(1)
      out4.close()

      // collect result for table
      var result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism)""".stripMargin).collect()
      assertResult(4) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, partitions => 'year=2022')""".stripMargin).collect()
      assertResult(4) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, partitions => 'year=2022/month=07')""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, partitions => 'year=2022/month=08/day=30,year=2022/month=08/day=31')""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, partitions => 'year=2023')""".stripMargin).collect()
      assertResult(0) {
        result.length
      }

      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, instants => '$instantTime')""".stripMargin).collect()
      assertResult(4) {
        result.length
      }
      result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath', parallelism => $customParallelism, instants => '$instantTime', partitions => 'year=2022/month=08')""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

    }
  }
}

