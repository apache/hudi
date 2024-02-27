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
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path

class TestShowInvalidParquetProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call show_invalid_parquet Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
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
           |  preCombineField = 'ts'
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
      val result = spark.sql(
        s"""call show_invalid_parquet(path => '$basePath')""".stripMargin).collect()
      assertResult(2) {
        result.length
      }
    }
  }
}
