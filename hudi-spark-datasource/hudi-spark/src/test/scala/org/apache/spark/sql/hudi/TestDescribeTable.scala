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

package org.apache.spark.sql.hudi.ddl

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

import java.util.function.Predicate

class TestDescribeTable extends HoodieSparkSqlTestBase {

  test("Test desc hudi table command") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      val basePath = s"${tmp.getCanonicalPath}/$tbName"

      spark.sql(
        s"""
           |create table $tbName (id int, driver string, precomb int, dat string)
           | using hudi
           | partitioned by(dat)
           | tblproperties(
           |   type='cow',
           |   primaryKey='id',
           |   preCombineField='precomb'
           | )
           | location '$basePath'
       """.stripMargin)

      // just for scala-2.11 compatibility
      val locationInFirstColumn: Predicate[Row] = new Predicate[Row] {
        def test(row: Row): Boolean = row(0).equals("Location")
      }

      spark.sql("set hoodie.schema.on.read.enable=false")
      var output: java.util.List[Row] = spark.sql(s"describe extended $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      spark.sql("set hoodie.schema.on.read.enable=true")
      output = spark.sql(s"desc formatted $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      output = spark.sql(s"describe table extended $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      // DESC returns only columns and partitions when run without 'extended' or 'formatted' keywords
      output = spark.sql(s"describe table $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))

      output = spark.sql(s"desc table $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))

      output = spark.sql(s"desc $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))
    }
  }

  test("Test desc non-hudi table command") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      val basePath = s"${tmp.getCanonicalPath}/$tbName"

      spark.sql(
        s"""
           |create table $tbName (
           | id int,
           | driver string,
           | precomb int,
           | dat string
           |)
           | using parquet
           | location '$basePath'
       """.stripMargin)

      // just for scala-2.11 compatibility
      val locationInFirstColumn: Predicate[Row] = new Predicate[Row] {
        def test(row: Row): Boolean = row(0).equals("Location")
      }

      spark.sql("set hoodie.schema.on.read.enable=false")
      var output: java.util.List[Row] = spark.sql(s"describe extended $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      spark.sql("set hoodie.schema.on.read.enable=true")
      output = spark.sql(s"desc formatted $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      output = spark.sql(s"describe table extended $tbName").collectAsList()
      assert(output.stream().anyMatch(locationInFirstColumn))

      // DESC returns only columns and partitions when run without 'extended' or 'formatted' keywords
      output = spark.sql(s"describe table $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))

      output = spark.sql(s"desc table $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))

      output = spark.sql(s"desc $tbName").collectAsList()
      assert(output.stream().noneMatch(locationInFirstColumn))
    }
  }
}
