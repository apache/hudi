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

package org.apache.spark.sql.hudi.procedure

import org.apache.spark.sql.hudi.TestHoodieSqlBase

class TestCompactionProcedure extends TestHoodieSqlBase {

  test("Test Call run_compaction Procedure by Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql("set hoodie.parquet.max.file.size = 10000")
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      spark.sql(s"update $tableName set price = 11 where id = 1")

      spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
      spark.sql(s"update $tableName set price = 12 where id = 2")
      spark.sql(s"call run_compaction('schedule', '$tableName')")
      val compactionRows = spark.sql(s"call show_compaction(table => '$tableName', limit => 10)").collect()
      val timestamps = compactionRows.map(_.getString(0))
      assertResult(2)(timestamps.length)

      spark.sql(s"call run_compaction(op => 'run', table => '$tableName', timestamp => ${timestamps(1)})")
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 10.0, 1000)
      )
      assertResult(1)(spark.sql(s"call show_compaction('$tableName')").collect().length)
      spark.sql(s"call run_compaction(op => 'run', table => '$tableName', timestamp => ${timestamps(0)})")
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 10.0, 1000)
      )
      assertResult(0)(spark.sql(s"call show_compaction(table => '$tableName')").collect().length)
    }
  }

  test("Test Call run_compaction Procedure by Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql("set hoodie.parquet.max.file.size = 10000")
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"update $tableName set price = 11 where id = 1")

      spark.sql(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}')")
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      assertResult(0)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect().length)
      // schedule compaction first
      spark.sql(s"update $tableName set price = 12 where id = 1")
      spark.sql(s"call run_compaction(op=> 'schedule', path => '${tmp.getCanonicalPath}')")

      // schedule compaction second
      spark.sql(s"update $tableName set price = 12 where id = 2")
      spark.sql(s"call run_compaction(op => 'schedule', path => '${tmp.getCanonicalPath}')")

      // show compaction
      assertResult(2)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect().length)
      // run compaction for all the scheduled compaction
      spark.sql(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}')")

      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 12.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      assertResult(0)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect().length)

      checkException(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}', timestamp => 12345L)")(
        s"Compaction instant: 12345 is not found in ${tmp.getCanonicalPath}, Available pending compaction instants are:  "
      )
    }
  }
}
