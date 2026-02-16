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

package org.apache.spark.sql.hudi.feature

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestCompactionTable extends HoodieSparkSqlTestBase {

  test("Test compaction table") {
    withRecordType()(withTempDir { tmp =>
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
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      withSQLConf(
        "hoodie.parquet.max.file.size" -> "10000",
        // disable automatic inline compaction
        "hoodie.compact.inline" -> "false",
        "hoodie.compact.schedule.inline" -> "false"
      ) {

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"schedule compaction  on $tableName")
        spark.sql(s"update $tableName set price = 12 where id = 2")
        spark.sql(s"schedule compaction  on $tableName")
        val compactionRows = spark.sql(s"show compaction on $tableName limit 10").collect()
        val timestamps = compactionRows.map(_.getString(0))
        assertResult(2)(timestamps.length)

        spark.sql(s"run compaction on $tableName at ${timestamps(1)}")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 11.0, 1000),
          Seq(2, "a2", 12.0, 1000),
          Seq(3, "a3", 10.0, 1000),
          Seq(4, "a4", 10.0, 1000)
        )
        assertResult(2)(spark.sql(s"show compaction on $tableName").collect().length)
        spark.sql(s"run compaction on $tableName at ${timestamps(0)}")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 11.0, 1000),
          Seq(2, "a2", 12.0, 1000),
          Seq(3, "a3", 10.0, 1000),
          Seq(4, "a4", 10.0, 1000)
        )
        assertResult(2)(spark.sql(s"show compaction on $tableName").collect().length)
      }
    })
  }

  test("Test compaction path") {
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
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      withSQLConf(
        "hoodie.parquet.max.file.size" -> "10000",
        // disable automatic inline compaction
        "hoodie.compact.inline" -> "false",
        "hoodie.compact.schedule.inline" -> "false"
      ) {

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"run compaction on '${tmp.getCanonicalPath}'")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 11.0, 1000),
          Seq(2, "a2", 10.0, 1000),
          Seq(3, "a3", 10.0, 1000)
        )
        assertResult(0)(spark.sql(s"show compaction on '${tmp.getCanonicalPath}'").collect().length)
        // schedule compaction first
        spark.sql(s"update $tableName set price = 12 where id = 1")
        spark.sql(s"schedule compaction on '${tmp.getCanonicalPath}'")

        // schedule compaction second
        spark.sql(s"update $tableName set price = 12 where id = 2")
        spark.sql(s"schedule compaction on '${tmp.getCanonicalPath}'")

        // show compaction
        assertResult(2)(spark.sql(s"show compaction on '${tmp.getCanonicalPath}'").collect().length)
        // run compaction for all the scheduled compaction
        spark.sql(s"run compaction on '${tmp.getCanonicalPath}'")

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 12.0, 1000),
          Seq(2, "a2", 12.0, 1000),
          Seq(3, "a3", 10.0, 1000)
        )
        assertResult(2)(spark.sql(s"show compaction on '${tmp.getCanonicalPath}'").collect().length)

        checkException(s"run compaction on '${tmp.getCanonicalPath}' at 12345")(
          s"specific 12345 instants is not exist"
        )
      }
    }
  }

  test("Test compaction before and after deletes") {
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
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      withSQLConf(
        "hoodie.parquet.max.file.size" -> "10000",
        // disable automatic inline compaction
        "hoodie.compact.inline" -> "false",
        "hoodie.compact.schedule.inline" -> "false",
        // set compaction frequency to every 2 commits
        "hoodie.compact.inline.max.delta.commits" -> "2"
      ) {
        // insert data
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        // update data
        spark.sql(s"update $tableName set price = 11 where id = 1")
        // update data
        spark.sql(s"update $tableName set price = 12 where id = 2")
        // schedule compaction
        spark.sql(s"schedule compaction on $tableName")
        // show compaction
        var compactionRows = spark.sql(s"show compaction on $tableName limit 10").collect()
        var timestamps = compactionRows.map(_.getString(0))
        assertResult(1)(timestamps.length)
        // run compaction
        spark.sql(s"run compaction on $tableName at ${timestamps(0)}")
        // check data
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 11.0, 1000),
          Seq(2, "a2", 12.0, 1000),
          Seq(3, "a3", 10.0, 1000)
        )
        // show compaction
        assertResult(2)(spark.sql(s"show compaction on $tableName").collect().length)
        // Try deleting non-existent row
        spark.sql(s"DELETE FROM $tableName WHERE id = 41")
        // Delete record identified by some field other than the primary-key
        spark.sql(s"DELETE FROM $tableName WHERE name = 'a2'")
        // schedule compaction
        spark.sql(s"schedule compaction on $tableName")
        // show compaction
        compactionRows = spark.sql(s"show compaction on $tableName limit 10").collect()
        timestamps = compactionRows.map(_.getString(0)).sorted
        assertResult(3)(timestamps.length)
        // run compaction
        spark.sql(s"run compaction on $tableName at ${timestamps(2)}")
        // check data, only 2 records should be present
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 11.0, 1000),
          Seq(3, "a3", 10.0, 1000)
        )
        // show compaction
        assertResult(3)(spark.sql(s"show compaction on $tableName limit 10").collect().length)
      }
    }
  }
}
