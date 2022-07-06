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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

import java.util

class TestCopyToTableProcedure extends HoodieSparkSqlTestBase {

  test("Test Call copy_to_table Procedure with default params") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val copyTableName = generateTableName
      // Check required fields
      checkExceptionContain(s"call copy_to_table(table=>'$tableName')")(s"Argument: new_table is required")

      val row = spark.sql(s"""call copy_to_table(table=>'$tableName',new_table=>'$copyTableName')""").collectAsList()
      assert(row.size() == 1 && row.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $copyTableName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)
    }
  }

  test("Test Call copy_to_table Procedure with snapshot") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert 4 rows data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val copyTableName = generateTableName
      val row = spark.sql(s"""call copy_to_table(table=>'$tableName',new_table=>'$copyTableName',query_type=>'snapshot')""").collectAsList()
      // check exit code
      assert(row.size() == 1 && row.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $copyTableName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)


      // insert 4 rows data to table
      spark.sql(s"insert into $tableName select 5, 'a1', 10, 1000")
      // mark max instanceTime.total row is 5
      val instanceTime = spark.sql(s"select max(_hoodie_commit_time) from $tableName").collectAsList().get(0).get(0)
      spark.sql(s"insert into $tableName select 6, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 7, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 8, 'a1', 10, 1000")

      // check row count after twice insert
      val finalTableCount = spark.sql(s"select * from $tableName").count()
      assert(finalTableCount == 8)

      // check snapshot copy with mark instanceTime
      val copyTableName2 = generateTableName
      val row2 = spark.sql(s"""call copy_to_table(table=>'$tableName',new_table=>'$copyTableName2',query_type=>'snapshot',as_of_instant=>'$instanceTime')""").collectAsList()
      // check exit code
      assert(row2.size() == 1 && row2.get(0).get(0) == 0)
      val df = spark.sql(s"""select * from $copyTableName2""")
      assert(df.count() == 5)

      val ids: util.List[Row] = df.selectExpr("id").collectAsList()
      assert(ids.containsAll(util.Arrays.asList(Row(1), Row(2), Row(3), Row(4), Row(5))))

    }
  }

  test("Test Call copy_to_table Procedure with incremental") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert 4 rows data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // mark beginTime
      val beginTime = spark.sql(s"select max(_hoodie_commit_time) from $tableName").collectAsList().get(0).get(0)
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")
      val endTime = spark.sql(s"select max(_hoodie_commit_time) from $tableName").collectAsList().get(0).get(0)


      val copyTableName = generateTableName
      // Check required fields
      checkExceptionContain(s"call copy_to_table(table=>'$tableName',new_table=>'$copyTableName',query_type=>'incremental')")("begin_instance_time and end_instance_time can not be null")

      //copy from tableName with begin_instance_time、end_instance_time
      val copyCmd = spark.sql(s"call copy_to_table" + s"(table=>'$tableName'" +
        s",new_table=>'$copyTableName'" +
        s",query_type=>'incremental'" +
        s",begin_instance_time=>'$beginTime'" +
        s",end_instance_time=>'$endTime')").collectAsList()
      assert(copyCmd.size() == 1 && copyCmd.get(0).get(0) == 0)

      val df = spark.sql(s"select * from $copyTableName")
      assert(df.count() == 2)
      val ids = df.selectExpr("id").collectAsList()
      assert(ids.containsAll(util.Arrays.asList(Row(3), Row(4))))
    }
  }

  test("Test Call copy_to_table Procedure with read_optimized") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create mor table with hoodie.compact.inline.max.delta.commits=5
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | options (
           |  type='mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.compact.inline.max.delta.commits='5',
           |  hoodie.compact.inline='true'
           |
           | )
       """.stripMargin)

      //add 4 delta commit
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"update $tableName set ts=2000 where id = 1")
      spark.sql(s"update $tableName set ts=3000 where id = 1")
      spark.sql(s"update $tableName set ts=4000 where id = 1")

      val copyTableName = generateTableName

      val copyCmd = spark.sql(s"""call copy_to_table(table=>'$tableName',new_table=>'$copyTableName',query_type=>'read_optimized')""").collectAsList()
      assert(copyCmd.size() == 1 && copyCmd.get(0).get(0) == 0)
      val copyDf = spark.sql(s"select * from $copyTableName")
      assert(copyDf.count() == 1)
      //check ts
      assert(copyDf.selectExpr("ts").collectAsList().contains(Row(1000)))

      // trigger compact (delta_commit==5)
      spark.sql(s"update $tableName set ts=5000 where id = 1")

      val copyTableName2 = generateTableName
      val copyCmd2 = spark.sql(s"""call copy_to_table(table=>'$tableName',new_table=>'$copyTableName2',query_type=>'read_optimized')""").collectAsList()
      assert(copyCmd2.size() == 1 && copyCmd2.get(0).get(0) == 0)
      val copyDf2 = spark.sql(s"select * from $copyTableName2")
      assert(copyDf2.count() == 1)
      //check ts
      assert(copyDf2.selectExpr("ts").collectAsList().contains(Row(5000)))
    }
  }

  test("Test Call copy_to_table Procedure with append_mode") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val copyTableName = generateTableName
      // Check required fields
      checkExceptionContain(s"call copy_to_table(table=>'$tableName')")(s"Argument: new_table is required")

      val copyCmd = spark.sql(s"call copy_to_table(table=>'$tableName',new_table=>'$copyTableName')").collectAsList()
      assert(copyCmd.size() == 1 && copyCmd.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $copyTableName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)

      // add 2 rows
      spark.sql(s"insert into $tableName select 5, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 6, 'a2', 20, 1500")

      val copyCmd2 = spark.sql(s"call copy_to_table(table=>'$tableName',new_table=>'$copyTableName',save_mode=>'append')").collectAsList()
      assert(copyCmd2.size() == 1 && copyCmd2.get(0).get(0) == 0)

      val df2 = spark.sql(s"""select * from $copyTableName""")
      // total insert 4+6=10 rows
      assert(df2.count() == 10)
      val ids2 = df2.selectExpr("id").collectAsList()
      assert(ids2.containsAll(util.Arrays.asList(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6))))

    }
  }

  test("Test Call copy_to_table Procedure with overwrite_mode") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val copyTableName = generateTableName
      // Check required fields
      checkExceptionContain(s"call copy_to_table(table=>'$tableName')")(s"Argument: new_table is required")

      val copyCmd = spark.sql(s"call copy_to_table(table=>'$tableName',new_table=>'$copyTableName')").collectAsList()
      assert(copyCmd.size() == 1 && copyCmd.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $copyTableName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)

      // add 2 rows
      spark.sql(s"insert into $tableName select 5, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 6, 'a2', 20, 1500")

      val copyCmd2 = spark.sql(s"call copy_to_table(table=>'$tableName',new_table=>'$copyTableName',save_mode=>'Overwrite')").collectAsList()
      assert(copyCmd2.size() == 1 && copyCmd2.get(0).get(0) == 0)

      val df2 = spark.sql(s"""select * from $copyTableName""")
      // total insert 6 rows
      assert(df2.count() == 6)
      val ids2 = df2.selectExpr("id").collectAsList()
      assert(ids2.containsAll(util.Arrays.asList(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6))))

    }
  }
}
