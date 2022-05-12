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

package org.apache.spark.sql.hudi

class TestDropTable extends HoodieSparkSqlTestBase {

  test("Test Drop Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        spark.sql(s"DROP TABLE $tableName")
        checkAnswer(s"show tables like '$tableName'")()
        assertResult(true)(existsPath(s"${tmp.getCanonicalPath}/$tableName"))
      }
    }
  }

  test("Test Drop Table with purge") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        spark.sql(s"DROP TABLE $tableName PURGE")
        checkAnswer(s"show tables like '$tableName'")()
        assertResult(false)(existsPath(s"${tmp.getCanonicalPath}/$tableName"))
      }
    }
  }

  test("Test Drop RO & RT table by purging base table.") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_ro using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='true'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_rt using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='false'
           | )
       """.stripMargin)

      spark.sql(s"drop table ${tableName} purge")
      checkAnswer("show tables")()
    }
  }

  test("Test Drop RO & RT table by one by one.") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_ro using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='true'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_rt using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='false'
           | )
       """.stripMargin)

      spark.sql(s"drop table ${tableName}_ro")
      checkAnswer("show tables")(
        Seq("default", tableName, false), Seq("default", s"${tableName}_rt", false))

      spark.sql(s"drop table ${tableName}_rt")
      checkAnswer("show tables")(Seq("default", tableName, false))

      spark.sql(s"drop table ${tableName}")
      checkAnswer("show tables")()
    }
  }

  test("Test Drop RO table with purge") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_ro using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='true'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           |create table ${tableName}_rt using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.query.as.ro.table='false'
           | )
       """.stripMargin)

      spark.sql(s"drop table ${tableName}_ro purge")
      checkAnswer("show tables")()
    }
  }


}
