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

class TestUpdateTable extends TestHoodieSqlBase {

  test("Test Update Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  ID int,
             |  NAME string,
             |  PRICE double,
             |  TS long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'ID',
             |  preCombineField = 'TS'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set price = 20 where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set price = price * 2 where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 40.0, 1000)
        )
      }
    }
  }

  test("Test ignoring case for Update Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
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
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = 20 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = PRICE * 2 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 40.0, 1000)
        )
      }
    }
  }
}
