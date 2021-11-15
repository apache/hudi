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

class TestDeleteTable extends TestHoodieSqlBase {

  test("Test Delete Table") {
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

        // delete data from table
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select count(1) from $tableName") (
          Seq(0)
        )

        spark.sql(s"insert into $tableName select 2, 'a2', 10, 1000")
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(2, "a2", 10.0, 1000)
        )

        spark.sql(s"delete from $tableName")
        checkAnswer(s"select count(1) from $tableName")(
          Seq(0)
        )
      }
    }
  }
}
