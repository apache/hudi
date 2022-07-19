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

import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

class TestCleanProcedure extends HoodieSparkSqlTestBase {

  test("Test Call run_clean Procedure by Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
          |create table $tableName (
          | id int,
          | name string,
          | price double,
          | ts long
          | ) using hudi
          | location '${tmp.getCanonicalPath}'
          | tblproperties (
          |   primaryKey = 'id',
          |   type = 'cow',
          |   preCombineField = 'ts'
          | )
          |""".stripMargin)

      spark.sql("set hoodie.parquet.max.file.size = 10000")
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set price = 11 where id = 1")
      spark.sql(s"update $tableName set price = 12 where id = 1")
      spark.sql(s"update $tableName set price = 13 where id = 1")

      val result1 = spark.sql(s"call run_clean(table => '$tableName', retainCommits => 1)")
        .collect()
        .map(row => Seq(row.getString(0), row.getLong(1), row.getInt(2), row.getString(3), row.getString(4), row.getInt(5)))

      assertResult(1)(result1.length)
      assertResult(2)(result1(0)(2))

      checkAnswer(s"select id, name, price, ts from $tableName order by id") (
        Seq(1, "a1", 13, 1000)
      )
    }
  }

}
