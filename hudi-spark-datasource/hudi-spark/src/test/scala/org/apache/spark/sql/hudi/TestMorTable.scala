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

class TestMorTable extends HoodieSparkSqlTestBase {
  test("Test Insert Into MOR table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  dt string,
           |  name string,
           |  price double,
           |  ts long,
           |  test_decimal_col decimal(25, 4)
           |) using hudi
           |options
           |(
           |    type = 'mor'
           |    ,primaryKey = 'id'
           |    ,hoodie.index.type = 'INMEMORY'
           |)
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}'
           | """.stripMargin)

      // Note: Do not write the field alias, the partition field must be placed last.
      spark.sql(
        s"""
           | insert into $tableName values
           | (1, 'a1', 10, 1000, 1.0, "2021-01-05"),
           | (2, 'a2', 20, 2000, 2.0, "2021-01-06"),
           | (3, 'a3', 30, 3000, 3.0, "2021-01-07")
          """.stripMargin)

      spark.sql(
        s"""
           | insert into $tableName values
           | (1, 'a1', 10, null, 1.0, "2021-01-05"),
           | (4, 'a2', 20, 2000, null, "2021-01-06")
          """.stripMargin)

      checkAnswer(s"select id, name, price, ts, cast(test_decimal_col AS string), dt from $tableName")(
        Seq(1, "a1", 10.0, null, "1.0000", "2021-01-05"),
        Seq(2, "a2", 20.0, 2000, "2.0000", "2021-01-06"),
        Seq(3, "a3", 30.0, 3000, "3.0000", "2021-01-07"),
        Seq(4, "a2", 20.0, 2000, null, "2021-01-06")
      )
    }
  }
}
