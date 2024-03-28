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

class TestBucketIndexQuery extends HoodieSparkSqlTestBase {

  test("bucket index query etl") {
    // table bucket prop can not read in query sql now, so need set these conf
    withSQLConf("hoodie.enable.data.skipping" -> "true",
      "hoodie.bucket.index.hash.field" -> "id",
      "hoodie.bucket.index.num.buckets" -> "20",
      "hoodie.index.type" -> "BUCKET") {
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
             |  ts long
             |) using hudi
             | tblproperties (
             | primaryKey = 'id,name',
             | preCombineField = 'ts',
             | hoodie.index.type = 'BUCKET',
             | hoodie.bucket.index.hash.field = 'id',
             | hoodie.bucket.index.num.buckets = '20')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', 10, 1000, "2021-01-05"),
             | (2, 'a2', 20, 2000, "2021-01-06"),
             | (3, 'a3', 30, 3000, "2021-01-07")
              """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1 and name = 'a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 2 or id = 5")(
          Seq(2, "a2", 20.0, 2000, "2021-01-06")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id in (2, 3)")(
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id != 4")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
        spark.sql("set hoodie.bucket.query.index = false")
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1 and name = 'a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 2 or id = 5")(
          Seq(2, "a2", 20.0, 2000, "2021-01-06")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id in (2, 3)")(
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
        checkAnswer(s"select id, name, price, ts, dt from $tableName where id != 4")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
        spark.sql("set hoodie.bucket.query.index = true")
      }
    }
  }
}
