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

class TestDataSkipQuery extends HoodieSparkSqlTestBase {

  test("Test data skipping query Table with array struct") {
    withSQLConf("hoodie.metadata.enable" -> "true",
                       "hoodie.metadata.index.column.stats.enable" -> "true",
                       "hoodie.enable.data.skipping" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |      id int,
             |      info array<struct<name: string, age: int>>,
             |      ts bigint,
             |      dt string
             |) using hudi
             |options (
             |      primaryKey = 'id',
             |      type = 'cow',
             |      preCombineField = 'ts'
             |)
             |partitioned by (dt)
       """.stripMargin)


        spark.sql(s"insert into $tableName " +
          s" select 1 as id, array(named_struct('name', 'Eva', 'age', 28)) as info, 1000 as ts, '20210101' as dt")
        spark.sql(s"select id, explode(info) from $tableName where id = 1 and dt = '20210101';").show(false)

        checkAnswer(s"select id, ts, dt from $tableName where id = 1 and dt = '20210101';")(
          Seq(1, 1000, "20210101")
        )

        checkAnswer(s"select id, ts, dt from $tableName where id = 1 and size(info) > 0 and dt = '20210101'")(
          Seq(1, 1000, "20210101")
        )

        checkAnswer(s"select id, ts, dt from $tableName where id is not null and size(info) > 0 and dt = '20210101'")(
          Seq(1, 1000, "20210101")
        )

        checkAnswer(s"select id, ts, dt from $tableName where id = 1 and info is not null and dt = '20210101'")(
          Seq(1, 1000, "20210101")
        )

        checkAnswer(s"select id, ts, dt from $tableName where ts = 1000 and id is not null and dt = '20210101'")(
          Seq(1, 1000, "20210101")
        )
      }
    }
  }
}
