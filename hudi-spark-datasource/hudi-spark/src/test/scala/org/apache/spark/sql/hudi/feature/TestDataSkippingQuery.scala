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

class TestDataSkippingQuery extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    initQueryIndexConf()
  }

  test("Test the data skipping query involves conditions " +
    "that cover both columns supported by column stats and those that are not supported.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql("set hoodie.metadata.enable = true")
        spark.sql("set hoodie.metadata.index.column.stats.enable = true")
        spark.sql("set hoodie.enable.data.skipping = true")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  attributes map<string, string>,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
             | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
             | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                  """.stripMargin)
        // Check the case where the WHERE condition only includes columns not supported by column stats
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Check the case where the WHERE condition only includes columns supported by column stats
        checkAnswer(s"select id, name, price, ts, dt from $tableName where name='a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Check the case where the WHERE condition includes both columns supported by column stats and those that are not
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red' and name='a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
      }
    }
  }

  test("Test data skipping when specifying columns with column stats support.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql("set hoodie.metadata.enable = true")
        spark.sql("set hoodie.metadata.index.column.stats.enable = true")
        spark.sql("set hoodie.enable.data.skipping = true")
        spark.sql("set hoodie.metadata.index.column.stats.column.list = name")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  attributes map<string, string>,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
             | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
             | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                  """.stripMargin)
        // Check the case where the WHERE condition only includes columns not supported by column stats
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Check the case where the WHERE condition only includes columns supported by column stats
        checkAnswer(s"select id, name, price, ts, dt from $tableName where name='a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Check the case where the WHERE condition includes both columns supported by column stats and those that are not
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red' and name='a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Check WHERE condition that includes both columns with existing column stats and columns of types
        // that support column stats but for which column stats do not exist
        checkAnswer(s"select id, name, price, ts, dt from $tableName where ts=1000 and name='a1'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
      }
    }
  }

  test("bucket index query") {
    Seq("cow", "mor").foreach { tableType =>
      // table bucket prop can not be read in the query sql now, so need to set these configs
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
               | orderingFields = 'ts',
               | type = '$tableType',
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
          spark.sql("set hoodie.bucket.index.query.pruning = false")
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
          spark.sql("set hoodie.bucket.index.query.pruning = true")
        }
      }
    }
  }
}
