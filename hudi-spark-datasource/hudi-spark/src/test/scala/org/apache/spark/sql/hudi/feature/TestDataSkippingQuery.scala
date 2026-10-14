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

import org.apache.hudi.ColumnStatsIndexSupport
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import scala.collection.JavaConverters._

class TestDataSkippingQuery extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    initQueryIndexConf()
  }

  test("Test the data skipping query involves conditions " +
    "that cover both columns supported by column stats and those that are not supported.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true"
        ) {
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
  }

  test("Test data skipping when specifying columns with column stats support.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true",
          "hoodie.metadata.index.column.stats.column.list" -> "name"
        ) {
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
  }

  test("Data skipping retains files with missing column statistics after index expansion") {
    withTempDir { tmp =>
      val tableName = generateTableName
      withSQLConf(
        "hoodie.metadata.enable" -> "true",
        "hoodie.metadata.index.column.stats.enable" -> "true",
        "hoodie.metadata.index.partition.stats.enable" -> "false",
        "hoodie.metadata.index.column.stats.column.list" -> "age",
        "hoodie.enable.data.skipping" -> "false"
      ) {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  age int,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = 'cow')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             |""".stripMargin)
        // The data column exists from the first write, but only age is indexed.
        spark.sql(s"insert into $tableName values (1, 'alice', 25, 1, 'par1')")

        withSQLConf("hoodie.metadata.index.column.stats.column.list" -> "age,name") {
          // A separate partition leaves the old file and its statistics untouched.
          spark.sql(s"insert into $tableName values (2, 'bob', 30, 2, 'par2')")
          val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
          val indexedColumns = metaClient.getIndexMetadata.get().getIndexDefinitions.get("column_stats").getSourceFields
          assert(indexedColumns.contains("age") && indexedColumns.contains("name"), indexedColumns.toString)
          val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
          val index = new ColumnStatsIndexSupport(spark, spark.table(tableName).schema,
            new TableSchemaResolver(metaClient).getTableSchema, metadataConfig, metaClient)
          val stats = index.loadColumnStatsIndexRecords(Seq("age", "name"), shouldReadInMemory = true)
            .collectAsList().asScala
          assert(stats.count(_.getColumnName == "age") == 2)
          assert(stats.count(_.getColumnName == "name") == 1)
          assert(stats.groupBy(_.getFileName).values.map(_.map(_.getColumnName).toSet).toSet ==
            Set(Set("age"), Set("age", "name")))
          val query = s"select id, name, age from $tableName where age >= 0 and name = 'alice'"
          checkAnswer(query)(Seq(1, "alice", 25))

          withSQLConf("hoodie.enable.data.skipping" -> "true") {
            spark.sql(s"alter table $tableName set tblproperties ('hoodie.enable.data.skipping' = 'true')")
            spark.catalog.refreshTable(tableName)
            withClue("Name-only predicate with data skipping enabled: ") {
              checkAnswer(s"select id, name, age from $tableName where name = 'alice'")(Seq(1, "alice", 25))
            }
            // Adding a matching age predicate must not discard the partially indexed file.
            withClue("Combined age/name predicate with data skipping enabled: ") {
              checkAnswer(query)(Seq(1, "alice", 25))
            }
          }
        }
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
          withSQLConf("hoodie.bucket.index.query.pruning" -> "false") {
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
          }
        }
      }
    }
  }

  test("Test column stats data skipping across multiple files with range, IN and equality predicates") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true",
          "hoodie.metadata.index.column.stats.column.list" -> "id,price",
          // keep every commit in its own base file so column-stats pruning has multiple files to skip
          "hoodie.parquet.small.file.limit" -> "0"
        ) {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = '$tableType')
               | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
          // Three separate commits, each landing in its own base file with disjoint id / price ranges.
          spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000)")
          spark.sql(s"insert into $tableName values (11, 'b1', 110, 2000), (12, 'b2', 120, 2000)")
          spark.sql(s"insert into $tableName values (21, 'c1', 210, 3000), (22, 'c2', 220, 3000)")

          // Equality on the indexed key column -> only the first file qualifies.
          checkAnswer(s"select id, name, price from $tableName where id = 1")(
            Seq(1, "a1", 10.0)
          )
          // Range predicate that only the last file can satisfy.
          checkAnswer(s"select id, name, price from $tableName where id > 20")(
            Seq(21, "c1", 210.0),
            Seq(22, "c2", 220.0)
          )
          // IN predicate touching the first and last files, skipping the middle one.
          checkAnswer(s"select id, name, price from $tableName where id in (2, 22)")(
            Seq(2, "a2", 20.0),
            Seq(22, "c2", 220.0)
          )
          // Range on a second indexed column keeps only the middle file.
          checkAnswer(s"select id, name, price from $tableName where price >= 110 and price < 210")(
            Seq(11, "b1", 110.0),
            Seq(12, "b2", 120.0)
          )
          // Predicate that matches nothing -> all files pruned, empty result.
          checkAnswer(s"select id, name, price from $tableName where id = 999")()

          // Data skipping must not change results compared to a full scan.
          withSQLConf("hoodie.enable.data.skipping" -> "false") {
            checkAnswer(s"select id, name, price from $tableName where id in (2, 22)")(
              Seq(2, "a2", 20.0),
              Seq(22, "c2", 220.0)
            )
          }
        }
      }
    }
  }
}
