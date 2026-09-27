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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.HoodieSparkUtils

class TestVectorizedReadWithSchemaEvolution extends HoodieSparkSqlTestBase {
  Seq("cow", "mor").foreach { tableType =>
    test(s"Test vectorized read for $tableType table") {
      if (HoodieSparkUtils.isSpark3) {
        withSQLConf(
          "hoodie.schema.on.read.enable" -> "true",
          "spark.sql.parquet.enableVectorizedReader" -> "true",
          "spark.sql.codegen.maxFields" -> "1",
          "hoodie.parquet.small.file.limit" -> "0"
        ) {
          withTempDir { tmp =>
            val tableName = generateTableName
            val tablePath = s"${tmp.getCanonicalPath}/$tableName"
            // create table
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 | partitioned by (ts)
                 | location '$tablePath'
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id',
                 |  orderingFields = 'ts'
                 | )
         """.stripMargin)
            // insert data to table
            spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

            // alter table change data type of column 'id'
            spark.sql(s"alter table $tableName alter column price type string")

            // insert new records
            spark.sql(s"insert into $tableName values(2, 'a2', '20', 2000)")

            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", "10.0", 1000),
              Seq(2, "a2", "20", 2000)
            )
          }
        }
      }
    }
  }

  /**
   * A scan over spark.sql.codegen.maxFields leaves returns rows. Files written before a
   * schema-on-read type change must read the same with the vectorized reader on and off: the
   * vectorized reader's own conversions skip some old types (binary) and write an overflowing
   * decimal as its unscaled value, where Cast returns null.
   */
  Seq("cow", "mor").foreach { tableType =>
    test(s"Test wide read after type changes matches with the vectorized reader on and off for $tableType table") {
      withSQLConf(
        "hoodie.schema.on.read.enable" -> "true",
        "spark.sql.parquet.enableVectorizedReader" -> "true",
        "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true",
        "spark.sql.codegen.maxFields" -> "1",
        "hoodie.parquet.small.file.limit" -> "0",
        // Cast returns null on overflow instead of failing (ANSI is on by default on Spark 4).
        "spark.sql.ansi.enabled" -> "false"
      ) {
        withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  i2l int,
               |  i2d int,
               |  f2d float,
               |  i2s int,
               |  d2s date,
               |  b2s binary,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'ts'
               | )
       """.stripMargin)
          // One insert per row keeps each row in its own file group, so the update below leaves the
          // file of id 1 in the old schema.
          spark.sql(s"insert into $tableName values (1, 1, 12345, cast(1.1 as float), 7, date'2020-01-01', X'616263', 1000)")
          spark.sql(s"insert into $tableName values (2, 2, 12, cast(2.5 as float), 8, date'2020-02-02', X'646566', 1000)")

          Seq("i2l long", "i2d decimal(4,2)", "f2d double", "i2s string", "d2s string", "b2s string")
            .foreach(change => spark.sql(s"alter table $tableName alter column ${change.replace(" ", " type ")}"))

          spark.sql(s"insert into $tableName values (3, 3, 12.34, 3.3, '9', '2021-03-03', 'xyz', 2000)")
          spark.sql(s"update $tableName set i2s = '99' where id = 2")

          def read(vectorized: String): Seq[String] =
            withSQLConf("spark.sql.parquet.enableVectorizedReader" -> vectorized) {
              spark.sql(s"select id, i2l, i2d, f2d, i2s, d2s, b2s from $tableName order by id")
                .collect().map(_.toString).toSeq
            }

          val rowBased = read("false")
          assertResult(3)(rowBased.size)
          assertResult(rowBased)(read("true"))
        }
      }
    }
  }
}
