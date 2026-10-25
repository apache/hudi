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

/**
 * Covers reading tables whose partition path does not line up one-to-one with the partition
 * columns, so that the partition values have to be recovered through
 * [[org.apache.hudi.HoodieSparkUtils#castStringToType]] rather than through Spark's partition
 * parser.
 */
class TestTypedPartitionValues extends HoodieSparkSqlTestBase {

  test("Test reading a date partition column laid out as yyyy/MM/dd") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  grass_date date
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  orderingFields = 'ts',
           |  'hoodie.datasource.write.slash.separated.date.partitioning' = 'true'
           | )
           | partitioned by (grass_date)
         """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 1000, date'2023-02-27')")
      spark.sql(s"insert into $tableName values(2, 'a2', 1000, date'2023-03-01')")

      // NOTE: The partition path holds three fragments for a single partition column, so the
      //       partition value is recovered through [[castStringToType]]. Handing it back as a
      //       string used to fail every read of the table with a [[ClassCastException]]
      checkAnswer(s"select id, name, grass_date, _hoodie_partition_path from $tableName order by id")(
        Seq(1, "a1", java.sql.Date.valueOf("2023-02-27"), "2023/02/27"),
        Seq(2, "a2", java.sql.Date.valueOf("2023-03-01"), "2023/03/01")
      )

      // Partition pruning evaluates the predicate against the recovered partition value
      checkAnswer(s"select id from $tableName where grass_date = date'2023-03-01'")(
        Seq(2)
      )
      checkAnswer(s"select id from $tableName where grass_date < date'2023-03-01'")(
        Seq(1)
      )
    }
  }
}
