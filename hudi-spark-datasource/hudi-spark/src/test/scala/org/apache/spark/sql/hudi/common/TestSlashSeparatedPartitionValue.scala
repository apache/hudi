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

class TestSlashSeparatedPartitionValue extends HoodieSparkSqlTestBase {

  test("Test slash separated date partitions") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` STRING
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.slash.separated.date.partitionpath'='true'
           | )
           | partitioned by (`datestr`)
           | location '$tablePath'
        """.stripMargin)

      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, "2026-01-05"),
           | (2, 'a2', 2000, "2026-01-06")
        """.stripMargin)

      // check result after insert and merge data into target table
      checkAnswer(s"select id, name, ts, datestr from $targetTable limit 10")(
        Seq("1", "a1", 1000, "2026-01-05"),
        Seq("2", "a2", 2000, "2026-01-06")
      )
    }
  }
}
