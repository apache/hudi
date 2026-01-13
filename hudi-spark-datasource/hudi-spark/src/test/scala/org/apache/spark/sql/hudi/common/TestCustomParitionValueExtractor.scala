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

import org.apache.hudi.sync.common.HoodieSyncConfig

class TestCustomParitionValueExtractor extends HoodieSparkSqlTestBase {
  test("Test custom partition value extractor interface") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` string,
           |  `country` string,
           |  `state` string,
           |  `city` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.hive_style_partitioning'='false',
           |  '${HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key()}'='org.apache.spark.sql.hudi.common.TestCustomSlashPartitionValueExtractor'
           | )
           | partitioned by (`datestr`, `country`, `state`, `city`)
           | location '$tablePath'
        """.stripMargin)
      // yyyy/mm/dd
      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, '2024-01-01'  "USA", "CA", "SFO"),
           | (2, 'a2', 2000, '2024-01-01', "USA", "CA", "LA")
        """.stripMargin)
      val catalogTable = spark.sessionState.catalog.externalCatalog.getTable("default", targetTable)
      // catalogTable.storage.

      // check result after insert and merge data into target table
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq("2", "a2", 2000, "2024-01-01", "USA", "CA", "LA")
      )
    }
  }
}
