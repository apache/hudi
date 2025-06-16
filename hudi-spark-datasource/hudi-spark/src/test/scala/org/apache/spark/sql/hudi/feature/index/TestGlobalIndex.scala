/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.feature.index

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestGlobalIndex extends HoodieSparkSqlTestBase {

  test("Test Type Casting with Global Index for Primary Key and Partition Key insert overwrite") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.index.type" -> "GLOBAL_SIMPLE",
        "hoodie.simple.index.update.partition.path" -> "true") {
        val tableName = generateTableName

        // Create table with both primary key and partition key
        spark.sql(
          s"""
             |create table $tableName (
             |c1 int,
             |c2 int,
             |c3 string,
             |ts long
             |) using hudi
             |partitioned by (c2)
             |location '${tmp.getCanonicalPath}/$tableName'
             |tblproperties (
             |type = 'mor',
             |primaryKey = 'c1',
             |preCombineField = 'ts'
             |)
          """.stripMargin)

        // Initial insert with double values
        spark.sql(
          s"""
             |insert into $tableName
             |select cast(1.0 as double) as c1,
             |cast(1.0 as double) as c2,
             |'a' as c3,
             |1000 as ts
          """.stripMargin)

        // Verify initial insert
        checkAnswer(s"select c1, c2, c3 from $tableName")(
          Seq(1, 1, "a")
        )

        // Update partition value
        spark.sql(
          s"""
             |insert into $tableName
             |select cast(1.1 as double) as c1,
             |cast(2.2 as double) as c2,
             |'a' as c3,
             |1001 as ts
          """.stripMargin)

        // Verify partition key update
        checkAnswer(
          s"select c1, c2, c3 from $tableName")(
          Seq(1, 2, "a")
        )

        // Test Case 3: Insert overwrite with double values
        spark.sql(
          s"""
             |insert overwrite table $tableName
             |partition (c2)
             |select cast(2.3 as double) as c1,
             |cast(3.3 as double) as c2,
             |'a' as c3,
             |1003 as ts
          """.stripMargin)

        // Additional verification: check complete table state with sorting
        checkAnswer(s"select c1, c2, c3 from $tableName order by c1, c2")(Seq(2, 3, "a"))
      }
    })
  }
}
