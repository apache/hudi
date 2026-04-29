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

class TestQueryMergeOnReadOptimizedTable extends HoodieSparkSqlTestBase {
  test("Test Query Merge_On_Read Read_Optimized table") {
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
           |  ts long,
           |  partition long
           |) using hudi
           | partitioned by (partition)
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      withSQLConf("hoodie.parquet.max.file.size" -> "10000") {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 21 where id = 2")
        spark.sql(s"update $tableName set price = 31 where id = 3")
        spark.sql(s"update $tableName set price = 41 where id = 4")

        // expect that all complete parquet files can be scanned
        assertQueryResult(4, tablePath)

        // async schedule compaction job
        spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
          .collect()

        // expect that all complete parquet files can be scanned with a pending compaction job
        assertQueryResult(4, tablePath)

        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1000, 1000)")

        // expect that all complete parquet files can be scanned with a pending compaction job
        assertQueryResult(5, tablePath)

        // async run compaction job
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          .collect()

        // assert that all complete parquet files can be scanned after compaction
        assertQueryResult(5, tablePath)
      }
    }
  }

  def assertQueryResult(expected: Any,
                        tablePath: String): Unit = {
    val actual = spark.read.format("org.apache.hudi").option("hoodie.datasource.query.type", "read_optimized").load(tablePath).count()
    assertResult(expected)(actual)
  }
}
