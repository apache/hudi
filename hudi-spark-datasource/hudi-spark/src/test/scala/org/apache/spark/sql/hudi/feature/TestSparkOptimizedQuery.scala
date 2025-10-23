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

import org.apache.spark.SparkConf
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

class TestSparkOptimizedQuery extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    initQueryIndexConf()
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      // set file relation cache size to 0 to mock the scenario that cache elimination happens
      .set("spark.sql.filesourceTableRelationCacheSize", "0")
  }

  test("Test ReusedExchange Appears When Query Hoodie Table With Same Filter Condition Multiple Times") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | (1, 'a11', map('color', 'red', 'size', 'S'), 20, 1000, '2021-01-05'),
             | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-05'),
             | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-06')
                  """.stripMargin)
        var frame = spark.sql(
          s"""
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-06'
             |        group by dt
             |union all
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-05'
             |        group by dt
             |""".stripMargin)
        // trigger the execution for AQE final plan generation
        frame.collect()
        assertFalse(frame.queryExecution.executedPlan.toString().contains("ReusedExchange"))
        checkAnswer(
          s"""
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-06'
             |        group by dt
             |union all
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-05'
             |        group by dt
             |""".stripMargin)(
          Seq("2021-01-05", 3),
          Seq("2021-01-05", 3),
          Seq("2021-01-06", 1)
        )

        frame = spark.sql(
          s"""
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2025-10-23'
             |        group by dt
             |union all
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2025-10-23'
             |        group by dt
             |""".stripMargin)
        frame.collect()
        assertTrue(frame.queryExecution.executedPlan.toString().contains("ReusedExchange"))
        checkAnswer(
          s"""
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-06'
             |        group by dt
             |union all
             |select  dt, count(1) as cnt
             |        from $tableName
             |        where dt <= '2021-01-06'
             |        group by dt
             |""".stripMargin)(
          Seq("2021-01-05", 3),
          Seq("2021-01-05", 3),
          Seq("2021-01-06", 1),
          Seq("2021-01-06", 1)
        )
      }
    }
  }
}
