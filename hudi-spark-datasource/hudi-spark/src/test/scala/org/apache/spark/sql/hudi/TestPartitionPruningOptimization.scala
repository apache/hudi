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

import org.apache.hudi.SparkAdapterSupport

class TestPartitionPruningOptimization extends HoodieSparkSqlTestBase with SparkAdapterSupport {

  test("Test PartitionPruning optimization successful") {
    withTempDir { tmp =>
      Seq("cow").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"


        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by(dt)
             | location '$tablePath'
           """.stripMargin)

        val tableName2 = generateTableName
        val tablePath2 = s"${tmp.getCanonicalPath}/$tableName2"

        spark.sql(
          s"""
             | create table $tableName2 (
             |  id int,
             |  name string,
             |  ts long,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by(dt)
             | location '$tablePath2'
           """.stripMargin)

        spark.sql(
          s"""
             |insert into $tableName values
             |    (1, 'a1', 10, 100),
             |    (2, 'a2', 20, 100),
             |    (3, 'a3', 30, 100),
             |    (4, 'a4', 40, 200),
             |    (5, 'a5', 50, 200),
             |    (6, 'a6', 60, 200)
          """.stripMargin)

        spark.sql(
          s"""
             |insert into $tableName2 values
             |    (1, 'a0', 10, 100),
             |    (2, 'a0', 20, 100),
             |    (3, 'a0', 30, 100),
             |    (4, 'a0', 40, 200),
             |    (5, 'a0', 50, 200),
             |    (6, 'a0', 60, 200)
          """.stripMargin)

        spark.sql(s"select * from $tableName2 where dt > 110").createOrReplaceTempView("tmpv")

        val joinDf = spark.sql(s"select * from $tableName a INNER JOIN tmpv b ON a.id == b.id and a.dt == b.dt")
        joinDf.explain(true)
        joinDf.show(true)
        println("here")
      }
    }
  }

}
