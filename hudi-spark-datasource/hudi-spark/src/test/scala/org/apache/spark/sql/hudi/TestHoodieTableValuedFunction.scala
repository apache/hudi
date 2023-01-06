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

import org.apache.hudi.HoodieSparkUtils

class TestHoodieTableValuedFunction extends HoodieSparkSqlTestBase {

  test(s"Test hudi_query Table-Valued Function") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               |)
               |location '${tmp.getCanonicalPath}/$tableName'
               |""".stripMargin
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
               | """.stripMargin
          )

          checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a3", 30.0, 1000)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
               | """.stripMargin
          )

          if (tableType == "cow") {
            checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
              Seq(1, "a1_1", 10.0, 1100),
              Seq(2, "a2_2", 20.0, 1100),
              Seq(3, "a3_3", 30.0, 1100)
            )
          } else {
            checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
              Seq(1, "a1", 10.0, 1000),
              Seq(2, "a2", 20.0, 1000),
              Seq(3, "a3", 30.0, 1000)
            )
          }
        }
      }
    }
  }
}
