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
          "hoodie.parquet.small.file.limit" -> "0",
          "hoodie.file.group.reader.enabled" -> "false"
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
}
