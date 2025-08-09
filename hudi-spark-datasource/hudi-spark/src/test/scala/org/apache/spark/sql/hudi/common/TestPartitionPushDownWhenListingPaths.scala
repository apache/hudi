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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.config.HoodieWriteConfig

class TestPartitionPushDownWhenListingPaths extends HoodieSparkSqlTestBase {

  test("Test push down different partitions") {
    Seq("true", "false").foreach { enableMetadata =>
      withSQLConf(HoodieMetadataConfig.ENABLE.key -> enableMetadata) {
        Seq("cow", "mor").foreach { tableType =>
          withTempDir { tmp =>
            val tableName = generateTableName
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  ts long,
                 |  date_par date,
                 |  country string,
                 |  hour int,
                 |  longValue long
                 |) using hudi
                 | location '${tmp.getCanonicalPath}'
                 | tblproperties (
                 |  primaryKey ='id',
                 |  type = '$tableType',
                 |  orderingFields = 'ts',
                 |  hoodie.datasource.write.hive_style_partitioning = 'true',
                 |  hoodie.datasource.write.partitionpath.urlencode = 'true'
                 | )
                 | PARTITIONED BY (date_par, country, hour, longValue)""".stripMargin)

            withSQLConf(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
              spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, date '2023-02-27', 'ID', 1, 102345L)")
              spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, date '2023-02-28', 'US', 4, 102346L)")
              spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000, date '2023-03-01', 'CN', 10, 102347L)")
            }

            // Only filter one partition column
            checkAnswer(s"select id, name, price, ts from $tableName where date_par = date'2023-03-01' order by id")(
              Seq(3, "a3", 10.0, 1000)
            )

            // Filter with And operation
            checkAnswer(s"select id, name, price, ts from $tableName where date_par = date'2023-02-28' and hour = 4 order by id")(
              Seq(2, "a2", 10.0, 1000)
            )

            // Filter with Or operation
            checkAnswer(s"select id, name, price, ts from $tableName where date_par = date'2023-02-28' or country = 'CN' order by id")(
              Seq(2, "a2", 10.0, 1000),
              Seq(3, "a3", 10.0, 1000)
            )

            // Filter with GT
            checkAnswer(s"select id, name, price, ts from $tableName where date_par > date'2023-02-27' order by id")(
              Seq(2, "a2", 10.0, 1000),
              Seq(3, "a3", 10.0, 1000)
            )

            // Filter with LT
            checkAnswer(s"select id, name, price, ts from $tableName where longValue < 102346L order by id")(
              Seq(1, "a1", 10.0, 1000)
            )

            // Filter with EQ
            checkAnswer(s"select id, name, price, ts from $tableName where longValue = 102346L order by id")(
              Seq(2, "a2", 10.0, 1000)
            )

            // Filter with GT_EQ
            checkAnswer(s"select id, name, price, ts from $tableName where date_par >= date'2023-02-27' order by id")(
              Seq(1, "a1", 10.0, 1000),
              Seq(2, "a2", 10.0, 1000),
              Seq(3, "a3", 10.0, 1000)
            )

            // Filter with LT_EQ
            checkAnswer(s"select id, name, price, ts from $tableName where date_par <= date'2023-02-27' order by id")(
              Seq(1, "a1", 10.0, 1000)
            )

            // Filter with In operation
            checkAnswer(s"select id, name, price, ts from $tableName where country in ('CN', 'US') order by id")(
              Seq(2, "a2", 10.0, 1000),
              Seq(3, "a3", 10.0, 1000)
            )
          }
        }
      }
    }
  }
}
