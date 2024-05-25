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

package org.apache.hudi.functional

import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Tag

import scala.collection.JavaConverters._

@Tag("functional")
class TestPartitionStatsIndexWithSql extends HoodieSparkSqlTestBase {

  val sqlTempTable = "hudi_tbl"

  test("Test partition stats index following insert, merge into, update and delete") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Create table with date type partition
      spark.sql(
        s"""
           | create table $tableName using hudi
           | partitioned by (dt)
           | tblproperties(
           |    primaryKey = 'id',
           |    preCombineField = 'ts',
           |    'hoodie.metadata.index.partition.stats.enable' = 'true'
           | )
           | location '$tablePath'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, cast('2021-05-06' as date) as dt
         """.stripMargin
      )

      assertResult(WriteOperationType.BULK_INSERT) {
        HoodieSparkSqlTestBase.getLastCommitMetadata(spark, tablePath).getOperationType
      }
      checkAnswer(s"select id, name, price, ts, cast(dt as string) from $tableName")(
        Seq(1, "a1", 10, 1000, "2021-05-06")
      )

      val partitionValue = "2021-05-06"

      // Check the missing properties for spark sql
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      val properties = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
      assertResult("dt")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
      assertResult("ts")(properties(HoodieTableConfig.PRECOMBINE_FIELD.key))
      assertResult(tableName)(metaClient.getTableConfig.getTableName)

      // Test insert into
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, cast('$partitionValue' as date))")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 10, 1000, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 10, 1000, partitionValue)
      )
      // Test merge into
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (select 1 as id, 'a1' as name, 11 as price, 1001 as ts, '$partitionValue' as dt) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin)
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 11, 1001, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 10, 1000, partitionValue)
      )
      // Test update
      spark.sql(s"update $tableName set price = price + 1 where id = 2")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 11, 1001, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 11, 1000, partitionValue)
      )
      // Test delete
      spark.sql(s"delete from $tableName where id = 1")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("2", s"dt=$partitionValue", 2, "a2", 11, 1000, partitionValue)
      )
    }
  }
}
