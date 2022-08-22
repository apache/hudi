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

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.junit.jupiter.api.Assertions.assertEquals

class TestCDCForSparkSQL extends HoodieSparkSqlTestBase {

  def cdcDataFrame(basePath: String, startingTs: Long, endingTs: Option[Long] = None): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(INCREMENTAL_OUTPUT_FORMAT.key, INCREMENTAL_OUTPUT_FORMAT_CDC_VAL)
      .option(BEGIN_INSTANTTIME.key, startingTs.toString)
    endingTs.foreach { ts =>
      reader.option(END_INSTANTTIME.key, ts.toString)
    }
    reader.load(basePath)
  }

  def assertCDCOpCnt(cdcData: DataFrame, expectedInsertCnt: Long,
                     expectedUpdateCnt: Long, expectedDeletedCnt: Long): Unit = {
    assertEquals(expectedInsertCnt, cdcData.where("op = 'i'").count())
    assertEquals(expectedUpdateCnt, cdcData.where("op = 'u'").count())
    assertEquals(expectedDeletedCnt, cdcData.where("op = 'd'").count())
  }

    /**
     * Test CDC in cases that it's a COW/MOR non--partitioned table and `cdcSupplementalLogging` is true or not.
     */
    test("Test Non-Partitioned Hoodie Table") {
    val databaseName = "hudi_database"
    spark.sql(s"create database if not exists $databaseName")
    spark.sql(s"use $databaseName")

    Seq("cow", "mor").foreach { tableType =>
      Seq(true, false).foreach { cdcSupplementalLogging =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |   'primaryKey' = 'id',
               |   'preCombineField' = 'ts',
               |   'hoodie.table.cdc.enabled' = 'true',
               |   'hoodie.table.cdc.supplemental.logging' = '${cdcSupplementalLogging.toString}',
               |   type = '$tableType'
               | )
               | location '$basePath'
       """.stripMargin)

          val metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(spark.sessionState.newHadoopConf())
            .build()

          spark.sql(s"insert into $tableName values (1, 'a1', 11.0, 1000), (2, 'a2', 12.0, 1000), (3, 'a3', 13.0, 1000)")
          val commitTime1 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly1 = cdcDataFrame(basePath, commitTime1.toLong - 1)
          cdcDataOnly1.show(false)
          assertCDCOpCnt(cdcDataOnly1, 3, 0, 0)

          spark.sql(s"insert into $tableName values (1, 'a1_v2', 11.0, 1100)")
          val commitTime2 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly2 = cdcDataFrame(basePath, commitTime2.toLong - 1)
          cdcDataOnly2.show(false)
          assertCDCOpCnt(cdcDataOnly2, 0, 1, 0)

          // Check the details
          val originSchema = spark.read.format("hudi").load(basePath).schema
          val change2 = cdcDataOnly2.select(
            col("op"),
            from_json(col("before"), originSchema).as("before"),
            from_json(col("after"), originSchema).as("after")
          ).select(
            col("op"),
            col("after.id"),
            col("before.name"),
            col("before.price"),
            col("after.name"),
            col("after.price")
          ).collect()
          checkAnswer(change2)(Array("u", 1, "a1", 11.0, "a1_v2", 11.0))

          spark.sql(s"update $tableName set name = 'a2_v2', ts = 1200 where id = 2")
          val commitTime3 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly3 = cdcDataFrame(basePath, commitTime3.toLong - 1)
          cdcDataOnly3.show(false)
          assertCDCOpCnt(cdcDataOnly3, 0, 1, 0)

          spark.sql(s"delete from $tableName where id = 3")
          val commitTime4 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly4 = cdcDataFrame(basePath, commitTime4.toLong - 1)
          cdcDataOnly4.show(false)
          assertCDCOpCnt(cdcDataOnly4, 0, 0, 1)

          spark.sql(
            s"""
               | merge into $tableName
               | using (
               |  select * from (
               |  select 1 as id, 'a1_v3' as name, cast(11.0 as double) as price, cast(1300 as long) as ts
               |  union all
               |  select 4 as id, 'a4' as name, cast(14.0 as double) as price, cast(1300 as long) as ts
               |  )
               | ) s0
               | on s0.id = $tableName.id
               | when matched then update set id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
               | when not matched then insert *
       """.stripMargin)
          val commitTime5 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly5 = cdcDataFrame(basePath, commitTime5.toLong - 1)
          cdcDataOnly5.show(false)
          assertCDCOpCnt(cdcDataOnly5, 1, 1, 0)

          // Check the details
          val change5 = cdcDataOnly5.select(
            col("op"),
            from_json(col("before"), originSchema).as("before"),
            from_json(col("after"), originSchema).as("after")
          ).select(
            col("op"),
            col("after.id"),
            col("before.name"),
            col("before.price"),
            col("after.name"),
            col("after.price")
          ).collect()
          checkAnswer(change5.sortBy(_.getInt(1)))(
            Array("u", 1, "a1_v2", 11.0, "a1_v3", 11.0),
            Array("i", 4, null, null, "a4", 14.0)
          )

          val totalCdcData = cdcDataFrame(basePath, commitTime1.toLong - 1)
          assertCDCOpCnt(totalCdcData, 4, 3, 1)
        }
      }
    }
  }

  /**
   * Test CDC in cases that it's a COW/MOR partitioned table and `cdcSupplementalLogging` is true or not.
   */
  test("Test Partitioned Hoodie Table") {
    val databaseName = "hudi_database"
    spark.sql(s"create database if not exists $databaseName")
    spark.sql(s"use $databaseName")

    Seq("cow", "mor").foreach { tableType =>
      Seq(true, false).foreach { cdcSupplementalLogging =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  pt string
               | ) using hudi
               | partitioned by (pt)
               | tblproperties (
               |   'primaryKey' = 'id',
               |   'preCombineField' = 'ts',
               |   'hoodie.table.cdc.enabled' = 'true',
               |   'hoodie.table.cdc.supplemental.logging' = '${cdcSupplementalLogging.toString}',
               |   'type' = '$tableType'
               | )
               | location '$basePath'
       """.stripMargin)

          val metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(spark.sessionState.newHadoopConf())
            .build()

          spark.sql(
            s"""
               | insert into $tableName values (1, 'a1', 11.0, 1000, '2021'),
               | (2, 'a2', 12.0, 1000, '2022'), (3, 'a3', 13.0, 1000, '2022')
        """.stripMargin)
          val commitTime1 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly1 = cdcDataFrame(basePath, commitTime1.toLong - 1)
          cdcDataOnly1.show(false)
          assertCDCOpCnt(cdcDataOnly1, 3, 0, 0)

          spark.sql(s"insert overwrite table $tableName partition (pt = '2021') values (1, 'a1_v2', 11.0, 1100)")
          val commitTime2 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly2 = cdcDataFrame(basePath, commitTime2.toLong - 1)
          cdcDataOnly2.show(false)
          assertCDCOpCnt(cdcDataOnly2, 1, 0, 1)

          spark.sql(s"update $tableName set name = 'a2_v2', ts = 1200 where id = 2")
          val commitTime3 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly3 = cdcDataFrame(basePath, commitTime3.toLong - 1)
          cdcDataOnly3.show(false)
          assertCDCOpCnt(cdcDataOnly3, 0, 1, 0)

          spark.sql(
            s"""
               | merge into $tableName
               | using (
               |  select * from (
               |  select 1 as id, 'a1_v3' as name, cast(11.0 as double) as price, cast(1300 as long) as ts, "2021" as pt
               |  union all
               |  select 4 as id, 'a4' as name, cast(14.0 as double) as price, cast(1300 as long) as ts, "2022" as pt
               |  )
               | ) s0
               | on s0.id = $tableName.id
               | when matched then update set id = s0.id, name = s0.name, price = s0.price, ts = s0.ts, pt = s0.pt
               | when not matched then insert *
       """.stripMargin)
          val commitTime4 = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp
          val cdcDataOnly4 = cdcDataFrame(basePath, commitTime4.toLong - 1)
          cdcDataOnly4.show(false)
          assertCDCOpCnt(cdcDataOnly4, 1, 1, 0)

          val totalCdcData = cdcDataFrame(basePath, commitTime1.toLong - 1)
          assertCDCOpCnt(totalCdcData, 5, 2, 1)
        }

      }
    }
  }
}
