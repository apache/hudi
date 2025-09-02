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

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieUpsertException
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestInsertDedupPolicy extends SparkClientFunctionalTestHarness {
  val firstInsertData: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (10, "3", "rider-C", "driver-C", 33.90, 10))
  val secondInsertData: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (11, "1", "rider-A", "driver-A", 1.1, 1),
    (11, "2", "rider-B", "driver-B", 2.2, 2),
    (11, "5", "rider-C", "driver-C", 3.3, 3))
  val expectedForDrop: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (10, "3", "rider-C", "driver-C", 33.90, 10),
    (11, "5", "rider-C", "driver-C", 3.3, 3))
  val expectedForNone: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (11, "1", "rider-A", "driver-A", 1.1, 1),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (11, "2", "rider-B", "driver-B", 2.2, 2),
    (10, "3", "rider-C", "driver-C", 33.90, 10),
    (11, "5", "rider-C", "driver-C", 3.3, 3))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testInsertLogic(tableType: String,
                      dedupPolicy: String): Unit = {
    val fgReaderOpts: Map[String, String] = Map(
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> "true",
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> "EVENT_TIME_ORDERING",
      DataSourceWriteOptions.OPERATION.key -> INSERT_OPERATION_OPT_VAL)
    val insertDedupPolicy: Map[String, String] = Map(INSERT_DUP_POLICY.key -> dedupPolicy)
    val opts = fgReaderOpts ++ insertDedupPolicy
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")

    // Write the first batch of data.
    val inserts = spark.createDataFrame(firstInsertData).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key, "key").
      option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
      option(TABLE_TYPE.key, tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key, "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key, "false").
      option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key, "true").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)

    val insertsWithDup = spark.createDataFrame(secondInsertData).toDF(columns: _*)
    if (dedupPolicy.equals(FAIL_INSERT_DUP_POLICY)) {
      // Write and check throws.
      Assertions.assertThrows(
        classOf[HoodieUpsertException],
        () => insertsWithDup.write.format("hudi").options(opts).mode(SaveMode.Append).save(basePath)
      )
    } else {
      // Write data.
      insertsWithDup.write.format("hudi").
        option(HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key, "true").
        option(HoodieMetadataConfig.SECONDARY_INDEX_NAME.key, "idx_rider").
        option(HoodieMetadataConfig.SECONDARY_INDEX_COLUMN.key, "rider").
        options(opts).
        mode(SaveMode.Append).
        save(basePath)
      // Validate the data.
      val df = spark.read.format("hudi").options(opts).load(basePath)
      val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expected = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop else expectedForNone
      val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedDf, finalDf)

      // Validate data with record key filter
      // RLI will be used with record key filter
      val recordKeyFilterDf = df.filter("key = '5'").select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expectedRecordKeyFilter = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop.filter(_._2 == "5") else expectedForNone.filter(_._2 == "5")
      val expectedRecordKeyFilterDf = spark.createDataFrame(expectedRecordKeyFilter).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedRecordKeyFilterDf, recordKeyFilterDf)

      // Validate data with secondary key filter
      // Secondary index will be used with secondary key filter
      val secondaryKeyFilterDf = df.filter("rider = 'rider-C'").select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expectedSecondaryKeyFilter = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop.filter(_._3 == "rider-C") else expectedForNone.filter(_._3 == "rider-C")
      val expectedSecondaryKeyFilterDf = spark.createDataFrame(expectedSecondaryKeyFilter).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedSecondaryKeyFilterDf, secondaryKeyFilterDf)
    }
  }
}

object TestInsertDedupPolicy {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("MERGE_ON_READ", NONE_INSERT_DUP_POLICY),
      Arguments.of("MERGE_ON_READ", DROP_INSERT_DUP_POLICY),
      Arguments.of("MERGE_ON_READ", FAIL_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", NONE_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", DROP_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", FAIL_INSERT_DUP_POLICY)
    )
  }

  def validate(expectedDf: Dataset[Row], actualDf: Dataset[Row]): Unit = {
    val expectedMinusActual = expectedDf.except(actualDf)
    val actualMinusExpected = actualDf.except(expectedDf)
    expectedDf.show(false)
    actualDf.show(false)
    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
  }
}


