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
package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.sql.{Date, Timestamp}

class TestGenericRecordAndRowConsistency extends HoodieSparkClientTestBase {

  var spark: SparkSession = _
  val commonOpts = Map(
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_type_consistency_tbl",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.TABLE_TYPE.key -> "COPY_ON_WRITE",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "str,eventTime",
    HoodieTableConfig.ORDERING_FIELDS.key() -> "typeId",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "typeId",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key -> "true"
  )

  /**
   * Setup method running before each test.
   */
  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_type_consistency_tbl")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
  }

  @Test
  def testTimestampTypeConsistency(): Unit = {
    val _spark = spark
    import _spark.implicits._

    val df = Seq(
      (1, Timestamp.valueOf("2014-01-01 23:00:01"), "abc"),
      (1, Timestamp.valueOf("2014-11-30 12:40:32"), "abc"),
      (2, Timestamp.valueOf("2016-12-29 09:54:00"), "def"),
      (2, Timestamp.valueOf("2016-05-09 10:12:43"), "def")
    ).toDF("typeId", "eventTime", "str")

    testConsistencyBetweenGenericRecordAndRow(df)
  }

  @Test
  def testDateTypeConsistency(): Unit = {
    val _spark = spark
    import _spark.implicits._

    val df = Seq(
      (1, Date.valueOf("2014-01-01"), "abc"),
      (1, Date.valueOf("2014-11-30"), "abc"),
      (2, Date.valueOf("2016-12-29"), "def"),
      (2, Date.valueOf("2016-05-09"), "def")
    ).toDF("typeId", "eventTime", "str")

    testConsistencyBetweenGenericRecordAndRow(df)
  }

  /**
   * Upserts and deletes that hit existing keys must order on the stored timestamp value
   * when the consistent logical timestamp key generation is on.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testUpsertWithTimestampOrderingField(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val opts = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_ts_ordering_tbl",
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
      DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key -> "true"
    )
    def ts(s: String): Timestamp = Timestamp.valueOf("2026-01-01 " + s)
    val columns = Seq("id", "ts", "part", "val", "_hoodie_is_deleted")

    Seq("key0", "key1", "key2", "key3", "key4", "key5")
      .map(id => (id, ts("10:00:00.000500"), "p0", "v1", false))
      .toDF(columns: _*)
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, "upsert")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save(basePath)

    Seq(
      ("key0", ts("10:00:00.000501"), "p0", "v2", false),
      ("key1", ts("10:00:00.000499"), "p0", "v2", false),
      ("key2", ts("10:00:01.000500"), "p0", "v2", false),
      ("key3", ts("09:59:59.000500"), "p0", "v2", false),
      ("key4", ts("09:59:59.000500"), "p0", "v2", true),
      ("key5", ts("10:00:01.000500"), "p0", "v2", true)
    ).toDF(columns: _*)
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, "upsert")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save(basePath)

    val actual = spark.read.format("hudi").load(basePath)
      .select("id", "val").as[(String, String)].collect().sorted.toSeq
    assertEquals(Seq(("key0", "v2"), ("key1", "v1"), ("key2", "v2"), ("key3", "v1"), ("key4", "v1")), actual)
  }

  private def testConsistencyBetweenGenericRecordAndRow(df: DataFrame): Unit = {
    val _spark = spark
    import _spark.implicits._

    // upsert operation generate recordKey by GenericRecord
    val tempRecordPath = basePath + "/record_tbl/"
    df.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, "upsert")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save(tempRecordPath)

    val data1 = spark.read.format("hudi")
      .load(tempRecordPath)
      .select("_hoodie_record_key")
      .map(_.toString()).collect().sorted

    // bulk_insert operation generate recordKey by Row
    val tempRowPath = basePath + "/row_tbl/"
    df.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, "bulk_insert")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save(tempRowPath)

    val data2 = spark.read.format("hudi")
      .load(tempRowPath)
      .select("_hoodie_record_key")
      .map(_.toString()).collect().sorted

    assertEquals(data1.toSeq, data2.toSeq)
  }
}
