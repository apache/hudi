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

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.sql.{Date, Timestamp}

class TestGenericRecordAndRowConsistency extends HoodieClientTestBase {

  var spark: SparkSession = _
  val commonOpts = Map(
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_type_consistency_tbl",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.TABLE_TYPE.key -> "COPY_ON_WRITE",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "str,eventTime",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "typeId",
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

    assert(data1 sameElements data2)
  }
}
