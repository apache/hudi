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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceWriteOptions, QuickstartUtils}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

import scala.collection.JavaConverters._

class TestPartialUpdateAvroPayload extends HoodieClientTestBase {
  var spark: SparkSession = null

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @CsvSource(Array(
    "COPY_ON_WRITE,false",
    "MERGE_ON_READ,false",
    "COPY_ON_WRITE,true",
    "MERGE_ON_READ,true"
  ))
  def testPartialUpdatesAvroPayloadPrecombine(tableType: String, useFileGroupReader: Boolean): Unit = {
    val hoodieTableType = HoodieTableType.valueOf(tableType)
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(1))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("ts", lit(1L))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.PartialUpdateAvroPayload")
      .option(HoodieWriteConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.CUSTOM.name())
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val upsert1 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert1DF = spark.read.json(sparkSession.createDataset(upsert1)(Encoders.STRING)).withColumn("ts", lit(4L))
      .withColumn("rider", lit("test_rider"))
      .withColumn("driver", typedLit(null).cast(StringType))
      .withColumn("fare", typedLit(null).cast(DoubleType))

    val upsert2 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert2DF = spark.read.json(sparkSession.createDataset(upsert2)(Encoders.STRING)).withColumn("ts", lit(6L))
      .withColumn("rider", typedLit(null).cast(StringType))
      .withColumn("driver", lit("test_driver"))
      .withColumn("fare", typedLit(null).cast(DoubleType))

    val upsert3 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert3DF = spark.read.json(sparkSession.createDataset(upsert3)(Encoders.STRING)).withColumn("ts", lit(3L))
      .withColumn("rider", typedLit(null).cast(StringType))
      .withColumn("driver", typedLit(null).cast(StringType))
      .withColumn("fare", lit(123456789d))

    val mergedDF = upsert1DF.union(upsert2DF).union(upsert3DF)

    mergedDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Append)
      .save(basePath)

    val finalDF = spark.read.format("hudi")
      .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), String.valueOf(useFileGroupReader))
      .load(basePath)
    assertEquals(finalDF.select("rider").collectAsList().get(0).getString(0), upsert1DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(0).getString(0), upsert2DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("fare").collectAsList().get(0).getDouble(0), upsert3DF.select("fare").collectAsList().get(0).getDouble(0))
    assertEquals(finalDF.select("ts").collectAsList().get(0).getLong(0), upsert2DF.select("ts").collectAsList().get(0).getLong(0))
  }
}
