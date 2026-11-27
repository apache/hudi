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
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType, PartialUpdateAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableVersion}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, typedLit}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}

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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testPartialUpdatesAvroPayloadPrecombine(hoodieTableType: HoodieTableType): Unit = {
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

    val finalDF = spark.read.format("hudi").load(basePath)
    assertEquals(finalDF.select("rider").collectAsList().get(0).getString(0), upsert1DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(0).getString(0), upsert2DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("fare").collectAsList().get(0).getDouble(0), upsert3DF.select("fare").collectAsList().get(0).getDouble(0))
    assertEquals(finalDF.select("ts").collectAsList().get(0).getLong(0), upsert2DF.select("ts").collectAsList().get(0).getLong(0))
  }

  /**
   * One batch carries an older soft delete and a newer live copy of a new key: the live copy must be written
   * exactly as it is when the delete and the live copy land in separate commits.
   * With small files the key is routed into an existing base file through the merge handle, else the create handle.
   */
  @ParameterizedTest
  @CsvSource(Array("6,true", "6,false", "8,true", "8,false"))
  def testNewerLiveRecordAfterOlderDeleteInSameBatch(tableVersion: Int, useSmallFiles: Boolean): Unit = {
    val schema = StructType(Seq(
      StructField("key", StringType, nullable = false),
      StructField("partition", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField(HoodieRecord.HOODIE_IS_DELETED_FIELD, BooleanType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("note", StringType, nullable = true)))
    val writeOptions = Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> classOf[PartialUpdateAvroPayload].getName,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> RecordMergeMode.CUSTOM.name(),
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion.toString,
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false",
      HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.SIMPLE.name(),
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> (if (useSmallFiles) HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.defaultValue() else "0"),
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test")
    val existingRow = Row("key1", "p1", 1L, null, "v1", "n1")
    // the live copy leaves note null, so any value filled from the deleted copy shows up
    val olderDeleteRow = Row("key2", "p1", 1L, true, "deleted", "deleted-note")
    val newerLiveRow = Row("key2", "p1", 2L, null, "live", null)

    def write(path: String, rows: Seq[Row], operation: String, mode: SaveMode): Unit = {
      spark.createDataFrame(rows.asJava, schema)
        .write.format("hudi")
        .options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .mode(mode)
        .save(path)
    }

    def readRows(path: String): Seq[Row] = {
      assertEquals(HoodieTableVersion.fromVersionCode(tableVersion), createMetaClient(spark, path).getTableConfig.getTableVersion)
      spark.read.format("hudi").load(path)
        .select(schema.fieldNames.map(col): _*)
        .orderBy("key")
        .collect()
        .toSeq
    }

    write(basePath, Seq(existingRow), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    write(basePath, Seq(olderDeleteRow, newerLiveRow), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    val separateCommitsPath = tempDir.resolve("separate_commits").toString
    write(separateCommitsPath, Seq(existingRow), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    write(separateCommitsPath, Seq(olderDeleteRow), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    write(separateCommitsPath, Seq(newerLiveRow), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    val expected = Seq(existingRow, newerLiveRow)
    assertEquals(expected, readRows(separateCommitsPath))
    assertEquals(expected, readRows(basePath))
  }
}
