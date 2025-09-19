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

package org.apache.hudi.functional

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers, QuickstartUtils, ScalaAssertionSupport}
import org.apache.hudi.DataSourceWriteOptions.{INLINE_CLUSTERING_ENABLE, KEYGENERATOR_CLASS_NAME}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.avro.AvroSchemaCompatibility.SchemaIncompatibilityType
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TIMEZONE_FORMAT, TIMESTAMP_TYPE_FIELD}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineUtils}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.common.testutils.HoodieTestUtils.{INSTANT_FILE_NAME_GENERATOR, INSTANT_GENERATOR}
import org.apache.hudi.common.util.{ClusteringUtils, Option}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.metrics.HoodieMetricsConfig
import org.apache.hudi.exception.{HoodieException, SchemaBackwardsCompatibilityException}
import org.apache.hudi.exception.ExceptionUtil.getRootCause
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator, GlobalDeleteKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.metrics.{Metrics, MetricsReporterType}
import org.apache.hudi.storage.{StoragePath, StoragePathFilter}
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Encoders, Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.functions.{col, concat, lit, udf, when}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{ArrayType, DataTypes, DateType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, EnumSource, MethodSource, ValueSource}

import java.sql.{Date, Timestamp}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Basic tests on the spark datasource for COW table.
 */
class TestCOWDataSource extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = null

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

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

  @Test
  def testShortNameStorage(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))
  }

  @ParameterizedTest
  @MethodSource(Array("tableVersionCreationTestCases"))
  def testTableVersionDuringTableCreation(autoUpgrade: String, targetTableVersion: String): Unit = {
    val writeOptions = scala.collection.mutable.Map(
      HoodieWriteConfig.TBL_NAME.key -> "testTableCreation",
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> autoUpgrade)
    if (!targetTableVersion.equals("null")) {
      writeOptions += (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> targetTableVersion)
    }
    val dataGen: HoodieTestDataGenerator = new HoodieTestDataGenerator(System.currentTimeMillis())
    val records = recordsToStrings(dataGen.generateInserts("001", 5))
    val inputDF: Dataset[Row] = spark.read.json(jsc.parallelize(records, 2))
    // Create table, and validate.
    val failSet = Set("1", "2", "3", "4", "5", "7")
    if (failSet.contains(targetTableVersion)) {
      val exception: IllegalArgumentException =
        assertThrows(classOf[IllegalArgumentException])(
          inputDF.write.format("hudi").partitionBy("partition")
            .options(writeOptions).mode(SaveMode.Overwrite).save(basePath))
      assertTrue(exception.getMessage.contains(
        "The value of hoodie.write.table.version should be one of 6,8,9"))
    } else {
      inputDF.write.format("hudi").partitionBy("partition")
        .options(writeOptions).mode(SaveMode.Overwrite).save(basePath)
      metaClient = HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
      // If no write version is specified, use current.
      if (!targetTableVersion.equals("null")) {
        assertEquals(
          HoodieTableVersion.fromVersionCode(Integer.valueOf(targetTableVersion)),
          metaClient.getTableConfig.getTableVersion)
      } else {
        // Otherwise, the table version is the target table version.
        assertEquals(HoodieTableVersion.current, metaClient.getTableConfig.getTableVersion)
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testNoPrecombine(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts
    inputDF.write.format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
  }

  @Test
  def testMultipleOrderingFields() {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // Insert Operation
    var records = recordsToStrings(dataGen.generateInserts("003", 100)).asScala.toList
    var inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))

    val commonOptsWithMultipleOrderingFields = writeOpts ++ Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "timestamp,rider",
      DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.EVENT_TIME_ORDERING.name(),
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    )
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    records = recordsToStrings(dataGen.generateUniqueUpdates("002", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(0, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-002'").count())

    records = recordsToStrings(dataGen.generateUniqueUpdates("004", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-004'").count())

    records = recordsToStrings(dataGen.generateUniqueUpdates("001", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(20))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-001'").count())
  }

  @Test
  def testInferPartitionBy(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, Map())
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts

    inputDF.write.partitionBy("partition").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot0 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    snapshot0.cache()
    assertEquals(100, snapshot0.count())

    // triggering 2nd batch to ensure table config validation does not fail.
    inputDF.write.partitionBy("partition").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // verify partition cols
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "'").count() > 0)
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH + "'").count() > 0)
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH + "'").count() > 0)
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    assertTrue(storage.exists(new StoragePath(basePath + "/" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)))
    assertTrue(storage.exists(new StoragePath(basePath + "/" + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)))
    assertTrue(storage.exists(new StoragePath(basePath + "/" + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)))

    // try w/ multi field partition paths
    // generate two batches of df w/ diff partition path values.
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val records2 = recordsToStrings(dataGen.generateInserts("000", 200)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    // hard code the value for rider and fare so that we can verify the partition paths with hudi
    val toInsertDf = inputDF1.withColumn("fare", lit(100)).withColumn("rider", lit("rider-123"))
      .union(inputDF2.withColumn("fare", lit(200)).withColumn("rider", lit("rider-456")))

    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    snapshot1.cache()
    assertEquals(300, snapshot1.count())

    metaClient = createMetaClient(spark, basePath)
    var partitionPaths = FSUtils.getAllPartitionPaths(
      new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().build())
    assertTrue(partitionPaths.contains("100/rider-123"))
    assertTrue(partitionPaths.contains("200/rider-456"))

    // verify partition cols
    assertEquals(snapshot1.filter("_hoodie_partition_path = '100/rider-123'").count(), 100)
    assertEquals(snapshot1.filter("_hoodie_partition_path = '200/rider-456'").count(), 200)

    // triggering 2nd batch to ensure table config validation does not fail.
    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // incase of non partitioned dataset, inference should not happen.
    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    partitionPaths = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().build())
    assertEquals(partitionPaths.size(), 1)
    assertEquals(partitionPaths.get(0), "")
  }

  @Test
  def testReuseTableConfigs() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts
    // this write should succeed even w/o setting any param for record key, partition path since table config will be re-used.
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  @Test
  def testSimpleKeyGenDroppingConfigs() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    )
    // this write should succeed even w/o though we don't set key gen explicitly.
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  @Test
  def testSimpleKeyGenExtraneuousAddition() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1"
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    )
    // this write should succeed even w/o though we set key gen explicitly, its the default
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  private def writeToHudi(opts: Map[String, String], df: Dataset[Row]): Unit = {
    df.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  @ParameterizedTest
  @CsvSource(Array("hoodie.datasource.write.recordkey.field,begin_lat", "hoodie.datasource.write.partitionpath.field,end_lon",
    "hoodie.datasource.write.keygenerator.class,org.apache.hudi.keygen.NonpartitionedKeyGenerator", "hoodie.table.ordering.fields,fare"))
  def testAlteringRecordKeyConfig(configKey: String, configValue: String) {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "timestamp",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts
    writeToHudi(commonOptsNoPreCombine, inputDF)

    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsForBatch2 = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false",
      configKey -> configValue
    )

    // this write should fail since we are setting a config explicitly which wasn't set in first commit and does not match the default value.
    val t = assertThrows(classOf[Throwable]) {
      writeToHudi(optsForBatch2, inputDF)
    }
    assertTrue(getRootCause(t).getMessage.contains("Config conflict"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedNonBooleanField(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val df = inputDF.withColumn(HoodieRecord.HOODIE_IS_DELETED_FIELD, lit("abc"))

    // Should have failed since _hoodie_is_deleted is not a BOOLEAN data type
    assertThrows(classOf[HoodieException]) {
      df.write.format("hudi")
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save(basePath)
    }
  }

  @Test
  def testInsertOverWriteTableWithInsertDropDupes(): Unit = {

    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.withColumn("batchId", lit("batch1")).write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(10, snapshotDF1.count())

    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("101", 4)).asScala.toList
    val records2 = recordsToStrings(dataGen.generateInserts("101", 4)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 1))
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 1))
    val inputDF4 = inputDF2.withColumn("batchId", lit("batch2"))
      .union(inputDF3.withColumn("batchId", lit("batch3")))

    inputDF4.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS.key(), "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(snapshotDF2.count(), 8)
  }

  @Test
  def testInsertOverWritePartitionWithInsertDropDupes(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)
    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.withColumn("batchId", lit("batch1")).write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val validRecordsFromBatch1 = inputDF1.where("partition!='2016/03/15'").count()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(100, snapshotDF1.count())

    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("100", 50)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 1))
    val inputDF4 = inputDF3.withColumn("batchId", lit("batch2")).where("partition='2016/03/15'")
    inputDF4.cache()
    val validRecordsFromBatch2 = inputDF4.count()

    inputDF4.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS.key(), "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(snapshotDF2.count(), (validRecordsFromBatch1 + validRecordsFromBatch2))
  }

  @Test
  def bulkInsertCompositeKeys(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val inputDf1 = inputDF.withColumn("new_col",lit("value1"))
    val inputDf2 = inputDF.withColumn("new_col", lit(null).cast("String") )

    inputDf1.union(inputDf2).write.format("hudi")
        .options(writeOpts)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "_row_key,new_col")
        .option(DataSourceWriteOptions.OPERATION.key(),"bulk_insert")
        .mode(SaveMode.Overwrite)
        .save(basePath)

    assertEquals(200, spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count())
  }

  /**
   * This tests the case that query by with a specified partition condition on hudi table which is
   * different between the value of the partition field and the actual partition path,
   * like hudi table written by TimestampBasedKeyGenerator.
   *
   * For COW table, test the snapshot query mode and incremental query mode.
   */
  @ParameterizedTest
  @CsvSource(Array("true,AVRO", "true,SPARK", "false,AVRO", "false,SPARK"))
  def testPrunePartitionForTimestampBasedKeyGenerator(enableFileIndex: Boolean,
                                                      recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val options = CommonOptionUtils.commonOpts ++ Map(
      "hoodie.compact.inline" -> "false",
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      TIMESTAMP_TYPE_FIELD.key -> "DATE_STRING",
      TIMESTAMP_OUTPUT_DATE_FORMAT.key -> "yyyy/MM/dd",
      TIMESTAMP_TIMEZONE_FORMAT.key -> "GMT+8:00",
      TIMESTAMP_INPUT_DATE_FORMAT.key -> "yyyy-MM-dd"
    ) ++ writeOpts

    val dataGen1 = new HoodieTestDataGenerator(Array("2022-01-01"))
    val records1 = recordsToStrings(dataGen1.generateInserts("001", 20)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = createMetaClient(spark, basePath)

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 30)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2CompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

    // snapshot query
    val pathForReader = getPathForReader(basePath, !enableFileIndex, 3)
    val snapshotQueryRes = spark.read.format("hudi").options(readOpts).load(pathForReader)
    assertEquals(snapshotQueryRes.where("partition = '2022-01-01'").count, 20)
    assertEquals(snapshotQueryRes.where("partition = '2022-01-02'").count, 30)

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit2CompletionTime)
      .option(DataSourceReadOptions.END_COMMIT.key, commit2CompletionTime)
      .load(basePath)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-01'").count, 0)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-02'").count, 30)
  }

  /**
   * Test for https://issues.apache.org/jira/browse/HUDI-1615. Null Schema in BulkInsert row writer flow.
   * This was reported by customer when archival kicks in as the schema in commit metadata is not set for bulk_insert
   * row writer flow.
   * In this test, we trigger a round of bulk_inserts and set archive related configs to be minimal. So, after 6 rounds,
   * archival should kick in and 2 commits should be archived. If schema is valid, no exception will be thrown. If not,
   * NPE will be thrown.
   */
  @Test
  def testArchivalWithBulkInsert(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    var structType: StructType = null
    for (i <- 1 to 7) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(writeOpts)
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "5")
        .option("hoodie.clean.commits.retained", "0")
        .option("hoodie.datasource.write.row.writer.enable", "true")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    val tableMetaClient = createMetaClient(spark, basePath)
    assertFalse(tableMetaClient.getArchivedTimeline.empty())

    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchema(false)
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(CommonOptionUtils.commonOpts(HoodieWriteConfig.TBL_NAME.key))
    spark.sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, structName, nameSpace)
    assertTrue(actualSchema != null)
    assertEquals(schema, actualSchema)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testCopyOnWriteDeletes(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(100, snapshotDF1.count())

    val records2 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(snapshotDF2.count(), 80)
  }

  @Test
  def testCopyOnWriteUpserts(): Unit = {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(100, snapshotDF1.count())

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("001", 20)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option("hoodie.write.merge.handle.class", "org.apache.hudi.io.FileGroupReaderBasedMergeHandle")
      .mode(SaveMode.Append)
      .save(basePath)
    val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val timeline = metaClient.reloadActiveTimeline()
    val firstCommit = timeline.getCommitTimeline.getInstants.get(timeline.countInstants() - 2)
    val secondCommit = timeline.getCommitTimeline.getInstants.get(timeline.countInstants() - 1)
    val commitMetadata = timeline.readCommitMetadata(secondCommit)
    DataSourceTestUtils.validateCommitMetadata(commitMetadata, firstCommit.requestedTime(), 100, 20, 0, 0)

    val snapshotDF2 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
      .drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)
    snapshotDF2.cache()
    assertEquals(snapshotDF2.count(), 100)

    assertEquals(0, inputDF2.except(snapshotDF2).count())

    val updates3 = recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toList
    val inserts3 = recordsToStrings(dataGen.generateInserts("002", 5)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(updates3, 1)
      .union(spark.sparkContext.parallelize(inserts3, 1)))
    inputDF3.cache()

    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF3 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
      .drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)
    snapshotDF3.cache()
    assertEquals(snapshotDF3.count(), 95)
    assertEquals(inputDF3.count(), inputDF3.except(snapshotDF3).count()) // none of the deleted records should be part of snapshot read
    val counts = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
      .groupBy("_hoodie_commit_time").count().orderBy("_hoodie_commit_time").collect()
    // validate the commit time metadata is not updated for the update operation
    assertEquals(2, counts.length)
    assertEquals(firstCommit.requestedTime(), counts.apply(0).getAs[String](0))
    assertEquals(secondCommit.requestedTime(), counts.apply(1).getAs[String](0))
    assertTrue(counts.apply(0).getAs[Long](1) > counts.apply(1).getAs[Long](1))
  }

  /**
   * Test retries on conflict failures.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(0, 2))
  def testCopyOnWriteConcurrentUpdates(numRetries: Integer): Unit = {
    initTestDataGenerator()
    val records1 = recordsToStrings(dataGen.generateInserts("000", 1000)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(CommonOptionUtils.commonOpts)
      .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
      .option("hoodie.clean.failed.writes.policy", "LAZY")
      .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF1 = spark.read.format("org.apache.hudi").load(basePath)
    assertEquals(1000, snapshotDF1.count())

    val countDownLatch = new CountDownLatch(2)
    val sharedUpdates = recordsToStrings(dataGen.generateUpdatesForAllRecords("300")).asScala.toList

    for (x <- 1 to 2) {
      val thread = new Thread(new UpdateThread(
        dataGen, spark, CommonOptionUtils.commonOpts, basePath, x + "00", countDownLatch, numRetries, sharedUpdates))
      thread.setName(x + "00_THREAD")
      thread.start()
    }
    countDownLatch.await(1, TimeUnit.MINUTES)

    val snapshotDF2 = spark.read.format("org.apache.hudi").load(basePath)
    if (numRetries > 0) {
      assertEquals(snapshotDF2.count(), 3000)
      assertEquals(HoodieDataSourceHelpers.listCommitsSince(storage, basePath, "000").size(), 3)
    } else {
      // only one among two threads will succeed and hence 2000
      assertEquals(snapshotDF2.count(), 2000)
      assertEquals(HoodieDataSourceHelpers.listCommitsSince(storage, basePath, "000").size(), 2)
    }
  }

  class UpdateThread(dataGen: HoodieTestDataGenerator,
                     spark: SparkSession,
                     commonOpts: Map[String, String],
                     basePath: String,
                     instantTime: String,
                     countDownLatch: CountDownLatch,
                     numRetries: Integer = 0,
                     sharedUpdates: List[String]) extends Runnable {
    override def run() {
      val insertRecs = recordsToStrings(dataGen.generateInserts(instantTime, 1000)).asScala.toList
      val updateDf = spark.read.json(spark.sparkContext.parallelize(sharedUpdates, 2))
      val insertDf = spark.read.json(spark.sparkContext.parallelize(insertRecs, 2))
      try {
        updateDf.union(insertDf).write.format("org.apache.hudi")
          .options(commonOpts)
          .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
          .option("hoodie.clean.failed.writes.policy", "LAZY")
          .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
          .option(HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key(), numRetries.toString)
          .mode(SaveMode.Append)
          .save(basePath)
      } finally {
        countDownLatch.countDown()
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings =  Array(
    "_row_key,non_existent_field|Record key field 'non_existent_field' does not exist in the input record",
    "non_existent_field|recordKey value: \"null\" for field: \"non_existent_field\" cannot be null or empty.",
    "_row_key,tip_history.non_existent_field|Record key field 'tip_history.non_existent_field' does not exist in the input record",
    "tip_history.non_existent_field|recordKey value: \"null\" for field: \"tip_history.non_existent_field\" cannot be null or empty."))
  def testMissingRecordkeyField(args: String): Unit = {
    val splits = args.split('|')
    val recordKeyFields = splits(0)
    val errorMessage = splits(1)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    try {
      inputDF1.write.format("hudi")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), recordKeyFields)
        .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
        .mode(SaveMode.Overwrite)
        .save(basePath)
      fail("should fail when the specified record key field does not exist")
    } catch {
      case e: Exception => assertTrue(containsErrorMessage(e, errorMessage))
    }
  }

  @tailrec
  private def containsErrorMessage(e: Throwable, message: String): Boolean = {
    if (e != null) {
      if (e.getMessage.contains(message)) {
        true
      } else {
        containsErrorMessage(e.getCause, message)
      }
    } else {
      false
    }
  }

  @Test
  def testOverWriteModeUseReplaceAction(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadPathsOnCopyOnWriteTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = dataGen.generateInsertsContainsAllPartitions("001", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val metaClient = createMetaClient(spark, basePath)

    val instantTime = metaClient.getActiveTimeline.filterCompletedInstants().getInstantsAsStream.findFirst().get().requestedTime

    val record1FilePaths = storage.listDirectEntries(new StoragePath(basePath, dataGen.getPartitionPaths.head))
      .asScala
      .filter(!_.getPath.getName.contains("hoodie_partition_metadata"))
      .filter(_.getPath.getName.endsWith("parquet"))
      .map(_.getPath.toString)
      .mkString(",")

    val records2 = dataGen.generateInsertsContainsAllPartitions("002", 20)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val inputDF3 = spark.read.options(readOpts).json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      // Use bulk insert here to make sure the files have different file groups.
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiReadPathDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), instantTime)
      .option(DataSourceReadOptions.READ_PATHS.key, record1FilePaths)
      .load()

    val expectedCount = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)
    assertEquals(expectedCount, hudiReadPathDF.count())
  }

  @Test
  def testOverWriteTableModeUseReplaceAction(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @Test
  def testOverWriteModeUseReplaceActionOnDisJointPartitions(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 records to hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step3: Write 6 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH using INSERT_OVERWRITE_OPERATION_OPT_VAL
    val records3 = recordsToStrings(dataGen.generateInsertsForPartition("001", 6, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step4: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForPartition1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("6", recordCountForPartition1(0).get(0).toString)

    // step5: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForPartition2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForPartition2(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(3, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("commit", commits(1))
    assertEquals("replacecommit", commits(2))
  }

  @Test
  def testOverWriteTableModeUseReplaceActionOnDisJointPartitions(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 more records using SaveMode.Overwrite for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step3: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForPartition1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("0", recordCountForPartition1(0).get(0).toString)

    // step4: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForPartition2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForPartition2(0).get(0).toString)

    // step5: Query the rows count from hoodie table
    val recordCount = spark.sql(String.format("select count(*) from tmpTable")).collect()
    assertEquals("7", recordCount(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testDropInsertDup(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords = dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2Time = "002"
    val inserts2New = dataGen.generateSameKeyInserts(inserts2Time, allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts(inserts2Time, inserts1.subList(0, insert2DupKeyCnt))

    val records1 = recordsToStrings(inserts1).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val inserts2 = new java.util.ArrayList[HoodieRecord[_]]
    inserts2.addAll(inserts2Dup)
    inserts2.addAll(inserts2New)
    val records2 = recordsToStrings(inserts2).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val commitCompletionTime2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime2)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testComplexDataTypeWriteAndReadConsistency(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val schema = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)
      :: StructField("timeStampValue", TimestampType, true) :: StructField("dateValue", DateType, true)
      :: StructField("decimalValue", DataTypes.createDecimalType(15, 10), true) :: StructField("timestamp", IntegerType, true)
      :: StructField("partition", IntegerType, true) :: Nil)

    val records = Seq(Row("11", "Andy", Timestamp.valueOf("1970-01-01 13:31:24"), Date.valueOf("1991-11-07"), BigDecimal.valueOf(1.0), 11, 1),
      Row("22", "lisi", Timestamp.valueOf("1970-01-02 13:31:24"), Date.valueOf("1991-11-08"), BigDecimal.valueOf(2.0), 11, 1),
      Row("33", "zhangsan", Timestamp.valueOf("1970-01-03 13:31:24"), Date.valueOf("1991-11-09"), BigDecimal.valueOf(3.0), 11, 1))
    val rdd = jsc.parallelize(records)
    val recordsDF = spark.createDataFrame(rdd, schema)
    recordsDF.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    recordsReadDF.printSchema()
    recordsReadDF.schema.foreach(f => {
      f.name match {
        case "timeStampValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.TimestampType)
        case "dateValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DateType)
        case "decimalValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DecimalType(15, 10))
        case _ =>
      }
    })
  }

  private def getDataFrameWriter(keyGenerator: String, opts: Map[String, String]): DataFrameWriter[Row] = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val writer = inputDF.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenerator)
      .mode(SaveMode.Overwrite)
    if (classOf[ComplexKeyGenerator].getCanonicalName.equals(keyGenerator)) {
      // Disable complex key generator validation so that the writer can succeed
      writer.option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
    } else {
      writer
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithCustomKeyGeneratorWithGlobbing(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)

    // Without fieldType, the default is SIMPLE
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= col("current_ts").cast("string")).count())

    // Specify fieldType as TIMESTAMP
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count())

    // Mixed fieldType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)

    // Test invalid partitionKeyType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer = writer.partitionBy("current_ts:DUMMY")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
    try {
      writer.save(basePath)
      fail("should fail when invalid PartitionKeyType is provided!")
    } catch {
      case e: Exception => assertTrue(e.getMessage.contains("Unable to instantiate class org.apache.hudi.keygen.CustomKeyGenerator"))
    }
  }

  @Disabled("HUDI-6320")
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithCustomKeyGenerator(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Specify fieldType as TIMESTAMP of type EPOCHMILLISECONDS and output date format as yyyy/MM/dd
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyy/MM/dd")))

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count())

    // Mixed fieldType with TIMESTAMP of type EPOCHMILLISECONDS and output date format as yyyy/MM/dd
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)
  }

  @ParameterizedTest
  @CsvSource(value = Array("6", "8"))
  def testPartitionPruningForTimestampBasedKeyGenerator(tableVersion: Int): Unit = {
    var (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(HoodieRecordType.AVRO, enableFileIndex = true)
    writeOpts = writeOpts + (HoodieTableConfig.VERSION.key() -> tableVersion.toString,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString)
    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .where("current_ts > '1970/01/16'")
    assertTrue(checkPartitionFilters(snapshotQueryRes.queryExecution.executedPlan.toString, "current_ts.* > 1970/01/16"))
  }

  def checkPartitionFilters(sparkPlan: String, partitionFilter: String): Boolean = {
    val partitionFilterPattern: Regex = """PartitionFilters: \[(.*?)\]""".r
    val tsPattern: Regex = (partitionFilter).r

    val partitionFilterMatch = partitionFilterPattern.findFirstMatchIn(sparkPlan)

    partitionFilterMatch match {
      case Some(m) =>
        val filters = m.group(1)
        tsPattern.findFirstIn(filters).isDefined
      case None =>
        false
    }
  }

  @Test
  def testSparkPartitionByWithSimpleKeyGenerator() {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(HoodieRecordType.AVRO)
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver,rider` field as the partition key, If no such field exists,
    // the default value [[PartitionPathEncodeUtils#DEFAULT_PARTITION_PATH]] is used
    writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName, writeOpts)
    val t = assertThrows(classOf[Throwable]) {
      writer.partitionBy("driver", "rider")
        .save(basePath)
    }

    assertEquals("Single partition-path field is expected; provided (driver,rider)", getRootCause(t).getMessage)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithComplexKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver`,`rider` field as the partition key
    writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider")
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= concat(col("driver"), lit("/"), col("rider"))).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithTimestampBasedKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)

    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithGlobalDeleteKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    val writer = getDataFrameWriter(classOf[GlobalDeleteKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithNonpartitionedKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Empty string column
    var writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName, writeOpts)
    writer.partitionBy("")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)

    // Non-existent column
    writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName, writeOpts)
    writer.partitionBy("abc")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  private def testPartitionPruning(enableFileIndex: Boolean,
                                   partitionEncode: Boolean,
                                   isMetadataEnabled: Boolean,
                                   recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    val pathForReader = getPathForReader(basePath, !enableFileIndex, if (partitionEncode) 1 else 3)
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(pathForReader)
      .filter("partition = '2016/03/15'")
      .count()
    assertEquals(countIn20160315, count1)

    // query the partition by path
    val partitionPath = if (partitionEncode) "2016%2F03%2F15" else "2016/03/15"
    val count2 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath + s"/$partitionPath")
      .count()
    assertEquals(countIn20160315, count2)

    // Second write with Append mode
    val records2 = dataGen.generateInsertsContainsAllPartitions("000", N + 1)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitCompletionTime2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime2)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
    assertEquals(false, Metrics.isInitialized(basePath))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,false,AVRO", "true,true,AVRO", "false,true,AVRO", "false,false,AVRO"
  ))
  def testQueryCOWWithBasePathAndFileIndex(partitionEncode: Boolean, isMetadataEnabled: Boolean, recordType: HoodieRecordType): Unit = {
    testPartitionPruning(
      enableFileIndex = true,
      partitionEncode = partitionEncode,
      isMetadataEnabled = isMetadataEnabled,
      recordType = recordType)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionPruningWithoutFileIndex(partitionEncode: Boolean): Unit = {
    testPartitionPruning(
      enableFileIndex = false,
      partitionEncode = partitionEncode,
      isMetadataEnabled = HoodieMetadataConfig.ENABLE.defaultValue,
      recordType = HoodieRecordType.SPARK)
  }

  @Test def testSchemaNotEqualData(): Unit = {
    val opts = CommonOptionUtils.commonOpts ++ Map("hoodie.avro.schema.validate" -> "true")
    val schema1 = StructType(StructField("_row_key", StringType, nullable = true) :: StructField("name", StringType, nullable = true) ::
      StructField("timestamp", IntegerType, nullable = true) :: StructField("age", StringType, nullable = true) :: StructField("partition", IntegerType, nullable = true) :: Nil)
    val records = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}",
      "{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}")
    val inputDF = spark.read.schema(schema1.toDDL).json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath)
    val resultSchema = new StructType(recordsReadDF.schema.filter(p => !p.name.startsWith("_hoodie")).toArray)
    assertEquals(resultSchema, schema1)
  }

  @ParameterizedTest
  @CsvSource(Array("true, AVRO", "false, AVRO", "true, SPARK", "false, SPARK"))
  def testCopyOnWriteWithDroppedPartitionColumns(enableDropPartitionColumns: Boolean, recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInsertsContainsAllPartitions("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key, enableDropPartitionColumns)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val snapshotDF1 = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(snapshotDF1.count(), 100)
    assertEquals(3, snapshotDF1.select("partition").distinct().count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testWriteSmallPrecisionDecimalTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      .withColumn("shortDecimal", lit(new java.math.BigDecimal(s"2090.0000"))) // create decimalType(8, 4)
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // update the value of shortDecimal
    val inputDF2 = inputDF1.withColumn("shortDecimal", lit(new java.math.BigDecimal(s"3090.0000")))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val readResult = spark.read.format("hudi").options(readOpts).load(basePath)
    assert(readResult.count() == 5)
    // compare the test result
    assertEquals(inputDF2.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","),
      readResult.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true, true, AVRO", "true, false, AVRO", "true, true, SPARK", "true, false, SPARK",
    "false, true, AVRO", "false, false, AVRO", "false, true, SPARK", "false, false, SPARK"
  ))
  def testPartitionColumnsProperHandling(enableFileIndex: Boolean,
                                         useGlobbing: Boolean,
                                         recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val _spark = spark
    import _spark.implicits._

    val df = Seq((1, "z3", 30, "v1", "2018-09-23"), (2, "z3", 35, "v1", "2018-09-24"))
      .toDF("id", "name", "age", "ts", "data_date")

    df.write.format("hudi")
      .options(writeOpts)
      .option("hoodie.insert.shuffle.parallelism", "4")
      .option("hoodie.upsert.shuffle.parallelism", "4")
      .option("hoodie.bulkinsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "data_date")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
      .option(TIMESTAMP_TYPE_FIELD.key, "DATE_STRING")
      .option(TIMESTAMP_INPUT_DATE_FORMAT.key, "yyyy-MM-dd")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .option(TIMESTAMP_TIMEZONE_FORMAT.key, "GMT+8:00")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save(basePath)

    // NOTE: We're testing here that both paths are appropriately handling
    //       partition values, regardless of whether we're reading the table
    //       t/h a globbed path or not
    val pathForReader = getPathForReader(basePath, useGlobbing || !enableFileIndex, 3)

    // Case #1: Partition columns are read from the data file
    val firstDF = spark.read.format("hudi").options(readOpts).load(pathForReader)

    assert(firstDF.count() == 2)

    assertEquals(
      Seq("2018-09-23", "2018-09-24"),
      firstDF.select("data_date").map(_.get(0).toString).collect().sorted.toSeq
    )

    assertEquals(
      Seq("2018/09/23", "2018/09/24"),
      firstDF.select("_hoodie_partition_path").map(_.get(0).toString).collect().sorted.toSeq
    )

    // Case #2: Partition columns are extracted from the partition path
    //
    // NOTE: This case is only relevant when globbing is NOT used, since when globbing is used Spark
    //       won't be able to infer partitioning properly
    if (!useGlobbing && enableFileIndex) {
      val secondDF = spark.read.format("hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key, "true")
        .load(pathForReader)

      assert(secondDF.count() == 2)

      // data_date is the partition field. Persist to the parquet file using the origin values, and read it.
      assertEquals(
        Seq("2018/09/23", "2018/09/24"),
        secondDF.select("data_date").map(_.get(0).toString).collect().sorted.toSeq
      )
      assertEquals(
        Seq("2018/09/23", "2018/09/24"),
        secondDF.select("_hoodie_partition_path").map(_.get(0).toString).collect().sorted.toSeq
      )
    }
  }

  @Test
  def testSaveAsTableInDifferentModes(): Unit = {
    val options = scala.collection.mutable.Map.empty ++ CommonOptionUtils.commonOpts ++ Map("path" -> basePath)
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, options.toMap)

    // first use the Overwrite mode
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")

    // init metaClient
    metaClient = createMetaClient(spark, basePath)
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 5)

    // use the Append mode
    val records2 = recordsToStrings(dataGen.generateInserts("002", 6)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 11)

    // use the Ignore mode
    val records3 = recordsToStrings(dataGen.generateInserts("003", 7)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Ignore)
      .saveAsTable("hoodie_test")
    // nothing to do for the ignore mode
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 11)

    // use the ErrorIfExists mode
    val records4 = recordsToStrings(dataGen.generateInserts("004", 8)).asScala.toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    try {
      inputDF4.write.format("org.apache.hudi")
        .partitionBy("partition")
        .options(writeOpts)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable("hoodie_test")
    } catch {
      case e: Throwable => // do nothing
    }

    // use the Overwrite mode
    val records5 = recordsToStrings(dataGen.generateInserts("005", 9)).asScala.toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 9)
  }

  @Test
  def testMetricsReporterViaDataSource(): Unit = {
    val (writeOpts, _) = getWriterReaderOpts(HoodieRecordType.AVRO, getQuickstartWriteConfigs.asScala.toMap)

    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(10))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true")
      .option(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.INMEMORY.name)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))
    assertEquals(false, Metrics.isInitialized(basePath), "Metrics should be shutdown")
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testMapArrayTypeSchemaEvolution(recordType: HoodieRecordType): Unit = {
    assertDoesNotThrow(
      new Executable {
        override def execute(): Unit = {
          val (writeOpts, _) = getWriterReaderOpts(recordType, getQuickstartWriteConfigs.asScala.toMap)

          val schema1 = StructType(
            StructField("_row_key", StringType, nullable = false) ::
              StructField("name", MapType(StringType,
                ArrayType(StringType, containsNull = false)), nullable = true) ::
              StructField("timestamp", LongType, nullable = true) ::
              StructField("partition", LongType, nullable = true) :: Nil)
          val records = List(Row("1", null, 1L, 1L))
          val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(records, 2), schema1)
          inputDF.write.format("org.apache.hudi")
            .options(CommonOptionUtils.commonOpts ++ writeOpts)
            .mode(SaveMode.Overwrite)
            .save(basePath)

          val schema2 = StructType(StructField("_row_key", StringType, nullable = false) ::
            StructField("name", MapType(StringType, ArrayType(StringType,
              containsNull = true)), nullable = true) ::
            StructField("timestamp", LongType, nullable = true) ::
            StructField("partition", LongType, nullable = true) :: Nil)
          val records2 = List(Row("1", null, 1L, 1L))
          val inputDF2 = spark.createDataFrame(spark.sparkContext.parallelize(records2, 2), schema2)
          inputDF2.write.format("org.apache.hudi")
            .options(CommonOptionUtils.commonOpts ++ writeOpts)
            .mode(SaveMode.Append)
            .save(basePath)
        }
      })
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testMapArrayTypeSchemaEvolutionDuringMerge(recordType: HoodieRecordType): Unit = {
    assertDoesNotThrow(
      new Executable {
        override def execute(): Unit = {
          val (writeOpts, _) = getWriterReaderOpts(recordType, getQuickstartWriteConfigs.asScala.toMap)

          val schema1 = StructType(
            StructField("_row_key", StringType, nullable = false) ::
              StructField("map_col", MapType(StringType, StringType, false)) ::
              StructField("array_col", ArrayType(LongType, containsNull = false)) ::
              StructField("timestamp", LongType, nullable = true) ::
              StructField("partition", LongType, nullable = true) :: Nil)
          val records = List(Row("1", Map("foo"-> "bar"), Array(1L), 1L, 1L))
          val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(records, 2), schema1)
          inputDF.write.format("org.apache.hudi")
            .options(CommonOptionUtils.commonOpts ++ writeOpts)
            .mode(SaveMode.Overwrite)
            .save(basePath)

          val schema2 = StructType(StructField("_row_key", StringType, nullable = false) ::
            StructField("map_col", MapType(StringType, StringType, true)) ::
            StructField("array_col", ArrayType(LongType, containsNull = true)) ::
            StructField("timestamp", LongType, nullable = true) ::
            StructField("partition", LongType, nullable = true) :: Nil)
          val records2 = List(Row("2", Map.empty, Array.empty, 1L, 1L))
          val inputDF2 = spark.createDataFrame(spark.sparkContext.parallelize(records2, 2), schema2)
          inputDF2.write.format("org.apache.hudi")
            .options(CommonOptionUtils.commonOpts ++ writeOpts)
            .mode(SaveMode.Append)
            .save(basePath)
        }
      })
  }

  def getWriterReaderOpts(recordType: HoodieRecordType = HoodieRecordType.AVRO,
                          opt: Map[String, String] = CommonOptionUtils.commonOpts,
                          enableFileIndex: Boolean = DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.defaultValue()):
  (Map[String, String], Map[String, String]) = {
    val fileIndexOpt: Map[String, String] =
      Map(DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.key -> enableFileIndex.toString)

    recordType match {
      case HoodieRecordType.SPARK => (opt ++ CommonOptionUtils.sparkOpts, CommonOptionUtils.sparkOpts ++ fileIndexOpt)
      case _ => (opt, fileIndexOpt)
    }
  }

  def getWriterReaderOptsLessPartitionPath(recordType: HoodieRecordType,
                                           opt: Map[String, String] = CommonOptionUtils.commonOpts,
                                           enableFileIndex: Boolean = DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.defaultValue()):
  (Map[String, String], Map[String, String]) = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, opt, enableFileIndex)
    (writeOpts.-(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()), readOpts)
  }

  def getPathForReader(basePath: String, useGlobbing: Boolean, partitionPathLevel: Int): String = {
    if (useGlobbing) {
      // When explicitly using globbing or not using HoodieFileIndex, we fall back to the old way
      // of reading Hudi table with globbed path
      basePath + "/*" * (partitionPathLevel + 1)
    } else {
      basePath
    }
  }

  @Test
  def testHiveStyleDelete(): Unit = {
    val columns = Seq("id", "precombine", "partition")
    val data = Seq((1, "1", "2021-01-05"),
      (2, "2", "2021-01-06"),
      (3, "3", "2021-01-05"))
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)
    var hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "tbl",
      DataSourceWriteOptions.OPERATION.key() -> "insert",
      DataSourceWriteOptions.TABLE_TYPE.key() -> "COPY_ON_WRITE",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "precombine",
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key() -> "true"
    )

    df.write.format("org.apache.hudi").options(hudiOptions).mode(SaveMode.Overwrite).save(basePath)

    hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "tbl",
      DataSourceWriteOptions.OPERATION.key() -> "delete",
      DataSourceWriteOptions.TABLE_TYPE.key() -> "COPY_ON_WRITE",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "precombine"
    )

    df.filter(df("id") === 1).
      write.format("org.apache.hudi").options(hudiOptions).
      mode(SaveMode.Append).save(basePath)
    val result = spark.read.format("hudi").load(basePath)
    assertEquals(2, result.count())
    assertEquals(0, result.filter(result("id") === 1).count())
  }

  /** Test case to verify MAKE_NEW_COLUMNS_NULLABLE config parameter. */
  @Test
  def testSchemaEvolutionWithNewColumn(): Unit = {
    val df1 = spark.sql("select '1' as event_id, '2' as ts, '3' as version, 'foo' as event_date")
    var hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "test_hudi_merger",
      KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key() -> "event_id",
      KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> "version",
      DataSourceWriteOptions.OPERATION.key() -> "insert",
      HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key() -> "ts",
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key() -> "true",
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key() -> "false",
      HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key() -> "org.apache.hudi.DefaultSparkRecordMerger"
    )
    df1.write.format("hudi").options(hudiOptions).mode(SaveMode.Append).save(basePath)

    // Try adding a string column. This operation is expected to throw 'schema not compatible' exception since
    // 'MAKE_NEW_COLUMNS_NULLABLE' parameter is 'false' by default.
    val df2 = spark.sql("select '2' as event_id, '2' as ts, '3' as version, 'foo' as event_date, 'bar' as add_col")
    try {
      (df2.write.format("hudi").options(hudiOptions).mode("append").save(basePath))
      fail("Option succeeded, but was expected to fail.")
    } catch {
      case ex: SchemaBackwardsCompatibilityException => {
        assertTrue(ex.getMessage.contains(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE.name()))
      }
      case ex: Exception => {
        fail(ex)
      }
    }

    // Try adding the string column again. This operation is expected to succeed since 'MAKE_NEW_COLUMNS_NULLABLE'
    // parameter has been set to 'true'.
    hudiOptions = hudiOptions + (HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true")
    try {
      (df2.write.format("hudi").options(hudiOptions).mode("append").save(basePath))
    } catch {
      case ex: Exception => {
        fail(ex)
      }
    }
  }

  def assertLastCommitIsUpsert(): Boolean = {
    val metaClient = createMetaClient(basePath)
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(WriteOperationType.UPSERT)
  }

  @ParameterizedTest
  @CsvSource(value = Array("REQUESTED,6", "REQUESTED,8", "INFLIGHT,6", "INFLIGHT,8", "COMPLETED,6", "COMPLETED,8"))
  def testInsertOverwriteCluster(firstClusteringState: String, tableVersion: Int): Unit = {
    var (writeOpts, _) = getWriterReaderOpts()
    writeOpts = writeOpts + (HoodieTableConfig.VERSION.key() -> tableVersion.toString,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val optsWithCluster = Map(
      INLINE_CLUSTERING_ENABLE.key() -> "true",
      "hoodie.clustering.inline.max.commits" -> "2",
      "hoodie.clustering.plan.strategy.sort.columns" -> "_row_key",
      "hoodie.clustering.plan.strategy.max.num.groups" -> "1",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts
    inputDF.write.format("hudi")
      .options(optsWithCluster)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = createMetaClient(basePath)

    assertTrue(metaClient.getActiveTimeline.getLastClusteringInstant.isEmpty)

    var lastClustering: HoodieInstant = null
    for (i <- 1 until 4) {
      val records = recordsToStrings(dataGen.generateInsertsForPartition("00" + i, 10, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.write.format("hudi")
        .options(optsWithCluster)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      val lastInstant = metaClient.reloadActiveTimeline.getCommitsTimeline.lastInstant.get
      if (i == 1 || i == 3) {
        // Last instant is clustering
        assertTrue(TimelineUtils.getCommitMetadata(lastInstant, metaClient.getActiveTimeline)
          .getOperationType.equals(WriteOperationType.CLUSTER))
        assertTrue(ClusteringUtils.isClusteringInstant(metaClient.getActiveTimeline,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, lastInstant.requestedTime),
          INSTANT_GENERATOR))
        lastClustering = lastInstant
        assertEquals(
          lastClustering,
          metaClient.getActiveTimeline.getLastClusteringInstant.get)
      } else {
        assertTrue(TimelineUtils.getCommitMetadata(lastInstant, metaClient.getActiveTimeline)
          .getOperationType.equals(WriteOperationType.INSERT_OVERWRITE))
        assertFalse(ClusteringUtils.isClusteringInstant(metaClient.getActiveTimeline, lastInstant, metaClient.getTimelineLayout.getInstantGenerator))
        assertEquals(
          lastClustering,
          metaClient.getActiveTimeline.getLastClusteringInstant.get)
      }
      if (i == 1) {
        val writeConfig = HoodieWriteConfig.newBuilder()
          .forTable("hoodie_test")
          .withPath(basePath)
          .withProps(optsWithCluster.asJava)
          .build()
        if (firstClusteringState == HoodieInstant.State.INFLIGHT.name()
          || firstClusteringState == HoodieInstant.State.REQUESTED.name()) {
          // Move the clustering to inflight for testing
          storage.deleteFile(new StoragePath(metaClient.getTimelinePath, INSTANT_FILE_NAME_GENERATOR.getFileName(lastInstant)))
          val inflightClustering = metaClient.reloadActiveTimeline.lastInstant.get
          assertTrue(inflightClustering.isInflight)
          assertEquals(
            inflightClustering,
            metaClient.getActiveTimeline.getLastClusteringInstant.get)
        }
        if (firstClusteringState == HoodieInstant.State.REQUESTED.name()) {
          val table = HoodieSparkTable.create(writeConfig, context)
          val client = new SparkRDDWriteClient(context, writeConfig)
          try {
            table.rollbackInflightClustering(
              metaClient.getActiveTimeline.getLastClusteringInstant.get,
              (commitToRollback: String) => {
                client.getTableServiceClient.getPendingRollbackInfo(table.getMetaClient, commitToRollback, false)
              }, client.getTransactionManager)
          } finally {
            client.close()
          }
          val requestedClustering = metaClient.reloadActiveTimeline.getCommitsTimeline.lastInstant.get
          assertTrue(requestedClustering.isRequested)
          assertEquals(
            requestedClustering,
            metaClient.getActiveTimeline.getLastClusteringInstant.get)
        }
        // This should not schedule any new clustering
        val client = new SparkRDDWriteClient(context, writeConfig)
        client.scheduleClustering(org.apache.hudi.common.util.Option.of(Map[String, String]().asJava))
        client.close()
        assertEquals(lastInstant.requestedTime,
          metaClient.reloadActiveTimeline.getCommitsTimeline.lastInstant.get.requestedTime)
      }
    }
    val timeline = metaClient.reloadActiveTimeline
    val instants = timeline.getCommitsTimeline.getInstants
    assertEquals(6, instants.size)
    val replaceInstants = instants.asScala.filter(i => i.getAction.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).toList
    assertEquals(5, replaceInstants.size)
    val clusterInstants = replaceInstants.filter(i => {
      TimelineUtils.getCommitMetadata(i, metaClient.getActiveTimeline).getOperationType.equals(WriteOperationType.CLUSTER)
    })
    assertEquals(2, clusterInstants.size)
  }

  @Test
  def testClusteringWithRowWriterEnabledAndDisable(): Unit = {
    val optsWithCluster = Map(
      INLINE_CLUSTERING_ENABLE.key() -> "true",
      "hoodie.clustering.inline.max.commits" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.OPERATION.key-> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL
    )

    val schema = StructType(
      StructField("_row_key", StringType, nullable = false) ::
        StructField("name", StringType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) ::
        StructField("partition", LongType, nullable = true) :: Nil)

    val record1 = List(Row("1", null, 1L, 1L))
    var inputDF = spark.createDataFrame(spark.sparkContext.parallelize(record1, 2), schema)
    inputDF.write.format("org.apache.hudi")
      .options(optsWithCluster)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // set hoodie.datasource.write.row.writer.enable = true
    val record2 = List(Row("2", null, 2L, 2L))
    inputDF = spark.createDataFrame(spark.sparkContext.parallelize(record2, 2), schema)
    inputDF.write.format("org.apache.hudi")
      .options(optsWithCluster)
      .option("hoodie.datasource.write.row.writer.enable", "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val record3 = List(Row("3", null, 3L, 3L))
    inputDF = spark.createDataFrame(spark.sparkContext.parallelize(record3, 2), schema)
    inputDF.write.format("org.apache.hudi")
      .options(optsWithCluster)
      .mode(SaveMode.Append)
      .save(basePath)

    // set hoodie.datasource.write.row.writer.enable = false
    val record4 = List(Row("4", null, 4L, 4L))
    inputDF = spark.createDataFrame(spark.sparkContext.parallelize(record4, 2), schema)
    inputDF.write.format("org.apache.hudi")
      .options(optsWithCluster)
      .option("hoodie.datasource.write.row.writer.enable", "false")
      .mode(SaveMode.Append)
      .save(basePath)
  }


  @Test
  def testReadOfAnEmptyTable(): Unit = {
    val (writeOpts, _) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val fileStatuses = storage.listDirectEntries(
      new StoragePath(basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME
        + StoragePath.SEPARATOR + HoodieTableMetaClient.TIMELINEFOLDER_NAME),
      new StoragePathFilter {
        override def accept(path: StoragePath): Boolean = {
          path.getName.endsWith(HoodieTimeline.COMMIT_ACTION)
        }
      })

    // delete completed instant
    storage.deleteFile(fileStatuses.get(0).getPath)
    // try reading the empty table
    val count = spark.read.format("hudi").load(basePath).count()
    assertEquals(count, 0)
  }

}

object TestCOWDataSource {
  def convertColumnsToNullable(df: DataFrame, cols: String*): DataFrame = {
    cols.foldLeft(df) { (df, c) =>
      // NOTE: This is the trick to make Spark convert a non-null column "c" into a nullable
      //       one by pretending its value could be null in some execution paths
      df.withColumn(c, when(col(c).isNotNull, col(c)).otherwise(lit(null)))
    }
  }

  def tableVersionCreationTestCases = {
    val autoUpgradeValues = Array("true", "false")
    val targetVersions = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "null")
    autoUpgradeValues.flatMap(
      (autoUpgrade: String) => targetVersions.map(
        (targetVersion: String) => Arguments.of(autoUpgrade, targetVersion)))
  }
}
