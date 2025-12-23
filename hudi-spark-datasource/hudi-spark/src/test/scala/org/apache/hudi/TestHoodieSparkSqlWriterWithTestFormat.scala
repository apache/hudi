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

package org.apache.hudi


import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model._
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.functional.TestBootstrap
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hudi.command.SqlKeyGenerator
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.Assertions.assertThrows
import org.scalatest.Matchers.intercept

import java.time.Instant
import java.util.{Collections, Date, UUID}

import scala.collection.JavaConverters._

/**
 * Test suite for SparkSqlWriter class with format as "test-format" that implements org.apache.hudi.common.table.HoodieTableFormat.
 * All cases of using of {@link HoodieTimelineTimeZone.UTC} should be done in a separate test class {@link TestHoodieSparkSqlWriterUtc}.
 * Otherwise UTC tests will generate infinite loops, if there is any initiated test with time zone that is greater then UTC+0.
 * The reason is in a saved value in the heap of static {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.lastInstantTime}.
 */
class TestHoodieSparkSqlWriterWithTestFormat extends HoodieSparkWriterTestBase {

  /**
   * Local utility method for performing bulk insert  tests.
   *
   * @param sortMode           Bulk insert sort mode
   * @param populateMetaFields Flag for populating meta fields
   */
  def testBulkInsertWithSortMode(sortMode: BulkInsertSortMode, populateMetaFields: Boolean = true, enableOCCConfigs: Boolean = false): Unit = {
    //create a new table
    var fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields))
      .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), sortMode.name())
      .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")

    if (enableOCCConfigs) {
      fooTableModifier = fooTableModifier
        .updated("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
        .updated("hoodie.clean.failed.writes.policy", "LAZY")
        .updated("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    }

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)

    // add some updates so that preCombine kicks in
    val toUpdateDataset = sqlContext.createDataFrame(DataSourceTestUtils.getUniqueRows(inserts, 40), structType)
    val updates = DataSourceTestUtils.updateRowsWithUpdatedTs(toUpdateDataset)
    val records = inserts.asScala.union(updates.asScala)
    val recordsSeq = convertRowListToSeq(records.asJava)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    // fetch all records from parquet files generated from write to hudi
    val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    if (!populateMetaFields) {
      List(0, 1, 2, 3, 4).foreach(i => assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(i)).filter(entry => !(entry.mkString(",").equals(""))).count()))
    }
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Test case for throw hoodie exception when there already exist a table
   * with different name with Append Save mode
   */
  @Test
  def testThrowExceptionAlreadyExistsWithAppendSaveMode(): Unit = {
    //create a new table
    val fooTableModifier = Map(
      "path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format")
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, dataFrame)

    //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
    val barTableModifier = Map(
      "path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_bar_tbl",
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4")
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableModifier, dataFrame2))
    assert(tableAlreadyExistException.getMessage.contains("Config conflict"))
    assert(tableAlreadyExistException.getMessage.contains(s"${HoodieWriteConfig.TBL_NAME.key}:\thoodie_bar_tbl\thoodie_foo_tbl"))

    //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
    val deleteTableModifier = barTableModifier ++ Map(DataSourceWriteOptions.OPERATION.key -> "delete")
    val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableModifier, dataFrame2))
    assert(tableAlreadyExistException.getMessage.contains("Config conflict"))
    assert(tableAlreadyExistException.getMessage.contains(s"${HoodieWriteConfig.TBL_NAME.key}:\thoodie_bar_tbl\thoodie_foo_tbl"))
  }

  /**
   * Test case for Do not validate table config if save mode is set to Overwrite
   */
  @Test
  def testValidateTableConfigWithOverwriteSaveMode(): Unit = {
    //create a new table
    val tableModifier1 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid", HoodieTableConfig.TABLE_FORMAT.key -> "test-format")
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier1, dataFrame)

    //on same path try write with different RECORDKEY_FIELD_NAME and Append SaveMode should throw an exception
    val tableModifier2 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "ts", HoodieTableConfig.TABLE_FORMAT.key -> "test-format")
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val hoodieException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableModifier2, dataFrame2))
    assert(hoodieException.getMessage.contains("Config conflict"))
    assert(hoodieException.getMessage.contains(s"RecordKey:\tts\tuuid"))

    //on same path try write with different RECORDKEY_FIELD_NAME and Overwrite SaveMode should be successful.
    assert(HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier2, dataFrame2)._1)
  }


  /**
   * Test case for each bulk insert sort mode
   *
   * @param sortMode Bulk insert sort mode
   */
  @ParameterizedTest
  @EnumSource(value = classOf[BulkInsertSortMode])
  def testBulkInsertForSortMode(sortMode: BulkInsertSortMode): Unit = {
    testBulkInsertWithSortMode(sortMode, populateMetaFields = true)
  }

  @Test
  def testBulkInsertForSortModeWithOCC(): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.GLOBAL_SORT, populateMetaFields = true, true)
  }

  /**
   * Test case for Bulk insert with populating meta fields or
   * without populating meta fields.
   *
   * @param populateMetaFields Flag for populating meta fields
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testBulkInsertForPopulateMetaFields(populateMetaFields: Boolean): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.NONE, populateMetaFields)
  }

  /**
   * Test case for disable and enable meta fields.
   */
  @Test
  def testDisableAndEnableMetaFields(): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.NONE, populateMetaFields = false)
    //create a new table
    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), BulkInsertSortMode.NONE.name())
      .updated(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true")
      .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)
    val df = spark.createDataFrame(sc.parallelize(inserts.asScala.toSeq), structType)
    try {
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      fail("Should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.startsWith("Config conflict"))
      case e: Exception => fail(e);
    }
  }

  /**
   * Test case for drop duplicates row writing for bulk_insert.
   */
  @Test
  def testDropDuplicatesRowForBulkInsert(): Unit = {
    try {
      //create a new table
      val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
        .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
        .updated(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "true")
        .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      fail("Drop duplicates with bulk insert in row writing should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.contains("Dropping duplicates with bulk_insert in row writer path is not supported yet"))
    }
  }

  /**
   * Test case for insert dataset without precombine field.
   */
  @Test
  def testInsertDatasetWithoutOrderingFields(): Unit = {

    val fooTableModifier = commonTableModifier.updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "false")
      .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier - HoodieTableConfig.ORDERING_FIELDS.key, df)

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }

    // fetch all records from parquet files generated from write to hudi
    val actualDf = spark.sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Test case for insert dataset without partitioning field
   */
  @Test
  def testInsertDatasetWithoutPartitionField(): Unit = {
    val tableOpts =
      commonTableModifier
        .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(1)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

    // try write to Hudi
    assertThrows[HoodieException] {
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableOpts - DataSourceWriteOptions.PARTITIONPATH_FIELD.key, df)
    }
  }

  /**
   * Test case for bulk insert dataset with datasource impl multiple rounds.
   */
  @Test
  def testBulkInsertDatasetWithDatasourceImplMultipleRounds(): Unit = {

    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieTableConfig.TABLE_FORMAT.key, "test-format")
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    val fullPartitionPaths = new Array[String](3)
    for (i <- 0 to 2) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    var totalExpectedDf = spark.createDataFrame(sc.emptyRDD[Row], structType)
    for (_ <- 0 to 2) {
      // generate the inserts
      val records = DataSourceTestUtils.generateRandomRows(200)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      // Fetch records from entire dataset
      val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
      // remove metadata columns so that expected and actual DFs can be compared as is
      val trimmedDf = dropMetaFields(actualDf)
      // find total df (union from multiple rounds)
      totalExpectedDf = totalExpectedDf.union(df)
      // find mismatch between actual and expected df
      assert(totalExpectedDf.except(trimmedDf).count() == 0)
    }
  }

  /**
   * Test cases for basic HoodieSparkSqlWriter functionality with datasource insert
   * for different tableTypes, fileFormats and options for population meta fields.
   *
   * @param tableType          Type of table
   * @param baseFileFormat     File format
   * @param populateMetaFields Flag for populating meta fields
   */
  @ParameterizedTest
  @MethodSource(Array("testDatasourceInsert"))
  def testDatasourceInsertForTableTypeBaseFileMetaFields(tableType: String, populateMetaFields: Boolean, baseFileFormat: String): Unit = {
    val hoodieFooTableName = "hoodie_foo_tbl"
    val fooTableModifier = Map("path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      HoodieWriteConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "4",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.POPULATE_META_FIELDS.key() -> String.valueOf(populateMetaFields),
      DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getCanonicalName)
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val modifiedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "trip", "example.schema")
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    initializeMetaClientForBootstrap(fooTableParams, tableType, addBootstrapPath = false, initBasePath = true)
    val client = spy[SparkRDDWriteClient[_]](DataSourceUtils.createHoodieClient(
      new JavaSparkContext(sc), modifiedSchema.toString, tempBasePath, hoodieFooTableName,
      fooTableParams.asJava).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df, Option.empty, Option(client))
    // Verify that asynchronous compaction is not scheduled
    verify(client, times(0)).scheduleCompaction(any())
    // Verify that HoodieWriteClient is closed correctly
    verify(client, times(1)).close()

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    // fetch all records from parquet files generated from write to hudi
    var actualDf: DataFrame = null
    if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.PARQUET.name())) {
      actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    } else if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.ORC.name())) {
      actualDf = sqlContext.read.orc(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    }
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Test cases for HoodieSparkSqlWriter functionality with datasource bootstrap
   * for different type of tables and table versions.
   *
   * @param tableType    Type of table
   * @param tableVersion Version of table
   */
  @ParameterizedTest
  @MethodSource(Array("bootstrapTestParams"))
  def testWithDatasourceBootstrapForTableType(tableType: String, tableVersion: Int): Unit = {
    val srcPath = java.nio.file.Files.createTempDirectory("hoodie_bootstrap_source_path")
    try {
      val sourceDF = TestBootstrap.generateTestRawTripDataset(Instant.now.toEpochMilli, 0, 100, Collections.emptyList(), sc,
        spark.sqlContext)
      // Write source data non-partitioned
      sourceDF.write.format("parquet").mode(SaveMode.Overwrite).save(srcPath.toAbsolutePath.toString)

      val fooTableModifier = Map("path" -> tempBasePath,
        HoodieBootstrapConfig.BASE_PATH.key -> srcPath.toAbsolutePath.toString,
        HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        HoodieBootstrapConfig.PARALLELISM_VALUE.key -> "4",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL,
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getCanonicalName,
        DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getCanonicalName,
        "hoodie.write.table.version" -> tableVersion.toString,
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
      initializeMetaClientForBootstrap(fooTableParams, tableType, addBootstrapPath = true, initBasePath = false)

      val client = spy[SparkRDDWriteClient[_]](DataSourceUtils.createHoodieClient(
        new JavaSparkContext(sc),
        null,
        tempBasePath,
        hoodieFooTableName,
        fooTableParams.asJava).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

      HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Append, fooTableModifier, spark.emptyDataFrame, Option.empty,
        Option.empty, Option(client))

      // Verify that HoodieWriteClient is closed correctly
      verify(client, times(1)).close()

      val ignoreResult = HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Ignore, fooTableModifier, spark.emptyDataFrame, Option.empty,
        Option.empty, Option(client))
      assertFalse(ignoreResult)
      verify(client, times(2)).close()

      // Assert the table version is adopted.
      val metaClient = createMetaClient(spark, tempBasePath)
      assertEquals(metaClient.getTableConfig.getTableVersion.versionCode(), tableVersion)
      // fetch all records from parquet files generated from write to hudi
      val actualDf = sqlContext.read.parquet(tempBasePath)
      assert(actualDf.count == 100)
    } finally {
      FileUtils.deleteDirectory(srcPath.toFile)
    }
  }

  def initializeMetaClientForBootstrap(fooTableParams: Map[String, String], tableType: String, addBootstrapPath: Boolean, initBasePath: Boolean): Unit = {
    // when metadata is enabled, directly instantiating write client using DataSourceUtils.createHoodieClient
    // will hit a code which tries to instantiate meta client for data table. if table does not exist, it fails.
    // hence doing an explicit instantiation here.
    val tableMetaClientBuilder = HoodieTableMetaClient.newTableBuilder()
      .setTableType(tableType)
      .setTableName(hoodieFooTableName)
      .setRecordKeyFields(fooTableParams(DataSourceWriteOptions.RECORDKEY_FIELD.key))
      .setBaseFileFormat(fooTableParams.getOrElse(HoodieWriteConfig.BASE_FILE_FORMAT.key,
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name))
      .setArchiveLogFolder(HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue())
      .setOrderingFields(fooTableParams.getOrElse(HoodieTableConfig.ORDERING_FIELDS.key, null))
      .setPartitionFields(fooTableParams(DataSourceWriteOptions.PARTITIONPATH_FIELD.key))
      .setKeyGeneratorClassProp(fooTableParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key,
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.defaultValue()))
    if (fooTableParams.contains(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key)) {
      tableMetaClientBuilder.setPayloadClassName(fooTableParams(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key))
    }
    if (fooTableParams.contains(HoodieWriteConfig.RECORD_MERGE_MODE.key)) {
      tableMetaClientBuilder.setRecordMergeMode(RecordMergeMode.valueOf(fooTableParams(HoodieWriteConfig.RECORD_MERGE_MODE.key)))
    }
    if (fooTableParams.contains(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key)) {
      tableMetaClientBuilder.setRecordMergeStrategyId(fooTableParams(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key))
    }
    if (addBootstrapPath) {
      tableMetaClientBuilder
        .setBootstrapBasePath(fooTableParams(HoodieBootstrapConfig.BASE_PATH.key))
    }
    if (initBasePath) {
      tableMetaClientBuilder.initTable(HadoopFSUtils.getStorageConfWithCopy(sc.hadoopConfiguration), tempBasePath)
    }
  }

  @Test
  def testNonpartitonedWithReuseTableConfig(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )

    // case 1: When commit C1 specifies a key generator and commit C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // NonpartitionedKeyGenerator is automatically inferred and used
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // In first commit, we explicitly over-ride it to Nonpartitioned, where as in 2nd batch, since re-using of table configs
    // come into play, no exception should be thrown even if we don't supply any key gen class.
    df2.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Append).save(tablePath1)
  }

  @Test
  def testDefaultKeyGenToNonpartitoned(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )

    // case 1: When commit C1 does not specify key generator and commit C2 specifies a key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // raise exception when NonpartitionedKeyGenerator is specified
    val configConflictException = intercept[HoodieException] {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
        .mode(SaveMode.Append).save(tablePath1)
    }
    assert(configConflictException.getMessage.contains("Config conflict"))
    assert(configConflictException.getMessage.contains(s"KeyGenerator:\t${classOf[NonpartitionedKeyGenerator].getName}\t${classOf[SimpleKeyGenerator].getName}"))
  }

  @Test
  def testNoKeyGenToSimpleKeyGen(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )

    // case 1: When commit C1 specifies a key generator and commkt C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // No Exception Should be raised
    try {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
        .mode(SaveMode.Append).save(tablePath1)
    } catch {
      case _: Throwable => fail("Switching from no keygen to explicit SimpleKeyGenerator should not fail");
    }
  }

  @Test
  def testSimpleKeyGenToNoKeyGen(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )

    // case 1: When commit C1 specifies a key generator and commkt C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // the first write need to specify KEYGENERATOR_CLASS_NAME params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // No Exception Should be raised when default keygen is used
    try {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .mode(SaveMode.Append).save(tablePath1)
    } catch {
      case _: Throwable => fail("Switching from  explicit SimpleKeyGenerator to default keygen should not fail");
    }
  }

  @Test
  def testGetOriginKeyGenerator(): Unit = {
    // for dataframe write
    val m1 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName,
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )
    val kg1 = HoodieWriterUtils.getOriginKeyGenerator(m1)
    assertTrue(kg1 == classOf[ComplexKeyGenerator].getName)

    // for sql write
    val m2 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> classOf[SimpleKeyGenerator].getName,
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )
    val kg2 = HoodieWriterUtils.getOriginKeyGenerator(m2)
    assertTrue(kg2 == classOf[SimpleKeyGenerator].getName)
  }

  /**
   *
   * Test that you can't have consistent hashing bucket index on a COW table
   * */
  @Test
  def testCOWConsistentHashing(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key -> "CONSISTENT_HASHING",
      HoodieIndexConfig.INDEX_TYPE.key -> "BUCKET",
      HoodieTableConfig.TABLE_FORMAT.key -> "test-format"
    )

    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")
    val exc = intercept[HoodieException] {
      df.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .mode(SaveMode.Overwrite).save(tablePath1)
    }
    assert(exc.getMessage.contains("Consistent hashing bucket index does not work with COW table. Use simple bucket index or an MOR table."))
  }

  private def fetchActualSchema(): HoodieSchema = {
    val tableMetaClient = createMetaClient(spark, tempBasePath)
    new TableSchemaResolver(tableMetaClient).getTableSchema()
  }
}

object TestHoodieSparkSqlWriterWithTestFormat {
  def testDatasourceInsert: java.util.stream.Stream[Arguments] = {
    val scenarios = Array(
      Seq("COPY_ON_WRITE", true),
      Seq("COPY_ON_WRITE", false),
      Seq("MERGE_ON_READ", true),
      Seq("MERGE_ON_READ", false)
    )

    val parquetScenarios = scenarios.map {
      _ :+ "parquet"
    }
    val orcScenarios = scenarios.map {
      _ :+ "orc"
    }
    val targetScenarios = parquetScenarios ++ orcScenarios

    java.util.Arrays.stream(targetScenarios.map(as => Arguments.arguments(as.map(_.asInstanceOf[AnyRef]): _*)))
  }

  def deletePartitionsWildcardTestParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments("*5/03/1*", Seq("2016/03/15")),
      Arguments.arguments("2016/03/*", Seq("2015/03/16", "2015/03/17")))
  }

  def bootstrapTestParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments("MERGE_ON_READ", Integer.valueOf(8)),
      Arguments.arguments("MERGE_ON_READ", Integer.valueOf(6)),
      Arguments.arguments("COPY_ON_WRITE", Integer.valueOf(8))
    )
  }
}
