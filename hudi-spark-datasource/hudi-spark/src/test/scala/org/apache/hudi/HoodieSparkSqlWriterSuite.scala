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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions.{INSERT_DROP_DUPS, INSERT_OPERATION_OPT_VAL, KEYGENERATOR_CLASS_NAME, MOR_TABLE_TYPE_OPT_VAL, OPERATION, PAYLOAD_CLASS_NAME, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord, HoodieRecordPayload, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.functional.TestBootstrap
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.Matchers.{assertResult, be, convertToAnyShouldWrapper, intercept}

import java.time.Instant
import java.util
import java.util.{Collections, Date, UUID}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters

/**
 * Test suite for SparkSqlWriter class.
 */
class HoodieSparkSqlWriterSuite {
  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _
  var tempPath: java.nio.file.Path = _
  var tempBootStrapPath: java.nio.file.Path = _
  var hoodieFooTableName = "hoodie_foo_tbl"
  var tempBasePath: String = _
  var commonTableModifier: Map[String, String] = Map()
  case class StringLongTest(uuid: String, ts: Long)

  /**
   * Setup method running before each test.
   */
  @BeforeEach
  def setUp() {
    initSparkContext()
    tempPath = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    tempBootStrapPath = java.nio.file.Files.createTempDirectory("hoodie_test_bootstrap")
    tempBasePath = tempPath.toAbsolutePath.toString
    commonTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
  }

  /**
   * Tear down method running after each test.
   */
  @AfterEach
  def tearDown(): Unit = {
    cleanupSparkContexts()
    FileUtils.deleteDirectory(tempPath.toFile)
    FileUtils.deleteDirectory(tempBootStrapPath.toFile)
  }

  /**
   * Utility method for initializing the spark context.
   */
  def initSparkContext(): Unit = {
    spark = SparkSession.builder()
      .appName(hoodieFooTableName)
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  /**
   * Utility method for cleaning up spark resources.
   */
  def cleanupSparkContexts(): Unit = {
    if (sqlContext != null) {
      sqlContext.clearCache();
      sqlContext = null;
    }
  }

  /**
   * Utility method for dropping all hoodie meta related columns.
   */
  def dropMetaFields(df: Dataset[Row]): Dataset[Row] = {
    df.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))
  }

  /**
   * Utility method for creating common params for writer.
   *
   * @param path               Path for hoodie table
   * @param hoodieFooTableName Name of hoodie table
   * @param tableType          Type of table
   * @return                   Map of common params
   */
  def getCommonParams(path: java.nio.file.Path, hoodieFooTableName: String, tableType: String): Map[String, String] = {
    Map("path" -> path.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
  }

  /**
   * Utility method for converting list of Row to list of Seq.
   *
   * @param inputList list of Row
   * @return list of Seq
   */
  def convertRowListToSeq(inputList: util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

  /**
   * Utility method for performing bulk insert  tests.
   *
   * @param sortMode           Bulk insert sort mode
   * @param populateMetaFields Flag for populating meta fields
   */
  def testBulkInsertWithSortMode(sortMode: BulkInsertSortMode, populateMetaFields: Boolean = true): Unit = {
    //create a new table
    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields))
      .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), sortMode.name())
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)

    // add some updates so that preCombine kicks in
    val toUpdateDataset = sqlContext.createDataFrame(DataSourceTestUtils.getUniqueRows(inserts, 40), structType)
    val updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset)
    val records = inserts.union(updates)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)

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
   * Utility method for performing bulk insert  tests.
   */
  @Test
  def testParametersWithWriteDefaults(): Unit = {
    val originals = HoodieWriterUtils.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION.key -> INSERT_OPERATION_OPT_VAL, TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieWriterUtils.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)
    originals foreach {
      case ("hoodie.datasource.write.operation", _) => matcher("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      case ("hoodie.datasource.write.table.type", _) => matcher("hoodie.datasource.write.table.type", MOR_TABLE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  /**
   * Test case for invalid serializer provided.
   */
  @Test
  def testThrowExceptionInvalidSerializer(): Unit = {
    spark.stop()
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    try {
      val sqlContext = session.sqlContext
      val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_tbl")
      val e = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.ErrorIfExists, options,
        session.emptyDataFrame))
      assert(e.getMessage.contains("spark.serializer"))
    } finally {
      session.stop()
      initSparkContext()
    }
  }

  /**
   * Test case for throw hoodie exception when there already exist a table
   * with different name with Append Save mode
   */
  @Test
  def testThrowExceptionAlreadyExistsWithAppendSaveMode(): Unit = {
    //create a new table
    val fooTableModifier = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.insert.shuffle.parallelism" -> "4", "hoodie.upsert.shuffle.parallelism" -> "4")
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, dataFrame)

    //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
    val barTableModifier = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> "hoodie_bar_tbl",
      "hoodie.insert.shuffle.parallelism" -> "4", "hoodie.upsert.shuffle.parallelism" -> "4")
    val barTableParams = HoodieWriterUtils.parametersWithWriteDefaults(barTableModifier)
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableParams, dataFrame2))
    assert(tableAlreadyExistException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))

    //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
    val deleteTableParams = barTableParams ++ Map(OPERATION.key -> "delete")
    val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableParams, dataFrame2))
    assert(deleteCmdException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))
  }

  /**
   * Test case for each bulk insert sort mode
   *
   * @param sortMode Bulk insert sort mode
   */
  @ParameterizedTest
  @EnumSource(value = classOf[BulkInsertSortMode])
  def testBulkInsertForSortMode(sortMode: BulkInsertSortMode): Unit = {
    testBulkInsertWithSortMode(sortMode, true)
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
    try {
      testBulkInsertWithSortMode(BulkInsertSortMode.NONE, false)
      //create a new table
      val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
        .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
        .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), BulkInsertSortMode.NONE.name())
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val inserts = DataSourceTestUtils.generateRandomRows(1000)
      val df = spark.createDataFrame(sc.parallelize(inserts), structType)
      try {
        // write to Hudi
        HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)
        Assertions.fail("Should have thrown exception")
      } catch {
        case e: HoodieException => assertTrue(e.getMessage.contains("hoodie.populate.meta.fields already disabled for the table. Can't be re-enabled back"))
      }
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
        .updated(INSERT_DROP_DUPS.key, "true")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)
      fail("Drop duplicates with bulk insert in row writing should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.contains("Dropping duplicates with bulk_insert in row writer path is not supported yet"))
    }
  }

  /**
   * Test case for insert dataset without precombine field.
   */
  @Test
  def testInsertDatasetWithoutPrecombineField(): Unit = {

    //create a new table
    val fooTableModifier = commonTableModifier.updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "false")
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams - DataSourceWriteOptions.PRECOMBINE_FIELD.key, df)

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
   * Test case for bulk insert dataset with datasource impl multiple rounds.
   */
  @Test
  def testBulkInsertDatasetWithDatasourceImplMultipleRounds(): Unit = {

    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    val fullPartitionPaths = new Array[String](3)
    for (i <- 0 to 2) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var totalExpectedDf = spark.createDataFrame(sc.emptyRDD[Row], structType)
    for (_ <- 0 to 2) {
      // generate the inserts
      val records = DataSourceTestUtils.generateRandomRows(200)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)
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
  @CsvSource(
    Array("COPY_ON_WRITE,parquet,true", "COPY_ON_WRITE,parquet,false", "MERGE_ON_READ,parquet,true", "MERGE_ON_READ,parquet,false",
      "COPY_ON_WRITE,orc,true", "COPY_ON_WRITE,orc,false", "MERGE_ON_READ,orc,true", "MERGE_ON_READ,orc,false"
    ))
  def testDatasourceInsertForTableTypeBaseFileMetaFields(tableType: String, baseFileFormat: String, populateMetaFields: Boolean): Unit = {
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
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[SimpleKeyGenerator].getCanonicalName)
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val modifiedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "trip", "example.schema")
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    initializeMetaClientForBootstrap(fooTableParams, tableType, false)
    val client = spy(DataSourceUtils.createHoodieClient(
      new JavaSparkContext(sc), modifiedSchema.toString, tempBasePath, hoodieFooTableName,
      mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df, Option.empty, Option(client))
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
   * for different type of tables.
   *
   * @param tableType Type of table
   */
  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testWithDatasourceBootstrapForTableType(tableType: String): Unit = {
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
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getCanonicalName)
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
      initializeMetaClientForBootstrap(fooTableParams, tableType, true)

         val client = spy(DataSourceUtils.createHoodieClient(
        new JavaSparkContext(sc),
        null,
        tempBasePath,
        hoodieFooTableName,
        mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

      HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Append, fooTableParams, spark.emptyDataFrame, Option.empty,
        Option(client))

      // Verify that HoodieWriteClient is closed correctly
      verify(client, times(1)).close()
      // fetch all records from parquet files generated from write to hudi
      val actualDf = sqlContext.read.parquet(tempBasePath)
      assert(actualDf.count == 100)
    } finally {
      FileUtils.deleteDirectory(srcPath.toFile)
    }
  }

  def initializeMetaClientForBootstrap(fooTableParams : Map[String, String], tableType: String, addBootstrapPath : Boolean) : Unit = {
    // when metadata is enabled, directly instantiating write client using DataSourceUtils.createHoodieClient
    // will hit a code which tries to instantiate meta client for data table. if table does not exist, it fails.
    // hence doing an explicit instantiation here.
    val tableMetaClientBuilder = HoodieTableMetaClient.withPropertyBuilder()
      .setTableType(tableType)
      .setTableName(hoodieFooTableName)
      .setRecordKeyFields(fooTableParams(DataSourceWriteOptions.RECORDKEY_FIELD.key))
      .setBaseFileFormat(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name())
      .setArchiveLogFolder(HoodieTableConfig.ARCHIVELOG_FOLDER.defaultValue())
      .setPayloadClassName(fooTableParams(PAYLOAD_CLASS_NAME.key))
      .setPreCombineField(fooTableParams(PRECOMBINE_FIELD.key))
      .setPartitionFields(fooTableParams(DataSourceWriteOptions.PARTITIONPATH_FIELD.key))
      .setKeyGeneratorClassProp(fooTableParams(KEYGENERATOR_CLASS_NAME.key))
      if(addBootstrapPath) {
        tableMetaClientBuilder
          .setBootstrapBasePath(fooTableParams(HoodieBootstrapConfig.BASE_PATH.key))
      }
    tableMetaClientBuilder.initTable(sc.hadoopConfiguration, tempBasePath)
  }

  /**
   * Test cases for schema evolution in different types of tables.
   *
   * @param tableType Type of table
   */
  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testSchemaEvolutionForTableType(tableType: String): Unit = {
    //create a new table
    val fooTableModifier = getCommonParams(tempPath, hoodieFooTableName, tableType)
      .updated(DataSourceWriteOptions.RECONCILE_SCHEMA.key, "true")
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var records = DataSourceTestUtils.generateRandomRows(10)
    var recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableParams, df1)

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF1.count())

    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf1 = dropMetaFields(snapshotDF1)
    assert(df1.except(trimmedDf1).count() == 0)

    // issue updates so that log files are created for MOR table
    val updatesSeq = convertRowListToSeq(DataSourceTestUtils.generateUpdates(records, 5))
    val updatesDf = spark.createDataFrame(sc.parallelize(updatesSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, updatesDf)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF2.count())

    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf2 = dropMetaFields(snapshotDF2)
    // ensure 2nd batch of updates matches.
    assert(updatesDf.intersect(trimmedDf2).except(updatesDf).count() == 0)

    // getting new schema with new column
    val evolSchema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
    val evolStructType = AvroConversionUtils.convertAvroSchemaToStructType(evolSchema)
    records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
    recordsSeq = convertRowListToSeq(records)
    val df3 = spark.createDataFrame(sc.parallelize(recordsSeq), evolStructType)
    // write to Hudi with new column
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df3)

    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(15, snapshotDF3.count())

    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf3 = dropMetaFields(snapshotDF3)
    // ensure 2nd batch of updates matches.
    assert(df3.intersect(trimmedDf3).except(df3).count() == 0)

    // ingest new batch with old schema.
    records = DataSourceTestUtils.generateRandomRows(10)
    recordsSeq = convertRowListToSeq(records)
    val df4 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df4)

    val snapshotDF4 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(25, snapshotDF4.count())

    val tableMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration)
      .setBasePath(tempBasePath).build()
    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchemaWithoutMetadataFields
    assertTrue(actualSchema != null)
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(hoodieFooTableName)
    val expectedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(evolStructType, structName, nameSpace)
    assertEquals(expectedSchema, actualSchema)
  }

  /**
   * Test case for build sync config for spark sql.
   */
  @Test
  def testBuildSyncConfigForSparkSql(): Unit = {
    val params = Map(
      "path" -> tempBasePath,
      DataSourceWriteOptions.TABLE_NAME.key -> "test_hoodie",
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key -> "partition",
      DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key -> "true",
      DataSourceWriteOptions.HIVE_CREATE_MANAGED_TABLE.key -> "true"
    )
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(params)
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)

    val buildSyncConfigMethod =
      HoodieSparkSqlWriter.getClass.getDeclaredMethod("buildSyncConfig", classOf[Path],
        classOf[HoodieConfig], classOf[SQLConf])
    buildSyncConfigMethod.setAccessible(true)

    val hiveSyncConfig = buildSyncConfigMethod.invoke(HoodieSparkSqlWriter,
      new Path(tempBasePath), hoodieConfig, spark.sessionState.conf).asInstanceOf[HiveSyncConfig]
    assertTrue(hiveSyncConfig.skipROSuffix)
    assertTrue(hiveSyncConfig.createManagedTable)
    assertTrue(hiveSyncConfig.syncAsSparkDataSourceTable)
    assertResult(spark.sessionState.conf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD))(hiveSyncConfig.sparkSchemaLengthThreshold)
  }

  /**
   * Test case for build sync config for skip Ro Suffix values.
   */
  @Test
  def testBuildSyncConfigForSkipRoSuffixValues(): Unit = {
    val params = Map(
      "path" -> tempBasePath,
      DataSourceWriteOptions.TABLE_NAME.key -> "test_hoodie",
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key -> "partition"
    )
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(params)
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)
    val buildSyncConfigMethod =
      HoodieSparkSqlWriter.getClass.getDeclaredMethod("buildSyncConfig", classOf[Path],
        classOf[HoodieConfig], classOf[SQLConf])
    buildSyncConfigMethod.setAccessible(true)
    val hiveSyncConfig = buildSyncConfigMethod.invoke(HoodieSparkSqlWriter,
      new Path(tempBasePath), hoodieConfig, spark.sessionState.conf).asInstanceOf[HiveSyncConfig]
    assertFalse(hiveSyncConfig.skipROSuffix)
  }

  /**
   * Test case for incremental view with replacement.
   */
  @Test
  def testIncrementalViewWithReplacement(): Unit = {
    List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      val baseBootStrapPath = tempBootStrapPath.toAbsolutePath.toString
      val options = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "col3",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "keyid",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        HoodieWriteConfig.TBL_NAME.key -> "hoodie_test")
      try {
        val df = spark.range(0, 1000).toDF("keyid")
          .withColumn("col3", expr("keyid"))
          .withColumn("age", lit(1))
          .withColumn("p", lit(2))

        df.write.format("hudi")
          .options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert")
          .option("hoodie.insert.shuffle.parallelism", "4")
          .mode(SaveMode.Overwrite).save(tempBasePath)

        df.write.format("hudi")
          .options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
          .option("hoodie.insert.shuffle.parallelism", "4")
          .mode(SaveMode.Append).save(tempBasePath)

        val currentCommits = spark.read.format("hudi").load(tempBasePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
        val incrementalKeyIdNum = spark.read.format("hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommits(0))
          .load(tempBasePath).select("keyid").orderBy("keyid").count
        assert(incrementalKeyIdNum == 1000)

        df.write.mode(SaveMode.Overwrite).save(baseBootStrapPath)
        spark.emptyDataFrame.write.format("hudi")
          .options(options)
          .option(HoodieBootstrapConfig.BASE_PATH.key, baseBootStrapPath)
          .option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getCanonicalName)
          .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
          .option(HoodieBootstrapConfig.PARALLELISM_VALUE.key, "4")
          .mode(SaveMode.Overwrite).save(tempBasePath)
        df.write.format("hudi").options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
          .option("hoodie.insert.shuffle.parallelism", "4").mode(SaveMode.Append).save(tempBasePath)
        val currentCommitsBootstrap = spark.read.format("hudi").load(tempBasePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
        val incrementalKeyIdNumBootstrap = spark.read.format("hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommitsBootstrap(0))
          .load(tempBasePath).select("keyid").orderBy("keyid").count
        assert(incrementalKeyIdNumBootstrap == 1000)
      }
    }
  }

  /**
   * Test case for deletion of partitions.
   * @param usePartitionsToDeleteConfig Flag for if use partitions to delete config
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeletePartitionsV2(usePartitionsToDeleteConfig: Boolean): Unit = {
    val fooTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableParams, df1)
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF1.count())
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf1 = dropMetaFields(snapshotDF1)
    assert(df1.except(trimmedDf1).count() == 0)
    // issue updates so that log files are created for MOR table
    val updatesSeq = convertRowListToSeq(DataSourceTestUtils.generateUpdates(records, 5))
    val updatesDf = spark.createDataFrame(sc.parallelize(updatesSeq), structType)
    // write updates to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, updatesDf)
    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF2.count())
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf2 = dropMetaFields(snapshotDF2)
    // ensure 2nd batch of updates matches.
    assert(updatesDf.intersect(trimmedDf2).except(updatesDf).count() == 0)
    if (usePartitionsToDeleteConfig) {
      fooTableParams.updated(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
    }
    // delete partitions contains the primary key
    val recordsToDelete = df1.filter(entry => {
      val partitionPath: String = entry.getString(1)
      partitionPath.equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH) ||
        partitionPath.equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
    })
    val updatedParams = fooTableParams.updated(DataSourceWriteOptions.OPERATION.key(), WriteOperationType.DELETE_PARTITION.name())
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, updatedParams, recordsToDelete)
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(0, snapshotDF3.filter(entry => {
      val partitionPath = entry.getString(3)
      !partitionPath.equals(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    }).count())
  }

  /**
   * Test case for non partition table with metatable support.
   */
  @Test
  def testNonPartitionTableWithMetatableSupport(): Unit = {
    List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      val options = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "col3",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "keyid",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
        "hoodie.insert.shuffle.parallelism" -> "1",
        "hoodie.metadata.enable" -> "true")
      val df = spark.range(0, 10).toDF("keyid")
        .withColumn("col3", expr("keyid"))
        .withColumn("age", expr("keyid + 1000"))
      df.write.format("hudi")
        .options(options.updated(DataSourceWriteOptions.OPERATION.key, "insert"))
        .mode(SaveMode.Overwrite).save(tempBasePath)
      // upsert same record again
      val df_update = spark.range(0, 10).toDF("keyid")
        .withColumn("col3", expr("keyid"))
        .withColumn("age", expr("keyid + 2000"))
      df_update.write.format("hudi")
        .options(options.updated(DataSourceWriteOptions.OPERATION.key, "upsert"))
        .mode(SaveMode.Append).save(tempBasePath)
      assert(spark.read.format("hudi").load(tempBasePath).count() == 10)
      assert(spark.read.format("hudi").load(tempBasePath).where("age >= 2000").count() == 10)
    }
  }
}
