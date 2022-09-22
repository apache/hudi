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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.common.util
import org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.metrics.HoodieMetricsConfig
import org.apache.hudi.exception.{HoodieException, HoodieUpsertException}
import org.apache.hudi.keygen._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.metrics.Metrics
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers, QuickstartUtils, HoodieSparkRecordMerger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, lit, udf}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}
import java.sql.{Date, Timestamp}
import java.util.function.Consumer
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
 * Basic tests on the spark datasource for COW table.
 */
class TestCOWDataSource extends HoodieClientTestBase {
  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1"
  )
  val sparkOpts = Map(
    HoodieWriteConfig.MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJava((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testShortNameStorage(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testNoPrecombine(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedNonBooleanField(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val df = inputDF.withColumn(HoodieRecord.HOODIE_IS_DELETED_FIELD, lit("abc"))

    assertThrows(classOf[HoodieException], new Executable {
      override def execute(): Unit = {
        df.write.format("hudi")
          .options(writeOpts)
          .mode(SaveMode.Overwrite)
          .save(basePath)
      }
    }, "Should have failed since _hoodie_is_deleted is not a BOOLEAN data type")
  }

  /**
   * This tests the case that query by with a specified partition condition on hudi table which is
   * different between the value of the partition field and the actual partition path,
   * like hudi table written by TimestampBasedKeyGenerator.
   *
   * For COW table, test the snapshot query mode and incremental query mode.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testPrunePartitionForTimestampBasedKeyGenerator(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val options = commonOpts ++ Map(
      "hoodie.compact.inline" -> "false",
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      Config.TIMESTAMP_TYPE_FIELD_PROP -> "DATE_STRING",
      Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyy/MM/dd",
      Config.TIMESTAMP_TIMEZONE_FORMAT_PROP -> "GMT+8:00",
      Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP -> "yyyy-MM-dd"
    ) ++ writeOpts

    val dataGen1 = new HoodieTestDataGenerator(Array("2022-01-01"))
    val records1 = recordsToStrings(dataGen1.generateInserts("001", 20)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val commit1Time = metaClient.getActiveTimeline.lastInstant().get().getTimestamp

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 30)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

    // snapshot query
    val snapshotQueryRes = spark.read.format("hudi").options(readOpts).load(basePath)
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    //assertEquals(snapshotQueryRes.where("partition = '2022-01-01'").count, 20)
    //assertEquals(snapshotQueryRes.where("partition = '2022-01-02'").count, 30)
    assertEquals(snapshotQueryRes.where("partition = '2022/01/01'").count, 20)
    assertEquals(snapshotQueryRes.where("partition = '2022/01/02'").count, 30)

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit1Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-01'").count, 0)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-02'").count, 30)
  }

  /**
   * Test for https://issues.apache.org/jira/browse/HUDI-1615. Null Schema in BulkInsert row writer flow.
   * This was reported by customer when archival kicks in as the schema in commit metadata is not set for bulk_insert
   * row writer flow.
   * In this test, we trigger a round of bulk_inserts and set archive related configs to be minimal. So, after 4 rounds,
   * archival should kick in and 2 commits should be archived. If schema is valid, no exception will be thrown. If not,
   * NPE will be thrown.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testArchivalWithBulkInsert(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    var structType : StructType = null
    for (i <- 1 to 4) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(writeOpts)
        .option("hoodie.keep.min.commits", "1")
        .option("hoodie.keep.max.commits", "2")
        .option("hoodie.cleaner.commits.retained", "0")
        .option("hoodie.datasource.write.row.writer.enable", "true")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    val tableMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build()
    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchemaWithoutMetadataFields
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(commonOpts(HoodieWriteConfig.TBL_NAME.key))
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
    val (writeOpts, readOpts) = getOpts(recordType)

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF1.count())

    val records2 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2 , 2))

    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(snapshotDF2.count(), 80)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteModeUseReplaceAction(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build();
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadPathsOnCopyOnWriteTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val records1 = dataGen.generateInsertsContainsAllPartitions("001", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()

    val instantTime = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.findFirst().get().getTimestamp

    val record1FilePaths = fs.listStatus(new Path(basePath, dataGen.getPartitionPaths.head))
      .filter(!_.getPath.getName.contains("hoodie_partition_metadata"))
      .filter(_.getPath.getName.endsWith("parquet"))
      .map(_.getPath.toString)
      .mkString(",")

    val records2 = dataGen.generateInsertsContainsAllPartitions("002", 20)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val inputDF3 = spark.read.options(readOpts).json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteTableModeUseReplaceAction(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteModeUseReplaceActionOnDisJointPartitions(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 records to hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step3: Write 6 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH using INSERT_OVERWRITE_OPERATION_OPT_VAL
    val records3 = recordsToStrings(dataGen.generateInsertsForPartition("001", 6, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath + "/*/*/*")
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

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(3, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("commit", commits(1))
    assertEquals("replacecommit", commits(2))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteTableModeUseReplaceActionOnDisJointPartitions(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 more records using SaveMode.Overwrite for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath + "/*/*/*")
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

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testDropInsertDup(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords =  dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = recordsToStrings(inserts1).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = recordsToStrings(inserts2Dup ++ inserts2New).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testComplexDataTypeWriteAndReadConsistency(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val schema = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)
      :: StructField("timeStampValue", TimestampType, true) :: StructField("dateValue", DateType, true)
      :: StructField("decimalValue", DataTypes.createDecimalType(15, 10), true) :: StructField("timestamp", IntegerType, true)
      :: StructField("partition", IntegerType, true) :: Nil)

    val records = Seq(Row("11", "Andy", Timestamp.valueOf("1970-01-01 13:31:24"), Date.valueOf("1991-11-07"), BigDecimal.valueOf(1.0), 11, 1),
      Row("22", "lisi", Timestamp.valueOf("1970-01-02 13:31:24"), Date.valueOf("1991-11-08"), BigDecimal.valueOf(2.0), 11, 1),
      Row("33", "zhangsan", Timestamp.valueOf("1970-01-03 13:31:24"), Date.valueOf("1991-11-09"), BigDecimal.valueOf(3.0), 11, 1))
    val rdd = jsc.parallelize(records)
    val  recordsDF = spark.createDataFrame(rdd, schema)
    recordsDF.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testWithAutoCommitOn(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  private def getDataFrameWriter(keyGenerator: String, opts: Map[String, String]): DataFrameWriter[Row] = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenerator)
      .mode(SaveMode.Overwrite)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithCustomKeyGenerator(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    // Without fieldType, the default is SIMPLE
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("current_ts").cast("string")).count() == 0)

    // Specify fieldType as TIMESTAMP
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)

    // Mixed fieldType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)

    // Test invalid partitionKeyType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer = writer.partitionBy("current_ts:DUMMY")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
    try {
      writer.save(basePath)
      fail("should fail when invalid PartitionKeyType is provided!")
    } catch {
      case e: Exception =>
        assertTrue(e.getCause.getMessage.contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"))
    }
  }

  @Test
  def testSparkPartitionByWithSimpleKeyGenerator() {
    val (writeOpts, readOpts) = getOpts(HoodieRecordType.AVRO)

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
    writer.partitionBy("driver", "rider")
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit(DEFAULT_PARTITION_PATH)).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithComplexKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

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
    val (writeOpts, readOpts) = getOpts(recordType)

    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithGlobalDeleteKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

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
    val (writeOpts, readOpts) = getOpts(recordType)

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

  @ParameterizedTest
  @CsvSource(Array(
    "true,false,AVRO", "true,true,AVRO", "false,true,AVRO", "false,false,AVRO",
    "true,false,SPARK", "true,true,SPARK", "false,true,SPARK", "false,false,SPARK"
  ))
  def testQueryCOWWithBasePathAndFileIndex(partitionEncode: Boolean, isMetadataEnabled: Boolean, recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
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
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
    assertEquals(false, Metrics.isInitialized)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSchemaEvolution(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    // open the schema validate
    val  opts = commonOpts ++ Map("hoodie.avro.schema.validate" -> "true") ++
      Map(DataSourceWriteOptions.RECONCILE_SCHEMA.key() -> "true") ++ writeOpts
    // 1. write records with schema1
    val schema1 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, false)::
      StructField("timestamp", IntegerType, true) :: StructField("partition", IntegerType, true)::Nil)
    val records1 = Seq(Row("1", "Andy", 1, 1),
      Row("2", "lisi", 1, 1),
      Row("3", "zhangsan", 1, 1))
    val rdd = jsc.parallelize(records1)
    val  recordsDF = spark.createDataFrame(rdd, schema1)
    recordsDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // 2. write records with schema2 add column age
    val schema2 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, false) ::
      StructField("age", StringType, true) :: StructField("timestamp", IntegerType, true) ::
      StructField("partition", IntegerType, true)::Nil)
    val records2 = Seq(Row("11", "Andy", "10", 1, 1),
      Row("22", "lisi", "11",1, 1),
      Row("33", "zhangsan", "12", 1, 1))
    val rdd2 = jsc.parallelize(records2)
    val  recordsDF2 = spark.createDataFrame(rdd2, schema2)
    recordsDF2.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)
    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    val tableMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build()
    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchemaWithoutMetadataFields
    assertTrue(actualSchema != null)
    val actualStructType = AvroConversionUtils.convertAvroSchemaToStructType(actualSchema)
    assertEquals(actualStructType, schema2)

    // 3. write records with schema4 by omitting a non nullable column(name). should fail
    try {
      val schema4 = StructType(StructField("_row_key", StringType, true) ::
        StructField("age", StringType, true) :: StructField("timestamp", IntegerType, true) ::
        StructField("partition", IntegerType, true)::Nil)
      val records4 = Seq(Row("11", "10", 1, 1),
        Row("22", "11",1, 1),
        Row("33", "12", 1, 1))
      val rdd4 = jsc.parallelize(records4)
      val  recordsDF4 = spark.createDataFrame(rdd4, schema4)
      recordsDF4.write.format("org.apache.hudi")
        .options(opts)
        .mode(SaveMode.Append)
        .save(basePath)
      fail("Delete column should fail")
    } catch {
      case ex: HoodieUpsertException =>
        assertTrue(ex.getMessage.equals("Failed upsert schema compatibility check."))
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSchemaNotEqualData(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val  opts = commonOpts ++ Map("hoodie.avro.schema.validate" -> "true")
    val schema1 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)::
      StructField("timestamp", IntegerType, true):: StructField("age", StringType, true)  :: StructField("partition", IntegerType, true)::Nil)
    val records = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}",
      "{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}")
    val inputDF = spark.read.schema(schema1.toDDL).json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    val resultSchema = new StructType(recordsReadDF.schema.filter(p=> !p.name.startsWith("_hoodie")).toArray)
    assertEquals(resultSchema, schema1)
  }

  @ParameterizedTest
  @CsvSource(Array("true, AVRO", "false, AVRO", "true, SPARK", "false, SPARK"))
  def testCopyOnWriteWithDroppedPartitionColumns(enableDropPartitionColumns: Boolean, recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInsertsContainsAllPartitions("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key, enableDropPartitionColumns)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val snapshotDF1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(snapshotDF1.count(), 100)
    assertEquals(3, snapshotDF1.select("partition").distinct().count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedCOW(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

    val numRecords = 100
    val numRecordsToDelete = 2
    val records0 = recordsToStrings(dataGen.generateInserts("000", numRecords)).toList
    val df0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    df0.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF0 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords, snapshotDF0.count())

    val df1 = snapshotDF0.limit(numRecordsToDelete)
    val dropDf = df1.drop(df1.columns.filter(_.startsWith("_hoodie_")): _*)
    val df2 = dropDf.withColumn("_hoodie_is_deleted", lit(true).cast(BooleanType))
    df2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords - numRecordsToDelete, snapshotDF2.count())
  }

  @Test
  def testWriteSmallPrecisionDecimalTable(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      .withColumn("shortDecimal", lit(new java.math.BigDecimal(s"2090.0000"))) // create decimalType(8, 4)
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // update the value of shortDecimal
    val inputDF2 = inputDF1.withColumn("shortDecimal", lit(new java.math.BigDecimal(s"3090.0000")))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val readResult = spark.read.format("hudi").load(basePath)
    assert(readResult.count() == 5)
    // compare the test result
    assertEquals(inputDF2.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","),
      readResult.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","))
  }

  @ParameterizedTest
  @CsvSource(Array("true, AVRO", "false, AVRO", "true, SPARK", "false, SPARK"))
  def testPartitionColumnsProperHandling(useGlobbing: Boolean, recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getOpts(recordType)

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
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "DATE_STRING")
      .option(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "yyyy-MM-dd")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyy/MM/dd")
      .option(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT+8:00")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save(basePath)

    // NOTE: We're testing here that both paths are appropriately handling
    //       partition values, regardless of whether we're reading the table
    //       t/h a globbed path or not
    val path = if (useGlobbing) {
      s"$basePath/*/*/*/*"
    } else {
      basePath
    }

    // Case #1: Partition columns are read from the data file
    val firstDF = spark.read.format("hudi").options(readOpts).load(path)

    assert(firstDF.count() == 2)

    // data_date is the partition field. Persist to the parquet file using the origin values, and read it.
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    val expectedValues = if (useGlobbing) {
      Seq("2018-09-23", "2018-09-24")
    } else {
      Seq("2018/09/23", "2018/09/24")
    }

    assertEquals(expectedValues, firstDF.select("data_date").map(_.get(0).toString).collect().sorted.toSeq)
    assertEquals(
      Seq("2018/09/23", "2018/09/24"),
      firstDF.select("_hoodie_partition_path").map(_.get(0).toString).collect().sorted.toSeq
    )

    // Case #2: Partition columns are extracted from the partition path
    //
    // NOTE: This case is only relevant when globbing is NOT used, since when globbing is used Spark
    //       won't be able to infer partitioning properly
    if (!useGlobbing) {
      val secondDF = spark.read.format("hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key, "true")
        .load(path)

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
    val options = scala.collection.mutable.Map.empty ++ commonOpts ++ Map("path" -> basePath)

    // first use the Overwrite mode
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(options)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")

    // init metaClient
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    assertEquals(spark.read.format("hudi").load(basePath).count(), 5)

    // use the Append mode
    val records2 = recordsToStrings(dataGen.generateInserts("002", 6)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(options)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").load(basePath).count(), 11)

    // use the Ignore mode
    val records3 = recordsToStrings(dataGen.generateInserts("003", 7)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(options)
      .mode(SaveMode.Ignore)
      .saveAsTable("hoodie_test")
    // nothing to do for the ignore mode
    assertEquals(spark.read.format("hudi").load(basePath).count(), 11)

    // use the ErrorIfExists mode
    val records4 = recordsToStrings(dataGen.generateInserts("004", 8)).toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    try {
      inputDF4.write.format("org.apache.hudi")
        .partitionBy("partition")
        .options(options)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable("hoodie_test")
    } catch {
      case e: Throwable => // do nothing
    }

    // use the Overwrite mode
    val records5 = recordsToStrings(dataGen.generateInserts("005", 9)).toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(options)
      .mode(SaveMode.Overwrite)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").load(basePath).count(), 9)
  }

  @Test
  def testMetricsReporterViaDataSource(): Unit = {
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts( 10))
    val recordsRDD = spark.sparkContext.parallelize(records, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true")
      .option(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), "CONSOLE")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    assertEquals(false, Metrics.isInitialized, "Metrics should be shutdown")
  }

  def getOpts(recordType: HoodieRecordType): (Map[String, String], Map[String, String]) = {
    val writeOpts = if (recordType == HoodieRecordType.SPARK) {
      commonOpts ++ sparkOpts
    } else {
      commonOpts
    }
    val readOpts = if (recordType == HoodieRecordType.SPARK) {
      sparkOpts
    } else {
      Map.empty[String, String]
    }

    (writeOpts, readOpts)
  }
}
