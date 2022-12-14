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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.DataSourceReadOptions.{FILE_INDEX_LISTING_MODE_EAGER, FILE_INDEX_LISTING_MODE_LAZY, QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieFileIndex.DataSkippingFailureMode
import org.apache.hudi.client.HoodieJavaWriteClient
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.engine.EngineType
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.TimestampType
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.execution.datasources.{NoopCache, PartitionDirectory}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, MethodSource, ValueSource}
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random

class TestHoodieFileIndex extends HoodieClientTestBase with ScalaAssertionSupport {

  var spark: SparkSession = _
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  var queryOpts = Map(
    DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
  )

  @BeforeEach
  override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
    initMetaClient()

    queryOpts = queryOpts ++ Map("path" -> basePath)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionSchema(partitionEncode: Boolean): Unit = {
    val props = new Properties()
    props.setProperty(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, String.valueOf(partitionEncode))
    initMetaClient(props)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, queryOpts)
    assertEquals("partition", fileIndex.partitionSchema.fields.map(_.name).mkString(","))
  }

  @ParameterizedTest
  @MethodSource(Array("keyGeneratorParameters"))
  def testPartitionSchemaForBuiltInKeyGenerator(keyGenerator: String): Unit = {
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    val writer: DataFrameWriter[Row] = inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, TimestampType.DATE_STRING.name())
      .option(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "yyyy/MM/dd")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyy-MM-dd")
      .mode(SaveMode.Overwrite)

    if (isNullOrEmpty(keyGenerator)) {
      writer.save(basePath)
    } else {
      writer.option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenerator)
        .save(basePath)
    }

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, queryOpts)
    assertEquals("partition", fileIndex.partitionSchema.fields.map(_.name).mkString(","))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(
    "org.apache.hudi.keygen.CustomKeyGenerator",
    "org.apache.hudi.keygen.CustomAvroKeyGenerator"))
  def testPartitionSchemaForCustomKeyGenerator(keyGenerator: String): Unit = {
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenerator)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partition:simple")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, queryOpts)
    assertEquals("partition", fileIndex.partitionSchema.fields.map(_.name).mkString(","))
  }

  @Test
  def testPartitionSchemaWithoutKeyGenerator(): Unit = {
    val metaClient = HoodieTestUtils.init(
      hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, HoodieTableMetaClient.withPropertyBuilder()
        .fromMetaClient(this.metaClient)
        .setRecordKeyFields("_row_key")
        .setPartitionFields("partition_path")
        .setTableName("hoodie_test").build())
    val props = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
    )
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withEngineType(EngineType.JAVA)
      .withPath(basePath)
      .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
      .withProps(props)
      .build()
    val context = new HoodieJavaEngineContext(new Configuration())
    val writeClient = new HoodieJavaWriteClient(context, writeConfig)
    val instantTime = makeNewCommitTime()

    val records: java.util.List[HoodieRecord[Nothing]] =
      dataGen.generateInsertsContainsAllPartitions(instantTime, 100)
        .asInstanceOf[java.util.List[HoodieRecord[Nothing]]]
    writeClient.startCommitWithTime(instantTime)
    writeClient.insert(records, instantTime)
    metaClient.reloadActiveTimeline()

    val fileIndex = HoodieFileIndex(spark, metaClient, None, queryOpts)
    assertEquals("partition_path", fileIndex.partitionSchema.fields.map(_.name).mkString(","))
  }

  @ParameterizedTest
  @CsvSource(Array("true,true", "true,false", "false,true", "false,false"))
  def testPartitionPruneWithPartitionEncode(partitionEncode: Boolean, listLazily: Boolean): Unit = {
    val props = new Properties()
    props.setProperty(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, String.valueOf(partitionEncode))
    initMetaClient(props)
    val partitions = Array("2021/03/08", "2021/03/09", "2021/03/10", "2021/03/11", "2021/03/12")
    val newDataGen = new HoodieTestDataGenerator(partitions)
    val records1 = newDataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)

    val listingMode = if (listLazily) FILE_INDEX_LISTING_MODE_LAZY else FILE_INDEX_LISTING_MODE_EAGER

    val opts = queryOpts + (DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key -> listingMode)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, opts)

    val partitionFilter1 = EqualTo(attribute("partition"), literal("2021/03/08"))
    val partitionName = if (partitionEncode) PartitionPathEncodeUtils.escapePathName("2021/03/08")
    else "2021/03/08"
    val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilter1), Seq.empty)
    assertEquals(1, partitionAndFilesAfterPrune.size)

    val PartitionDirectory(partitionValues, filesInPartition) = partitionAndFilesAfterPrune(0)
    assertEquals(partitionValues.toSeq(Seq(StringType)).mkString(","), "2021/03/08")
    assertEquals(getFileCountInPartitionPath(partitionName), filesInPartition.size)

    val partitionFilter2 = And(
      GreaterThanOrEqual(attribute("partition"), literal("2021/03/08")),
      LessThan(attribute("partition"), literal("2021/03/10"))
    )
    val prunedPartitions = fileIndex.listFiles(Seq(partitionFilter2), Seq.empty)
      .map(_.values.toSeq(Seq(StringType))
      .mkString(","))
      .toList
      .sorted

    assertEquals(List("2021/03/08", "2021/03/09"), prunedPartitions)
  }

  @ParameterizedTest
  @CsvSource(value = Array("lazy,true,true", "lazy,true,false", "lazy,false,true", "lazy,false,false",
    "eager,true,true", "eager,true,false", "eager,false,true", "eager,false,false"))
  def testPartitionPruneWithMultiplePartitionColumns(listingModeOverride: String,
                                                     useMetadataTable: Boolean,
                                                     enablePartitionPathPrefixAnalysis: Boolean): Unit = {
    val _spark = spark
    import _spark.implicits._

    val writerOpts: Map[String, String] = commonOpts ++ Map(
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      RECORDKEY_FIELD.key -> "id",
      PRECOMBINE_FIELD.key -> "version",
      PARTITIONPATH_FIELD.key -> "dt,hh",
      KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName,
      HoodieMetadataConfig.ENABLE.key -> useMetadataTable.toString
    )

    val readerOpts: Map[String, String] = queryOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> useMetadataTable.toString,
      DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key -> listingModeOverride,
      DataSourceReadOptions.FILE_INDEX_LISTING_PARTITION_PATH_PREFIX_ANALYSIS_ENABLED.key -> enablePartitionPathPrefixAnalysis.toString
    )

    var fileIndex: HoodieFileIndex = null

    {
      // Case #1: Partition pruning expected to be able to prune partitions successfully (based on provided filters)

      // Test the case the partition column size is equal to the partition directory level.
      val inputDF1 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 10000,
        s"2021-03-0${i % 2 + 1}", "10")).toDF("id", "name", "price", "version", "dt", "hh")

      inputDF1.write.format("hudi")
        .options(writerOpts)
        .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, "false")
        .mode(SaveMode.Overwrite)
        .save(basePath)

      // NOTE: We're init-ing file-index in advance to additionally test refreshing capability
      metaClient = HoodieTableMetaClient.reload(metaClient)
      fileIndex = HoodieFileIndex(spark, metaClient, None, readerOpts)


      val partitionFilters = And(
        EqualTo(attribute("dt"), literal("2021-03-01")),
        EqualTo(attribute("hh"), literal("10"))
      )

      val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilters), Seq.empty)
      assertEquals(1, partitionAndFilesAfterPrune.size)

      val PartitionDirectory(partitionValues, filesAfterPrune) = partitionAndFilesAfterPrune.head
      assertEquals("2021-03-01,10", partitionValues.toSeq(Seq(StringType)).mkString(","))
      assertEquals(getFileCountInPartitionPath("2021-03-01/10"), filesAfterPrune.size)

      val readDF = spark.read.format("hudi").options(readerOpts).load()

      assertEquals(10, readDF.count())
      assertEquals(5, readDF.filter("dt = '2021-03-01' and hh = '10'").count())
    }

    {
      // Case #2: Partition pruning expected to NOT be able to prune partitions successfully (due to
      //          non-encoded slash being used w/in partition-value)

      // Test the case that partition column size not match the partition directory level and
      // partition column size is > 1. We will not trait it as partitioned table when read.
      val inputDF2 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 100 * i + 10000,
        s"2021/03/0${i % 2 + 1}", "10")).toDF("id", "name", "price", "version", "dt", "hh")

      inputDF2.write.format("hudi")
        .options(writerOpts)
        .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, "false")
        .mode(SaveMode.Overwrite)
        .save(basePath)

      // NOTE: That if file-index is in lazy-listing mode and we can't parse partition values, there's no way
      //       to recover from this since Spark by default have to inject partition values parsed from the partition paths.
      if (listingModeOverride == DataSourceReadOptions.FILE_INDEX_LISTING_MODE_LAZY) {
        assertThrows(classOf[HoodieException]) { fileIndex.refresh() }
      } else {
        fileIndex.refresh()

        val partitionFilter2 = And(
          EqualTo(attribute("dt"), literal("2021/03/01")),
          EqualTo(attribute("hh"), literal("10"))
        )

        val partitionAndFilesNoPruning = fileIndex.listFiles(Seq(partitionFilter2), Seq.empty)

        assertEquals(1, partitionAndFilesNoPruning.size)
        // The partition prune would not work for this case, so the partition value it
        // returns is a InternalRow.empty.
        assertTrue(partitionAndFilesNoPruning.forall(_.values.numFields == 0))
        // The returned file size should equal to the whole file size in all the partition paths.
        assertEquals(getFileCountInPartitionPaths("2021/03/01/10", "2021/03/02/10"),
          partitionAndFilesNoPruning.flatMap(_.files).length)

        val readDF = spark.read.format("hudi").options(readerOpts).load()

        assertEquals(10, readDF.count())
        // There are 5 rows in the  dt = 2021/03/01 and hh = 10
        assertEquals(5, readDF.filter("dt = '2021/03/01' and hh ='10'").count())
      }
    }

    {
      // Case #3: Partition pruning expected to be able to prune partitions successfully (slashes
      //          will be URL-encoded inside partition-values)

      val inputDF3 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 100 * i + 10000,
        s"2021/03/0${i % 2 + 1}", "10")).toDF("id", "name", "price", "version", "dt", "hh")

      inputDF3.write.format("hudi")
        .options(writerOpts)
        .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, "true")
        .mode(SaveMode.Overwrite)
        .save(basePath)

      fileIndex.refresh()

      val partitionFilters = And(
        EqualTo(attribute("dt"), literal("2021/03/01")),
        EqualTo(attribute("hh"), literal("10"))
      )

      val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilters), Seq.empty)
      assertEquals(1, partitionAndFilesAfterPrune.size)

      val PartitionDirectory(partitionValues, filesAfterPrune) = partitionAndFilesAfterPrune.head
      assertEquals("2021/03/01,10", partitionValues.toSeq(Seq(StringType)).mkString(","))
      assertEquals(getFileCountInPartitionPath("2021%2F03%2F01/10"), filesAfterPrune.size)

      val readDF = spark.read.format("hudi").options(readerOpts).load()

      assertEquals(10, readDF.count())
      assertEquals(5, readDF.filter("dt = '2021/03/01' and hh = '10'").count())
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array("true", "false"))
  def testFileListingPartitionPrefixAnalysis(enablePartitionPathPrefixAnalysis: Boolean): Unit = {
    val _spark = spark
    import _spark.implicits._

    val writerOpts: Map[String, String] = commonOpts ++ Map(
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      RECORDKEY_FIELD.key -> "id",
      PRECOMBINE_FIELD.key -> "version",
      PARTITIONPATH_FIELD.key -> "dt,hh",
      KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName
    )

    val readerOpts: Map[String, String] = queryOpts ++ Map(
      DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key -> "eager",
      DataSourceReadOptions.FILE_INDEX_LISTING_PARTITION_PATH_PREFIX_ANALYSIS_ENABLED.key -> enablePartitionPathPrefixAnalysis.toString
    )

    // Test the case the partition column size is equal to the partition directory level.
    val inputDF1 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 10000,
      s"2021/03/0${i % 2 + 1}", s"${i % 3}")).toDF("id", "name", "price", "version", "dt", "hh")

    inputDF1.write.format("hudi")
      .options(writerOpts)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, readerOpts)

    val partitionFilters = EqualTo(attribute("dt"), literal("2021/03/01"))
    val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilters), Seq.empty)

    // In this case only partitions nested under `2021-03-01` folder should be listed
    assertEquals(1, partitionAndFilesAfterPrune.size)

    val (partitionValuesSeq, perPartitionFilesSeq) = partitionAndFilesAfterPrune.map {
      case PartitionDirectory(values, files) => (values.toSeq(Seq(StringType)), files)
    }.unzip

    val expectedListedFiles = if (enablePartitionPathPrefixAnalysis) {
      getFileCountInPartitionPaths("2021/03/01/0", "2021/03/01/1", "2021/03/01/2")
    } else {
      fileIndex.allFiles.length
    }

    assertEquals(expectedListedFiles, perPartitionFilesSeq.map(_.size).sum)

    val readDF = spark.read.format("hudi").options(readerOpts).load()

    assertEquals(10, readDF.count())
    assertEquals(3, readDF.filter("hh = '1'").count())
    assertEquals(5, readDF.filter("dt = '2021/03/01'").count())
    assertEquals(1, readDF.filter("dt = '2021/03/01' and hh = '1'").count())
  }

  @ParameterizedTest
  @CsvSource(Array("true,a.b.c","false,a.b.c","true,c","false,c"))
  def testQueryPartitionPathsForNestedPartition(useMetaFileList:Boolean, partitionBy:String): Unit = {
    val inputDF = spark.range(100)
      .withColumn("c",lit("c"))
      .withColumn("b",struct("c"))
      .withColumn("a",struct("b"))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(PARTITIONPATH_FIELD.key, partitionBy)
      .option(HoodieMetadataConfig.ENABLE.key(), useMetaFileList)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None,
      queryOpts ++ Map(HoodieMetadataConfig.ENABLE.key -> useMetaFileList.toString))
    // test if table is partitioned on nested columns, getAllQueryPartitionPaths does not break
    assert(fileIndex.getAllQueryPartitionPaths.get(0).path.equals("c"))
  }

  @Test
  def testDataSkippingWhileFileListing(): Unit = {
    val r = new Random(0xDEED)
    val tuples = for (i <- 1 to 1000) yield (i, 1000 - i, r.nextString(5), r.nextInt(4))

    val _spark = spark
    import _spark.implicits._
    val inputDF = tuples.toDF("id", "inv_id", "str", "rand")

    val writeMetadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "id",
      PRECOMBINE_FIELD.key -> "id",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ writeMetadataOpts

    // If there are any failures in the Data Skipping flow, test should fail
    spark.sqlContext.setConf(DataSkippingFailureMode.configName, DataSkippingFailureMode.Strict.value);

    inputDF.repartition(4)
      .write
      .format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 100 * 1024)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    case class TestCase(enableMetadata: Boolean,
                        enableColumnStats: Boolean,
                        enableDataSkipping: Boolean,
                        columnStatsProcessingModeOverride: String = null)

    val testCases: Seq[TestCase] =
      TestCase(enableMetadata = false, enableColumnStats = false, enableDataSkipping = false) ::
      TestCase(enableMetadata = false, enableColumnStats = false, enableDataSkipping = true) ::
      TestCase(enableMetadata = true, enableColumnStats = false, enableDataSkipping = true) ::
      TestCase(enableMetadata = false, enableColumnStats = true, enableDataSkipping = true) ::
      TestCase(enableMetadata = true, enableColumnStats = true, enableDataSkipping = true) ::
      TestCase(enableMetadata = true, enableColumnStats = true, enableDataSkipping = true, columnStatsProcessingModeOverride = HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY) ::
      TestCase(enableMetadata = true, enableColumnStats = true, enableDataSkipping = true, columnStatsProcessingModeOverride = HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE) ::
      Nil

    for (testCase <- testCases) {
      val readMetadataOpts = Map(
        // NOTE: Metadata Table has to be enabled on the read path as well
        HoodieMetadataConfig.ENABLE.key -> testCase.enableMetadata.toString,
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> testCase.enableColumnStats.toString,
        HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
      )

      val props = Map[String, String](
        "path" -> basePath,
        QUERY_TYPE.key -> QUERY_TYPE_SNAPSHOT_OPT_VAL,
        HoodieMetadataConfig.ENABLE.key -> testCase.enableMetadata.toString,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> testCase.enableDataSkipping.toString,
        HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE.key -> testCase.columnStatsProcessingModeOverride
      ) ++ readMetadataOpts

      val fileIndex = HoodieFileIndex(spark, metaClient, Option.empty, props, NoopCache)

      val allFilesPartitions = fileIndex.listFiles(Seq(), Seq())
      assertEquals(10, allFilesPartitions.head.files.length)

      if (testCase.enableDataSkipping && testCase.enableMetadata) {
        // We're selecting a single file that contains "id" == 1 row, which there should be
        // strictly 1. Given that 1 is minimal possible value, Data Skipping should be able to
        // truncate search space to just a single file
        val dataFilter = EqualTo(AttributeReference("id", IntegerType, nullable = false)(), Literal(1))
        val filteredPartitions = fileIndex.listFiles(Seq(), Seq(dataFilter))
        assertEquals(1, filteredPartitions.head.files.length)
      }
    }
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def getFileCountInPartitionPath(partitionPath: String): Int = {
    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants)
    fileSystemView.getAllBaseFiles(partitionPath).iterator().asScala.toSeq.length
  }

  private def getFileCountInPartitionPaths(partitionPaths: String*): Int = {
    partitionPaths.map(getFileCountInPartitionPath).sum
  }
}

object TestHoodieFileIndex {

  def keyGeneratorParameters(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(null.asInstanceOf[String]),
      Arguments.arguments("org.apache.hudi.keygen.ComplexKeyGenerator"),
      Arguments.arguments("org.apache.hudi.keygen.SimpleKeyGenerator"),
      Arguments.arguments("org.apache.hudi.keygen.TimestampBasedKeyGenerator")
    )
  }
}
