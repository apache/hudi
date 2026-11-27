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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.hash.{FileIndexID, PartitionIndexID}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.{JavaConversions, JFunction}

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, not}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import scala.collection.{mutable, JavaConverters}
import scala.collection.JavaConverters._

class TestBloomFiltersIndexSupport extends HoodieSparkClientTestBase {

  val sqlTempTable = "hudi_tbl_bloom"
  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key -> "true",
    HoodieMetadataConfig.BLOOM_FILTER_INDEX_FOR_COLUMNS.key -> "_row_key"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initQueryIndexConf()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()

    setTableName("hoodie_test")
    initMetaClient()

    instantTime = new AtomicInteger(1)

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testIndexInitialization(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateBloomFilters(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
  }

  /**
   * Test case to do a write with updates and then validate file pruning using bloom filters.
   */
  @Test
  def testBloomFiltersIndexFilePruning(): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts + (
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    doWriteAndValidateBloomFilters(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidate = false)
    doWriteAndValidateBloomFilters(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    createTempTable(hudiOpts)
    verifyQueryPredicate(hudiOpts, "_row_key")
  }

  /**
   * Regression for #20668: base-file blooms do not cover new keys in logs. A negative base
   * bloom must not prune a whole base+log slice, and log-only slices must also survive.
   * Layout: base-and-log = base(a) + log(b), base-only = base(c), log-only = log(d).
   */
  @Test
  def testBloomFiltersIndexWithLogOnlyKeys(): Unit = {
    val session = spark
    import session.implicits._

    // One bucket sends b to a's file group. Disable other query indexes and compaction
    // so that metadata bloom pruning is tested against records that remain in logs.
    val opts = commonOpts ++ Map(
      TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
      HoodieIndexConfig.INDEX_TYPE.key -> "BUCKET",
      HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key -> "1",
      HoodieIndexConfig.BUCKET_QUERY_INDEX.key -> "false",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.schedule.inline" -> "false",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    Seq(("a", "base-and-log", 1L), ("c", "base-only", 1L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(opts)
      .option(OPERATION.key, INSERT_OVERWRITE_OPERATION_OPT_VAL).mode(SaveMode.Overwrite).save(basePath)
    Seq(("b", "base-and-log", 2L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(opts)
      .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)
    // A new bucket would create a base file; INMEMORY allows a genuine log-only insert.
    Seq(("d", "log-only", 2L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(opts)
      .option(HoodieIndexConfig.INDEX_TYPE.key, "INMEMORY")
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)

    def query(keys: Seq[String], skipping: Boolean = true, readOptimized: Boolean = false): Set[String] = {
      spark.read.format("hudi").options(opts)
        .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, skipping.toString)
        .option(DataSourceReadOptions.QUERY_TYPE.key,
          if (readOptimized) DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL else DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
        .load(basePath).filter(col("_row_key").isin(keys: _*)).select("_row_key").as[String].collect().toSet
    }

    // Without skipping all four keys are visible. Bloom pruning must preserve both log keys
    // and the base record a in the retained base+log slice (not just its log files).
    val allKeys = Seq("a", "b", "c", "d")
    assertEquals(allKeys.toSet, query(allKeys, skipping = false))
    assertEquals(allKeys.toSet, query(allKeys))
    assertEquals(Set("b"), query(Seq("b")))
    assertEquals(Set("d"), query(Seq("d")))
    assertEquals(Set("b", "d"), query(Seq("b", "d")))
    assertEquals(Set.empty[String], query(Seq("missing")))

    // Read-optimized only reads bases: a and c are visible, while log-only keys b and d are not.
    assertEquals(Set("a", "c"), query(allKeys, readOptimized = true))
    assertEquals(Set.empty[String], query(Seq("b", "d"), readOptimized = true))
  }

  @Test
  def testBloomPruningWithDeclaredIndexType(): Unit = {
    val session = spark
    import session.implicits._

    val opts = commonOpts ++ Map(
      TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
      HoodieIndexConfig.BUCKET_QUERY_INDEX.key -> "false",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.schedule.inline" -> "false",
      "hoodie.parquet.small.file.limit" -> "0",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    val writeOpts = opts + (HoodieIndexConfig.INDEX_TYPE.key -> "SIMPLE")

    // SIMPLE writes new keys into bases; updating a appends a log whose key is covered by its base bloom.
    Seq(("a", "base-and-log", 1L), ("c", "base-only", 1L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(writeOpts)
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL).mode(SaveMode.Overwrite).save(basePath)
    Seq(("a", "base-and-log", 2L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(writeOpts)
      .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)
    // Even with a declared safe type, a slice without a base must never be pruned by base blooms.
    Seq(("d", "log-only", 2L)).toDF("_row_key", "partition", "timestamp")
      .write.format("hudi").options(opts)
      .option(HoodieIndexConfig.INDEX_TYPE.key, "INMEMORY")
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)

    val safeTypes = Seq("SIMPLE", "GLOBAL_SIMPLE", "BLOOM", "GLOBAL_BLOOM",
      "RECORD_INDEX", "RECORD_LEVEL_INDEX", "GLOBAL_RECORD_LEVEL_INDEX", "simple")
    val cases = safeTypes.map(indexType => (Map(HoodieIndexConfig.INDEX_TYPE.key -> indexType), true)) ++ Seq(
      (Map.empty[String, String], false),
      (Map(HoodieIndexConfig.INDEX_TYPE.key -> "BUCKET"), false),
      (Map(HoodieIndexConfig.INDEX_TYPE.key -> "INMEMORY"), false),
      (Map(HoodieIndexConfig.INDEX_TYPE.key -> "UNKNOWN"), false),
      (Map(HoodieIndexConfig.INDEX_TYPE.key -> "SIMPLE",
        HoodieIndexConfig.INDEX_CLASS_NAME.key -> "example.CustomIndex"), false))
    val missingKey = EqualTo(attribute("_row_key"), Literal("missing"))
    cases.foreach { case (indexOptions, canPruneLogs) =>
      val readOpts = opts ++ indexOptions + ("path" -> basePath)
      val fileIndex = HoodieFileIndex(spark, metaClient, None, readOpts, includeLogFiles = true)
      val retained = fileIndex.filterFileSlices(Seq(missingKey), Seq.empty).flatMap(_._2)
      val expected = if (canPruneLogs) Set("log-only") else Set("base-and-log", "log-only")
      assertEquals(expected, retained.map(_.getPartitionPath).toSet, indexOptions.toString)
      // Read-optimized strips logs before pruning, regardless of the declared index configuration.
      val readOptimizedIndex = HoodieFileIndex(spark, metaClient, None, readOpts, includeLogFiles = false)
      assertTrue(readOptimizedIndex.filterFileSlices(Seq(missingKey), Seq.empty).flatMap(_._2).isEmpty, indexOptions.toString)
    }

    // A positive base bloom retains the whole slice, so the updated log value wins; log-only d survives too.
    val rows = spark.read.format("hudi").options(writeOpts).load(basePath)
      .filter(col("_row_key").isin("a", "d")).select("_row_key", "timestamp").as[(String, Long)].collect().toSet
    assertEquals(Set(("a", 2L), ("d", 2L)), rows)
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.createOrReplaceTempView(sqlTempTable)
  }

  def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String): Unit = {
    val reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs(columnName).toString)
    val dataFilter = EqualTo(attribute(columnName), Literal(reckey(0)))
    verifyFilePruning(hudiOpts, dataFilter)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }


  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount < getLatestDataFilesCount(opts))

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filesCountWithNoSkipping == getLatestDataFilesCount(opts))
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    getTableFileSystemView(opts).getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
      .values()
      .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
        (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
          slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
            + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    new HoodieTableFileSystemView(metadataWriter(getWriteConfig(opts)).getTableMetadata, metaClient, metaClient.getActiveTimeline)
  }

  private def doWriteAndValidateBloomFilters(hudiOpts: Map[String, String],
                                             operation: String,
                                             saveMode: SaveMode,
                                             shouldValidate: Boolean = true): Unit = {
    var latestBatch: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 1))
      records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 1)))
      latestBatch = records.asScala
    } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
      latestBatch = recordsToStrings(dataGen.generateInsertsForPartition(
        getInstantTime(), 5, dataGen.getPartitionPaths.last)).asScala
    } else {
      latestBatch = recordsToStrings(dataGen.generateInserts(getInstantTime(), 5)).asScala
    }
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch.toSeq, 2))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    calculateMergedDf(latestBatchDf, operation)
    if (shouldValidate) {
      validateBloomFiltersIndex(hudiOpts)
    }
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }

  private def validateBloomFiltersIndex(hudiOpts: Map[String, String]): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()

    assertTrue(rowArr.length > 0)

    val bloomFilterMetadataMap = spark.read.format("hudi")
      .load(HoodieTableMetadata.getMetadataTableBasePath(basePath))
      .where("type = 4")
      .select("key", "BloomFilterMetadata")
      .collect()
      .map(row => (row.get(0), row.getStruct(1)))
      .toMap

    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val commitTime: String = FSUtils.getCommitTime(fileName)
      val bloomFilterRecordKey = HoodieMetadataPayload.getBloomFilterIndexKey(
        new PartitionIndexID(partitionPath), new FileIndexID(fileName))
      // Validate bloom filter record in the metadata table
      assertTrue(bloomFilterMetadataMap.contains(bloomFilterRecordKey))
      assertEquals(commitTime, bloomFilterMetadataMap(bloomFilterRecordKey).get(1))

      // Validate table metadata API fetching the bloom filter
      val bloomFilter = metadata.getBloomFilter(partitionPath, fileName)
      assertTrue(bloomFilter.isPresent, "BloomFilter should be present for " + fileName)
      assertTrue(bloomFilter.get().mightContain(recordKey))
    }
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  private def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ latestBatchDf
      sparkSession.emptyDataFrame
    } else {
      if (operation == INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL) {
        mergedDfList = mergedDfList :+ latestBatchDf
        prevDfOpt.get
      } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
        val overwrittenPartitions = latestBatchDf.select("partition")
          .collectAsList().stream().map[String](JavaConversions.getFunction[Row, String](r => r.getString(0))).collect(Collectors.toList[String])
        val prevDf = prevDfOpt.get
        val latestSnapshot = prevDf
          .filter(not(col("partition").isInCollection(overwrittenPartitions)))
          .union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot
        prevDf.filter(col("partition").isInCollection(overwrittenPartitions))
      } else {
        val prevDf = prevDfOpt.get
        val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key")
          && prevDf("partition") === latestBatchDf("partition") && prevDf("trip_type") === latestBatchDf("trip_type"), "leftanti")
        val latestSnapshot = prevDfOld.union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot
        sparkSession.emptyDataFrame
      }
    }
  }

}
