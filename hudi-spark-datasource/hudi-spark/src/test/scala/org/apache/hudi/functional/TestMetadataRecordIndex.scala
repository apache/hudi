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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieMetadataPayload, HoodieRecordIndexEntrySizeEstimator, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.metadata.model.FileSliceAndPartition
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, lpad, sha2, substring, when}
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

@Tag("functional")
class TestMetadataRecordIndex extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts = Map(
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
  override def setUp() {
    initPath()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()

    setTableName("hoodie_test")
    initMetaClient()

    instantTime = new AtomicInteger(1)

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  protected def withRDDPersistenceValidation(f: => Unit): Unit = {
    org.apache.hudi.testutils.SparkRDDValidationUtils.withRDDPersistenceValidation(spark, new org.apache.hudi.testutils.SparkRDDValidationUtils.ThrowingRunnable {
      override def run(): Unit = f
    })
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testClusteringWithRecordIndex(tableType: HoodieTableType): Unit = {
    withRDDPersistenceValidation {
      val hudiOpts = commonOpts ++ Map(
        TABLE_TYPE.key -> tableType.name(),
        HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
        HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "2"
      )

      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Overwrite)
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)

      val lastClusteringInstant = getLatestClusteringInstant()
      assertTrue(lastClusteringInstant.isPresent)

      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)

      assertTrue(getLatestClusteringInstant().get().requestedTime.compareTo(lastClusteringInstant.get().requestedTime) > 0)
      validateDataAndRecordIndices(hudiOpts)
    }
  }

  private def getLatestClusteringInstant(): Option[HoodieInstant] = {
    metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant()
  }

  private def doWriteAndValidateDataAndRecordIndex(hudiOpts: Map[String, String],
                                                   operation: String,
                                                   saveMode: SaveMode,
                                                   validate: Boolean = true): DataFrame = {
    var records1: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 20))
      records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 20)))
      records1 = records.asScala
    } else {
      records1 = recordsToStrings(dataGen.generateInserts(getInstantTime(), 100)).asScala
    }
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1.toSeq, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    calculateMergedDf(inputDF1)
    if (validate) {
      validateDataAndRecordIndices(hudiOpts)
    }
    inputDF1
  }

  def calculateMergedDf(inputDF1: DataFrame): Unit = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ inputDF1
    } else {
      val prevDf = prevDfOpt.get
      val prevDfOld = prevDf.join(inputDF1, prevDf("_row_key") === inputDF1("_row_key")
        && prevDf("partition") === inputDF1("partition"), "leftanti")
      val unionDf = prevDfOld.union(inputDF1)
      mergedDfList = mergedDfList :+ unionDf
    }
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.getAndIncrement()))
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    val tableMetadata = getHoodieTable(metaClient, writeConfig).getTableMetadata
    tableMetadata.asInstanceOf[HoodieBackedTableMetadata].getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX)
  }

  private def validateDataAndRecordIndices(hudiOpts: Map[String, String]): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val res = metadata.readRecordIndexLocationsWithKeys(
      HoodieListData.eager(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList.asJava));
    val recordIndexMap = HoodieDataUtils.dedupeAndCollectAsMap(res);
    res.unpersistWithDependencies()

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocation = recordIndexMap.get(recordKey)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      if (!writeConfig.inlineClusteringEnabled && !writeConfig.isAsyncClusteringEnabled) {
        // The file id changes after clustering, so only assert it for usual upsert and compaction operations
        assertTrue(fileName.contains(recordLocation.getFileId), fileName + " does not contain " + recordLocation.getFileId)
      }
    }

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(
      MetadataPartitionType.RECORD_INDEX, () => rowArr.length, 48,
      writeConfig.getGlobalRecordLevelIndexMinFileGroupCount, writeConfig.getGlobalRecordLevelIndexMaxFileGroupCount,
      writeConfig.getRecordIndexGrowthFactor, writeConfig.getRecordIndexMaxFileGroupSizeBytes)
    assertEquals(estimatedFileGroupCount, getFileGroupCountForRecordIndex(writeConfig))
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
  }

  /**
   * The global record index takes its file group count from an entry size estimated from the base file footers and
   * file slices, and that estimate covers, without far overshooting, the bytes its entries take in the HFiles for keys
   * of one uuid column. Wide keys of several columns whose neighbours differ in a few characters compress far better in
   * the base files than in the HFiles, so they may be estimated up to about 20% short. An entry size set in the config
   * takes the place of the estimate.
   */
  @ParameterizedTest
  @CsvSource(Array("uuid,", "composite,", "uuid,48"))
  def testGlobalRecordIndexFileGroupsFollowTheEstimatedEntrySize(keyShape: String, configuredEntrySize: String): Unit = {
    val rows = 200000L
    val id = col("id")
    val hash = sha2(id.cast("string"), 256)
    val uuid = concat_ws("-", substring(hash, 1, 8), substring(hash, 9, 4), substring(hash, 13, 4), substring(hash, 17, 4),
      substring(hash, 21, 12))
    // six key columns joined by the complex key generator as name:value pairs
    val compositeKeyFields = Seq("customer_account_id", "store_number", "department_code", "item_upc", "order_channel", "line_number")
    val keyColumns = if (keyShape == "composite") {
      Seq(concat(lit("CA"), lpad((id % 50000).cast("string"), 10, "0")).as(compositeKeyFields(0)),
        (id % 4700).cast("string").as(compositeKeyFields(1)), lpad((id % 97).cast("string"), 3, "0").as(compositeKeyFields(2)),
        lpad((id * 7919 % 1000000007L).cast("string"), 12, "0").as(compositeKeyFields(3)),
        when(id % 2 === 0, "ONLINE").otherwise("STORE").as(compositeKeyFields(4)), id.cast("string").as(compositeKeyFields(5)))
    } else {
      Seq(uuid.as("_row_key"))
    }
    val df = spark.range(rows).select(keyColumns ++ Seq(when(id % 2 === 0, "p1").otherwise("p2").as("partition"), id.as("timestamp")): _*)
    val keyOpts = if (keyShape == "composite") {
      Map(RECORDKEY_FIELD.key -> compositeKeyFields.mkString(","), KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName)
    } else {
      Map.empty[String, String]
    }
    val entrySizeOpts = scala.Option(configuredEntrySize)
      .map(size => Map(HoodieMetadataConfig.RECORD_INDEX_AVERAGE_RECORD_SIZE_PROP.key -> size)).getOrElse(Map.empty[String, String])
    val writeOpts = commonOpts ++ keyOpts
    df.write.format("hudi").options(writeOpts + (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false"))
      .option(OPERATION.key, BULK_INSERT_OPERATION_OPT_VAL).mode(SaveMode.Overwrite).save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileSlices = storage.listFiles(new StoragePath(basePath)).asScala
      .filter(f => f.getPath.getName.endsWith(".parquet") && !f.getPath.toString.contains("/.hoodie/"))
      .map { f =>
        val baseFile = new HoodieBaseFile(f)
        val partition = FSUtils.getRelativePartitionPath(new StoragePath(basePath), f.getPath.getParent)
        val fileSlice = new FileSlice(partition, baseFile.getCommitTime, baseFile.getFileId)
        fileSlice.setBaseFile(baseFile)
        FileSliceAndPartition.of(partition, fileSlice)
      }
    lazy val entrySize = HoodieRecordIndexEntrySizeEstimator.estimateEntrySize(context, metaClient, fileSlices.asJava,
      HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_ENCODING_UUID,
      HoodieMetadataConfig.RECORD_INDEX_ENTRY_SIZE_KEY_FACTOR_PROP.defaultValue().toDouble)

    // 1MB file groups, so that the 200k entries spread over several; the HFile bloom filters are left out so that the
    // HFile sizes are those of the entries
    val maxFileGroupBytes = 1024 * 1024L
    df.limit(10).write.format("hudi").options(writeOpts ++ entrySizeOpts ++ Map(
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1000",
      HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key -> maxFileGroupBytes.toString,
      HoodieStorageConfig.HFILE_WITH_BLOOM_FILTER_ENABLED.key -> "false"))
      .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)

    val indexDir = new StoragePath(metaClient.getMetaPath, "metadata/" + MetadataPartitionType.RECORD_INDEX.getPartitionPath)
    val hfiles = storage.listDirectEntries(indexDir).asScala.filter(f => FSUtils.isBaseFile(f.getPath))
    val fileGroups = hfiles.map(f => FSUtils.getFileId(f.getPath.getName)).distinct.size
    def fileGroupsFor(size: Int): Int = math.max(1L, (rows * 2) / (maxFileGroupBytes / size)).toInt
    if (configuredEntrySize != null) {
      assertEquals(fileGroupsFor(configuredEntrySize.toInt), fileGroups)
    } else {
      // the writer evaluates the value size over its own file slice order, which can round the entry size a byte apart
      val expectedFileGroups = (entrySize - 1 to entrySize + 1).map(fileGroupsFor)
      assertTrue(expectedFileGroups.contains(fileGroups), s"$fileGroups file groups, expected one of $expectedFileGroups")
      // the index is initialized in one commit, so each file group has one base file holding its share of the entries
      val actualEntrySize = hfiles.map(_.getLength).sum.toDouble / rows
      val lowestEntrySize = if (keyShape == "composite") 0.8 * actualEntrySize else actualEntrySize
      assertTrue(entrySize >= lowestEntrySize && entrySize <= 1.5 * actualEntrySize,
        s"estimated $entrySize bytes per entry against $actualEntrySize in the HFiles")
    }
  }
}
