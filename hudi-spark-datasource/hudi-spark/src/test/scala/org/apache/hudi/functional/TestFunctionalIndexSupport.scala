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

import org.apache.hudi.DataSourceWriteOptions.{INSERT_OPERATION_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.DataSourceReadOptions.ENABLE_DATA_SKIPPING
import org.apache.hudi.{FunctionalIndexSupport, HoodieFileIndex}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, ByteType, DateType, DecimalType, IntegerType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util.stream.Collectors

class TestFunctionalIndexSupport extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var dfList: Seq[DataFrame] = Seq()
  val sourceTableSchema: StructType = new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9, 3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

  @BeforeEach
  override def setUp() {
    initPath()
    initQueryIndexConf()
    initSparkContexts()
    initHoodieStorage()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @Test
  def testFunctionalIndexUsingColumnStatsWithPartitionAndFilesFilter(): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key -> "true"
    )
    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_fi",
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8"
    )
    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)
    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // Create a functional index on column c6
    spark.sql(s"create table hoodie_test_fi using hudi location '$basePath'")
    spark.sql(s"create index idx_datestr on hoodie_test_fi using column_stats(c6) options(func='identity')")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
    assertTrue(metaClient.getIndexMetadata.isPresent)
    assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

    // check functional index records
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()
    val functionalIndexSupport = new FunctionalIndexSupport(spark, metadataConfig, metaClient)
    val prunedPartitions = Set("9")
    var indexDf = functionalIndexSupport.loadFunctionalIndexDataFrame("func_index_idx_datestr", prunedPartitions, shouldReadInMemory = true)
    // check only one record returned corresponding to the pruned partition
    assertTrue(indexDf.count() == 1)
    // select fileName from indexDf
    val fileName = indexDf.select("fileName").collect().map(_.getString(0)).head
    val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient,
      HoodieMetadataConfig.newBuilder().enable(false).build())
    fsv.loadAllPartitions()
    val partitionPaths = fsv.getPartitionPaths
    val partitionToBaseFiles : java.util.Map[String, java.util.List[StoragePath]] = new java.util.HashMap[String, java.util.List[StoragePath]]
    // select file names for each partition from file system view
    partitionPaths.forEach(partitionPath =>
      partitionToBaseFiles.put(partitionPath.getName, fsv.getLatestBaseFiles(partitionPath.getName)
        .map[StoragePath](baseFile => baseFile.getStoragePath).collect(Collectors.toList[StoragePath]))
    )
    fsv.close()
    val expectedFileNames = partitionToBaseFiles.get(prunedPartitions.head).stream().map[String](baseFile => baseFile.getName).collect(Collectors.toSet[String])
    assertTrue(expectedFileNames.size() == 1)
    // verify the file names match
    assertTrue(expectedFileNames.contains(fileName))

    // check more records returned if no partition filter provided
    indexDf = functionalIndexSupport.loadFunctionalIndexDataFrame("func_index_idx_datestr", Set(), shouldReadInMemory = true)
    assertTrue(indexDf.count() > 1)
  }

  @Test
  def testComputeCandidateFileNames(): Unit = {
    // in this test, we will create a table with inserts going to log file so that there is a file slice with only log file and no base file
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key -> "true"
    )
    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_lf_only",
      TABLE_TYPE.key -> "MERGE_ON_READ",
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8",
      // setting IndexType to be INMEMORY to simulate Global Index nature
      HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.INMEMORY.name()
    )
    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)
    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // Create a functional index on column c6
    spark.sql(s"create table hoodie_test_lf_only using hudi location '$basePath'")
    spark.sql(s"create index idx_datestr on hoodie_test_lf_only using column_stats(c6) options(func='identity')")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
    assertTrue(metaClient.getIndexMetadata.isPresent)
    assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())
    // check functional index records
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()
    val fileIndex = new HoodieFileIndex(sparkSession, metaClient, Option.empty, opts ++ metadataOpts ++ Map("glob.paths" -> s"$basePath/9", ENABLE_DATA_SKIPPING.key -> "true"), includeLogFiles = true)
    val functionalIndexSupport = new FunctionalIndexSupport(spark, metadataConfig, metaClient)
    val partitionFilter: Expression = EqualTo(AttributeReference("c8", IntegerType)(), Literal(9))
    val (isPruned, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
    assertTrue(isPruned)
    val prunedPartitionAndFileNames = functionalIndexSupport.getPrunedPartitionsAndFileNames(prunedPaths, includeLogFiles = true)
    assertTrue(prunedPartitionAndFileNames._1.size == 1) // partition
    assertTrue(prunedPartitionAndFileNames._2.size == 1) // log file
    assertTrue(FSUtils.isLogFile(prunedPartitionAndFileNames._2.head))
  }
}
