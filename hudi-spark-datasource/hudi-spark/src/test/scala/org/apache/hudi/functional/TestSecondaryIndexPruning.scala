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

import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, HIVE_STYLE_PARTITIONING, INSERT_OVERWRITE_OPERATION_OPT_VAL, INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{ActionType, FileSlice, HoodieCommitMetadata, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, MetadataConversionUtils}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.TestSecondaryIndexPruning.SecondaryIndexTestCase
import org.apache.hudi.metadata.{HoodieBackedTableMetadataWriter, HoodieMetadataFileSystemView, SparkHoodieBackedTableMetadataWriter}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.util.{JFunction, JavaConversions}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, not}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}
import org.scalatest.Assertions.assertResult

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}

/**
 * Test cases for secondary index
 */
@Tag("functional")
class TestSecondaryIndexPruning extends SparkClientFunctionalTestHarness {

  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "record_key_col",
    PARTITIONPATH_FIELD.key -> "partition_key_col",
    HIVE_STYLE_PARTITIONING.key -> "true",
    PRECOMBINE_FIELD.key -> "ts"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty
  var tableName = "hoodie_"
  var metaClient: HoodieTableMetaClient = _
  var instantTime: AtomicInteger = new AtomicInteger(1)

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexWithFilters(testCase: SecondaryIndexTestCase): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = testCase.tableType
      val isPartitioned = testCase.isPartitioned
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_with_filters" + (if (isPartitioned) "_partitioned" else "") + sqlTableType
      val partitionedByClause = if (isPartitioned) "partitioned by(partition_key_col)" else ""

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  record_key_col string,
           |  not_record_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  type = '$sqlTableType',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | $partitionedByClause
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")

      // create another secondary index on non-string column
      spark.sql(s"create index idx_ts on $tableName using secondary_index(ts)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_ts"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "ts")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("1", "row1"),
        Seq("2", "row2"),
        Seq("3", "row3"),
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
    }
  }

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexPruningWithUpdates(testCase: SecondaryIndexTestCase): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = testCase.tableType
      val isPartitioned = testCase.isPartitioned
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_pruning_with_updates" + (if (isPartitioned) "_partitioned" else "") + sqlTableType
      val partitionedByClause = if (isPartitioned) "partitioned by(partition_key_col)" else ""

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  record_key_col string,
           |  not_record_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | $partitionedByClause
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")

      // update the secondary key column
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row1'")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1", true),
        Seq("cde", "row2", false),
        Seq("def", "row3", false),
        Seq("xyz", "row1", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")
    }
  }

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexWithPartitionStatsIndex(testCase: SecondaryIndexTestCase): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = testCase.tableType
      val isPartitioned = testCase.isPartitioned
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_with_partition_stats_index" + (if (isPartitioned) "_partitioned" else "") + sqlTableType
      val partitionedByClause = if (isPartitioned) "partitioned by(partition_key_col)" else ""

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  name string,
           |  record_key_col string,
           |  secondary_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  type = '$sqlTableType',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  'hoodie.metadata.index.partition.stats.enable' = 'true',
           |  'hoodie.metadata.index.column.stats.column.list' = 'name',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | $partitionedByClause
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'gandhi', 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'nehru', 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'patel', 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_secondary_key_col on $tableName using secondary_index(secondary_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_secondary_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, secondary_key_col, partition_key_col from $tableName where secondary_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "secondary_key_col")

      // create another secondary index on non-string column
      spark.sql(s"create index idx_ts on $tableName using secondary_index(ts)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_ts"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "ts")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("1", "row1"),
        Seq("2", "row2"),
        Seq("3", "row3"),
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
    }
  }

  private def checkAnswer(query: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(query).collect().sortBy(_.toString()))
  }

  private def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String): Unit = {
    mergedDfList = spark.read.format("hudi").options(hudiOpts).load(basePath).repartition(1).cache() :: mergedDfList
    val secondaryKey = mergedDfList.last.limit(1).collect().map(row => row.getAs(columnName).toString)
    val dataFilter = EqualTo(attribute(columnName), Literal(secondaryKey(0)))
    verifyFilePruning(hudiOpts, dataFilter)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, nullable = true)()
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    val latestDataFilesCount = getLatestDataFilesCount(opts)
    assertTrue(filteredFilesCount > 0 && filteredFilesCount < latestDataFilesCount)

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
    fileIndex.close()
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieMetadataFileSystemView = getTableFileSystemView(opts)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    } finally {
      fsView.close()
    }
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  private def metadataWriter(clientConfig: HoodieWriteConfig): HoodieBackedTableMetadataWriter[_] = SparkHoodieBackedTableMetadataWriter.create(
    storageConf, clientConfig, new HoodieSparkEngineContext(jsc)).asInstanceOf[HoodieBackedTableMetadataWriter[_]]

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateSecondaryIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateSecondaryIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateSecondaryIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    rollbackLastInstant(hudiOpts)
    validateDataAndSecondaryIndex(hudiOpts, partitionName = "secondary_index_idx_not_record_key_col")
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithDelete(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val insertDf = doWriteAndValidateSecondaryIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    val deleteDf = insertDf.limit(1)
    deleteDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.except(deleteDf)
    validateDataAndSecondaryIndex(hudiOpts, partitionName = "secondary_index_idx_not_record_key_col")
  }

  protected def doWriteAndValidateSecondaryIndex(hudiOpts: Map[String, String],
                                                 operation: String,
                                                 saveMode: SaveMode,
                                                 validate: Boolean = true,
                                                 numUpdates: Int = 1,
                                                 onlyUpdates: Boolean = false): DataFrame = {
    var latestBatch: mutable.Buffer[String] = null
    val dataGen = new HoodieTestDataGenerator()
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, numUpdates))
      if (!onlyUpdates) {
        records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 1)))
      }
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
    val deletedDf = calculateMergedDf(latestBatchDf, operation)
    deletedDf.cache()
    // initialization of meta client is required again after writing data so that
    // latest table configs are picked up
    metaClient = HoodieTableMetaClient.reload(metaClient)
    if (validate) {
      // TODO: pass the partition name as an argument
      validateDataAndSecondaryIndex(hudiOpts, deletedDf, "secondary_index_idx_not_record_key_col")
    }

    deletedDf.unpersist()
    latestBatchDf
  }

  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = {
    calculateMergedDf(latestBatchDf, operation, false)
  }

  /**
   * @return [[DataFrame]] that should not exist as of the latest instant; used for non-existence validation.
   */
  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String, globalIndexEnableUpdatePartitions: Boolean): DataFrame = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ latestBatchDf
      spark.emptyDataFrame
    } else {
      if (operation == INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL) {
        mergedDfList = mergedDfList :+ latestBatchDf
        // after insert_overwrite_table, all previous snapshot's records should be deleted from RLI
        prevDfOpt.get
      } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
        val overwrittenPartitions = latestBatchDf.select("partition")
          .collectAsList().stream().map[String](JavaConversions.getFunction[Row, String](r => r.getString(0))).collect(Collectors.toList[String])
        val prevDf = prevDfOpt.get
        val latestSnapshot = prevDf
          .filter(not(col("partition").isInCollection(overwrittenPartitions)))
          .union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot

        // after insert_overwrite (partition), all records in the overwritten partitions should be deleted from RLI
        prevDf.filter(col("partition").isInCollection(overwrittenPartitions))
      } else {
        val prevDf = prevDfOpt.get
        if (globalIndexEnableUpdatePartitions) {
          val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key"), "leftanti")
          val latestSnapshot = prevDfOld.union(latestBatchDf)
          mergedDfList = mergedDfList :+ latestSnapshot
        } else {
          val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key")
            && prevDf("partition") === latestBatchDf("partition"), "leftanti")
          val latestSnapshot = prevDfOld.union(latestBatchDf)
          mergedDfList = mergedDfList :+ latestSnapshot
        }
        spark.emptyDataFrame
      }
    }
  }

  protected def validateDataAndSecondaryIndex(hudiOpts: Map[String, String],
                                              deletedDf: DataFrame = spark.emptyDataFrame,
                                              partitionName: String): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val recordIndexMap = metadata.readRecordIndex(
      JavaConverters.seqAsJavaListConverter(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList).asJava)

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocations = recordIndexMap.get(recordKey)
      assertFalse(recordLocations.isEmpty)
      // assuming no duplicate keys for now
      val recordLocation = recordLocations.get(0)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      assertTrue(fileName.startsWith(recordLocation.getFileId), fileName + " should start with " + recordLocation.getFileId)
    }

    val deletedRows = deletedDf.collect()
    val recordIndexMapForDeletedRows = metadata.readSecondaryIndex(
      JavaConverters.seqAsJavaListConverter(deletedRows.map(row => row.getAs("_row_key").toString).toList).asJava, partitionName)
    assertEquals(0, recordIndexMapForDeletedRows.size(), "deleted records should not present in RLI")
    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
        "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
  }

  protected def rollbackLastInstant(hudiOpts: Map[String, String]): HoodieInstant = {
    val lastInstant = getLatestMetaClient(false).getActiveTimeline
      .filter(JavaConversions.getPredicate(instant => instant.getAction != ActionType.rollback.name()))
      .lastInstant().get()
    if (getLatestCompactionInstant() != getLatestMetaClient(false).getActiveTimeline.lastInstant()
      && lastInstant.getAction != ActionType.replacecommit.name()
      && lastInstant.getAction != ActionType.clean.name()) {
      mergedDfList = mergedDfList.take(mergedDfList.size - 1)
    }
    val writeConfig = getWriteConfig(hudiOpts)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      .rollback(lastInstant.getTimestamp)

    if (lastInstant.getAction != ActionType.clean.name()) {
      assertEquals(ActionType.rollback.name(), getLatestMetaClient(true).getActiveTimeline.lastInstant().get().getAction)
    }
    lastInstant
  }

  protected def getLatestMetaClient(enforce: Boolean): HoodieTableMetaClient = {
    val lastInsant = HoodieInstantTimeGenerator.getLastInstantTime
    if (enforce || metaClient.getActiveTimeline.lastInstant().get().getTimestamp.compareTo(lastInsant) < 0) {
      println("Reloaded timeline")
      metaClient.reloadActiveTimeline()
      metaClient
    }
    metaClient
  }

  protected def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    getLatestMetaClient(false).getActiveTimeline
      .filter(JavaConversions.getPredicate(s => Option(
        try {
          val commitMetadata = MetadataConversionUtils.getHoodieCommitMetadata(metaClient, s)
            .orElse(new HoodieCommitMetadata())
          commitMetadata
        } catch {
          case _: Exception => new HoodieCommitMetadata()
        })
        .map(c => c.getOperationType == WriteOperationType.COMPACT)
        .get))
      .lastInstant()
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }
}

object TestSecondaryIndexPruning {

  case class SecondaryIndexTestCase(tableType: String, isPartitioned: Boolean)

  def testSecondaryIndexPruningParameters(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(SecondaryIndexTestCase("COPY_ON_WRITE", isPartitioned = true)),
      arguments(SecondaryIndexTestCase("COPY_ON_WRITE", isPartitioned = false)),
      arguments(SecondaryIndexTestCase("MERGE_ON_READ", isPartitioned = true)),
      arguments(SecondaryIndexTestCase("MERGE_ON_READ", isPartitioned = false))
    )
  }
}
