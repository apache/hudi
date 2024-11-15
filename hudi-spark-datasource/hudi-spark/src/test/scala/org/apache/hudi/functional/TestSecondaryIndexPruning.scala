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

import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, MOR_TABLE_TYPE_OPT_VAL, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{ActionType, FileSlice, HoodieBaseFile, HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, HoodieTableType, WriteConcurrencyMode, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieMetadataIndexException, HoodieWriteConflictException}
import org.apache.hudi.functional.TestSecondaryIndexPruning.SecondaryIndexTestCase
import org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieBackedTableMetadataWriter, HoodieMetadataFileSystemView, MetadataPartitionType, SparkHoodieBackedTableMetadataWriter}
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.util.{JFunction, JavaConversions}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}
import org.scalatest.Assertions.{assertResult, assertThrows}

import java.util.concurrent.Executors
import scala.collection.JavaConverters
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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

  override def conf: SparkConf = conf(getSparkSqlConf)

  @Test
  def testSecondaryIndexWithoutRecordIndex(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      tableName += "test_secondary_index_without_rli"

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
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // do a couple of inserts
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      // create secondary index without RLI and assert exception
      assertThrows[HoodieMetadataIndexException] {
        spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSecondaryIndexWithFilters(hoodieTableType: HoodieTableType): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = hoodieTableType.name()
      val isPartitioned = true
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
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
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
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
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"1${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"2${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"3${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3"),
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
      )
      // update the secondary key column after creating multiple secondary indexes
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row1'")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        // row1 record is updated
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"1${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"2${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"3${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where ts = 1")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
      verifyQueryPredicate(hudiOpts, "ts")
    }
  }

  @Test
  def testCreateAndDropSecondaryIndex(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      tableName += "test_secondary_index_create_drop_partitioned_mor"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
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
      // validate the secondary index records themselves
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
      )
      // drop secondary index
      spark.sql(s"drop index secondary_index_idx_not_record_key_col on $tableName")
      // validate index dropped successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(!metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // query metadata table and check no records for secondary index
      assert(spark.sql(s"select * from hudi_metadata('$basePath') where type=7").count() == 0)
    }
  }

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexPruningWithUpdates(testCase: SecondaryIndexTestCase): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = testCase.tableType
      val isPartitioned = testCase.isPartitioned
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
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
      // validate the secondary index records themselves
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
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
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
    }
  }

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexWithPartitionStatsIndex(testCase: SecondaryIndexTestCase): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableType = testCase.tableType
      val isPartitioned = testCase.isPartitioned
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_with_partition_stats_index" + (if (isPartitioned) "_partitioned" else "") + sqlTableType
      val partitionedByClause = if (isPartitioned) "partitioned by(partition_key_col)" else ""
      val partitionStatsEnable = if (isPartitioned) "'hoodie.metadata.index.partition.stats.enable' = 'true'," else ""
      val columnsToIndex = if (isPartitioned) "'hoodie.metadata.index.column.stats.column.list' = 'name'," else ""

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
           |  $partitionStatsEnable
           |  $columnsToIndex
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
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
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
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"1${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"2${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"3${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3"),
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
      )
    }
  }

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithConcurrentWrites(tableType: HoodieTableType): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableName = "hudi_multi_writer_table_" + tableType.name()

      // Common Hudi options
      val hudiOpts = commonOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
        HoodieWriteConfig.TBL_NAME.key -> tableName,
        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key() -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
        HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> HoodieFailedWritesCleaningPolicy.LAZY.name,
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> classOf[InProcessLockProvider].getName,
        HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key() -> classOf[SimpleConcurrentFileWritesConflictResolutionStrategy].getName
      )

      // Create the Hudi table
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  ts BIGINT,
           |  record_key_col STRING,
           |  not_record_key_col STRING,
           |  partition_key_col STRING
           |) USING hudi
           | OPTIONS (
           |  primaryKey = 'record_key_col',
           |  preCombineField = 'ts',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | PARTITIONED BY (partition_key_col)
           | LOCATION '$basePath'
       """.stripMargin)
      // Insert some data
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")

      val executor = Executors.newFixedThreadPool(2)
      implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
      val function = new (Int => Boolean) {
        override def apply(writerId: Int): Boolean = {
          try {
            val data = if(writerId == 1) Seq(
              (System.currentTimeMillis(), s"row$writerId", s"value${writerId}_1", s"p$writerId")
            ) else Seq(
              (System.currentTimeMillis(), s"row$writerId", s"value${writerId}_2", s"p$writerId")
            )

            val df = spark.createDataFrame(data).toDF("ts", "record_key_col", "not_record_key_col", "partition_key_col")
            df.write.format("hudi")
              .options(hudiOpts)
              .mode("append")
              .save(basePath)
            true
          } catch {
            case _: HoodieWriteConflictException => false
            case e => throw new Exception("Multi write failed", e)
          }
        }
      }
      // Set up futures for two writers
      val f1 = Future[Boolean] {
        function.apply(1)
      }(executorContext)
      val f2 = Future[Boolean] {
        function.apply(2)
      }(executorContext)

      Await.result(f1, Duration("5 minutes"))
      Await.result(f2, Duration("5 minutes"))

      assertTrue(f1.value.get.get || f2.value.get.get)
      executor.shutdownNow()

      // Query the secondary index metadata
      checkAnswer(
        s"""
           |SELECT key, SecondaryIndexMetadata.isDeleted
           |FROM hudi_metadata('$basePath')
           |WHERE type=7
       """.stripMargin)(
        Seq(s"value1_1${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"value2_2${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false)
      )
    }
  }

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithCompactionAndCleaning(tableType: HoodieTableType): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1")
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        hudiOpts = hudiOpts ++ Map(
          HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
          HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2",
          HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0"
        )
      }
      val sqlTableType = if (tableType == HoodieTableType.COPY_ON_WRITE) "cow" else "mor"
      tableName += "test_secondary_index_pruning_compact_clean_" + sqlTableType

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
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key} = '1'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        spark.sql("set hoodie.compact.inline=true")
        spark.sql("set hoodie.compact.inline.max.delta.commits=2")
        spark.sql("set hoodie.metadata.compact.num.delta.commits=15")
      }
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
      // validate the secondary index records themselves
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
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
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
    }
  }

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   * Any one table type is enough to test this as we are validating the metadata table.
   */
  @Test
  def testSecondaryIndexWithMDTCompaction(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "2")
      val tableName = "test_secondary_index_with_mdt_compaction"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '2'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()

      // do another insert and validate compaction in metadata table
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      val metadataTableFSView = HoodieSparkTable.create(getWriteConfig(hudiOpts), context()).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata].getMetadataFileSystemView
      try {
        val compactionTimeline = metadataTableFSView.getVisibleCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants()
        val lastCompactionInstant = compactionTimeline
          .filter(JavaConversions.getPredicate((instant: HoodieInstant) =>
            HoodieCommitMetadata.fromBytes(compactionTimeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
              .getOperationType == WriteOperationType.COMPACT))
          .lastInstant()
        val compactionBaseFile = metadataTableFSView.getAllBaseFiles("secondary_index_idx_not_record_key_col")
          .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().getTimestamp)))
          .findAny()
        assertTrue(compactionBaseFile.isPresent)
      } finally {
        metadataTableFSView.close()
      }

      // validate the secondary index records themselves
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
    }
  }

  @Test
  def testSecondaryIndexWithMultipleUpdatesForSameRecord(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "20")
      val tableName = "test_secondary_index_with_multiple_updates_same_record"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'abc', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'hjk', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"hjk${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3",false)
      )

      // do another insert and validate compaction in metadata table
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'xyz1' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'xyz2' where record_key_col = 'row3'")

      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('xyz','xyz1')")(

      )
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('xyz','xyz2')")(
        Seq(3, "row3", "xyz2", "p2")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "xyz2")
    }
  }

  @Test
  def testSecondaryIndexWithOnlyDeleteLogs(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "20")
      val tableName = "test_secondary_index_with_only_delete"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'hjk', 'p1')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"hjk${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3",false)
      )

      // do a hard delete
      spark.sql(s"delete from $tableName where record_key_col = 'row2'")
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('abc')")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
    }
  }

  @Test
  def testSecondaryIndexWithUpdateFollowedByDelete(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "20")
      val tableName = "test_secondary_index_with_update_and_delete"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'def', 'p1')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'hjk', 'p1')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"hjk${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false)
      )

      // few updates for same key
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'xyz1' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row2'")
      // delete for same key that was just updated
      spark.sql(s"delete from $tableName where record_key_col = 'row2'")

      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('xyz','xyz1')")(
        Seq(3, "row3", "xyz1", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "xyz1")
      // query with all secondary keys including deleted
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('abc','def','hjk','xyz','xyz1')")(
        Seq(1, "row1", "abc", "p1"),
        Seq(3, "row3", "xyz1", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
    }
  }

  @Test
  def testSecondaryIndexWithSameSecondaryKeyUpdatesForMultipleRecords(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "20")
      val tableName = "test_secondary_index_with_same_sec_key"

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
           |  type = 'mor',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'abc', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'hjk', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"hjk${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false)
      )

      // do updates for different records with same secondary key
      spark.sql(s"update $tableName set not_record_key_col = 'def' where record_key_col = 'row1'")
      spark.sql(s"update $tableName set not_record_key_col = 'abc' where record_key_col = 'row3'")
      spark.sql(s"update $tableName set not_record_key_col = 'poc' where record_key_col = 'row2'")

      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col in ('abc')")(
        Seq(3, "row3", "abc", "p2")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col", "abc")
    }
  }


  /**
   * Test case to write with updates and validate secondary index with clustering.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithClusteringAndCleaning(tableType: HoodieTableType): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "1")
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        hudiOpts = hudiOpts ++ Map(
          HoodieClusteringConfig.INLINE_CLUSTERING.key -> "true",
          HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> "1",
          HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> "0"
        )
      }
      val sqlTableType = if (tableType == HoodieTableType.COPY_ON_WRITE) "cow" else "mor"
      tableName += "test_secondary_index_pruning_cluster_clean_" + sqlTableType

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
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
           |  ${HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key} = '1'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql("set hoodie.clustering.inline=true")
      spark.sql("set hoodie.clustering.inline.max.commits=1")

      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()

      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      confirmLastCommitType(ActionType.replacecommit)
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      confirmLastCommitType(ActionType.replacecommit)
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      confirmLastCommitType(ActionType.replacecommit)

      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false))
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
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")

      // update the secondary key column by insert.
      spark.sql(s"insert into $tableName values (5, 'row2',  'efg', 'p2')")
      confirmLastCommitType(ActionType.replacecommit)
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"efg${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false))

      // update the secondary key column by update.
      spark.sql(s"update $tableName set not_record_key_col = 'fgh' where record_key_col = 'row2'")
      confirmLastCommitType(ActionType.replacecommit)
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"fgh${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false))

      // update the secondary index by delete.
      spark.sql(s"delete from $tableName where record_key_col = 'row3'")
      confirmLastCommitType(ActionType.replacecommit)
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"fgh${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
      )
    }
  }

  private def confirmLastCommitType(actionType: ActionType): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val instants = metaClient.getActiveTimeline.getInstants
    System.out.println("current instants: " + instants)
    assertFalse(instants.isEmpty)
    assertTrue(instants.get(instants.size - 1).getAction.equals(actionType.name))
  }

  /**
   * 1. Enable secondary index and record_index (files already enabled by default).
   * 2. Do an insert and validate the secondary index initialization.
   * 3. Do an update and validate the secondary index.
   * 4. Do a savepoint and restore, and validate secondary index deleted.
   * 5. Do an update and validate the secondary index is recreated.
   */
  @Test
  def testSecondaryIndexWithSavepointAndRestore(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      val tableName = "test_secondary_index_with_savepoint_and_restore"
      val hudiOpts = commonOpts ++ Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = "mor"

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
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      // by setting small file limit to 0, each insert will create a new file
      // need to generate more file for non-partitioned table to test data skipping
      // as the partitioned table will have only one file per partition
      spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")

      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      // validate the secondary index records themselves
      checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
        Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1")
      )
      // Do a savepoint
      val firstCompletedInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant()
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), getWriteConfig(hudiOpts))
      writeClient.savepoint(firstCompletedInstant.get().getTimestamp, "testUser", "savepoint to first commit")
      val savepointTimestamp = metaClient.reloadActiveTimeline().getSavePointTimeline.filterCompletedInstants().lastInstant().get().getTimestamp
      assertEquals(firstCompletedInstant.get().getTimestamp, savepointTimestamp)
      // Restore to savepoint
      writeClient.restoreToSavepoint(savepointTimestamp)
      // verify restore completed
      assertTrue(metaClient.reloadActiveTimeline().getRestoreTimeline.lastInstant().isPresent)
      // verify secondary index partition is deleted
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath))
      // however index definition should still be present
      assertTrue(metaClient.getIndexMetadata.isPresent && metaClient.getIndexMetadata.get.getIndexDefinitions.get("secondary_index_idx_not_record_key_col").getIndexType.equals("secondary_index"))

      // update the secondary key column
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row1'")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
      )
    }
  }

  private def checkAnswer(query: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(query).collect().sortBy(_.toString()))
  }

  private def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String, nonExistantKey: String = ""): Unit = {
    mergedDfList = mergedDfList :+ spark.read.format("hudi").options(hudiOpts).load(basePath).repartition(1).cache()
    val secondaryKey = mergedDfList.last.limit(2).collect().filter(row => !row.getAs(columnName).toString.equals(nonExistantKey))
      .map(row => row.getAs(columnName).toString).head
    val dataFilter = EqualTo(attribute(columnName), Literal(secondaryKey))
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
