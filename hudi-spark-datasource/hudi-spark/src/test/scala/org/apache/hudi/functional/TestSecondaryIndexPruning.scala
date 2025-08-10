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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSparkUtils}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config._
import org.apache.hudi.exception.{HoodieMetadataIndexException, HoodieWriteConflictException}
import org.apache.hudi.functional.TestSecondaryIndexPruning.SecondaryIndexTestCase
import org.apache.hudi.metadata._
import org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.util.{JavaConversions, JFunction}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.disableComplexKeygenValidation
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}
import org.junit.jupiter.params.provider.Arguments.arguments
import org.scalatest.Assertions.{assertResult, assertThrows}

import java.util.concurrent.Executors

import scala.collection.JavaConverters
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Test cases for secondary index
 */
@Tag("functional-c")
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
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
    DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name()
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty
  var tableName = "hoodie_"
  var metaClient: HoodieTableMetaClient = _

  override def conf: SparkConf = conf(getSparkSqlConf)

  @Test
  def testSecondaryIndexWithoutRecordIndex(): Unit = {
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
      spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSecondaryIndexWithFilters(hoodieTableType: HoodieTableType): Unit = {
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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
    spark.sql(s"create index idx_ts on $tableName (ts)")
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

  @Test
  def testCreateAndDropSecondaryIndex(): Unit = {
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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
    spark.sql(s"drop index idx_not_record_key_col on $tableName")
    // validate index dropped successfully
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assert(!metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
    // query metadata table and check no records for secondary index
    assert(spark.sql(s"select * from hudi_metadata('$basePath') where type=7").count() == 0)
  }

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexPruningWithUpdates(testCase: SecondaryIndexTestCase): Unit = {
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

  @ParameterizedTest
  @MethodSource(Array("testSecondaryIndexPruningParameters"))
  def testSecondaryIndexWithPartitionStatsIndex(testCase: SecondaryIndexTestCase): Unit = {
    val tableType = testCase.tableType
    val isPartitioned = testCase.isPartitioned
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
    tableName += "test_secondary_index_with_partition_stats_index" + (if (isPartitioned) "_partitioned" else "") + sqlTableType
    val partitionedByClause = if (isPartitioned) "partitioned by(partition_key_col)" else ""
    val partitionStatsEnable = if (isPartitioned)
      s"""
         |'hoodie.metadata.index.partition.stats.enable' = 'true',
         |'hoodie.metadata.index.column.stats.enable' = 'true',
        """.stripMargin else ""
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_secondary_key_col on $tableName (secondary_key_col)")
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
    spark.sql(s"create index idx_ts on $tableName (ts)")
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

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithConcurrentWrites(tableType: HoodieTableType): Unit = {
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
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | PARTITIONED BY (partition_key_col)
         | LOCATION '$basePath'
       """.stripMargin)
    // Insert some data
    spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
    spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
    // create secondary index
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")

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

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithCompactionAndCleaning(tableType: HoodieTableType): Unit = {
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
         |  ${HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key} = '1',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

  /**
   * Test case to write with updates and validate secondary index with multiple writers.
   * Any one table type is enough to test this as we are validating the metadata table.
   */
  @Test
  def testSecondaryIndexWithMDTCompaction(): Unit = {
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
         |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '2',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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
          compactionTimeline.readCommitMetadata(instant)
            .getOperationType == WriteOperationType.COMPACT))
        .lastInstant()
      val compactionBaseFile = metadataTableFSView.getAllBaseFiles("secondary_index_idx_not_record_key_col")
        .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().requestedTime)))
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

  /**
   * Test case to write with updates and validate secondary index with EVENT_TIME_ORDERING merge mode.
   */
  @Test
  def testSecondaryIndexWithEventTimeOrderingMerge(): Unit = {
    val tableName = "test_secondary_index_with_event_time_ordering_merge"

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
         |  type = 'mor',
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true'
         | )
         | PARTITIONED BY (partition_key_col)
         | LOCATION '$basePath'
       """.stripMargin)
    // by setting small file limit to 0, each insert will create a new file
    // need to generate more file for non-partitioned table to test data skipping
    // as the partitioned table will have only one file per partition
    spark.sql("set hoodie.parquet.small.file.limit=0")
    // Insert some data
    spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
    spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
    // create secondary index
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")

    // Update the secondary key column with higher ts value
    spark.sql(s"update $tableName set not_record_key_col = 'xyz', ts = 3 where record_key_col = 'row1'")
    // validate data and SI
    checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
      Seq(3, "row1", "xyz", "p1")
    )
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
    )

    // update the secondary key column with higher ts value and to original secondary key value 'abc'
    spark.sql(s"update $tableName set not_record_key_col = 'abc', ts = 4 where record_key_col = 'row1'")
    // validate data and SI
    checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
      Seq(4, "row1", "abc", "p1")
    )
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
    )

    // update the secondary key column with lower ts value, this should be ignored
    spark.sql(s"update $tableName set not_record_key_col = 'xyz', ts = 0 where record_key_col = 'row1'")
    // validate data and SI
    checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
      Seq(4, "row1", "abc", "p1")
    )
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"cde${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"abc${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
    )
  }

  /**
   * Test case to write with updates and validate secondary index with CUSTOM merge mode using CDC payload.
   */
  @Test
  def testSecondaryIndexWithCustomMergeMode(): Unit = {
    val tableName = "test_secondary_index_with_custom_merge"

    // Create the Hudi table
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  record_key_col BIGINT,
         |  Op STRING,
         |  replicadmstimestamp STRING,
         |  not_record_key_col STRING
         |) USING hudi
         | OPTIONS (
         |  primaryKey = 'record_key_col',
         |  type = 'mor',
         |  orderingFields = 'replicadmstimestamp',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.AWSDmsAvroPayload',
         |  hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
         |  hoodie.write.record.merge.mode = 'CUSTOM',
         |  hoodie.table.cdc.enabled = 'true',
         |  hoodie.table.cdc.supplemental.logging.mode = 'data_before_after'
         | )
         | LOCATION '$basePath'
       """.stripMargin)
    if (HoodieSparkUtils.gteqSpark3_4) {
      spark.sql("set spark.sql.defaultColumn.enabled=false")
    }
    // by setting small file limit to 0, each insert will create a new file
    // need to generate more file for non-partitioned table to test data skipping
    // as the partitioned table will have only one file per partition
    spark.sql("set hoodie.parquet.small.file.limit=0")
    // Insert some data
    spark.sql(
      s"""|INSERT INTO $tableName(record_key_col, Op, replicadmstimestamp, not_record_key_col) VALUES
          |    (1, 'I', '2023-06-14 15:46:06.953746', 'A'),
          |    (2, 'I', '2023-06-14 15:46:07.953746', 'B'),
          |    (3, 'I', '2023-06-14 15:46:08.953746', 'C');
          |    """.stripMargin)
    // create secondary index
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")

    // validate data and SI
    checkAnswer(s"select record_key_col, Op, replicadmstimestamp, not_record_key_col from $tableName")(
      Seq(1, "I", "2023-06-14 15:46:06.953746", "A"),
      Seq(2, "I", "2023-06-14 15:46:07.953746", "B"),
      Seq(3, "I", "2023-06-14 15:46:08.953746", "C")
    )
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"A${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}1", false),
      Seq(s"B${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}2", false),
      Seq(s"C${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}3", false)
    )

    // Update the delete Op value for record_key_col = 3
    spark.sql(s"update $tableName set Op = 'D' where record_key_col = 3")

    // validate data and SI
    checkAnswer(s"select record_key_col, Op, replicadmstimestamp, not_record_key_col from $tableName")(
      Seq(1, "I", "2023-06-14 15:46:06.953746", "A"),
      Seq(2, "I", "2023-06-14 15:46:07.953746", "B")
    )
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"A${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}1", false),
      Seq(s"B${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}2", false)
    )
  }

  @Test
  def testSecondaryIndexWithMultipleUpdatesForSameRecord(): Unit = {
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
         |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

  @Test
  def testSecondaryIndexWithOnlyDeleteLogs(): Unit = {
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
         |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

  @Test
  def testSecondaryIndexWithUpdateFollowedByDelete(): Unit = {
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
         |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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
    verifyQueryPredicate(hudiOpts, "not_record_key_col")
  }

  @Test
  def testSecondaryIndexWithSameSecondaryKeyUpdatesForMultipleRecords(): Unit = {
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
         |  ${HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key} = '20',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

  /**
   * Test case to write with updates and validate secondary index with clustering.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSecondaryIndexWithClusteringAndCleaning(tableType: HoodieTableType): Unit = {
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
         |  ${HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key} = '1',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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

    // update the secondary key column by update.
    spark.sql(s"update $tableName set not_record_key_col = 'efg' where record_key_col = 'row2'")
    confirmLastCommitType(ActionType.replacecommit)
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
      Seq(s"efg${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false))

    // update the secondary key column by update.
    spark.sql(s"update $tableName set not_record_key_col = 'fgh' where record_key_col = 'row2'")
    confirmLastCommitType(ActionType.replacecommit)
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"def${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3", false),
      Seq(s"fgh${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false))

    // update the secondary index by delete.
    spark.sql(s"delete from $tableName where record_key_col = 'row3'")
    confirmLastCommitType(ActionType.replacecommit)
    // validate the secondary index records themselves
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"fgh${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2", false),
      Seq(s"xyz${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1", false)
    )
  }

  private def confirmLastCommitType(actionType: ActionType): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val instants = metaClient.getActiveTimeline.getInstants
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
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
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
    spark.sql(s"create index idx_not_record_key_col on $tableName (not_record_key_col)")
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
    writeClient.savepoint(firstCompletedInstant.get().requestedTime, "testUser", "savepoint to first commit")
    writeClient.close()
    val savepointTimestamp = metaClient.reloadActiveTimeline().getSavePointTimeline.filterCompletedInstants().lastInstant().get().requestedTime
    assertEquals(firstCompletedInstant.get().requestedTime, savepointTimestamp)
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testUpdatesReInsertsDeletes(hoodieTableType: HoodieTableType): Unit = {
    val tableType = hoodieTableType.name()
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
    val tableName = s"test_updates_reinserts_deletes_$sqlTableType"

    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |    ts BIGINT,
         |    id STRING,
         |    rider STRING,
         |    driver STRING,
         |    fare DOUBLE,
         |    city STRING,
         |    state STRING
         |) USING HUDI
         | options(
         |    primaryKey ='id',
         |    type = '$sqlTableType',
         |    hoodie.metadata.enable = 'true',
         |    hoodie.metadata.record.index.enable = 'true',
         |    hoodie.datasource.write.recordkey.field = 'id',
         |    hoodie.enable.data.skipping = 'true',
         |    hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | PARTITIONED BY (city, state)
         | location '$basePath'
         |""".stripMargin)

    spark.sql("set hoodie.parquet.small.file.limit=0")
    spark.sql("set hoodie.enable.data.skipping=true")
    spark.sql("set hoodie.metadata.enable=true")
    if (HoodieSparkUtils.gteqSpark3_4) {
      spark.sql("set spark.sql.defaultColumn.enabled=false")
    }

    disableComplexKeygenValidation(spark, tableName)
    // TODO(yihua): understand why test fails without this config
    spark.sql(
      s"""
         |ALTER TABLE $tableName
         |SET TBLPROPERTIES (hoodie.write.complex.keygen.encode.single.record.key.field.name = 'false')
         |""".stripMargin)
    spark.sql(
      s"""|INSERT INTO $tableName(ts, id, rider, driver, fare, city, state) VALUES
          |    (1695159649,'trip1','rider-A','driver-K',19.10,'san_francisco','california'),
          |    (1695091554,'trip2','rider-C','driver-M',27.70,'sunnyvale','california'),
          |    (1695332066,'trip3','rider-E','driver-O',93.50,'austin','texas'),
          |    (1695516137,'trip4','rider-F','driver-P',34.15,'houston','texas');
          |    """.stripMargin)
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName;")(
      Seq(1695159649, "trip1", "rider-A", "driver-K", 19.10, "san_francisco", "california"),
      Seq(1695091554, "trip2", "rider-C", "driver-M", 27.70, "sunnyvale", "california"),
      Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas"),
      Seq(1695516137, "trip4", "rider-F", "driver-P", 34.15, "houston", "texas"))

    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()
    spark.sql(s"create index idx_rider_$tableName ON $tableName (rider)")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where rider = 'rider-E'")(
      Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas"))
    verifyQueryPredicate(hudiOpts, "rider")

    spark.sql(s"create index idx_driver_$tableName ON $tableName (driver)")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where driver = 'driver-P'")(
      Seq(1695516137, "trip4", "rider-F", "driver-P", 34.15, "houston", "texas")
    )
    verifyQueryPredicate(hudiOpts, "driver")

    // update such that there are two rider-E records
    spark.sql(s"update $tableName set rider = 'rider-E' where rider = 'rider-F'")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where rider = 'rider-E'")(
      Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas"),
      Seq(1695516137, "trip4", "rider-E", "driver-P", 34.15, "houston", "texas")
    )
    verifyQueryPredicate(hudiOpts, "rider")

    // delete one of those records
    spark.sql(s"delete from $tableName where id = 'trip4'")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where rider = 'rider-E'")(
      Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas")
    )

    // reinsert a rider-E record  while changing driver value as well.
    spark.sql(s"insert into $tableName values(1695516137,'trip4','rider-G','driver-Q',34.15,'houston','texas')")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where driver = 'driver-Q';")(
      Seq(1695516137, "trip4", "rider-G", "driver-Q", 34.15, "houston", "texas")
    )

    // update two other records to rider-E as well.
    spark.sql(s"update $tableName set rider = 'rider-E' where rider in ('rider-C','rider-G');")
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where rider = 'rider-E'")(
      Seq(1695091554, "trip2", "rider-E", "driver-M", 27.70, "sunnyvale", "california"),
      Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas"),
      Seq(1695516137, "trip4", "rider-E", "driver-Q", 34.15, "houston", "texas")
    )
    checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName where driver = 'driver-Q'")(
      Seq(1695516137, "trip4", "rider-E", "driver-Q", 34.15, "houston", "texas")
    )
    verifyQueryPredicate(hudiOpts, "rider")
  }

  @Test
  def testSecondaryIndexWithComplexTypes(): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts ++ Map(
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    tableName += "test_secondary_index_with_complex_data_types"

    // Create table with complex data types
    spark.sql(
      s"""
         |create table $tableName (
         |  record_key_col string,
         |  array_col array<int>,
         |  map_col map<string, int>,
         |  struct_col struct<field1:int, field2:string>,
         |  partition_key_col string
         |) using hudi
         | options (
         |  primaryKey ='record_key_col',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | partitioned by(partition_key_col)
         | location '$basePath'
       """.stripMargin)

    // Insert dummy records
    spark.sql("set hoodie.parquet.small.file.limit=0")
    spark.sql(s"insert into $tableName values ('row1', array(1, 2, 3), map('key1', 10, 'key2', 20), named_struct('field1', 1, 'field2', 'value1'), 'p1')")
    spark.sql(s"insert into $tableName values ('row2', array(4, 5, 6), map('key1', 30, 'key2', 40), named_struct('field1', 2, 'field2', 'value2'), 'p2')")

    // Creation of secondary indexes for complex columns should fail
    val secondaryIndexColumns = Seq("struct_col", "array_col", "map_col")
    secondaryIndexColumns.foreach { col =>
      assertThrows[HoodieMetadataIndexException] {
        spark.sql(s"create index idx_$col on $tableName ($col)")
      }
    }
  }

  @Test
  def testSecondaryIndexWithNestedFields(): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts ++ Map(
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    tableName += "test_secondary_index_with_nested_fields"

    // Create table with nested fields
    spark.sql(
      s"""
         |create table $tableName (
         |  record_key_col string,
         |  student struct<
         |    name: struct<first_name:string, last_name:string>,
         |    age: int
         |  >,
         |  ts bigint
         |) using hudi
         | options (
         |  primaryKey ='record_key_col',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | location '$basePath'
      """.stripMargin)
    // insert initial records
    spark.sql(s"insert into $tableName values('id1', named_struct('name', named_struct('first_name', 'John', 'last_name', 'Doe'), 'age', 20), 1)")
    spark.sql(s"insert into $tableName values('id2', named_struct('name', named_struct('first_name', 'Jane', 'last_name', 'Smith'), 'age', 25), 2)")
    // create secondary index on student.name.last_name field
    spark.sql(s"create index idx_last_name on $tableName (student.name.last_name)")
    // validate index creation
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()
    assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_last_name"))
    // validate index records
    checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
      Seq(s"Doe${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1"),
      Seq(s"Smith${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2")
    )
    // verify pruning
    checkAnswer(s"select record_key_col, student.name.last_name, ts from $tableName where student.name.last_name = 'Doe'")(
      Seq("id1", "Doe", 1)
    )
    // update nested field
    spark.sql(s"update $tableName set student = named_struct('name', named_struct('first_name', 'John', 'last_name', 'Brown'), 'age', 20) where record_key_col = 'id1'")
    // validate updated index records
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"Brown${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1", false),
      Seq(s"Smith${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2", false)
    )
    // verify pruning
    checkAnswer(s"select record_key_col, student.name.last_name, ts from $tableName where student.name.last_name = 'Brown'")(
      Seq("id1", "Brown", 1)
    )
  }

  /**
   * Test Secondary Index with partially failed commit.
   * 1. Create a table and insert some records
   * 2. Create a secondary index on a column
   * 3. Two rounds of Updates the column value for some records
   * 4. Fail the commit partially
   * 5. Validate the secondary index records
   */
  @Test
  def testSecondaryIndexWithPartiallyFailedCommit(): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
    tableName += "test_secondary_index_with_partially_failed_commit"

    // Create table and insert some records
    spark.sql(
      s"""
         |create table $tableName (
         |  record_key_col string,
         |  name string,
         |  age int,
         |  partition_key_col string
         |) using hudi
         | options (
         |  primaryKey ='record_key_col',
         |  type = 'mor',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | partitioned by(partition_key_col)
         | location '$basePath'
       """.stripMargin)
    // insert initial records
    spark.sql(s"insert into $tableName values('id1', 'John', 30, 'p1')")
    spark.sql(s"insert into $tableName values('id2', 'Jane', 25, 'p2')")
    // create secondary index on name field
    spark.sql(s"create index idx_name on $tableName (name)")
    // validate index creation
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()
    assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_name"))
    // validate index records
    checkAnswer(s"select key from hudi_metadata('$basePath') where type=7")(
      Seq(s"John${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1"),
      Seq(s"Jane${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2")
    )
    // update name field
    spark.sql(s"update $tableName set name = 'John Doe' where record_key_col = 'id1'")
    // validate updated index records
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"John Doe${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1", false),
      Seq(s"Jane${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2", false)
    )
    // do another update
    spark.sql(s"update $tableName set name = 'Jane Doe' where record_key_col = 'id2'")
    // validate updated index records
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"John Doe${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1", false),
      Seq(s"Jane Doe${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2", false)
    )
    // fail the commit partially
    metaClient = HoodieTableMetaClient.reload(metaClient)
    deleteLastCompletedCommitFromTimeline(hudiOpts, metaClient)
    // validate index records
    checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
      Seq(s"John Doe${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id1", false),
      Seq(s"Jane${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}id2", false)
    )
  }

  private def deleteLastCompletedCommitFromTimeline(hudiOpts: Map[String, String], metaClient: HoodieTableMetaClient): Unit = {
    val lastInstant = metaClient.reloadActiveTimeline.getCommitsTimeline.lastInstant().get()
    assertTrue(hoodieStorage().deleteFile(new StoragePath(metaClient.getTimelinePath, HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR.getFileName(lastInstant))))
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
  }

  private def checkAnswer(query: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(query).collect().sortBy(_.toString()))
  }

  private def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String, nonExistentKey: String = ""): Unit = {
    mergedDfList = mergedDfList :+ spark.read.format("hudi").options(hudiOpts).load(basePath).repartition(1).cache()
    val secondaryKey = mergedDfList.last.limit(2).collect().filter(row => !row.getAs(columnName).toString.equals(nonExistentKey))
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
    val fsView: HoodieTableFileSystemView = getTableFileSystemView(opts)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
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

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    val metadataTable = new HoodieBackedTableMetadata(context(), metaClient.getStorage, getWriteConfig(opts).getMetadataConfig, metaClient.getBasePath.toString, true);
    new HoodieTableFileSystemView(metadataTable, metaClient, metaClient.getActiveTimeline)
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
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
