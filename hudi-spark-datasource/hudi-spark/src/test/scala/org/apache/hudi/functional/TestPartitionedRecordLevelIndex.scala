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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecordGlobalLocation, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestPartitionedRecordLevelIndex.TestPartitionedRecordLevelIndexTestCase
import org.apache.hudi.index.HoodieIndex.IndexType.PARTITIONED_RECORD_INDEX
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

import java.util.stream.Collectors

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

@Tag("functional")
class TestPartitionedRecordLevelIndex extends RecordLevelIndexTestBase {
  private class testPartitionedRecordLevelIndexHolder {
    var bulkRecordKeys: java.util.List[String] = null
    var options: Map[String, String] = null
    var recordKeys: java.util.List[String] = null
    var newRecordKeys: java.util.List[String] = null
  }

  def testPartitionedRecordLevelIndex(tableType: HoodieTableType, streamingWriteEnabled: Boolean, holder: testPartitionedRecordLevelIndexHolder): Unit = {
    val dataGen = new HoodieTestDataGenerator();
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatch = recordsToStrings(inserts).asScala.toSeq
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 1))
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.PARTITIONED_RECORD_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> PARTITIONED_RECORD_INDEX.name())
    holder.options = options
    insertDf.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(options).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    var metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.recordKeys = recordKeys
    var partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    var partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    var df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newDeletes =  dataGen.generateUpdates("004", 1)
    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val nextBatch = recordsToStrings(updates).asScala.toSeq
    val nextBatchDf = spark.read.json(spark.sparkContext.parallelize(nextBatch, 1))
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))

    updateDf.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newInserts =  dataGen.generateInserts("003", 3)
    val newInsertBatch = recordsToStrings(newInserts).asScala.toSeq
    val newInsertBatchDf = spark.read.json(spark.sparkContext.parallelize(newInsertBatch, 1))
    val newInsertDf = newInsertBatchDf.withColumn("data_partition_path", lit("partition2")).union(newInsertBatchDf.withColumn("data_partition_path", lit("partition3")))
    newInsertDf.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(16, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newRecordKeys = newInserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.newRecordKeys = newRecordKeys
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    var partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    val newDeletesBatch = recordsToStrings(newDeletes).asScala.toSeq
    val newDeletesBatchDf = spark.read.json(spark.sparkContext.parallelize(newDeletesBatch, 1))
    val newDeletesDf = newDeletesBatchDf.withColumn("data_partition_path", lit("partition1"))
    newDeletesDf.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(15, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    assertFalse(partition1Locations.contains(newDeletes.get(0).getRecordKey))
    assertTrue(partition2Locations.contains(newDeletes.get(0).getRecordKey))
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    validateDFWithLocations(df, partition3Locations, "partition3")

    val bulkInserts = dataGen.generateInserts("005", 5)
    val bulkInsertBatch = recordsToStrings(bulkInserts).asScala.toSeq
    val bulkInsertDf = spark.read.json(spark.sparkContext.parallelize(bulkInsertBatch, 1))
    val bulkInsertPartitionedDf = bulkInsertDf.withColumn("data_partition_path", lit("partition0"))

    // Use bulk_insert operation explicitly
    bulkInsertPartitionedDf.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val bulkRecordKeys = bulkInserts.asScala.map(_.getRecordKey).asJava
    holder.bulkRecordKeys = bulkRecordKeys
    metadata = metadataWriter(writeConfig).getTableMetadata
    val partition0Locations = readRecordIndex(metadata, bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition0Locations, "partition0")
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    val writeConfig = getWriteConfig(holder.options)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      .rollback(metaClient.getActiveTimeline.lastInstant().get().requestedTime())
    val metadata = metadataWriter(writeConfig).getTableMetadata
    try {
      val partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
      fail("rollback happened, so partition should be deleted")
    } catch {
      case t: Throwable => assertTrue(t.isInstanceOf[ArithmeticException])
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexCompact(streamingWriteEnabled: Boolean): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, holder)
    assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options)
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleCompaction(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.compact(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    assertEquals(if (testCase.tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
      metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleClustering(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.cluster(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  private def validateDFWithLocations(df: Array[Row], locations: Map[String, HoodieRecordGlobalLocation],
                                       partition: String): Unit = {
    var count: Int = 0
    for (row <- df) {
      val recordKey = row.getString(2)
      locations.get(recordKey).foreach { loc =>
        if (partition == row.getString(3)) {
          count += 1
          assertEquals(row.getString(3), loc.getPartitionPath)
          assertEquals(FSUtils.getFileId(row.getString(4)), loc.getFileId)
        }
      }
    }
    assertEquals(locations.size, count)
  }

  private def doAllAssertions(holder: testPartitionedRecordLevelIndexHolder, metadata: HoodieBackedTableMetadata): Unit = {
    val df = spark.read.format("hudi").load(basePath).collect()
    var partition0Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    var partition1Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    validateDFWithLocations(df, partition1Locations, "partition1")
    var partition2Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    var partition3Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)

    partition0Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    partition1Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    partition3Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    validateDFWithLocations(df, partition0Locations, "partition0")
    partition1Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition2"))
    assertEquals(0, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationBasic(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = true, compact = false, cluster = false)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexInitializationCompact(streamingWriteEnabled: Boolean): Unit = {
    testPartitionedRecordLevelIndexInitialization(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, failAndDoRollback = false, compact = true, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = true)
  }

  def testPartitionedRecordLevelIndexInitialization(tableType: HoodieTableType,
                                                    streamingWriteEnabled: Boolean,
                                                    failAndDoRollback: Boolean,
                                                    compact: Boolean,
                                                    cluster: Boolean): Unit = {
    initMetaClient(tableType)
    val dataGen = new HoodieTestDataGenerator()
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatch = recordsToStrings(inserts).asScala.toSeq
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 1))
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> PARTITIONED_RECORD_INDEX.name())
    insertDf.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(10, spark.read.format("hudi").load(basePath).count())

    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val nextBatch = recordsToStrings(updates).asScala.toSeq
    val nextBatchDf = spark.read.json(spark.sparkContext.parallelize(nextBatch, 1))
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))
    updateDf.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    metaClient.reloadActiveTimeline()
    val tableSchemaResolver = new TableSchemaResolver(metaClient)
    val latestTableSchemaFromCommitMetadata = tableSchemaResolver.getTableAvroSchemaFromLatestCommit(false)

    if (failAndDoRollback) {
      val updatesToFail =  dataGen.generateUniqueUpdates("003", 3)
      val batchToFail = recordsToStrings(updatesToFail).asScala.toSeq
      val batchToFailDf = spark.read.json(spark.sparkContext.parallelize(batchToFail, 1))
      val failDf = batchToFailDf.withColumn("data_partition_path", lit("partition1")).union(batchToFailDf.withColumn("data_partition_path", lit("partition3")))
      failDf.write.format("org.apache.hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      assertEquals(13, spark.read.format("hudi").load(basePath).count())

      metaClient.reloadActiveTimeline()
      val lastInstant = metaClient.getActiveTimeline.lastInstant().get()
      assertTrue(storage.deleteFile(new StoragePath(metaClient.getTimelinePath, metaClient.getInstantFileNameGenerator.getFileName(lastInstant))))
      assertEquals(10, spark.read.format("hudi").load(basePath).count())

      // rollback
      val writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(TypedProperties.fromMap(JavaConverters
          .mapAsJavaMapConverter(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)).asJava))
        .withPath(basePath)
        .build()
      new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
        .rollback(lastInstant.requestedTime())
    }

    if (compact) {
      assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++
        Map(HoodieCompactionConfig.COMPACTION_STRATEGY.key() -> classOf[UnBoundedCompactionStrategy].getName,
          HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleCompaction(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.compact(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    }

    if (cluster) {
      assertEquals(if (tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
        metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleClustering(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.cluster(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    }

    //init mdt
    val updateOptions = options ++ Map(HoodieMetadataConfig.PARTITIONED_RECORD_INDEX_ENABLE_PROP.key() -> "true",
      HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(updateOptions).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    val partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    val partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    val df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")
  }

  def readRecordIndex(metadata: HoodieBackedTableMetadata, recordKeys: java.util.List[String], dataTablePartition: HOption[String]): Map[String, HoodieRecordGlobalLocation] = {
    metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys), dataTablePartition)
      .collectAsList().asScala.map(p => p.getKey -> p.getValue).toMap
  }
}

object TestPartitionedRecordLevelIndex {

  case class TestPartitionedRecordLevelIndexTestCase(tableType: HoodieTableType, streamingWriteEnabled: Boolean)

  def testArgsForPartitionedRecordLevelIndex: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = false)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = false))
    )
  }
}
