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
import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecordGlobalLocation, HoodieTableType}
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType.PARTITIONED_RECORD_INDEX
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.stream.Collectors

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

@Tag("functional")
class TestPartitionedRecordLevelIndex extends RecordLevelIndexTestBase {
  class testPartitionedRecordLevelIndexHolder {
    var bulkRecordKeys: java.util.List[String] = null
    var options: Map[String, String] = null
    var recordKeys: java.util.List[String] = null
    var newRecordKeys: java.util.List[String] = null
  }

  def testPartitionedRecordLevelIndex(tableType: HoodieTableType, holder: testPartitionedRecordLevelIndexHolder): Unit = {
    val dataGen = new HoodieTestDataGenerator();
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatch = recordsToStrings(inserts).asScala.toSeq
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 1))
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      PRECOMBINE_FIELD.key -> "timestamp",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.PARTITIONED_RECORD_INDEX_ENABLE_PROP.key() -> "true",
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
    var partition1Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(5, partition1Locations.size())
    var partition2Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(5, partition2Locations.size())
    var df = spark.read.format("hudi").load(basePath).collect()
    validateDfwithLocations(df, partition1Locations, "partition1")
    validateDfwithLocations(df, partition2Locations, "partition2")

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
    partition1Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(5, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(5, partition2Locations.size())
    df = spark.read.format("hudi").load(basePath).collect()
    validateDfwithLocations(df, partition1Locations, "partition1")
    validateDfwithLocations(df, partition2Locations, "partition2")

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
    partition1Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(5, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(5, partition2Locations.size())
    df = spark.read.format("hudi").load(basePath).collect()
    validateDfwithLocations(df, partition1Locations, "partition1")
    validateDfwithLocations(df, partition2Locations, "partition2")

    val newRecordKeys = newInserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.newRecordKeys = newRecordKeys
    partition1Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(0, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(3, partition2Locations.size())
    var partition3Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition3"))
    assertEquals(3, partition3Locations.size())
    validateDfwithLocations(df, partition3Locations, "partition3")

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
    partition1Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(4, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(5, partition2Locations.size())
    df = spark.read.format("hudi").load(basePath).collect()
    validateDfwithLocations(df, partition1Locations, "partition1")
    validateDfwithLocations(df, partition2Locations, "partition2")

    assertFalse(partition1Locations.containsKey(newDeletes.get(0).getRecordKey))
    assertTrue(partition2Locations.containsKey(newDeletes.get(0).getRecordKey))
    partition1Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(0, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(3, partition2Locations.size())
    partition3Locations = metadata.readRecordIndex(newRecordKeys, org.apache.hudi.common.util.Option.of("partition3"))
    assertEquals(3, partition3Locations.size())
    validateDfwithLocations(df, partition2Locations, "partition2")
    validateDfwithLocations(df, partition3Locations, "partition3")

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
    val partition0Locations = metadata.readRecordIndex(bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition0"))
    assertEquals(5, partition0Locations.size())
    df = spark.read.format("hudi").load(basePath).collect()
    validateDfwithLocations(df, partition0Locations, "partition0")
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexRollback(tableType: HoodieTableType): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(tableType, holder)
    val writeConfig = getWriteConfig(holder.options)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      .rollback(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    try {
      val partition0Locations = metadata.readRecordIndex(holder.bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition0"))
      fail("rollback happened, so partition should be deleted")
    } catch {
      case t: Throwable => assertTrue(t.isInstanceOf[ArithmeticException])
    }
  }

  @Test
  def testPartitionedRecordLevelIndexCompact(): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(HoodieTableType.MERGE_ON_READ, holder)
    assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options)
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.compact(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("commit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexCluster(tableType: HoodieTableType): Unit = {
    val holder = new testPartitionedRecordLevelIndexHolder
    testPartitionedRecordLevelIndex(tableType, holder)
    assertEquals(if (tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
      metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleClustering(org.apache.hudi.common.util.Option.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.cluster(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  private def validateDfwithLocations(df: Array[Row], locations: java.util.Map[String, HoodieRecordGlobalLocation], partition: String): Unit = {
    var count: Int = 0
    for (row <- df) {
      val recordKey = row.getString(2)
      if (locations.containsKey(recordKey)) {
        if (partition == row.getString(3)) {
          count += 1
          val loc = locations.get(recordKey)
          assertEquals(row.getString(3), loc.getPartitionPath)
          assertEquals(FSUtils.getFileId(row.getString(4)), loc.getFileId)
        }
      }
    }
    assertEquals(locations.size, count)
  }

  private def doAllAssertions(holder: testPartitionedRecordLevelIndexHolder, metadata: HoodieBackedTableMetadata): Unit = {
    val df = spark.read.format("hudi").load(basePath).collect()
    var partition0Locations = metadata.readRecordIndex(holder.recordKeys, org.apache.hudi.common.util.Option.of("partition0"))
    assertEquals(0, partition0Locations.size())
    var partition1Locations = metadata.readRecordIndex(holder.recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(4, partition1Locations.size())
    validateDfwithLocations(df, partition1Locations, "partition1")
    var partition2Locations = metadata.readRecordIndex(holder.recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(5, partition2Locations.size())
    validateDfwithLocations(df, partition2Locations, "partition2")
    var partition3Locations = metadata.readRecordIndex(holder.recordKeys, org.apache.hudi.common.util.Option.of("partition3"))
    assertEquals(0, partition3Locations.size())

    partition0Locations = metadata.readRecordIndex(holder.newRecordKeys, org.apache.hudi.common.util.Option.of("partition0"))
    assertEquals(0, partition0Locations.size())
    partition1Locations = metadata.readRecordIndex(holder.newRecordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(0, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(holder.newRecordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(3, partition2Locations.size())
    validateDfwithLocations(df, partition2Locations, "partition2")
    partition3Locations = metadata.readRecordIndex(holder.newRecordKeys, org.apache.hudi.common.util.Option.of("partition3"))
    assertEquals(3, partition3Locations.size())
    validateDfwithLocations(df, partition3Locations, "partition3")

    partition0Locations = metadata.readRecordIndex(holder.bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition0"))
    assertEquals(5, partition0Locations.size())
    validateDfwithLocations(df, partition0Locations, "partition0")
    partition1Locations = metadata.readRecordIndex(holder.bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition1"))
    assertEquals(0, partition1Locations.size())
    partition2Locations = metadata.readRecordIndex(holder.bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition2"))
    assertEquals(0, partition2Locations.size())
    partition3Locations = metadata.readRecordIndex(holder.bulkRecordKeys, org.apache.hudi.common.util.Option.of("partition3"))
    assertEquals(0, partition3Locations.size())
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexInitializationBasic(tableType: HoodieTableType): Unit = {
    testPartitionedRecordLevelIndexInitialization(tableType, failWrite = false, failAndDoRollback = false, compact = false, cluster = false)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexInitializationFailedWrite(tableType: HoodieTableType): Unit = {
    testPartitionedRecordLevelIndexInitialization(tableType, failWrite = true, failAndDoRollback = false, compact = false, cluster = false)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexInitializationRollback(tableType: HoodieTableType): Unit = {
    testPartitionedRecordLevelIndexInitialization(tableType, failWrite = false, failAndDoRollback = true, compact = false, cluster = false)
  }

  @Test
  def testPartitionedRecordLevelIndexInitializationCompact(): Unit = {
    testPartitionedRecordLevelIndexInitialization(HoodieTableType.MERGE_ON_READ, failWrite = false, failAndDoRollback = false, compact = true, cluster = false)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionedRecordLevelIndexInitializationCluster(tableType: HoodieTableType): Unit = {
    testPartitionedRecordLevelIndexInitialization(tableType, failWrite = false, failAndDoRollback = false, compact = false, cluster = true)
  }

  def testPartitionedRecordLevelIndexInitialization(tableType: HoodieTableType,
                                                    failWrite: Boolean,
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
      PRECOMBINE_FIELD.key -> "timestamp",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()-> "false",
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

    if (failWrite || failAndDoRollback) {
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
      assertTrue(fs.delete(new Path(metaClient.getMetaPath, lastInstant.getFileName), false))
      assertEquals(10, spark.read.format("hudi").load(basePath).count())

      if (failAndDoRollback) {
        val writeConfig = HoodieWriteConfig.newBuilder()
          .withProps(TypedProperties.fromMap(JavaConverters
            .mapAsJavaMapConverter(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)).asJava))
          .withPath(basePath)
          .build()
        new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
          .rollback(lastInstant.getTimestamp)
      }
    }

    if (compact) {
      assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++
        Map(HoodieCompactionConfig.COMPACTION_STRATEGY.key() -> classOf[UnBoundedCompactionStrategy].getName,
          HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.compact(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("commit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    }

    if (cluster) {
      assertEquals(if (tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
        metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleClustering(org.apache.hudi.common.util.Option.empty())
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
    try {
      val partition1Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition1"))
      assertEquals(5, partition1Locations.size())
      val partition2Locations = metadata.readRecordIndex(recordKeys, org.apache.hudi.common.util.Option.of("partition2"))
      assertEquals(5, partition2Locations.size())
      val df = spark.read.format("hudi").load(basePath).collect()
      validateDfwithLocations(df, partition1Locations, "partition1")
      validateDfwithLocations(df, partition2Locations, "partition2")
      if (failWrite) {
        fail("MDT should not initialize")
      }
    } catch {
      case e: IllegalStateException => assertEquals("Record index is not initialized in MDT",  e.getMessage)
    }
  }
}
