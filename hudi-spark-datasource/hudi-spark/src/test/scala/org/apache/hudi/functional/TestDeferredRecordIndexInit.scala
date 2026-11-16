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

import org.apache.hudi.{DataSourceWriteOptions, SparkDatasetMixin}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestDeferredRecordIndexInit.{expectedFileGroupCount, DeferredInitTestCase}
import org.apache.hudi.metadata.{HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util

import scala.collection.JavaConverters._

/**
 * End to end coverage of hoodie.metadata.record.level.index.defer.init on its own, with no record threshold:
 * the record index is not built by the first commit, is built by the second one and sized from the first commit's
 * data, and the switch from the fallback index produces no duplicates.
 */
@Tag("functional")
class TestDeferredRecordIndexInit extends RecordLevelIndexTestBase with SparkDatasetMixin {

  private def deferredInitOptions(testCase: DeferredInitTestCase): Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.name(),
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition_path",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.WRITE_TABLE_VERSION.key -> testCase.tableVersion.versionCode().toString,
    HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> (!testCase.partitioned).toString,
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> testCase.partitioned.toString,
    HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key -> testCase.streamingWrites.toString,
    // Pinned low so the sizing result is not simply the floor.
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1000",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1000",
    // 9600 bytes / 48 bytes per entry = 200 entries per file group.
    HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key -> "9600",
    HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key -> "true",
    HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
    HoodieIndexConfig.INDEX_TYPE.key -> (if (testCase.partitioned) "RECORD_LEVEL_INDEX" else "GLOBAL_RECORD_LEVEL_INDEX"))

  @ParameterizedTest
  @MethodSource(Array("testArgsForDeferredInit"))
  def testRecordIndexDeferredUntilSecondCommit(testCase: DeferredInitTestCase): Unit = {
    val options = deferredInitOptions(testCase)
    val dataGen = new HoodieTestDataGenerator()
    val allRecords = new util.ArrayList[HoodieRecord[_]]()
    val write = (records: util.List[HoodieRecord[_]], operation: String, mode: SaveMode) =>
      toDataset(spark, records).write.format("hudi").options(options)
        .option(DataSourceWriteOptions.OPERATION.key, operation).mode(mode).save(basePath)

    // Commit 1: the record index must not be built, since the table had no completed commit when the write started.
    val firstBatch = dataGen.generateInserts("001", 2000)
    allRecords.addAll(firstBatch)
    write(firstBatch.asInstanceOf[util.List[HoodieRecord[_]]], testCase.firstOperation, SaveMode.Overwrite)
    assertTableVersion(testCase.tableVersion)
    assertFalse(isRecordIndexAvailable, "Record index should not be built by the first commit")

    // What the index should be sized from once it is built.
    val rowsPerPartition = spark.read.format("hudi").load(basePath).groupBy("partition_path").count().collect()
      .map(row => row.getString(0) -> row.getLong(1)).toMap
    val rowsBeforeBuild = rowsPerPartition.values.sum

    // Commit 2: updates of records tagged through the fallback index, plus new records. The index is built here.
    val updates = dataGen.generateUpdates("002", allRecords.subList(0, 300))
    val newRecords = dataGen.generateInserts("002", 10)
    val secondBatch = new util.ArrayList[HoodieRecord[_]](updates)
    secondBatch.addAll(newRecords)
    allRecords.addAll(newRecords)
    write(secondBatch, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertTrue(isRecordIndexAvailable, "Record index should be built by the second commit")

    // Commit 3: upserts tagged through the record index itself.
    val laterUpdates = dataGen.generateUpdates("003", allRecords.subList(0, 300))
    write(laterUpdates.asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    // No duplicates across the fallback to record index switch.
    val keys = spark.read.format("hudi").load(basePath).select("_row_key").collect().map(_.getString(0))
    assertEquals(allRecords.asScala.map(_.getRecordKey).sorted.toSeq, keys.sorted.toSeq)

    // The index is sized from the data the first commit left behind, not from an empty table.
    val fileGroupIds = recordIndexFileGroupIds()
    if (testCase.partitioned) {
      val perPartition = fileGroupIds.groupBy(HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName)
        .mapValues(_.size).toMap
      assertEquals(rowsPerPartition.mapValues(rows => expectedFileGroupCount(rows)).toMap, perPartition)
    } else {
      assertEquals(expectedFileGroupCount(rowsBeforeBuild), fileGroupIds.size)
    }
  }

  /**
   * The commit that builds the record index replaces the data the index was just sized from, so the index has to
   * carry the replacing records and none of the replaced ones.
   */
  @ParameterizedTest
  @MethodSource(Array("testArgsForInsertOverwrite"))
  def testRecordIndexBuiltByInsertOverwrite(testCase: DeferredInitTestCase): Unit = {
    val options = deferredInitOptions(testCase)
    val dataGen = new HoodieTestDataGenerator()
    val write = (records: util.List[HoodieRecord[_]], operation: String, mode: SaveMode) =>
      toDataset(spark, records).write.format("hudi").options(options)
        .option(DataSourceWriteOptions.OPERATION.key, operation).mode(mode).save(basePath)

    val firstBatch = dataGen.generateInserts("001", 2000)
    write(firstBatch.asInstanceOf[util.List[HoodieRecord[_]]], BULK_INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    assertFalse(isRecordIndexAvailable, "Record index should not be built by the first commit")

    // Commit 2 builds the record index and then replaces every partition the first commit wrote.
    val replacingBatch = dataGen.generateInserts("002", 500)
    write(replacingBatch.asInstanceOf[util.List[HoodieRecord[_]]], INSERT_OVERWRITE_OPERATION_OPT_VAL, SaveMode.Append)
    assertTrue(isRecordIndexAvailable, "Record index should be built by the second commit")

    val replacingKeys = replacingBatch.asScala.map(_.getRecordKey).sorted.toSeq
    assertEquals(replacingKeys, spark.read.format("hudi").load(basePath).select("_row_key")
      .collect().map(_.getString(0)).sorted.toSeq)

    // Commit 3 is tagged through the record index. Replaced records must not come back and survivors must not double.
    val laterUpdates = dataGen.generateUpdates("003", replacingBatch.subList(0, 100))
    write(laterUpdates.asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertEquals(replacingKeys, spark.read.format("hudi").load(basePath).select("_row_key")
      .collect().map(_.getString(0)).sorted.toSeq)
  }

  /**
   * The same deferral through Spark SQL: INSERT INTO leaves the record index unbuilt and the following MERGE INTO
   * builds it, sized from the rows the first statement wrote.
   */
  @ParameterizedTest
  @MethodSource(Array("testArgsForSql"))
  def testRecordIndexDeferredUntilSecondStatement(testCase: DeferredInitTestCase): Unit = {
    val tableName = s"rli_deferred_sql_${if (testCase.partitioned) "partitioned" else "global"}_v${testCase.tableVersion.versionCode()}"
    val tablePath = tempDir.resolve(tableName).toAbsolutePath.toString
    val indexEnableProp = if (testCase.partitioned) {
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key
    } else {
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key
    }
    val minFileGroupProp = if (testCase.partitioned) {
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key
    } else {
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key
    }
    val maxFileGroupProp = if (testCase.partitioned) {
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key
    } else {
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key
    }
    val indexType = if (testCase.partitioned) "RECORD_LEVEL_INDEX" else "GLOBAL_RECORD_LEVEL_INDEX"
    spark.sql(
      s"""
         |create table $tableName (
         |  id string,
         |  ts long,
         |  name string,
         |  part string
         |) using hudi
         | options (
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'true',
         |  ${HoodieWriteConfig.WRITE_TABLE_VERSION.key} = '${testCase.tableVersion.versionCode()}',
         |  ${HoodieWriteConfig.AUTO_UPGRADE_VERSION.key} = 'false',
         |  $indexEnableProp = 'true',
         |  hoodie.index.type = '$indexType',
         |  ${HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key} = 'true',
         |  ${HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key} = '9600',
         |  $minFileGroupProp = '1',
         |  $maxFileGroupProp = '1000'
         | )
         | partitioned by (part)
         | location '$tablePath'
       """.stripMargin)
    val sqlMetaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(storageConf).build()
    val recordIndexBuilt = () => HoodieTableMetaClient.reload(sqlMetaClient).getTableConfig.getMetadataPartitions
      .contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
    val rowsOf = (from: Int, to: Int, ts: Long, name: String) =>
      s"select cast(id as string) as id, ${ts}L as ts, '$name' as name, concat('p', cast(id % 3 as string)) as part from range($from, $to)"

    try {
      spark.conf.set("hoodie.spark.sql.insert.into.operation", testCase.firstOperation)
      spark.sql(s"insert into $tableName ${rowsOf(0, 2000, 1L, "v1")}")
    } finally {
      spark.conf.unset("hoodie.spark.sql.insert.into.operation")
    }
    assertFalse(recordIndexBuilt(), "Record index should not be built by the first statement")

    // The index is sized from the rows the first statement wrote, so capture them before the merge adds more.
    // Keyed by the partition path the table writes, which is Hive styled for a SQL created table.
    val rowsPerPartition = spark
      .sql(s"select _hoodie_partition_path, count(*) as rows from $tableName group by _hoodie_partition_path")
      .collect().map(row => row.getString(0) -> row.getLong(1)).toMap
    val rowsBeforeBuild = rowsPerPartition.values.sum

    spark.sql(
      s"""
         |merge into $tableName t
         |using (${rowsOf(0, 300, 4L, "v4")} union all ${rowsOf(2000, 2010, 4L, "v4")}) s
         |on t.id = s.id
         |when matched then update set *
         |when not matched then insert *
       """.stripMargin)
    assertTrue(recordIndexBuilt(), "Record index should be built by the second statement")

    // Every key appears once, and the merge updated exactly the rows it matched.
    assertEquals(2010L, spark.sql(s"select id from $tableName").count())
    assertEquals(2010L, spark.sql(s"select distinct id from $tableName").count())
    assertEquals(310L, spark.sql(s"select id from $tableName where name = 'v4'").count())

    // The index is sized from the rows the first statement left behind, not from the table as the merge left it.
    val fileGroupIds = recordIndexFileGroupIds(tablePath)
    if (testCase.partitioned) {
      val perPartition = fileGroupIds.groupBy(HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName)
        .mapValues(_.size).toMap
      assertEquals(rowsPerPartition.mapValues(rows => expectedFileGroupCount(rows)).toMap, perPartition)
    } else {
      assertEquals(expectedFileGroupCount(rowsBeforeBuild), fileGroupIds.size)
    }
  }

  private def assertTableVersion(expected: HoodieTableVersion): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(expected, metaClient.getTableConfig.getTableVersion, "Table was written at an unexpected version")
  }

  private def isRecordIndexAvailable: Boolean = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
  }

  private def recordIndexFileGroupIds(tablePath: String = basePath): Seq[String] = {
    val metadataMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(tablePath))
      .setConf(storageConf)
      .build()
    HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, HOption.empty(),
      MetadataPartitionType.RECORD_INDEX.getPartitionPath).asScala.map(_.getFileId).toSeq
  }
}

object TestDeferredRecordIndexInit {

  case class DeferredInitTestCase(tableType: HoodieTableType,
                                  partitioned: Boolean,
                                  tableVersion: HoodieTableVersion,
                                  streamingWrites: Boolean,
                                  firstOperation: String) {
    override def toString: String =
      s"${tableType.name()}/${if (partitioned) "partitioned" else "global"}/v${tableVersion.versionCode()}" +
        s"/streaming=$streamingWrites/$firstOperation"
  }

  // The partitioned record index and streaming metadata writes both need a table version above six.
  def testArgsForDeferredInit: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.MERGE_ON_READ, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.MERGE_ON_READ, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.MERGE_ON_READ, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL))
    )
  }

  def testArgsForSql: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, "bulk_insert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "bulk_insert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "insert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "upsert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, "bulk_insert"))
    )
  }

  def testArgsForInsertOverwrite: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.MERGE_ON_READ, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL))
    )
  }

  /** Mirrors HoodieTableMetadataUtil.estimateFileGroupCount with growth factor 2 and 200 entries per file group. */
  def expectedFileGroupCount(records: Long): Int = {
    val estimated = (math.ceil(records * 2.0).toLong / 200).toInt
    if (estimated >= 1000) 1000 else if (estimated <= 1) 1 else estimated
  }
}
