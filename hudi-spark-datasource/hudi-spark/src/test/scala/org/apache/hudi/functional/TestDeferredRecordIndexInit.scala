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
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.DeferredRecordIndexTestUtils._
import org.apache.hudi.functional.TestDeferredRecordIndexInit.DeferredInitTestCase

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

import java.util

import scala.collection.JavaConverters._

/**
 * End to end coverage of hoodie.metadata.record.level.index.defer.init on its own, with no record threshold: the
 * first commit leaves nothing of the record index in the metadata table, the second builds it sized from the first
 * commit's data, and the index agrees with the table after the second commit and after the third, the first one
 * tagged through the index alone.
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
    val write = (batch: DataFrame, operation: String, mode: SaveMode) =>
      batch.write.format("hudi").options(options).option(DataSourceWriteOptions.OPERATION.key, operation).mode(mode).save(basePath)

    // Commit 1: the record index must not be built, since the table had no completed commit when the write started.
    val firstBatch = dataGen.generateInserts("001", 2000)
    write(toDataset(spark, firstBatch), testCase.firstOperation, SaveMode.Overwrite)
    assertTableVersion(testCase.tableVersion)
    assertRecordIndexNotBuilt(basePath, storageConf)
    val rowsBeforeBuild = rowsPerPartition(spark, basePath)

    // Commit 2 builds the index from what commit 1 left, then writes updates, deletes and enough new rows that an
    // index sized after this write would get a different file group count.
    // It also starts a partition commit 1 did not write, which the build cannot have sized from commit 1's data.
    val newRecords = dataGen.generateInserts("002", 2000)
    val firstNewPartition = "2026/02/01"
    val firstNewPartitionRecords = dataGen.generateInsertsForPartition("002", 20, firstNewPartition)
    val deletedRecords = firstBatch.subList(300, 350)
    write(toDataset(spark, dataGen.generateUpdates("002", firstBatch.subList(0, 300)))
      .union(toDataset(spark, dataGen.generateUpdates("002", deletedRecords)).withColumn("_hoodie_is_deleted", lit(true)))
      .union(toDataset(spark, newRecords))
      .union(toDataset(spark, firstNewPartitionRecords)), UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertRecordIndexBuilt(basePath, storageConf, testCase.partitioned)
    val rowsSizedFrom = new util.HashMap[String, java.lang.Long](rowsBeforeBuild)
    rowsSizedFrom.put(firstNewPartition, 0L)
    assertRecordIndexSizedFrom(basePath, storageConf, toProperties(options), testCase.partitioned, rowsSizedFrom)
    val deletedKeys = deletedRecords.asScala.map(_.getRecordKey).toSet
    val keysAfterBuild = (firstBatch.asScala ++ newRecords.asScala ++ firstNewPartitionRecords.asScala)
      .map(_.getRecordKey).filterNot(deletedKeys)
    assertTableKeys(keysAfterBuild)
    assertRecordIndexMatchesTable(spark, basePath, storageConf)

    // Commit 3 is the first write tagged through the index alone: it updates rows commit 2 inserted, brings back the
    // keys commit 2 deleted, and starts a partition no earlier commit wrote.
    val newPartitionRecords = dataGen.generateInsertsForPartition("003", 20, "2026/01/01")
    write(toDataset(spark, dataGen.generateUpdates("003", newRecords.subList(0, 300)))
      .union(toDataset(spark, dataGen.generateUpdates("003", deletedRecords)))
      .union(toDataset(spark, newPartitionRecords)), UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertTableKeys(keysAfterBuild ++ deletedKeys ++ newPartitionRecords.asScala.map(_.getRecordKey))
    assertRecordIndexMatchesTable(spark, basePath, storageConf)
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
    write(firstBatch.asInstanceOf[util.List[HoodieRecord[_]]], testCase.firstOperation, SaveMode.Overwrite)
    assertTableVersion(testCase.tableVersion)
    assertRecordIndexNotBuilt(basePath, storageConf)

    // Commit 2 builds the record index and then replaces every partition the first commit wrote.
    val replacingBatch = dataGen.generateInserts("002", 500)
    write(replacingBatch.asInstanceOf[util.List[HoodieRecord[_]]], INSERT_OVERWRITE_OPERATION_OPT_VAL, SaveMode.Append)
    assertRecordIndexBuilt(basePath, storageConf, testCase.partitioned)
    val replacingKeys = replacingBatch.asScala.map(_.getRecordKey)
    assertTableKeys(replacingKeys)
    assertRecordIndexMatchesTable(spark, basePath, storageConf)

    // Commit 3 is tagged through the record index. Replaced records must not come back and survivors must not double.
    val laterUpdates = dataGen.generateUpdates("003", replacingBatch.subList(0, 100))
    write(laterUpdates.asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertTableKeys(replacingKeys)
    assertRecordIndexMatchesTable(spark, basePath, storageConf)
  }

  /**
   * A first commit that fails is rolled back by the next write and leaves no data behind, so the retry must defer the
   * build as well rather than build the index from an empty table.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testRecordIndexDeferredPastFailedFirstCommit(partitioned: Boolean): Unit = {
    val options = deferredInitOptions(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned,
      HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL))
    val dataGen = new HoodieTestDataGenerator()
    val write = (records: util.List[HoodieRecord[_]], operation: String, mode: SaveMode) =>
      toDataset(spark, records).write.format("hudi").options(options)
        .option(DataSourceWriteOptions.OPERATION.key, operation).mode(mode).save(basePath)

    // Commit 1 is turned back into an inflight commit, as a writer that failed before completing it would leave it.
    write(dataGen.generateInserts("001", 2000).asInstanceOf[util.List[HoodieRecord[_]]], BULK_INSERT_OPERATION_OPT_VAL,
      SaveMode.Overwrite)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    metaClient.getActiveTimeline.revertToInflight(metaClient.getCommitsTimeline.filterCompletedInstants().lastInstant().get())

    // The retry rolls commit 1 back first, so it starts on a table with no data and must not build the index.
    val retriedBatch = dataGen.generateInserts("002", 1000)
    write(retriedBatch.asInstanceOf[util.List[HoodieRecord[_]]], BULK_INSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertTableKeys(retriedBatch.asScala.map(_.getRecordKey))
    assertRecordIndexNotBuilt(basePath, storageConf)
    val rowsBeforeBuild = rowsPerPartition(spark, basePath)

    val newRecords = dataGen.generateInserts("003", 1000)
    write(newRecords.asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertRecordIndexBuilt(basePath, storageConf, partitioned)
    assertRecordIndexSizedFrom(basePath, storageConf, toProperties(options), partitioned, rowsBeforeBuild)
    assertTableKeys((retriedBatch.asScala ++ newRecords.asScala).map(_.getRecordKey))
    assertRecordIndexMatchesTable(spark, basePath, storageConf)
  }

  /**
   * The commit that builds the record index fails after the build and is rolled back by the retry. The index must still
   * agree with the table, holding commit 1's rows and the retry's, and none of what the failed commit wrote.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testRecordIndexAfterFailedBuildingCommit(partitioned: Boolean): Unit = {
    val options = deferredInitOptions(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned,
      HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL))
    val dataGen = new HoodieTestDataGenerator()
    val write = (records: util.List[HoodieRecord[_]], operation: String, mode: SaveMode) =>
      toDataset(spark, records).write.format("hudi").options(options)
        .option(DataSourceWriteOptions.OPERATION.key, operation).mode(mode).save(basePath)

    val firstBatch = dataGen.generateInserts("001", 2000)
    write(firstBatch.asInstanceOf[util.List[HoodieRecord[_]]], BULK_INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    assertRecordIndexNotBuilt(basePath, storageConf)

    // Commit 2 builds the index, then is turned back into an inflight commit as a writer that failed would leave it.
    write(dataGen.generateInserts("002", 500).asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL,
      SaveMode.Append)
    assertRecordIndexBuilt(basePath, storageConf, partitioned)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    metaClient.getActiveTimeline.revertToInflight(metaClient.getCommitsTimeline.filterCompletedInstants().lastInstant().get())

    // The retry rolls commit 2 back first, then updates commit 1's rows and adds its own.
    val retriedRecords = dataGen.generateInserts("003", 500)
    write(dataGen.generateUpdates("003", firstBatch.subList(0, 300)).asInstanceOf[util.List[HoodieRecord[_]]],
      UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    write(retriedRecords.asInstanceOf[util.List[HoodieRecord[_]]], UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertRecordIndexBuilt(basePath, storageConf, partitioned)
    assertTableKeys((firstBatch.asScala ++ retriedRecords.asScala).map(_.getRecordKey))
    assertRecordIndexMatchesTable(spark, basePath, storageConf)
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
         |  ${HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key} = '${testCase.streamingWrites}',
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
    val rowsOf = (from: Int, to: Int, ts: Long, name: String) =>
      s"select cast(id as string) as id, ${ts}L as ts, '$name' as name, concat('p', cast(id % 3 as string)) as part from range($from, $to)"
    val merge = (source: String) => spark.sql(
      s"""
         |merge into $tableName t
         |using ($source) s
         |on t.id = s.id
         |when matched and s.name != 'delete' then update set *
         |when matched and s.name = 'delete' then delete
         |when not matched then insert *
       """.stripMargin)
    val assertSqlTable = (expectedIds: Seq[Int], updatedName: String, updatedRows: Long) => {
      assertEquals(expectedIds.map(_.toString).sorted, spark.sql(s"select id from $tableName").collect().map(_.getString(0)).toSeq.sorted)
      assertEquals(updatedRows, spark.sql(s"select id from $tableName where name = '$updatedName'").count())
      assertRecordIndexMatchesTable(spark, tablePath, storageConf)
    }

    try {
      spark.conf.set("hoodie.spark.sql.insert.into.operation", testCase.firstOperation)
      spark.sql(s"insert into $tableName ${rowsOf(0, 2000, 1L, "v1")}")
    } finally {
      spark.conf.unset("hoodie.spark.sql.insert.into.operation")
    }
    assertEquals(testCase.tableVersion, HoodieTableMetaClient.reload(sqlMetaClient).getTableConfig.getTableVersion,
      "Table was written at an unexpected version")
    assertRecordIndexNotBuilt(tablePath, storageConf)
    // Keyed by the partition path the table writes, which is Hive styled for a SQL created table.
    val rowsBeforeBuild = rowsPerPartition(spark, tablePath)

    // The merge builds the index from what the insert left, then updates, deletes and adds enough rows that an index
    // sized after the merge would get a different file group count.
    merge(s"${rowsOf(0, 300, 4L, "v4")} union all ${rowsOf(300, 350, 4L, "delete")} union all ${rowsOf(2000, 4000, 4L, "v4")}")
    assertRecordIndexBuilt(tablePath, storageConf, testCase.partitioned)
    assertRecordIndexSizedFrom(tablePath, storageConf, toProperties(Map(
      HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key -> "9600",
      minFileGroupProp -> "1",
      maxFileGroupProp -> "1000")), testCase.partitioned, rowsBeforeBuild)
    val idsAfterBuild = (0 until 300) ++ (350 until 4000)
    assertSqlTable(idsAfterBuild, "v4", 2300L)

    // The next merge is tagged through the index alone: it updates rows the first merge inserted and brings back the
    // rows it deleted.
    merge(s"${rowsOf(2000, 2300, 5L, "v5")} union all ${rowsOf(300, 350, 5L, "v5")}")
    assertSqlTable(idsAfterBuild ++ (300 until 350), "v5", 350L)
  }

  private def assertTableVersion(expected: HoodieTableVersion): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(expected, metaClient.getTableConfig.getTableVersion, "Table was written at an unexpected version")
  }

  private def assertTableKeys(expectedKeys: Iterable[String]): Unit =
    assertEquals(expectedKeys.toSeq.sorted, spark.read.format("hudi").load(basePath).select("_row_key").collect()
      .map(_.getString(0)).toSeq.sorted)
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

  // The partitioned record index and streaming metadata writes both need a table version above six, and one case
  // each here and on the streamer path keeps version six covered.
  def testArgsForDeferredInit: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.SIX, streamingWrites = false, BULK_INSERT_OPERATION_OPT_VAL)),
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
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "bulk_insert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "insert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, "upsert")),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, "bulk_insert"))
    )
  }

  def testArgsForInsertOverwrite: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.COPY_ON_WRITE, partitioned = true, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL)),
      Arguments.arguments(DeferredInitTestCase(HoodieTableType.MERGE_ON_READ, partitioned = false, HoodieTableVersion.TEN, streamingWrites = true, BULK_INSERT_OPERATION_OPT_VAL))
    )
  }
}
