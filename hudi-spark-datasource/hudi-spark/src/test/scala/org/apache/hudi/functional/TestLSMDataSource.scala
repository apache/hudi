/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceUtils, DataSourceWriteOptions}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieRecord, HoodieRecordPayload, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.{BulkInsertSortMode, RowCustomColumnsSortPartitioner}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.testutils.{HoodieClientTestUtils, SparkClientFunctionalTestHarness}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}

import scala.collection.JavaConverters._

@Tag("functional")
class TestLSMDataSource extends SparkClientFunctionalTestHarness {

  private val FirstPartition = "p1"
  private val SecondPartition = "p2"

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType], names = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testStandardWriteOperations(tableType: HoodieTableType): Unit = {
    val tablePath = s"${basePath}_${tableType.name.toLowerCase}_lsm_dataframe"
    val options = baseOptions(tableType) +
      (DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "false")

    val inserts = rows(Seq(
      ("😀-insert-p1", "v1", 1L, FirstPartition),
      ("Ａ-insert-p1", "v1", 1L, FirstPartition),
      ("middle-insert-p1", "v1", 1L, FirstPartition),
      ("😀-insert-p2", "v1", 1L, SecondPartition),
      ("Ａ-insert-p2", "v1", 1L, SecondPartition)))
    write(inserts, tablePath, options, WriteOperationType.INSERT, SaveMode.Overwrite)
    assertLatestBaseFilesSorted(tablePath, WriteOperationType.INSERT)
    assertSnapshot(tablePath, Map(
      "😀-insert-p1" -> "v1",
      "Ａ-insert-p1" -> "v1",
      "middle-insert-p1" -> "v1",
      "😀-insert-p2" -> "v1",
      "Ａ-insert-p2" -> "v1"))

    val updates = rows(Seq(
      ("😀-insert-p1", "v2", 2L, FirstPartition),
      ("Ａ-insert-p1", "v2", 2L, FirstPartition)))
    write(updates, tablePath, options, WriteOperationType.UPSERT)
    assertChangedFilesSorted(tablePath, tableType, WriteOperationType.UPSERT, "log")
    assertSnapshot(tablePath, Map(
      "😀-insert-p1" -> "v2",
      "Ａ-insert-p1" -> "v2",
      "middle-insert-p1" -> "v1",
      "😀-insert-p2" -> "v1",
      "Ａ-insert-p2" -> "v1"))

    val deletes = rows(Seq(
      ("😀-insert-p1", "v2", 3L, FirstPartition),
      ("Ａ-insert-p1", "v2", 3L, FirstPartition)))
    write(deletes, tablePath, options, WriteOperationType.DELETE)
    assertChangedFilesSorted(tablePath, tableType, WriteOperationType.DELETE, "deletes")
    assertSnapshot(tablePath, Map(
      "middle-insert-p1" -> "v1",
      "😀-insert-p2" -> "v1",
      "Ａ-insert-p2" -> "v1"))

    val partitionOverwrite = rows(Seq(
      ("😀-overwrite-p2", "overwrite", 4L, SecondPartition),
      ("Ａ-overwrite-p2", "overwrite", 4L, SecondPartition)))
    write(partitionOverwrite, tablePath, options, WriteOperationType.INSERT_OVERWRITE)
    assertLatestBaseFilesSorted(tablePath, WriteOperationType.INSERT_OVERWRITE)
    assertSnapshot(tablePath, Map(
      "middle-insert-p1" -> "v1",
      "😀-overwrite-p2" -> "overwrite",
      "Ａ-overwrite-p2" -> "overwrite"))

    val tableOverwrite = rows(Seq(
      ("😀-overwrite-table", "table-overwrite", 5L, FirstPartition),
      ("Ａ-overwrite-table", "table-overwrite", 5L, FirstPartition)))
    write(tableOverwrite, tablePath, options, WriteOperationType.INSERT_OVERWRITE_TABLE)
    assertLatestBaseFilesSorted(tablePath, WriteOperationType.INSERT_OVERWRITE_TABLE)
    assertSnapshot(tablePath, Map(
      "😀-overwrite-table" -> "table-overwrite",
      "Ａ-overwrite-table" -> "table-overwrite"))

    write(
      tableOverwrite.limit(0),
      tablePath,
      options + (DataSourceWriteOptions.PARTITIONS_TO_DELETE.key -> FirstPartition),
      WriteOperationType.DELETE_PARTITION)
    assertTrue(latestBaseFiles(tablePath).isEmpty)
    assertSnapshot(tablePath, Map.empty)
  }

  @ParameterizedTest
  @MethodSource(Array("bulkInsertWithHoodieRecordPathParams"))
  def testBulkInsertWithHoodieRecordPath(
      tableType: HoodieTableType,
      sortMode: BulkInsertSortMode): Unit = {
    val tablePath = s"${basePath}_${tableType.name.toLowerCase}_lsm_bulk_insert_${sortMode.name.toLowerCase}"
    val options = baseOptions(tableType) ++ Map(
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "false",
      HoodieWriteConfig.BULK_INSERT_SORT_MODE.key -> sortMode.name,
      HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key -> "2")
    val inserts = rows(Seq(
      ("😀-bulk-p1", "v1", 1L, FirstPartition),
      ("Ａ-bulk-p1", "v1", 1L, FirstPartition),
      ("middle-bulk-p1", "v1", 1L, FirstPartition),
      ("😀-bulk-p2", "v1", 1L, SecondPartition),
      ("Ａ-bulk-p2", "v1", 1L, SecondPartition)))

    write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite)

    assertLatestBaseFilesSorted(tablePath, WriteOperationType.BULK_INSERT)
    assertSnapshot(tablePath, Map(
      "😀-bulk-p1" -> "v1",
      "Ａ-bulk-p1" -> "v1",
      "middle-bulk-p1" -> "v1",
      "😀-bulk-p2" -> "v1",
      "Ａ-bulk-p2" -> "v1"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[BulkInsertSortMode], names = Array(
    "GLOBAL_SORT", "PARTITION_SORT", "PARTITION_PATH_REPARTITION_AND_SORT"))
  def testBulkInsertWithRowWriter(sortMode: BulkInsertSortMode): Unit = {
    val tablePath = s"${basePath}_cow_lsm_row_bulk_insert_${sortMode.name.toLowerCase}"
    val options = baseOptions(HoodieTableType.COPY_ON_WRITE) ++ Map(
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
      HoodieWriteConfig.BULK_INSERT_SORT_MODE.key -> sortMode.name,
      HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key -> "2")
    val inserts = rows(Seq(
      ("😀-row-p1", "v1", 1L, FirstPartition),
      ("Ａ-row-p1", "v1", 1L, FirstPartition),
      ("middle-row-p1", "v1", 1L, FirstPartition),
      ("😀-row-p2", "v1", 1L, SecondPartition),
      ("Ａ-row-p2", "v1", 1L, SecondPartition)))

    write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite)

    assertLatestBaseFilesSorted(tablePath, WriteOperationType.BULK_INSERT)
    assertSnapshot(tablePath, Map(
      "😀-row-p1" -> "v1",
      "Ａ-row-p1" -> "v1",
      "middle-row-p1" -> "v1",
      "😀-row-p2" -> "v1",
      "Ａ-row-p2" -> "v1"))
  }

  @ParameterizedTest
  @MethodSource(Array("bucketBulkInsertParams"))
  def testBucketIndexBulkInsert(
      tableType: HoodieTableType,
      bucketEngineType: HoodieIndex.BucketIndexEngineType,
      enableRowWriter: Boolean): Unit = {
    val tablePath = s"${basePath}_${tableType.name.toLowerCase}_${bucketEngineType.name.toLowerCase}_$enableRowWriter"
    val options = baseOptions(tableType) ++ Map(
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> enableRowWriter.toString,
      HoodieWriteConfig.BULK_INSERT_SORT_MODE.key -> BulkInsertSortMode.NONE.name,
      HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.BUCKET.name,
      HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key -> bucketEngineType.name,
      HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key -> "id",
      HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key -> "1")
    val inserts = rows(Seq(
      ("😀-bucket-p1", "v1", 1L, FirstPartition),
      ("Ａ-bucket-p1", "v1", 1L, FirstPartition),
      ("middle-bucket-p1", "v1", 1L, FirstPartition),
      ("😀-bucket-p2", "v1", 1L, SecondPartition),
      ("Ａ-bucket-p2", "v1", 1L, SecondPartition)))

    write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite)

    assertLatestBaseFilesSorted(tablePath, WriteOperationType.BULK_INSERT)
    val initialFileIds = latestBaseFiles(tablePath).map(_.getFileId).toSet
    assertEquals(2, initialFileIds.size)
    assertSnapshot(tablePath, Map(
      "😀-bucket-p1" -> "v1",
      "Ａ-bucket-p1" -> "v1",
      "middle-bucket-p1" -> "v1",
      "😀-bucket-p2" -> "v1",
      "Ａ-bucket-p2" -> "v1"))

    val upserts = rows(Seq(
      ("😀-bucket-p1", "v2", 2L, FirstPartition),
      ("Ａ-bucket-p1", "v2", 2L, FirstPartition),
      ("middle-bucket-p1", "v2", 2L, FirstPartition),
      ("ascii-new-p1", "new", 2L, FirstPartition)))
    write(upserts, tablePath, options, WriteOperationType.UPSERT)

    assertChangedFilesSorted(tablePath, tableType, WriteOperationType.UPSERT, "log")
    assertEquals(initialFileIds, latestBaseFiles(tablePath).map(_.getFileId).toSet)
    assertSnapshot(tablePath, Map(
      "😀-bucket-p1" -> "v2",
      "Ａ-bucket-p1" -> "v2",
      "middle-bucket-p1" -> "v2",
      "ascii-new-p1" -> "new",
      "😀-bucket-p2" -> "v1",
      "Ａ-bucket-p2" -> "v1"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[BulkInsertSortMode], names = Array(
    "NONE", "PARTITION_PATH_REPARTITION"))
  def testBulkInsertRejectsNonSortingModes(sortMode: BulkInsertSortMode): Unit = {
    Seq(false, true).foreach { enableRowWriter =>
      val tablePath = s"${basePath}_cow_lsm_reject_${sortMode.name.toLowerCase}_$enableRowWriter"
      val options = baseOptions(HoodieTableType.COPY_ON_WRITE) ++ Map(
        DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> enableRowWriter.toString,
        HoodieWriteConfig.BULK_INSERT_SORT_MODE.key -> sortMode.name)
      val inserts = rows(Seq(("key-1", "v1", 1L, FirstPartition)))

      val exception = assertThrows(classOf[HoodieException], () =>
        write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite))

      assertExceptionChainContains(exception,
        s"""The bulk insert sort mode "${sortMode.name}" does not guarantee record ordering""")
    }
  }

  private def assertExceptionChainContains(exception: Throwable, expectedMessage: String): Unit = {
    var current = exception
    var found = false
    while (current != null && !found) {
      found = current.getMessage != null && current.getMessage.contains(expectedMessage)
      current = current.getCause
    }
    assertTrue(found,
      s"Expected exception chain to contain '$expectedMessage', but caught: $exception")
  }

  @Test
  def testLsmRowWriterRejectsDisabledMetaFields(): Unit = {
    val tablePath = s"${basePath}_cow_lsm_row_without_meta_fields"
    val options = baseOptions(HoodieTableType.COPY_ON_WRITE) ++ Map(
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "false")
    val inserts = rows(Seq(
      ("key-2", "v2", 1L, FirstPartition),
      ("key-1", "v1", 1L, FirstPartition)))

    val exception = assertThrows(classOf[HoodieException], () =>
      write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite))

    assertTrue(exception.getMessage.contains(
      "The Dataset Row writer requires hoodie.populate.meta.fields=true for LSM tables"))
    val instants = createMetaClient(tablePath).reloadActiveTimeline().getInstants.asScala
    assertEquals(1, instants.size)
    assertEquals(org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED, instants.head.getState)
  }

  @Test
  def testLsmRowWriterRejectsCustomPartitionerBeforeInflight(): Unit = {
    val tablePath = s"${basePath}_cow_lsm_row_custom_partitioner"
    val options = baseOptions(HoodieTableType.COPY_ON_WRITE) ++ Map(
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
      HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME.key ->
        classOf[RowCustomColumnsSortPartitioner].getName,
      HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS.key -> "value")
    val inserts = rows(Seq(("key-1", "v1", 1L, FirstPartition)))

    assertThrows(classOf[HoodieException], () =>
      write(inserts, tablePath, options, WriteOperationType.BULK_INSERT, SaveMode.Overwrite))

    val instants = createMetaClient(tablePath).reloadActiveTimeline().getInstants.asScala
    assertEquals(1, instants.size)
    assertEquals(org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED, instants.head.getState)
  }

  @Test
  def testCompactionProducesSortedBaseFile(): Unit = {
    val tablePath = s"${basePath}_mor_lsm_compaction"
    val options = baseOptions(HoodieTableType.MERGE_ON_READ) ++ Map(
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1")

    write(rows(Seq(
      ("😀-compact", "v1", 1L, FirstPartition),
      ("Ａ-compact", "v1", 1L, FirstPartition))),
      tablePath, options, WriteOperationType.INSERT, SaveMode.Overwrite)
    write(rows(Seq(
      ("Ａ-compact", "v2", 2L, FirstPartition))),
      tablePath, options, WriteOperationType.UPSERT)

    val compactionInstant = withWriteClient(tablePath, options) { client =>
      val instant = client.scheduleCompaction(Option.empty()).get()
      val statuses = client.compact(instant, true).getWriteStatuses.collect()
      assertFalse(statuses.isEmpty)
      assertTrue(statuses.asScala.forall(status => !status.hasErrors))
      instant
    }

    val metaClient = createMetaClient(tablePath)
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants.containsInstant(compactionInstant))
    val compactedBaseFiles = latestBaseFiles(tablePath)
    assertFalse(compactedBaseFiles.isEmpty)
    assertTrue(compactedBaseFiles.forall(_.getCommitTime == compactionInstant))
    compactedBaseFiles.foreach(baseFile => assertParquetFileSorted(baseFile.getPath, WriteOperationType.COMPACT))
    assertSnapshot(tablePath, Map(
      "😀-compact" -> "v1",
      "Ａ-compact" -> "v2"))
  }

  @Test
  def testLogCompactionProducesSortedNativeRun(): Unit = {
    val tablePath = s"${basePath}_mor_lsm_log_compaction"
    val options = baseOptions(HoodieTableType.MERGE_ON_READ) ++ Map(
      HoodieCompactionConfig.ENABLE_LOG_COMPACTION.key -> "true",
      HoodieCompactionConfig.LOG_COMPACTION_BLOCKS_THRESHOLD.key -> "1")

    write(rows(Seq(
      ("😀-log-compact", "v1", 1L, FirstPartition),
      ("Ａ-log-compact", "v1", 1L, FirstPartition))),
      tablePath, options, WriteOperationType.INSERT, SaveMode.Overwrite)
    write(rows(Seq(
      ("Ａ-log-compact", "v2", 2L, FirstPartition))),
      tablePath, options, WriteOperationType.UPSERT)
    write(rows(Seq(
      ("😀-log-compact", "v2", 3L, FirstPartition))),
      tablePath, options, WriteOperationType.UPSERT)

    val expectedSnapshot = Map(
      "😀-log-compact" -> "v2",
      "Ａ-log-compact" -> "v2")
    assertSnapshot(tablePath, expectedSnapshot)

    val logCompactionInstant = withWriteClient(tablePath, options) { client =>
      val instant = client.scheduleLogCompaction(Option.empty()).get()
      client.logCompact(instant, true)
      instant
    }

    val metaClient = createMetaClient(tablePath)
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants.containsInstant(logCompactionInstant))
    val compactedLogFiles = HoodieTestUtils.listNativeLogFiles(metaClient.getStorage, tablePath).asScala
      .filter(path => path.getName.contains(logCompactionInstant) && path.getName.endsWith(".log.parquet"))
    assertEquals(1, compactedLogFiles.size)
    assertParquetFileSorted(compactedLogFiles.head.toString, WriteOperationType.LOG_COMPACT)
    assertSnapshot(tablePath, expectedSnapshot)
  }

  private def baseOptions(tableType: HoodieTableType): Map[String, String] = Map(
    DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieTableConfig.TABLE_STORAGE_LAYOUT.key -> HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue,
    HoodieWriteConfig.TBL_NAME.key -> s"hoodie_lsm_${tableType.name.toLowerCase}",
    HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    "hoodie.delete.shuffle.parallelism" -> "1")

  private def rows(values: Seq[(String, String, Long, String)]): DataFrame = {
    val _spark = spark
    import _spark.implicits._
    values.toDF("id", "value", "ts", "partition").repartition(1)
  }

  private def write(
      input: DataFrame,
      tablePath: String,
      options: Map[String, String],
      operationType: WriteOperationType,
      saveMode: SaveMode = SaveMode.Append): Unit = {
    input.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operationType.value)
      .mode(saveMode)
      .save(tablePath)
  }

  private def withWriteClient[T](
      tablePath: String,
      options: Map[String, String])(
      operation: SparkRDDWriteClient[HoodieRecordPayload[Nothing]] => T): T = {
    val client = DataSourceUtils.createHoodieClient(
      spark.sparkContext,
      "",
      tablePath,
      options(HoodieWriteConfig.TBL_NAME.key),
      options.asJava)
      .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
    try {
      operation(client)
    } finally {
      client.close()
    }
  }

  private def assertChangedFilesSorted(
      tablePath: String,
      tableType: HoodieTableType,
      operationType: WriteOperationType,
      nativeLogExtension: String): Unit = {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      assertLatestBaseFilesSorted(tablePath, operationType)
    } else {
      val metaClient = createMetaClient(tablePath)
      val instantTime = metaClient.getActiveTimeline.filterCompletedInstants.lastInstant.get.requestedTime
      val nativeLogSuffix = s".$nativeLogExtension.parquet"
      val logFiles = HoodieTestUtils.listNativeLogFiles(metaClient.getStorage, tablePath).asScala
        .filter(path => path.getName.contains(instantTime) && path.getName.endsWith(nativeLogSuffix))
      assertFalse(logFiles.isEmpty, s"$operationType should produce an LSM native log run")
      val runSizes = logFiles.map(path => assertParquetFileSorted(path.toString, operationType))
      assertTrue(runSizes.exists(_ > 1), s"$operationType should produce a non-trivial sorted native run")
    }
  }

  private def assertLatestBaseFilesSorted(tablePath: String, operationType: WriteOperationType): Unit = {
    val baseFiles = latestBaseFiles(tablePath)
    assertFalse(baseFiles.isEmpty, s"$operationType should produce an LSM base-file run")
    val runSizes = baseFiles.map(baseFile => assertParquetFileSorted(baseFile.getPath, operationType))
    assertTrue(runSizes.exists(_ > 1), s"$operationType should produce a non-trivial sorted base-file run")
  }

  private def assertParquetFileSorted(path: String, operationType: WriteOperationType): Int = {
    val actualRecordKeys = spark.read.parquet(path)
      .select(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .collect()
      .map(_.getString(0))
      .toSeq
    val expectedRecordKeys = actualRecordKeys.sortWith((left, right) => StringUtils.compareUtf8Bytes(left, right) < 0)
    assertEquals(expectedRecordKeys, actualRecordKeys, s"$operationType output is not sorted: $path")
    actualRecordKeys.size
  }

  private def latestBaseFiles(tablePath: String): Seq[HoodieBaseFile] = {
    val metaClient = createMetaClient(tablePath)
    Seq(FirstPartition, SecondPartition).flatMap { partitionPath =>
      HoodieClientTestUtils.getLatestBaseFiles(
        tablePath,
        metaClient.getStorage,
        s"$tablePath/$partitionPath/*").asScala
    }
  }

  private def assertSnapshot(tablePath: String, expected: Map[String, String]): Unit = {
    val actual = spark.read.format("hudi").load(tablePath)
      .select("id", "value")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
    assertEquals(expected, actual)
  }

  private def createMetaClient(tablePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(storageConf.newInstance())
      .build()
}

object TestLSMDataSource {

  def bucketBulkInsertParams(): java.util.stream.Stream[Arguments] =
    java.util.stream.Stream.of(
      Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.BucketIndexEngineType.SIMPLE, Boolean.box(false)),
      Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.BucketIndexEngineType.SIMPLE, Boolean.box(true)),
      Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.BucketIndexEngineType.SIMPLE, Boolean.box(false)),
      Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.BucketIndexEngineType.SIMPLE, Boolean.box(true)),
      Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING, Boolean.box(false)),
      Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING, Boolean.box(true)))

  def bulkInsertWithHoodieRecordPathParams(): java.util.stream.Stream[Arguments] =
    java.util.stream.Stream.of(
      Arguments.of(HoodieTableType.COPY_ON_WRITE, BulkInsertSortMode.GLOBAL_SORT),
      Arguments.of(HoodieTableType.COPY_ON_WRITE, BulkInsertSortMode.PARTITION_SORT),
      Arguments.of(HoodieTableType.COPY_ON_WRITE, BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT),
      Arguments.of(HoodieTableType.MERGE_ON_READ, BulkInsertSortMode.GLOBAL_SORT),
      Arguments.of(HoodieTableType.MERGE_ON_READ, BulkInsertSortMode.PARTITION_SORT),
      Arguments.of(HoodieTableType.MERGE_ON_READ, BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT))
}
