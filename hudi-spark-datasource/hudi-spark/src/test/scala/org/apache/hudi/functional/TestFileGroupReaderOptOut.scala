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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSource, IncrementalRelationV1, IncrementalRelationV2, MergeOnReadIncrementalRelationV1, MergeOnReadIncrementalRelationV2, MergeOnReadSnapshotRelation}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

private case class OptOutTestRow(id: String, ts: Long, value: Long, partition: String)

/**
 * Pins the batch read path honoring `hoodie.file.group.reader.enabled`: snapshot and incremental
 * queries read through [[HoodieFileGroupReaderBasedFileFormat]] by default and through the legacy
 * relations when the config is off, with both paths returning the same rows.
 */
class TestFileGroupReaderOptOut extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val tblName = "fgr_opt_out_tbl"

  private def writeOpts(tableType: String): Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
    DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.TBL_NAME.key -> tblName,
    // Inline compaction is on by default for batch MOR writes, so it is pinned off to keep the
    // second commit in a log file and make the snapshot read merge base and log records.
    HoodieCompactionConfig.INLINE_COMPACT.key -> "false")

  @BeforeEach override def setUp(): Unit = {
    setTableName(tblName)
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  private def makeRows(ids: Seq[Int], ts: Long, valueFn: Int => Long): Seq[OptOutTestRow] =
    ids.map(i => OptOutTestRow(i.toString, ts, valueFn(i), "p" + (i % 3)))

  /** Commit 1 inserts ids 1..30; commit 2 upserts ids 1..10, so a snapshot read has to merge. */
  private def writeTwoCommits(tableType: String): Unit = {
    spark.createDataFrame(makeRows(1 to 30, 1L, i => i * 10L))
      .write.format("hudi").options(writeOpts(tableType))
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(basePath)
    spark.createDataFrame(makeRows(1 to 10, 2L, i => i * 100L))
      .write.format("hudi").options(writeOpts(tableType))
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(basePath)
  }

  private def snapshotRelation(fileGroupReaderEnabled: Boolean): BaseRelation =
    new DefaultSource().createRelation(spark.sqlContext, Map(
      "path" -> basePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> fileGroupReaderEnabled.toString))

  private def usesFileGroupReader(relation: BaseRelation): Boolean = relation match {
    case relation: HadoopFsRelation => relation.fileFormat.isInstanceOf[HoodieFileGroupReaderBasedFileFormat]
    case _ => false
  }

  private def incrementalRelation(fileGroupReaderEnabled: Boolean): BaseRelation =
    new DefaultSource().createRelation(spark.sqlContext, Map(
      "path" -> basePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> "000",
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> fileGroupReaderEnabled.toString))

  private def readRows(relation: BaseRelation): Seq[Row] =
    spark.baseRelationToDataFrame(relation)
      .select("id", "ts", "value", "partition")
      .orderBy("id", "ts").collect().toSeq

  @Test
  def testCopyOnWriteSnapshotHonorsOptOut(): Unit = {
    writeTwoCommits(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)

    val fileGroupReaderRelation = snapshotRelation(fileGroupReaderEnabled = true)
    val legacyRelation = snapshotRelation(fileGroupReaderEnabled = false)

    assertTrue(usesFileGroupReader(fileGroupReaderRelation))
    assertFalse(usesFileGroupReader(legacyRelation),
      "Disabling the file group reader must route the COW snapshot to the legacy parquet format")
    assertEquals(readRows(fileGroupReaderRelation), readRows(legacyRelation))
  }

  @Test
  def testMergeOnReadSnapshotHonorsOptOut(): Unit = {
    writeTwoCommits(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)

    val fileGroupReaderRelation = snapshotRelation(fileGroupReaderEnabled = true)
    val legacyRelation = snapshotRelation(fileGroupReaderEnabled = false)

    assertTrue(usesFileGroupReader(fileGroupReaderRelation))
    assertTrue(legacyRelation.isInstanceOf[MergeOnReadSnapshotRelation],
      "Disabling the file group reader must route the MOR snapshot to MergeOnReadSnapshotRelation")
    assertEquals(readRows(fileGroupReaderRelation), readRows(legacyRelation))
  }

  @Test
  def testCopyOnWriteIncrementalHonorsOptOut(): Unit = {
    writeTwoCommits(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)

    val fileGroupReaderRelation = incrementalRelation(fileGroupReaderEnabled = true)
    val legacyRelation = incrementalRelation(fileGroupReaderEnabled = false)

    assertTrue(usesFileGroupReader(fileGroupReaderRelation))
    assertTrue(legacyRelation.isInstanceOf[IncrementalRelationV1] || legacyRelation.isInstanceOf[IncrementalRelationV2],
      "Disabling the file group reader must route the COW incremental query to IncrementalRelationV1/V2")
    assertEquals(readRows(fileGroupReaderRelation), readRows(legacyRelation))
  }

  @Test
  def testMergeOnReadIncrementalHonorsOptOut(): Unit = {
    writeTwoCommits(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)

    val fileGroupReaderRelation = incrementalRelation(fileGroupReaderEnabled = true)
    val legacyRelation = incrementalRelation(fileGroupReaderEnabled = false)

    assertTrue(usesFileGroupReader(fileGroupReaderRelation))
    assertTrue(legacyRelation.isInstanceOf[MergeOnReadIncrementalRelationV1]
      || legacyRelation.isInstanceOf[MergeOnReadIncrementalRelationV2],
      "Disabling the file group reader must route the MOR incremental query to MergeOnReadIncrementalRelationV1/V2")
    assertEquals(readRows(fileGroupReaderRelation), readRows(legacyRelation))
  }
}
