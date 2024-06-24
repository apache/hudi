/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.MetadataConversionUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestCase
import org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.util.JavaConversions
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GreaterThan, Literal}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.JavaConverters._

class TestColumnStatsIndexWithSQL extends ColumnStatIndexTestBase {

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndexWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ) ++ metadataOpts
    setupTable(testCase, metadataOpts, commonOpts, shouldValidate = true)
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexSQLWithInMemoryIndex(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieIndexConfig.INDEX_TYPE.key() -> INMEMORY.name()
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidate = false)

    assertEquals(4, getLatestDataFilesCount(commonOpts))
    assertEquals(0, getLatestDataFilesCount(commonOpts, includeLogFiles = false))
    var dataFilter = GreaterThan(attribute("c5"), literal("90"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = GreaterThan(attribute("c5"), literal("95"))
    verifyPruningFileCount(commonOpts, dataFilter)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndexDeletionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ) ++ metadataOpts
    setupTable(testCase, metadataOpts, commonOpts, shouldValidate = true)
    val lastDf = dfList.last

    lastDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    verifyFileIndexAndSQLQueries(commonOpts, isTableDataSameAsAfterSecondInstant = true)

    // Add the last df back and verify the queries
    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false)
    verifyFileIndexAndSQLQueries(commonOpts, verifyFileCount = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexCompactionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
    ) ++ metadataOpts
    setupTable(testCase, metadataOpts, commonOpts, shouldValidate = false)

    assertFalse(hasLogFiles())
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexScheduledCompactionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
    ) ++ metadataOpts
    setupTable(testCase, metadataOpts, commonOpts, shouldValidate = false)

    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), getWriteConfig(commonOpts))
    writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty())
    writeClient.close()

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false)
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  private def setupTable(testCase: ColumnStatsTestCase, metadataOpts: Map[String, String], commonOpts: Map[String, String], shouldValidate: Boolean): Unit = {
    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW
    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated2-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated2-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate)
  }

  def verifyFileIndexAndSQLQueries(opts: Map[String, String], isTableDataSameAsAfterSecondInstant: Boolean = false, verifyFileCount: Boolean = true): Unit = {
    var commonOpts = opts
    val inputDF1 = spark.read.format("hudi")
      .options(commonOpts)
      .option("as.of.instant", metaClient.getActiveTimeline.getInstants.get(1).getTimestamp)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, "false")
      .load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
    val numRecordsForFirstQuery = spark.sql("select * from tbl where c5 > 70").count()
    val numRecordsForSecondQuery = spark.sql("select * from tbl where c5 > 70 and c6 >= '2020-03-28'").count()
    // verify snapshot query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    // verify read_optimized query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    // verify incremental query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)
    commonOpts = commonOpts + (DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> "true")
    // TODO: https://issues.apache.org/jira/browse/HUDI-6657 - Investigate why below assertions fail with full table scan enabled.
    //verifySQLQueries(numRecordsForFirstQuery, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    var dataFilter: Expression = GreaterThan(attribute("c5"), literal("70"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = And(dataFilter, GreaterThan(attribute("c6"), literal("'2020-03-28'")))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = GreaterThan(attribute("c5"), literal("90"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = And(dataFilter, GreaterThan(attribute("c6"), literal("'2020-03-28'")))
    verifyPruningFileCount(commonOpts, dataFilter)
  }

  private def verifyPruningFileCount(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount < getLatestDataFilesCount(opts))

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filteredFilesCount < filesCountWithNoSkipping)
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    val fsView = getTableFileSystemView(opts)
    fsView.loadAllPartitions()
    fsView.getPartitionPaths.asScala.flatMap { partitionPath =>
      val relativePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath, partitionPath)
      fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, metaClient.reloadActiveTimeline().lastInstant().get().getTimestamp).iterator().asScala.toSeq
    }.foreach(
      slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
        + (if (slice.getBaseFile.isPresent) 1 else 0))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }

  protected def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def verifySQLQueries(numRecordsForFirstQueryAtPrevInstant: Long, numRecordsForSecondQueryAtPrevInstant: Long,
                               queryType: String, opts: Map[String, String], isLastOperationDelete: Boolean): Unit = {
    val firstQuery = "select * from tbl where c5 > 70"
    val secondQuery = "select * from tbl where c5 > 70 and c6 >= '2020-03-28'"
    // 2 records are updated with c5 greater than 70 and one record is inserted with c5 value greater than 70
    var commonOpts: Map[String, String] = opts
    createSQLTable(commonOpts, queryType)
    val incrementFirstQuery = if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL) && hasLogFiles()) {
      1 // only one insert
    } else if (isLastOperationDelete) {
      0 // no increment
    } else {
      3 // one insert and two upserts
    }
    val incrementSecondQuery = if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL) && hasLogFiles()) {
      1 // only one insert
    } else if (isLastOperationDelete) {
      0 // no increment
    } else {
      2 // one insert and two upserts
    }
    assertEquals(spark.sql(firstQuery).count(), numRecordsForFirstQueryAtPrevInstant + incrementFirstQuery)
    assertEquals(spark.sql(secondQuery).count(), numRecordsForSecondQueryAtPrevInstant + incrementSecondQuery)
    val numRecordsForFirstQueryWithDataSkipping = spark.sql(firstQuery).count()
    val numRecordsForSecondQueryWithDataSkipping = spark.sql(secondQuery).count()

    if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)) {
      createIncrementalSQLTable(commonOpts, metaClient.reloadActiveTimeline().getInstants.get(1).getTimestamp)
      assertEquals(spark.sql(firstQuery).count(), if (isLastOperationDelete) 0 else 3)
      assertEquals(spark.sql(secondQuery).count(), if (isLastOperationDelete) 0 else 2)
    }

    commonOpts = opts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false")
    createSQLTable(commonOpts, queryType)
    val numRecordsForFirstQueryWithoutDataSkipping = spark.sql(firstQuery).count()
    val numRecordsForSecondQueryWithoutDataSkipping = spark.sql(secondQuery).count()
    assertEquals(numRecordsForFirstQueryWithDataSkipping, numRecordsForFirstQueryWithoutDataSkipping)
    assertEquals(numRecordsForSecondQueryWithDataSkipping, numRecordsForSecondQueryWithoutDataSkipping)
  }

  private def createSQLTable(hudiOpts: Map[String, String], queryType: String): Unit = {
    val opts = hudiOpts + (
      DataSourceReadOptions.QUERY_TYPE.key -> queryType,
      DataSourceReadOptions.BEGIN_INSTANTTIME.key() -> metaClient.getActiveTimeline.getInstants.get(0).getTimestamp.replaceFirst(".", "0")
    )
    val inputDF1 = spark.read.format("hudi").options(opts).load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
  }

  private def createIncrementalSQLTable(hudiOpts: Map[String, String], instantTime: String): Unit = {
    val opts = hudiOpts + (
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.BEGIN_INSTANTTIME.key() -> instantTime
    )
    val inputDF1 = spark.read.format("hudi").options(opts).load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
  }

  private def hasLogFiles(): Boolean = {
    isTableMOR && getLatestCompactionInstant() != metaClient.getActiveTimeline.lastInstant()
  }

  private def isTableMOR(): Boolean = {
    metaClient.getTableType == HoodieTableType.MERGE_ON_READ
  }

  protected def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    metaClient.reloadActiveTimeline()
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
}
