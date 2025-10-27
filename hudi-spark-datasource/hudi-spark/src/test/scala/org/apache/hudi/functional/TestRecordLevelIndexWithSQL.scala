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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, RecordLevelIndexSupport}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.index.record.HoodieRecordIndex
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{DataFrame, HoodieCatalystExpressionUtils, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, Literal, Or}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

import scala.util.Using

@Tag("functional")
class TestRecordLevelIndexWithSQL extends RecordLevelIndexTestBase {
  val sqlTempTable = "tbl"

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testRLICreationUsingSQL(isPartitioned: Boolean): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      "hoodie.metadata.index.column.stats.enable" -> "false",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "false")

    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HoodieTestUtils.getDefaultStorageConf).build()
    assertFalse(MetadataPartitionType.RECORD_INDEX.isMetadataPartitionAvailable(getLatestMetaClient(true)))
    spark.sql(s"create table $sqlTempTable using hudi location '$basePath'")
    spark.sql(s"create index record_index on $sqlTempTable (_row_key) options(${HoodieRecordIndex.IS_PARTITIONED_OPTION}='$isPartitioned')")
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HoodieTestUtils.getDefaultStorageConf).build()
    assertEquals(isPartitioned, HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndex(MetadataPartitionType.RECORD_INDEX.getPartitionPath).get()))
    assertTrue(MetadataPartitionType.RECORD_INDEX.isMetadataPartitionAvailable(getLatestMetaClient(true)))
    spark.sql("set hoodie.write.lock.provider=")
    spark.sql(s"drop table $sqlTempTable")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testRLIWithSQL(tableType: String): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      "hoodie.metadata.index.column.stats.enable" -> "false",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    // some negative test cases in this class assumes
    // only RLI being enabled. So, disabling col stats for now.

    val df = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false,
      numUpdates = df.collect().length,
      onlyUpdates = true)

    createTempTable(hudiOpts)
    val latestSnapshotDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    verifyInQuery(hudiOpts, Array("_row_key"), latestSnapshotDf, sqlTempTable, "partition", Seq("2015/03/16", "2015/03/17"), true)
    verifyEqualToQuery(hudiOpts, "_row_key", latestSnapshotDf)
    verifyNegativeTestCases(hudiOpts, "_row_key", latestSnapshotDf)
    // verify the same for _hoodie_record_key
    verifyInQuery(hudiOpts, Array(RECORD_KEY_METADATA_FIELD.getFieldName), latestSnapshotDf, sqlTempTable, "partition", Seq("2015/03/16", "2015/03/17"), true)
    verifyEqualToQuery(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName, latestSnapshotDf)
    verifyNegativeTestCases(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName, latestSnapshotDf)
  }

  private def verifyNegativeTestCases(hudiOpts: Map[String, String], colName: String, latestSnapshotDf: DataFrame): Unit = {
    val commonOpts = hudiOpts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)

    // when no data filter is applied
    assertEquals(getLatestDataFilesCount(commonOpts), fileIndex.listFiles(Seq.empty, Seq.empty).flatMap(s => s.files).size)
    assertEquals(5, spark.sql("select * from " + sqlTempTable).count())

    // non existing entries in EqualTo query
    var dataFilter: Expression = EqualTo(attribute(colName), Literal("xyz"))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // non existing entries in IN query
    dataFilter = In(attribute(colName), List.apply(Literal("xyz"), Literal("abc")))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // not supported GreaterThan query
    val reckey = latestSnapshotDf.limit(2).collect().map(row => row.getAs(colName).toString)
    dataFilter = GreaterThan(attribute(colName), Literal(reckey(0)))
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)

    // not supported OR query
    dataFilter = Or(EqualTo(attribute(colName), Literal(reckey(0))), GreaterThanOrEqual(attribute("timestamp"), Literal(0)))
    assertEquals(5, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)
  }

  def verifyEqualToQuery(hudiOpts: Map[String, String], colName: String, latestSnapshotDf: DataFrame): Unit = {
    val reckey = latestSnapshotDf.limit(1).collect().map(row => row.getAs(colName).toString)
    val dataFilter = EqualTo(attribute(colName), Literal(reckey(0)))
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    val numFiles = if (isTableMOR()) 2 else 1
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles, shouldPrune = true)
  }

  def verifyInQuery(hudiOpts: Map[String, String], colNames: Array[String], latestSnapshotDf: DataFrame, tableName: String,
                    partitionFieldName: String, partitions: Seq[String], shouldPrune: Boolean): Unit = {
    var dataFilter: Expression = null
    var numFiles: Int = 0
    var recKeys: Array[String] = null
    var inQueries: Array[In] = colNames.map(colName => {
      recKeys = latestSnapshotDf.limit(1).collect().map(row => row.getAs(colName).toString)
      In(attribute(colName), recKeys.map(l => literal(l)).toList)
    })
    dataFilter = inQueries.reduceLeft(And)
    assertEquals(1, spark.sql("select * from " + tableName + " where " + dataFilter.sql).count())
    numFiles = if (isTableMOR()) 2 else 1
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles, shouldPrune)

    inQueries = colNames.map(colName => {
      recKeys = latestSnapshotDf.collect().filter(row => partitions.contains(row.getAs(partitionFieldName).toString))
        .map(row => row.getAs(colName).toString)
      In(attribute(colName), recKeys.map(l => literal(l)).toList)
    })
    dataFilter = inQueries.reduceLeft(And)
    assertEquals(recKeys.length, spark.sql("select * from " + tableName + " where " + dataFilter.sql).count())
    numFiles = if (isTableMOR()) 4 else 2
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles, shouldPrune)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def verifyPruningFileCount(opts: Map[String, String], dataFilter: Expression, numFiles: Int, shouldPrune: Boolean): Unit = {
    verifyPruningFileCount(opts, dataFilter, numFiles, HoodieTableMetaClient.reload(metaClient), shouldPrune)
  }

  private def verifyPruningFileCount(opts: Map[String, String], dataFilter: Expression, numFiles: Int, metaClient: HoodieTableMetaClient, shouldPrune: Boolean): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
    this.metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    if (shouldPrune) {
      assertEquals(numFiles, filteredFilesCount)
    } else {
      assertEquals(filteredFilesCount, getLatestDataFilesCount(opts))
    }

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    if (!shouldPrune) {
      assertEquals(filteredFilesCount, filesCountWithNoSkipping)
    }
  }

  private def isTableMOR(): Boolean = {
    metaClient.getTableType == HoodieTableType.MERGE_ON_READ
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    Using(getTableFileSystemView(opts)) { fsView =>
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
      totalLatestDataFiles
    }.get
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    val props = new Properties()
    opts.foreach(kv => props.setProperty(kv._1, kv._2))
    FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().fromProperties(props).build())
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.registerTempTable(sqlTempTable)
  }

  @Test
  def testInFilterOnNonRecordKey(): Unit = {
    var hudiOpts = commonOpts
    val recordKeyFields = "record_key_col"
    hudiOpts = hudiOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    val dummyTablePath = tempDir.resolve("dummy_table").toAbsolutePath.toString
    spark.sql(
      s"""
         |create table dummy_table (
         |  record_key_col string,
         |  not_record_key_col string,
         |  partition_key_col string
         |) using hudi
         | options (
         |  primaryKey ='$recordKeyFields',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = '$recordKeyFields',
         |  hoodie.enable.data.skipping = 'true'
         | )
         | partitioned by(partition_key_col)
         | location '$dummyTablePath'
       """.stripMargin)
    spark.sql(s"insert into dummy_table values('row1', 'row2', 'p1')")
    spark.sql(s"insert into dummy_table values('row2', 'row1', 'p2')")
    spark.sql(s"insert into dummy_table values('row3', 'row1', 'p2')")

    assertEquals(2, spark.read.format("hudi").options(hudiOpts).load(dummyTablePath).filter("not_record_key_col in ('row1', 'abc')").count())
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPrunedStoragePaths(includeLogFiles: Boolean): Unit = {
    val hudiOpts = commonOpts ++ metadataOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> "MERGE_ON_READ")
    val df = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    if (includeLogFiles) {
      // number of updates are set to same size as number of inserts so that every partition gets
      // a log file. Further onlyUpdates is set to true so that no inserts are included in the batch.
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append,
        validate = false,
        numUpdates = df.collect().length,
        onlyUpdates = true)
    }
    val globbedPaths = basePath + "/2015/03/16," + basePath + "/2015/03/17," + basePath + "/2016/03/15"
    val fileIndex = new HoodieFileIndex(sparkSession, metaClient, Option.empty, Map("glob.paths" -> globbedPaths), includeLogFiles = includeLogFiles)
    val selectedPartition = "2016/03/15"
    val partitionFilter: Expression = EqualTo(AttributeReference("partition", StringType)(), Literal(selectedPartition))
    val (isPruned, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
    val storagePaths = RecordLevelIndexSupport.getPrunedStoragePaths(prunedPaths, fileIndex)
    // verify pruned paths contain the selected partition and the size of the pruned file paths
    // when includeLogFiles is set to true, there are two storages paths - base file and log file
    // every partition contains only one file slice
    assertTrue(isPruned)
    assertEquals(if (includeLogFiles) 2 else 1, storagePaths.size)
    assertTrue(storagePaths.forall(path => path.toString.contains(selectedPartition)))

    val recordKey: String = df.filter("partition = '" + selectedPartition + "'").limit(1).collect().apply(0).getAs("_row_key")
    val dataFilter = EqualTo(attribute("_row_key"), Literal(recordKey))
    val rliIndexSupport = new RecordLevelIndexSupport(spark, getConfig.getMetadataConfig, metaClient)
    val fileNames = rliIndexSupport.computeCandidateFileNames(fileIndex, Seq(dataFilter), null, prunedPaths, false)
    assertEquals(if (includeLogFiles) 2 else 1, fileNames.get.size)
  }

  @Test
  def testRLIWithTwoRecordKeyFields(): Unit = {
    val tableName = "dummy_table_two_pk"
    val recordKeyFields = "record_key_col,name"
    val dummyTablePath = tempDir.resolve(tableName).toAbsolutePath.toString
    val hudiOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> s"$recordKeyFields",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_key_col",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      "hoodie.metadata.index.column.stats.enable" -> "false"
    ) ++ metadataOpts

    spark.sql(
      s"""
         |create table $tableName (
         |  record_key_col string,
         |  name string,
         |  not_record_key_col string,
         |  partition_key_col string
         |) using hudi
         | options (
         |  primaryKey ='$recordKeyFields',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = '$recordKeyFields',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.metadata.index.column.stats.enable = 'false'
         | )
         | partitioned by(partition_key_col)
         | location '$dummyTablePath'
       """.stripMargin)
    spark.sql(s"insert into $tableName values('row1', 'name1', 'a', 'p1')")
    spark.sql(s"insert into $tableName values('row2', 'name2', 'b', 'p2')")
    spark.sql(s"insert into $tableName values('row3', 'name3', 'c', 'p3')")

    val latestSnapshotDf = spark.read.format("hudi").options(hudiOpts).load(dummyTablePath)
    this.metaClient = HoodieTableMetaClient.builder()
      .setBasePath(dummyTablePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()
    verifyEqualToQuery(hudiOpts, Seq("record_key_col", "name"), tableName, latestSnapshotDf, HoodieTableMetaClient.reload(metaClient))
    verifyInQuery(hudiOpts, Array("record_key_col", "name"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), true)
    verifyInQuery(hudiOpts, Array("name", "record_key_col"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), true)
    verifyInQuery(hudiOpts, Array("record_key_col"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), false)
  }

  @Test
  def testRLIWithThreeRecordKeyFields(): Unit = {
    val tableName = "dummy_table_three_pk"
    val recordKeyFields = "record_key_col1,record_key_col2,record_key_col3"
    val dummyTablePath = tempDir.resolve(tableName).toAbsolutePath.toString
    val hudiOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyFields,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_key_col",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
    ) ++ metadataOpts

    spark.sql(
      s"""
         |create table $tableName (
         |  record_key_col1 string,
         |  record_key_col2 string,
         |  record_key_col3 string,
         |  partition_key_col string
         |) using hudi
         | options (
         |  primaryKey = '$recordKeyFields',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = '$recordKeyFields',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.metadata.index.column.stats.enable = 'false'
         | )
         | partitioned by(partition_key_col)
         | location '$dummyTablePath'
       """.stripMargin)
    spark.sql(s"insert into $tableName values('row1', 'name1', 'a', 'p1')")
    spark.sql(s"insert into $tableName values('row2', 'name2', 'b', 'p2')")
    spark.sql(s"insert into $tableName values('row3', 'name3', 'c', 'p3')")

    val latestSnapshotDf = spark.read.format("hudi").options(hudiOpts).load(dummyTablePath)
    this.metaClient = HoodieTableMetaClient.builder()
      .setBasePath(dummyTablePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()
    verifyEqualToQuery(hudiOpts, Seq("record_key_col1", "record_key_col2", "record_key_col3"), tableName, latestSnapshotDf, HoodieTableMetaClient.reload(metaClient))
    verifyInQuery(hudiOpts, Array("record_key_col1", "record_key_col2", "record_key_col3"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), true)
    verifyInQuery(hudiOpts, Array("record_key_col3", "record_key_col1", "record_key_col2"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), true)

    // Verify pruning does not work
    // Two columns repeated
    verifyInQuery(hudiOpts, Array("record_key_col3", "record_key_col3", "record_key_col2"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), false)
    // Only two out of three columns
    verifyInQuery(hudiOpts, Array("record_key_col1", "record_key_col2"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), false)
    // Only one out of three columns
    verifyInQuery(hudiOpts, Array("record_key_col1"), latestSnapshotDf, tableName, "partition_key_col", Seq("p2", "p3"), false)

    // Verify pruning count using multiple literals
    val tableSchema: StructType =
      StructType(
        Seq(
          StructField("record_key_col1", StringType),
          StructField("record_key_col2", StringType),
          StructField("record_key_col3", StringType),
          StructField("partition_key_col", StringType)
        )
      )
    // All record key combinations are included so all files will be scanned
    verifyPruningFileCount(hudiOpts, HoodieCatalystExpressionUtils.resolveExpr(spark, "record_key_col2 in ('name1', 'name2', 'name3')" +
      " AND record_key_col1 in ('row1', 'row2', 'row3') AND record_key_col3 in ('a', 'b', 'c')", tableSchema), 3, metaClient, true)
    // Only two record key combinations are valid, so only two files will be scanned
    verifyPruningFileCount(hudiOpts, HoodieCatalystExpressionUtils.resolveExpr(spark, "record_key_col2 in ('name1', 'name2')" +
      " AND record_key_col1 in ('row1', 'row2') AND record_key_col3 in ('a', 'b')", tableSchema), 2, metaClient, true)
    // There is only one valid record key combination, so only one file will be scanned
    verifyPruningFileCount(hudiOpts, HoodieCatalystExpressionUtils.resolveExpr(spark, "record_key_col2 in ('name1', 'name2')" +
      " AND record_key_col1 in ('row2') AND record_key_col3 in ('a', 'b', 'c')", tableSchema), 1, metaClient, true)
  }

  def verifyEqualToQuery(hudiOpts: Map[String, String], colNames: Seq[String], tableName: String, latestSnapshotDf: DataFrame, metaClient: HoodieTableMetaClient): Unit = {
    val rowValues = latestSnapshotDf.limit(1).collect().map(row => {
      colNames.map(colName => row.getAs(colName).toString)
    }).head

    // Build the data filter using EqualTo for each column and combine with And
    val dataFilter = colNames.zip(rowValues).map {
      case (colName, value) => EqualTo(attribute(colName), Literal(value))
    }.reduceLeft(And)

    assertEquals(1, spark.sql("select * from " + tableName + " where " + dataFilter.sql).count())
    verifyPruningFileCount(hudiOpts, dataFilter, 1, metaClient, shouldPrune = true)
  }
}
