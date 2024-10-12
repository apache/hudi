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

import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, RecordLevelIndexSupport}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, Literal, Or}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.util.Using

@Tag("functional")
class TestRecordLevelIndexWithSQL extends RecordLevelIndexTestBase {
  val sqlTempTable = "tbl"

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testRLIWithSQL(tableType: String): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

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
    verifyInQuery(hudiOpts, "_row_key", latestSnapshotDf)
    verifyEqualToQuery(hudiOpts, "_row_key", latestSnapshotDf)
    verifyNegativeTestCases(hudiOpts, "_row_key", latestSnapshotDf)
    // verify the same for _hoodie_record_key
    verifyInQuery(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName, latestSnapshotDf)
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
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles)
  }

  def verifyInQuery(hudiOpts: Map[String, String], colName: String, latestSnapshotDf: DataFrame): Unit = {
    var reckey = latestSnapshotDf.limit(1).collect().map(row => row.getAs(colName).toString)
    var dataFilter = In(attribute(colName), reckey.map(l => literal(l)).toList)
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    var numFiles = if (isTableMOR()) 2 else 1
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles)

    val partitions = Seq("2015/03/16", "2015/03/17")
    reckey = latestSnapshotDf.collect().filter(row => partitions.contains(row.getAs("partition").toString))
      .map(row => row.getAs(colName).toString)
    dataFilter = In(attribute(colName), reckey.map(l => literal(l)).toList)
    assertEquals(reckey.length, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    numFiles = if (isTableMOR()) 4 else 2
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def verifyPruningFileCount(opts: Map[String, String], dataFilter: Expression, numFiles: Int): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount < getLatestDataFilesCount(opts))
    assertEquals(numFiles, filteredFilesCount)

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filteredFilesCount < filesCountWithNoSkipping)
  }

  private def isTableMOR(): Boolean = {
    metaClient.getTableType == HoodieTableType.MERGE_ON_READ
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    Using(getTableFileSystenView(opts)) { fsView =>
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getRequestTime)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
      totalLatestDataFiles
    }.get
  }

  private def getTableFileSystenView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.registerTempTable(sqlTempTable)
  }

  @Test
  def testInFilterOnNonRecordKey(): Unit = {
    var hudiOpts = commonOpts
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
         |  primaryKey ='record_key_col',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'record_key_col',
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
}
