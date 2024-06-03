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

import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, Literal, Or}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    createTempTable(hudiOpts)
    verifyInQuery(hudiOpts)
    verifyEqualToQuery(hudiOpts)
    verifyNegativeTestCases(hudiOpts)
  }

  private def verifyNegativeTestCases(hudiOpts: Map[String, String]): Unit = {
    val commonOpts = hudiOpts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)

    // when no data filter is applied
    assertEquals(getLatestDataFilesCount(commonOpts), fileIndex.listFiles(Seq.empty, Seq.empty).flatMap(s => s.files).size)
    assertEquals(6, spark.sql("select * from " + sqlTempTable).count())

    // non existing entries in EqualTo query
    var dataFilter: Expression = EqualTo(attribute("_row_key"), Literal("xyz"))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // non existing entries in IN query
    dataFilter = In(attribute("_row_key"), List.apply(Literal("xyz"), Literal("abc")))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // not supported GreaterThan query
    val reckey = mergedDfList.last.limit(2).collect().map(row => row.getAs("_row_key").toString)
    dataFilter = GreaterThan(attribute("_row_key"), Literal(reckey(0)))
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)

    // not supported OR query
    dataFilter = Or(EqualTo(attribute("_row_key"), Literal(reckey(0))), GreaterThanOrEqual(attribute("timestamp"), Literal(0)))
    assertEquals(6, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)
  }

  def verifyEqualToQuery(hudiOpts: Map[String, String]): Unit = {
    val reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs("_row_key").toString)
    val dataFilter = EqualTo(attribute("_row_key"), Literal(reckey(0)))
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyPruningFileCount(hudiOpts, dataFilter, 1)
  }

  def verifyInQuery(hudiOpts: Map[String, String]): Unit = {
    var reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs("_row_key").toString)
    var dataFilter = In(attribute("_row_key"), reckey.map(l => literal(l)).toList)
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    var numFiles = if (isTableMOR()) 2 else 1
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles)

    reckey = mergedDfList.last.limit(2).collect().map(row => row.getAs("_row_key").toString)
    dataFilter = In(attribute("_row_key"), reckey.map(l => literal(l)).toList)
    assertEquals(2, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    numFiles = if (isTableMOR()) 2 else 2
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
    assertEquals(filteredFilesCount, numFiles)

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
    getTableFileSystenView(opts).getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
      .values()
      .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
        (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
          slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
            + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    totalLatestDataFiles
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
}
