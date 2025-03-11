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
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, Literal, Or}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.util.Using

@Tag("functional")
class TestRecordLevelIndexWithSQL extends RecordLevelIndexTestBase {
  val sqlTempTable = "tbl"
  // dummy record key field
  val defaultRecordKeyField = "_row_key"

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
    // verify for default record key field
    verifyInQuery(hudiOpts, defaultRecordKeyField)
    verifyEqualToQuery(hudiOpts, defaultRecordKeyField)
    verifyNegativeTestCases(hudiOpts, defaultRecordKeyField)

    // verify the same for _hoodie_record_key
    verifyInQuery(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName)
    verifyEqualToQuery(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName)
    verifyNegativeTestCases(hudiOpts, RECORD_KEY_METADATA_FIELD.getFieldName)
  }

  private def verifyNegativeTestCases(hudiOpts: Map[String, String], colName: String): Unit = {
    val commonOpts = hudiOpts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)

    // when no data filter is applied
    assertEquals(getLatestDataFilesCount(commonOpts), fileIndex.listFiles(Seq.empty, Seq.empty).flatMap(s => s.files).size)
    assertEquals(6, spark.sql("select * from " + sqlTempTable).count())

    // non existing entries in EqualTo query
    var dataFilter: Expression = EqualTo(attribute(colName), Literal("xyz"))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // non existing entries in IN query
    dataFilter = In(attribute(colName), List.apply(Literal("xyz"), Literal("abc")))
    assertEquals(0, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertEquals(0, fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size)

    // not supported GreaterThan query
    var reckey = mergedDfList.last.limit(2).collect().map(row => row.getAs(defaultRecordKeyField).toString)
    dataFilter = GreaterThan(attribute(colName), Literal(reckey(0)))
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)

    // not supported OR query
    dataFilter = Or(EqualTo(attribute(colName), Literal(reckey(0))), GreaterThanOrEqual(attribute("timestamp"), Literal(0)))
    assertEquals(6, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    assertTrue(fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size >= 3)
  }

  def verifyEqualToQuery(hudiOpts: Map[String, String], colName: String): Unit = {
    val reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs(defaultRecordKeyField).toString)
    val dataFilter = EqualTo(attribute(colName), Literal(reckey(0)))
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyPruningFileCount(hudiOpts, dataFilter, 1)
  }

  def verifyInQuery(hudiOpts: Map[String, String], colName: String): Unit = {
    var reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs(defaultRecordKeyField).toString)
    var dataFilter = In(attribute(colName), reckey.map(l => literal(l)).toList)
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyPruningFileCount(hudiOpts, dataFilter, 1)

    reckey = mergedDfList.last.limit(2).collect().map(row => row.getAs(defaultRecordKeyField).toString)
    dataFilter = In(attribute(colName), reckey.map(l => literal(l)).toList)
    assertEquals(2, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    val numFiles = if (isTableMOR()) 2 else 2
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
    Using(getTableFileSystemView(opts)) { fsView =>
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
      totalLatestDataFiles
    }.get
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    val writeConfig = getWriteConfig(opts)
    val metadataTable = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(jsc), writeConfig.getMetadataConfig, writeConfig.getBasePath, true)
    new HoodieTableFileSystemView(metadataTable, metaClient, metaClient.getActiveTimeline)
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.registerTempTable(sqlTempTable)
  }
}
