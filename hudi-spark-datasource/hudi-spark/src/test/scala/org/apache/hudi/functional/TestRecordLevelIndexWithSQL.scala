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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Tag
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
    testInQuery(hudiOpts)
    testEqualToQuery(hudiOpts)
  }

  def testEqualToQuery(hudiOpts: Map[String, String]): Unit = {
    val reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs("_row_key").toString)
    val dataFilter = EqualTo(attribute("_row_key"), Literal(reckey(0)))
    assertEquals(1, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyPruningFileCount(hudiOpts, dataFilter, 1)
  }

  def testInQuery(hudiOpts: Map[String, String]): Unit = {
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
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, opts + ("path" -> basePath))
    fileIndex.setIncludeLogFiles(isTableMOR())
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount < getLatestDataFilesCount(opts))
    assertEquals(filteredFilesCount, numFiles)
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
    readDf.printSchema()
    readDf.registerTempTable(sqlTempTable)
  }
}
