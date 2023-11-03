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

package org.apache.spark.sql.hudi

import org.apache.avro.Schema
import org.apache.hudi.DataSourceReadOptions.USE_NEW_HUDI_PARQUET_FILE_FORMAT
import org.apache.hudi.DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED
import org.apache.hudi.common.config.HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.function.SerializableFunctionUnchecked
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig, SyncableFileSystemView}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestUtils.{getDefaultHadoopConf, getLogFileListFromFileSlice}
import org.apache.hudi.config.HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT
import org.apache.hudi.metadata.HoodieTableMetadata
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.io.File
import java.util.{Collections, List}
import scala.collection.JavaConverters._

class TestPartialUpdateForMergeInto extends HoodieSparkSqlTestBase {

  test("Test Partial Update") {
    withTempDir { tmp =>
      testPartialUpdate(tmp, "cow", "avro")
      testPartialUpdate(tmp, "mor", "avro")
      testPartialUpdate(tmp, "mor", "parquet")
    }
  }

  def testPartialUpdate(tmp: File,
                        tableType: String,
                        logDataBlockFormat: String): Unit = {
    val tableName = generateTableName
    val basePath = tmp.getCanonicalPath + "/" + tableName
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = 0")
    spark.sql(s"set ${ENABLE_MERGE_INTO_PARTIAL_UPDATES.key} = true")
    spark.sql(s"set ${LOGFILE_DATA_BLOCK_FORMAT.key} = $logDataBlockFormat")
    spark.sql(s"set ${FILE_GROUP_READER_ENABLED.key} = true")
    spark.sql(s"set ${USE_NEW_HUDI_PARQUET_FILE_FORMAT.key} = true")
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type ='$tableType',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)
         |location '$basePath'
        """.stripMargin)
    spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

    spark.sql(
      s"""
         |merge into $tableName t0
         |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as ts ) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price, _ts = s0.ts
         |""".stripMargin)

    checkAnswer(s"select id, name, price, _ts from $tableName")(
      Seq(1, "a1", 12.0, 1001)
    )

    if (tableType.equals("mor")) {
      validateLogBlock(basePath, 1, Seq("price", "_ts"))
    }

    if (tableType.equals("cow")) {
      // No preCombine field
      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           | id int,
           | name string,
           | price double
           |) using hudi
           |tblproperties(
           | type ='$tableType',
           | primaryKey = 'id'
           |)
           |location '${tmp.getCanonicalPath}/$tableName2'
        """.stripMargin)
      spark.sql(s"insert into $tableName2 values(1, 'a1', 10)")

      spark.sql(
        s"""
           |merge into $tableName2 t0
           |using ( select 1 as id, 'a1' as name, 12 as price) s0
           |on t0.id = s0.id
           |when matched then update set price = s0.price
           |""".stripMargin)

      checkAnswer(s"select id, name, price from $tableName2")(
        Seq(1, "a1", 12.0)
      )
    }
  }

  test("Test MergeInto Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'cow',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)

    val failedToResolveErrorMessage = if (HoodieSparkUtils.gteqSpark3_1) {
      "Failed to resolve pre-combine field `_ts` w/in the source-table output"
    } else {
      "Failed to resolve pre-combine field `_ts` w/in the source-table output;"
    }

    checkExceptionContain(
      s"""
         |merge into $tableName t0
         |using ( select 1 as id, 'a1' as name, 12 as price) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price
      """.stripMargin)(failedToResolveErrorMessage)

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'mor',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)
  }

  def validateLogBlock(basePath: String,
                       expectedNumLogFile: Int,
                       changedFields: Seq[String]): Unit = {
    val hadoopConf = getDefaultHadoopConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(hadoopConf).setBasePath(basePath).build
    val metadataConfig = HoodieMetadataConfig.newBuilder.build
    val engineContext = new HoodieLocalEngineContext(hadoopConf)
    val viewManager: FileSystemViewManager = FileSystemViewManager.createViewManager(
      engineContext, metadataConfig, FileSystemViewStorageConfig.newBuilder.build,
      HoodieCommonConfig.newBuilder.build,
      new SerializableFunctionUnchecked[HoodieTableMetaClient, HoodieTableMetadata] {
        override def apply(v1: HoodieTableMetaClient): HoodieTableMetadata = {
          HoodieTableMetadata.create(
            engineContext, metadataConfig, metaClient.getBasePathV2.toString)
        }
      }
    )
    val fsView: SyncableFileSystemView = viewManager.getFileSystemView(metaClient)
    val fileSlice: FileSlice = fsView.getAllFileSlices("").findFirst.get
    val logFilePathList: List[String] = getLogFileListFromFileSlice(fileSlice)
    Collections.sort(logFilePathList)
    assertEquals(expectedNumLogFile, logFilePathList.size)

    val avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema
    val logReader = new HoodieLogFileReader(
      metaClient.getFs, new HoodieLogFile(logFilePathList.get(0)),
      avroSchema, 1024 * 1024, true, false, false,
      "id", null)
    assertTrue(logReader.hasNext)
    val logBlockHeader = logReader.next().getLogBlockHeader
    assertTrue(logBlockHeader.containsKey(HeaderMetadataType.SCHEMA))
    assertTrue(logBlockHeader.containsKey(HeaderMetadataType.IS_PARTIAL))
    val partialSchema = new Schema.Parser().parse(logBlockHeader.get(HeaderMetadataType.SCHEMA))
    val expectedPartialSchema = HoodieAvroUtils.addMetadataFields(HoodieAvroUtils.generateProjectionSchema(
      avroSchema, changedFields.asJava), false)
    assertEquals(expectedPartialSchema, partialSchema)
    assertTrue(logBlockHeader.get(HeaderMetadataType.IS_PARTIAL).toBoolean)
  }
}
