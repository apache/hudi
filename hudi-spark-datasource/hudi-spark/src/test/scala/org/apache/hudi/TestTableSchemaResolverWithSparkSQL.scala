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

package org.apache.hudi

import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.model.HoodieMetadataRecord
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

/**
 * Test suite for TableSchemaResolver with SparkSqlWriter.
 */
@Tag("functional")
class TestTableSchemaResolverWithSparkSQL extends HoodieSparkWriterTestBase {

  @Test
  def testTableSchemaResolverInMetadataTable(): Unit = {
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    //create a new table
    val tableName = hoodieFooTableName
    val fooTableModifier = Map("path" -> tempPath.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.avro.schema" -> schema.toString(),
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
      "hoodie.metadata.compact.max.delta.commits" -> "2",
      HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key -> "true"
    )

    // generate the inserts
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    // do update
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df1)

    val metadataTablePath = tempPath.toAbsolutePath.toString + "/.hoodie/metadata"
    val metaClient = createMetaClient(spark, metadataTablePath)

    // Delete latest metadata table deltacommit
    // Get schema from metadata table hfile format base file.
    val latestInstant = metaClient.getActiveTimeline.getCommitsTimeline.getReverseOrderedInstants.findFirst()
    val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator
    val path = new Path(metadataTablePath + "/.hoodie", instantFileNameGenerator.getFileName(latestInstant.get()))
    val fs = path.getFileSystem(new Configuration())
    fs.delete(path, false)
    schemaValuationBasedOnDataFile(metaClient, HoodieMetadataRecord.getClassSchema.toString())
  }

  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,parquet", "COPY_ON_WRITE,orc", "COPY_ON_WRITE,hfile",
    "MERGE_ON_READ,parquet", "MERGE_ON_READ,orc", "MERGE_ON_READ,hfile"))
  def testTableSchemaResolver(tableType: String, baseFileFormat: String): Unit = {
    val schema = DataSourceTestUtils.getStructTypeExampleSchema

    //create a new table
    val tableName = hoodieFooTableName
    val fooTableModifier = Map("path" -> tempPath.toAbsolutePath.toString,
      HoodieWriteConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.avro.schema" -> schema.toString(),
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
      HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key -> "true"
    )

    // generate the inserts
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    val metaClient = createMetaClient(spark, tempPath.toAbsolutePath.toString)

    assertTrue(new TableSchemaResolver(metaClient).hasOperationField)
    schemaValuationBasedOnDataFile(metaClient, schema.toString())
  }

  /**
   * Test and valuate schema read from data file --> getTableAvroSchemaFromDataFile
   * @param metaClient
   * @param schemaString
   */
  def schemaValuationBasedOnDataFile(metaClient: HoodieTableMetaClient, schemaString: String): Unit = {
    metaClient.reloadActiveTimeline()
    var tableSchemaResolverParsingException: Exception = null
    try {
      val schemaFromData = new TableSchemaResolver(metaClient).getTableSchemaFromDataFile
      val structFromData = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(org.apache.hudi.common.schema.HoodieSchemaUtils.removeMetadataFields(schemaFromData))
      val schemeDesign = HoodieSchema.parse(schemaString)
      val structDesign = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schemeDesign)
      assertEquals(structFromData, structDesign)
    } catch {
      case e: Exception => tableSchemaResolverParsingException = e;
    }
    assert(tableSchemaResolverParsingException == null)
  }
}
