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

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.model.HoodieMetadataRecord
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters

/**
 * Test suite for TableSchemaResolver with SparkSqlWriter.
 */
@Tag("functional")
class TestTableSchemaResolverWithSparkSQL {
  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _
  var tempPath: java.nio.file.Path = _
  var tempBootStrapPath: java.nio.file.Path = _
  var hoodieFooTableName = "hoodie_foo_tbl"
  var tempBasePath: String = _
  var commonTableModifier: Map[String, String] = Map()

  case class StringLongTest(uuid: String, ts: Long)

  /**
   * Setup method running before each test.
   */
  @BeforeEach
  def setUp(): Unit = {
    initSparkContext()
    tempPath = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    tempBootStrapPath = java.nio.file.Files.createTempDirectory("hoodie_test_bootstrap")
    tempBasePath = tempPath.toAbsolutePath.toString
    commonTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
  }

  /**
   * Tear down method running after each test.
   */
  @AfterEach
  def tearDown(): Unit = {
    cleanupSparkContexts()
    FileUtils.deleteDirectory(tempPath.toFile)
    FileUtils.deleteDirectory(tempBootStrapPath.toFile)
  }

  /**
   * Utility method for initializing the spark context.
   */
  def initSparkContext(): Unit = {
    spark = SparkSession.builder()
      .appName(hoodieFooTableName)
      .master("local[2]")
      .withExtensions(new HoodieSparkSessionExtension)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  /**
   * Utility method for cleaning up spark resources.
   */
  def cleanupSparkContexts(): Unit = {
    if (sqlContext != null) {
      sqlContext.clearCache();
      sqlContext = null;
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
    if (spark != null) {
      spark.close()
    }
  }

  /**
   * Utility method for creating common params for writer.
   *
   * @param path               Path for hoodie table
   * @param hoodieFooTableName Name of hoodie table
   * @param tableType          Type of table
   * @return Map of common params
   */
  def getCommonParams(path: java.nio.file.Path, hoodieFooTableName: String, tableType: String): Map[String, String] = {
    Map("path" -> path.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
  }

  /**
   * Utility method for converting list of Row to list of Seq.
   *
   * @param inputList list of Row
   * @return list of Seq
   */
  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

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
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    // do update
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df1)

    val metadataTablePath = tempPath.toAbsolutePath.toString + "/.hoodie/metadata"
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(metadataTablePath)
      .setConf(spark.sessionState.newHadoopConf())
      .build()

    // Delete latest metadata table deltacommit
    // Get schema from metadata table hfile format base file.
    val latestInstant = metaClient.getActiveTimeline.getCommitsTimeline.getReverseOrderedInstants.findFirst()
    val path = new Path(metadataTablePath + "/.hoodie", latestInstant.get().getFileName)
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
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tempPath.toAbsolutePath.toString)
      .setConf(spark.sessionState.newHadoopConf())
      .build()

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
      val schemaFromData = new TableSchemaResolver(metaClient).getTableAvroSchemaFromDataFile
      val structFromData = AvroConversionUtils.convertAvroSchemaToStructType(HoodieAvroUtils.removeMetadataFields(schemaFromData))
      val schemeDesign = new Schema.Parser().parse(schemaString)
      val structDesign = AvroConversionUtils.convertAvroSchemaToStructType(schemeDesign)
      assertEquals(structFromData, structDesign)
    } catch {
      case e: Exception => tableSchemaResolverParsingException = e;
    }
    assert(tableSchemaResolverParsingException == null)
  }
}
