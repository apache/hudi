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

package org.apache.hudi

import org.apache.commons.io.FileUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection.JavaConverters

class HoodieSparkWriterTestBase {
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
    val sparkConf = HoodieClientTestUtils.getSparkConfForTest(getClass.getSimpleName)

    spark = SparkSession.builder()
      .withExtensions(new HoodieSparkSessionExtension)
      .config(sparkConf)
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
   * Utility method for dropping all hoodie meta related columns.
   */
  def dropMetaFields(df: Dataset[Row]): Dataset[Row] = {
    df.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))
  }

  /**
   * Utility method for converting list of Row to list of Seq.
   *
   * @param inputList list of Row
   * @return list of Seq
   */
  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

}
