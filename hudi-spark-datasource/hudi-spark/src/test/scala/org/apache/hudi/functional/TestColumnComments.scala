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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{HoodieTableType}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource


class TestColumnComments {
  var spark     : SparkSession = _
  var sqlContext: SQLContext   = _
  var sc        : SparkContext = _

  def initSparkContext(): Unit = {
    val sparkConf = getSparkConfForTest(getClass.getSimpleName)
    spark = SparkSession.builder()
      .withExtensions(new HoodieSparkSessionExtension)
      .config(sparkConf)
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  @BeforeEach
  def setUp() {
    initSparkContext()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType], names = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testColumnCommentWithSparkDatasource(tableType: HoodieTableType): Unit = {
    val basePath = java.nio.file.Files.createTempDirectory("hoodie_comments_path").toAbsolutePath.toString
    val opts     = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_comments",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.toString,
      DataSourceWriteOptions.OPERATION.key -> "bulk_insert",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition"
    )
    val inputDF  = spark.sql("select '0' as _row_key, '1' as content, '2' as partition, '3' as ts")
    val struct   = new StructType()
      .add("_row_key", "string", true, "dummy comment")
      .add("content", "string", true)
      .add("partition", "string", true)
      .add("ts", "string", true)
    spark.createDataFrame(inputDF.rdd, struct)
      .write.format("hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    spark.read.format("hudi").load(basePath).registerTempTable("test_tbl")

    // now confirm the comment is present at read time
    assertEquals(1, spark.sql("desc extended test_tbl")
      .filter("col_name = '_row_key' and comment = 'dummy comment'").count)
  }
}
