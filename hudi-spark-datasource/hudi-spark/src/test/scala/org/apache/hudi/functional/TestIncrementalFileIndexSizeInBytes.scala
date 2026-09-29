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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieIncrementalFileIndex}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

/**
 * Verifies the size reported to the Spark planner by [[HoodieIncrementalFileIndex]], which must never be 0
 * or Spark may pick a broadcast join against the incremental source.
 */
class TestIncrementalFileIndexSizeInBytes extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach
  override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testIncrementalFileIndexSizeInBytes(tableType: HoodieTableType): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    spark.read.json(spark.sparkContext.parallelize(records, 2)).write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Reports Long.MaxValue by default, matching the pre-1.0 IncrementalRelation.
    assertEquals(Long.MaxValue, incrementalFileIndexOf(Map.empty).sizeInBytes)

    // Reports the size of the files affected by the commits in range when disabled.
    val fileIndex = incrementalFileIndexOf(Map(
      DataSourceReadOptions.INCREMENTAL_REPORT_MAX_FILE_SIZE.key -> "false",
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> "false"))
    val size = fileIndex.sizeInBytes
    assertTrue(size > 0 && size < Long.MaxValue, s"Expected the affected files size but got $size")
  }

  private def incrementalFileIndexOf(opts: Map[String, String]): HoodieIncrementalFileIndex = {
    val df: DataFrame = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .options(opts)
      .load(basePath)
    df.queryExecution.analyzed.collectFirst {
      case relation: LogicalRelation if relation.relation.isInstanceOf[HadoopFsRelation] =>
        relation.relation.asInstanceOf[HadoopFsRelation].location
    } match {
      case Some(fileIndex: HoodieIncrementalFileIndex) => fileIndex
      case other => throw new IllegalStateException(s"Expected a HoodieIncrementalFileIndex but got $other")
    }
  }
}
