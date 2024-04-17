/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestCase
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{BloomFiltersIndexSupport, DataSourceWriteOptions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection.JavaConverters._

class TestBloomFiltersIndexSupport extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var dfList: Seq[DataFrame] = Seq()

  val sourceTableSchema =
    new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9, 3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    initHoodieStorage()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  protected def doWriteAndValidateBloomFilters(testCase: ColumnStatsTestCase,
                                               metadataOpts: Map[String, String],
                                               hudiOpts: Map[String, String],
                                               dataSourcePath: String,
                                               expectedColStatsSourcePath: String,
                                               operation: String,
                                               saveMode: SaveMode,
                                               shouldValidate: Boolean = true): Unit = {
    val sourceJSONTablePath = getClass.getClassLoader.getResource(dataSourcePath).toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(hudiOpts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    dfList = dfList :+ inputDF

    metaClient = HoodieTableMetaClient.reload(metaClient)

    if (shouldValidate) {
      // Currently, routine manually validating the column stats (by actually reading every column of every file)
      // only supports parquet files. Therefore we skip such validation when delta-log files are present, and only
      // validate in following cases: (1) COW: all operations; (2) MOR: insert only.
      val shouldValidateColumnStatsManually = testCase.tableType == HoodieTableType.COPY_ON_WRITE ||
        operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

      validateBloomFiltersIndex(
        testCase, metadataOpts, expectedColStatsSourcePath, shouldValidateColumnStatsManually)
    }

    def validateBloomFiltersIndex(testCase: ColumnStatsTestCase,
                                  metadataOpts: Map[String, String],
                                  expectedColStatsSourcePath: String,
                                  validateColumnStatsManually: Boolean): Unit = {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(toProperties(metadataOpts))
        .build()

      val bloomFiltersIndex = new BloomFiltersIndexSupport(spark, metadataConfig, metaClient)

      val indexedColumns: Set[String] = {
        val customIndexedColumns = metadataConfig.getColumnsEnabledForBloomFilterIndex
        if (customIndexedColumns.isEmpty) {
          sourceTableSchema.fieldNames.toSet
        } else {
          customIndexedColumns.asScala.toSet
        }
      }
    }
  }
}
