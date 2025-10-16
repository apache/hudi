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

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}

import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Random

class DataGenerationForTests extends SparkClientFunctionalTestHarness{
  val dataGen = new HoodieTestDataGenerator(0)
  val random = new Random
  val folderPath = "/Users/linliu/emr/test_data/generated/"
  val tmpFolderPath = "/Users/linliu/emr/test_data/temp/"
  val tableFolderPath = "/Users/linliu/emr/1.1.0-rc2-test"

  @Test
  def generatorData(): Unit = {
    var totalRecords = 0
    for (i <- 0 to 30) {
      val recordToChange = random.nextInt(90) + 10
      System.out.println(s"Round $i: # of records to update/insert: $recordToChange")
      val records = if (i % 2 == 0) {
        totalRecords += recordToChange
        dataGen.generateInserts(String.format(s"%0${10}d", Integer.valueOf(i)), recordToChange)
      } else {
        dataGen.generateUpdates(String.format(s"%0${10}d", Integer.valueOf(i)), totalRecords / 2)
      }
      val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records).asScala.toSeq, 1))
      val fileName = String.format(s"%0${10}d.parquet", Integer.valueOf(i))
      writeToParquet(inputDF, folderPath, fileName, tmpFolderPath)
    }
  }

  @ParameterizedTest
  @MethodSource(Array("getParameters"))
  def testPartitionedRLI(tableType: HoodieTableType, mergeMode: RecordMergeMode): Unit = {
    val tableWithBloomFilter = "table_bloom_filter"
    val tablePath1 = s"$tableFolderPath/$tableWithBloomFilter"
    for (i <- 0 to 30) {
      val fileName = String.format(s"%0${10}d", Integer.valueOf(i))
      val df = spark.read.parquet(s"$folderPath/$fileName.parquet")
      val mode = if (i == 0) SaveMode.Overwrite else SaveMode.Append
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableWithBloomFilter)
        .option(HoodieIndexConfig.INDEX_TYPE.key, "BLOOM")
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key, "true")
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key, "true")
        .option(HoodieIndexConfig.BLOOM_INDEX_USE_METADATA.key, "true")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rider")
        .option(HoodieWriteConfig.RECORD_MERGE_MODE.key, mergeMode.name)
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "timestamp")
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .mode(mode)
        .save(tablePath1)
    }

    val tableWithPartitionedRLI = "table_partitioned_rli"
    val tablePath2 = s"$tableFolderPath/$tableWithPartitionedRLI"
    for (i <- 0 to 30) {
      val fileName = String.format(s"%0${10}d", Integer.valueOf(i))
      val df = spark.read.parquet(s"$folderPath/$fileName.parquet")
      val mode = if (i == 0) SaveMode.Overwrite else SaveMode.Append
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableWithPartitionedRLI)
        .option(HoodieIndexConfig.INDEX_TYPE.key, "RECORD_INDEX")
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key, "true")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rider")
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "timestamp")
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .option(HoodieWriteConfig.RECORD_MERGE_MODE.key, mergeMode.name)
        .mode(mode)
        .save(tablePath2)
    }

    val df1 = spark.read.format("hudi").load(tablePath1).select("rider", "driver", "fare").sort("rider")
    val df2 = spark.read.format("hudi").load(tablePath2).select( "rider", "driver", "fare").sort("rider")
    df1.show(false)
    df2.show(false)
    assertTrue(df1.except(df2).isEmpty)
    assertTrue(df2.except(df1).isEmpty)
  }

  def writeToParquet(df: DataFrame, folderPath: String, fileName: String, tmpFolderPath: String): Unit = {
    val outputPath = folderPath
    df.write
      .format("parquet")
      .mode("overwrite") // Overwrite if file exists
      .save(tmpFolderPath)

    val hadoopConf = df.sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Find the single part-*.parquet file in the temp directory
    val partFile = fs.globStatus(new Path(s"$tmpFolderPath/part-*.parquet"))(0).getPath

    // Rename to the desired file name
    fs.rename(partFile, new Path(folderPath, fileName))
  }
}

object DataGenerationForTests {
  def getParameters: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(HoodieTableType.COPY_ON_WRITE, RecordMergeMode.COMMIT_TIME_ORDERING),
      Arguments.arguments(HoodieTableType.COPY_ON_WRITE, RecordMergeMode.EVENT_TIME_ORDERING),
      Arguments.arguments(HoodieTableType.MERGE_ON_READ, RecordMergeMode.COMMIT_TIME_ORDERING),
      Arguments.arguments(HoodieTableType.MERGE_ON_READ, RecordMergeMode.EVENT_TIME_ORDERING),
    )
  }
}
