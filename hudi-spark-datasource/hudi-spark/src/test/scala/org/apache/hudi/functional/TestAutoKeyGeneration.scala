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

import org.apache.hudi.{DataSourceWriteOptions, ScalaAssertionSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestAutoKeyGeneration extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = null
  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[SimpleKeyGenerator].getName,
    HoodieClusteringConfig.INLINE_CLUSTERING.key -> "true",
    HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> "3"
  )

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testAutoKeyGenerationWithImmutableFlow(tableType: HoodieTableType): Unit = {
    val compactionEnabled = if (tableType == HoodieTableType.MERGE_ON_READ) "true" else "false"
    val writeOptions = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCompactionConfig.INLINE_COMPACT.key -> compactionEnabled
    )
    // Bulk insert first.
    val records = recordsToStrings(dataGen.generateInserts("001", 5))
    val inputDF: Dataset[Row] = spark.read.json(jsc.parallelize(records, 2))
    inputDF.write.format("hudi").partitionBy("partition")
      .options(writeOptions)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(basePath)
    metaClient = HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    // Ensure no key fields are set.
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)

    // 30 inserts; every insert adds 5 records.
    for (i <- 0 until 30) {
      val records = recordsToStrings(dataGen.generateInserts("$i%03d", 5))
      val inputDF: Dataset[Row] = spark.read.json(jsc.parallelize(records, 2))
      inputDF.write.format("hudi").partitionBy("partition")
        .options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(basePath)
    }

    // Validate configs.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
    // Validate all records are unique.
    val numRecords = spark.read.format("hudi").load(basePath).count()
    assertEquals(155, numRecords)
    // Validate clean, archive, and clustering operation exists.
    assertFalse(metaClient.getActiveTimeline.getCleanerTimeline.empty())
    assertFalse(metaClient.getArchivedTimeline.empty())
    assertFalse(metaClient.getActiveTimeline.getCompletedReplaceTimeline.empty())
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertFalse(metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.empty())
      val fsv: HoodieTableFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
        context, metaClient, HoodieMetadataConfig.newBuilder.enable(true).build)
      fsv.loadAllPartitions()
      assertFalse(fsv.getAllFileGroups.flatMap(_.getAllFileSlices).anyMatch(_.hasLogFiles))
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testAutoKeyGenerationWithMutableFlow(tableType: HoodieTableType): Unit = {
    val compactionEnabled = if (tableType == HoodieTableType.MERGE_ON_READ) "true" else "false"
    val writeOptions = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCompactionConfig.INLINE_COMPACT.key -> compactionEnabled
    )
    // Bulk insert first.
    val records = recordsToStrings(dataGen.generateInserts("001", 5))
    val inputDF: Dataset[Row] = spark.read.json(jsc.parallelize(records, 2))
    inputDF.write.format("hudi").partitionBy("partition")
      .options(writeOptions)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(basePath)
    metaClient = HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    // Ensure no key fields are set.
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
    // 10 inserts.
    for (i <- 0 until 10) {
      val records = recordsToStrings(dataGen.generateInserts("$i%03d", 5))
      val inputDF: Dataset[Row] = spark.read.json(jsc.parallelize(records, 2))
      inputDF.write.format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(basePath)
    }
    // 10 updates.
    for (i <- 10 until 20) {
      val inputDF = spark.read.format("hudi").load(basePath)
        .withColumn("weight", col("weight") * 2)
      inputDF.write.format("hudi").partitionBy("partition")
        .options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(basePath)
    }
    // 10 deletes.
    for (i <- 20 until 30) {
      val inputDF = spark.read.format("hudi").load(basePath).limit(1)
      inputDF.write.format("hudi").partitionBy("partition")
        .options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(basePath)
    }
    // Validate configs.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
    // Validate all records are unique.
    val numRecords = spark.read.format("hudi").load(basePath).count()
    assertEquals(45, numRecords)
    // Validate clean, archive, and clustering operation exists.
    assertFalse(metaClient.getActiveTimeline.getCleanerTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getArchivedTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.isEmpty)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertFalse(metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.empty())
      val fsv: HoodieTableFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
        context, metaClient, HoodieMetadataConfig.newBuilder.enable(true).build)
      fsv.loadAllPartitions()
      assertTrue(fsv.getAllFileGroups.flatMap(_.getAllFileSlices).anyMatch(_.hasLogFiles))
    }
  }
}
