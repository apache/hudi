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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, RecordLevelIndexSupport}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.core.index.record.HoodieRecordIndex
import org.apache.hudi.metadata.MetadataPartitionType

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * Data skipping coverage for a partitioned Record Level Index (RLI). This reuses the full coverage of
 * [[TestGlobalRecordLevelIndexWithSQL]] by flipping [[isPartitionedRli]] to true so that every inherited test
 * exercises a partitioned RLI, and adds the partitioned-specific scenarios (e.g. the max-candidate-partitions
 * threshold fallback).
 */
@Tag("functional")
class TestRecordLevelIndexWithSQL extends TestGlobalRecordLevelIndexWithSQL {

  override protected def isPartitionedRli: Boolean = true

  /**
   * Verifies that the record level index is created as partitioned and that pruning is skipped (without error)
   * once the number of candidate data table partitions exceeds the hard-coded threshold.
   */
  @Test
  def testPartitionedRliPartitionsThreshold(): Unit = {
    val hudiOpts = commonOpts ++ rliEnableOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      "hoodie.metadata.index.column.stats.enable" -> "false",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    val partitionPaths = (0 to RecordLevelIndexSupport.MAX_PARTITIONS)
      .map(i => f"2026/06/$i%02d")
      .toArray
    initTestDataGenerator(partitionPaths)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false,
      numInserts = partitionPaths.length)

    // The record index should have been created as partitioned
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(HoodieRecordIndex.isPartitioned(
      metaClient.getIndexMetadata.get().getIndex(MetadataPartitionType.RECORD_INDEX.getPartitionPath).get()))

    createTempTable(hudiOpts)
    val latestSnapshotDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    val recordKey = latestSnapshotDf.limit(1).collect().head.getAs[String]("_row_key")
    val dataFilter: Expression = EqualTo(attribute("_row_key"), Literal(recordKey))

    // The record-key-only query sees all candidate partitions and exceeds the hard-coded threshold.
    verifyPruningFileCount(hudiOpts, dataFilter, numFiles = -1, shouldPrune = false)

    val partitionValue = latestSnapshotDf.limit(1).collect().head.getAs[String]("partition")
    val partitionFilter: Expression = EqualTo(attribute("partition"), Literal(partitionValue))

    // Spark passes partition predicates separately, and HoodieFileIndex only prunes partitions from that channel.
    // With one candidate partition, the hard-coded threshold is not hit and RLI pruning still applies.
    verifyPruningFileCount(hudiOpts, Seq(dataFilter), Seq(partitionFilter), numFiles = 1,
      metaClient = HoodieTableMetaClient.reload(metaClient), shouldPrune = true)
  }
}
