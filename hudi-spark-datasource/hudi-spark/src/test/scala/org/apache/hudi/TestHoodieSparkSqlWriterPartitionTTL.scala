/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.{fixInstantTimeCompatibility, instantTimePlusMillis}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieTTLConfig, HoodieWriteConfig}
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.ttl.strategy.KeepByTimeStrategy
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Test


/**
 * The origin PartitionTTLStrategy calculate the expire time by DAYs, it's too long for test.
 * Override the method isPartitionExpired to calculate expire time by minutes.
 * @param hoodieTable
 * @param instantTime
 */
class HoodieSparkSqlWriterTestStrategy(hoodieTable: HoodieTable[_, _, _, _], instantTime: String)
  extends KeepByTimeStrategy(hoodieTable, instantTime) {
  override def isPartitionExpired(referenceTime: String): Boolean = {
    val expiredTime = instantTimePlusMillis(referenceTime, ttlInMilis / 24 / 3600)
    fixInstantTimeCompatibility(instantTime).compareTo(expiredTime) > 0
  }
}

class TestHoodieSparkSqlWriterPartitionTTL extends HoodieSparkWriterTestBase {

  /**
   *  Test partition ttl with HoodieSparkSqlWriter.
   */
  @Test
  def testSparkSqlWriterWithPartitionTTL(): Unit = {
    val hoodieFooTableName = "hoodie_foo_tbl"
    val fooTableModifier = Map("path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      HoodieWriteConfig.BASE_FILE_FORMAT.key -> HoodieFileFormat.PARQUET.name,
      DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL,
      HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "4",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.POPULATE_META_FIELDS.key() -> "true",
      HoodieTTLConfig.INLINE_PARTITION_TTL.key() -> "true",
      HoodieTTLConfig.DAYS_RETAIN.key() -> "1",
      HoodieTTLConfig.PARTITION_TTL_STRATEGY_CLASS_NAME.key() -> "org.apache.hudi.HoodieSparkSqlWriterTestStrategy"
    )

    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val recordsForPart1 = DataSourceTestUtils.generateRandomRowsByPartition(100, "part1")
    val recordsSeqForPart1 = convertRowListToSeq(recordsForPart1)
    val part1DF = spark.createDataFrame(sc.parallelize(recordsSeqForPart1), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, part1DF)

    val recordsForPart2 = DataSourceTestUtils.generateRandomRowsByPartition(100, "part2")
    val recordsSeqForPart2 = convertRowListToSeq(recordsForPart2)
    val part2DF = spark.createDataFrame(sc.parallelize(recordsSeqForPart2), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, part2DF)

    val timeline = HoodieTestUtils.createMetaClient(tempBasePath).getActiveTimeline
    assert(timeline.getCompletedReplaceTimeline.getInstants.size() > 0)
    val replaceInstant = timeline.getCompletedReplaceTimeline.getInstants.get(0)
    val replaceMetadata = timeline.readReplaceCommitMetadataToAvro(replaceInstant)
    assert(replaceMetadata.getPartitionToReplaceFileIds.containsKey("part1"))
  }

}
