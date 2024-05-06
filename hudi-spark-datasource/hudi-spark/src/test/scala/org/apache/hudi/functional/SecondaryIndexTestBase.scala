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

import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.util.concurrent.atomic.AtomicInteger

class SecondaryIndexTestBase extends HoodieSparkClientTestBase {

  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val targetColumnsToIndex: Seq[String] = Seq("rider", "driver")
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_si",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition,trip_type",
    HIVE_STYLE_PARTITIONING.key -> "true",
    PRECOMBINE_FIELD.key -> "timestamp"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()
    setTableName("hoodie_test")
    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupResources()
  }

}
