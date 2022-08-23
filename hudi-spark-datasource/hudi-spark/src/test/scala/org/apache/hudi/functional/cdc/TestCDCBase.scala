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

package org.apache.hudi.functional.cdc

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals

import scala.collection.JavaConverters._

abstract class TestCDCBase extends HoodieClientTestBase {

  var spark: SparkSession = _

  val commonOpts = Map(
    HoodieTableConfig.CDC_ENABLED.key -> "true",
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    RECORDKEY_FIELD.key -> "_row_key",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
    HoodieCleanConfig.AUTO_CLEAN.key -> "false"
  )

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  protected def cdcDataFrame(basePath: String, startingInstant: String, endingInstant: String): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(INCREMENTAL_OUTPUT_FORMAT.key, INCREMENTAL_OUTPUT_FORMAT_CDC_VAL)
      .option("hoodie.datasource.read.begin.instanttime", startingInstant)
    if (endingInstant != null) {
      reader.option("hoodie.datasource.read.end.instanttime", endingInstant)
    }
    reader.load(basePath)
  }

  protected def cdcDataFrame(startingInstant: String, endingInstant: String = null): DataFrame = {
    cdcDataFrame(basePath, startingInstant, endingInstant)
  }

  /**
   * whether this instant will create a cdc log file.
   */
  protected def hasCDCLogFile(instant: HoodieInstant): Boolean = {
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      metaClient.reloadActiveTimeline().getInstantDetails(instant).get(),
      classOf[HoodieCommitMetadata]
    )
    val hoodieWriteStats = commitMetadata.getWriteStats.asScala
    hoodieWriteStats.exists { hoodieWriteStat =>
      val cdcPath = hoodieWriteStat.getCdcPath
      cdcPath != null && cdcPath.nonEmpty
    }
  }

  protected def assertCDCOpCnt(cdcData: DataFrame, expectedInsertCnt: Long,
                               expectedUpdateCnt: Long, expectedDeletedCnt: Long): Unit = {
    assertEquals(expectedInsertCnt, cdcData.where("op = 'i'").count())
    assertEquals(expectedUpdateCnt, cdcData.where("op = 'u'").count())
    assertEquals(expectedDeletedCnt, cdcData.where("op = 'd'").count())
  }
}
