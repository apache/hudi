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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.NonPartitionedExtractor
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test}


@Tag("functional")
class TestCOWDataSourceConsistentInserts extends SparkClientFunctionalTestHarness {

  val baseOpts = Map(
    DataSourceWriteOptions.TABLE_NAME.key() -> "language",
    DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "lang",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "score",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> classOf[NonpartitionedKeyGenerator].getCanonicalName,
    DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS.key() -> classOf[NonPartitionedExtractor].getCanonicalName,
    DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key() -> "true",
    DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "false",
    HoodieWriteConfig.TBL_NAME.key() -> "language",
    HoodieWriteConfig.MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key() -> "true",
    HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key() -> "1",
    HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key() -> "1",
    HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key() -> "1",
    HoodieWriteConfig.FINALIZE_WRITE_PARALLELISM_VALUE.key() -> "1",
    HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> (120 * 1024 * 1024).toString,
    "spark.default.parallelism" -> "1",
    "spark.sql.shuffle.partitions" -> "1"
  )

  @Test
  def insertsWithDuplicatesShouldBeConsistent(): Unit = {
    val opts = baseOpts ++ Map(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() -> "false")
    val inserts1 = spark.createDataFrame(("python", 1) :: ("scala", 5) :: ("scala", 4) :: ("haskell", 5) :: Nil).toDF("lang", "score")
    inserts1.write.format("hudi").options(opts).mode(Append).save(basePath)
    val tableData1 = spark.read.format("hudi").load(basePath)
    assertEquals(2, tableData1.where(col("_hoodie_record_key") === "scala").count())

    val inserts2 = spark.createDataFrame(("scala", 5) ::  ("java", 3) :: ("java", 4) :: Nil).toDF("lang", "score")
    inserts2.write.format("hudi").options(opts).mode(Append).save(basePath)
    val tableData2 = spark.read.format("hudi").load(basePath)
    assertEquals(3, tableData2.where(col("_hoodie_record_key") === "scala").count())
    assertEquals(2, tableData2.where(col("_hoodie_record_key") === "java").count())
  }

  @Test
  def insertsWithPreCombineEnabledShouldDeduplicateInput(): Unit = {
    val opts = baseOpts ++ Map(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() -> "true")
    val inserts1 = spark.createDataFrame(("python", 1) :: ("scala", 5) :: ("scala", 4) :: ("haskell", 5) :: Nil).toDF("lang", "score")
    inserts1.write.format("hudi").options(opts).mode(Append).save(basePath)
    val tableData1 = spark.read.format("hudi").load(basePath)
    assertEquals(1, tableData1.where(col("_hoodie_record_key") === "scala").count())

    val inserts2 = spark.createDataFrame(("scala", 5) ::  ("java", 3) :: ("java", 4) :: Nil).toDF("lang", "score")
    inserts2.write.format("hudi").options(opts).mode(Append).save(basePath)
    val tableData2 = spark.read.format("hudi").load(basePath)
    assertEquals(2, tableData2.where(col("_hoodie_record_key") === "scala").count())
    assertEquals(1, tableData2.where(col("_hoodie_record_key") === "java").count())
  }
}
