/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, ORDERING_FIELDS, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.debezium.{DebeziumConstants, PostgresDebeziumAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Functional test for [[PostgresDebeziumAvroPayload]]'s toasted / unavailable-value handling on a
 * Merge-on-Read table.
 *
 * Postgres TOAST semantics: when a Debezium UPDATE cannot capture a large STRING/BYTES column that
 * did not change, it ships the sentinel [[PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE]]
 * (`__debezium_unavailable_value`) for that column. When the base file and delta log are merged at
 * read time, such columns must KEEP the prior (base) value rather than be overwritten with the
 * sentinel.
 *
 * Covers table versions 6, 8 and 9.
 */
class TestDebeziumToastedValue extends SparkClientFunctionalTestHarness {

  private val sentinel = PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE
  private val columns = Seq(
    "id",
    "name",
    "city",
    DebeziumConstants.FLATTENED_OP_COL_NAME,
    DebeziumConstants.FLATTENED_LSN_COL_NAME)

  @ParameterizedTest
  @ValueSource(strings = Array("6", "8", "9"))
  def testToastedColumnKeepsPriorValueOnRead(tableVersion: String): Unit = {
    val opts = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
      HoodieMetadataConfig.ENABLE.key() -> "false",
      // Preserve the table version across writes: disable auto-upgrade and pin the write version so
      // a later write cannot migrate the table to a newer version mid-test.
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key() -> "false",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion)

    // 1. Base records at LSN 10.
    val base = Seq(
      (1, "alice", "NYC", "c", 10L),
      (2, "bob", "LA", "c", 10L))
    spark.createDataFrame(base).toDF(columns: _*).write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(ORDERING_FIELDS.key(), DebeziumConstants.FLATTENED_LSN_COL_NAME)
      .option(TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name())
      .option(DataSourceWriteOptions.TABLE_NAME.key(), "toasted_table")
      .option(OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // 2. Newer log records at LSN 20, each carrying the toasted sentinel in one column:
    //    id=1 -> name updated, `city` toasted (must keep base "NYC")
    //    id=2 -> name toasted (must keep base "bob"), `city` updated
    val update = Seq(
      (1, "alice2", sentinel, "u", 20L),
      (2, sentinel, "SF", "u", 20L))
    spark.createDataFrame(update).toDF(columns: _*).write.format("hudi")
      .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    // Sanity: the table is at the requested version.
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(tableVersion.toInt, metaClient.getTableConfig.getTableVersion.versionCode())

    // 3. Snapshot read merges base + log; sentinel columns must retain the prior base value.
    val rows = spark.read.format("hudi").load(basePath)
      .select("id", "name", "city")
      .orderBy("id")
      .collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2)))
      .toSeq

    assertEquals(
      Seq((1, "alice2", "NYC"), (2, "bob", "SF")),
      rows,
      s"toasted columns must keep the prior base value (tableVersion=$tableVersion)")
  }
}
