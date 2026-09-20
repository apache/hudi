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

package org.apache.hudi.hive;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.sync.common.util.HoodieMetastoreTableDescriptor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins {@link HoodieMetastoreTableDescriptor}'s format-name constants to the values hive-sync
 * actually emits.
 *
 * <p>The constants exist because {@link HoodieInputFormatUtils} lives in {@code hudi-hadoop-mr} and
 * pulls in Hadoop MapReduce, so a caller without that on its classpath cannot reach it. Repeating
 * the names is safe only while something checks that the copies agree; this is that check, and it
 * lives here because {@code hudi-hive-sync} is the nearest module that can see both.
 *
 * <p>If this fails, a format name changed on one side only. A Hudi table registered with an input
 * format the reader does not recognise is a table that reader refuses to query at all, since the
 * input format is the only record of the table type in the metastore.
 */
class TestHoodieMetastoreTableDescriptorFormatNames {

  @Test
  void inputFormatNamesMatchHiveSync() {
    assertEquals(
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, false),
        HoodieMetastoreTableDescriptor.PARQUET_INPUT_FORMAT_CLASS);
    assertEquals(
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, true),
        HoodieMetastoreTableDescriptor.PARQUET_REALTIME_INPUT_FORMAT_CLASS);
  }

  @Test
  void outputFormatAndSerdeNamesMatchHiveSync() {
    assertEquals(
        HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET),
        HoodieMetastoreTableDescriptor.PARQUET_OUTPUT_FORMAT_CLASS);
    assertEquals(
        HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET),
        HoodieMetastoreTableDescriptor.PARQUET_SERDE_CLASS);
  }

  @Test
  void theDescriptorResolvesTheSameInputFormatHiveSyncWouldForEachTableType() {
    // Copy-on-Write syncs one table with the non-realtime format; Merge-on-Read's snapshot view
    // uses the realtime one. See HiveSyncTool#doSync.
    assertEquals(
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, false),
        HoodieMetastoreTableDescriptor.inputFormatClassName(HoodieTableType.COPY_ON_WRITE, false));
    assertEquals(
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, true),
        HoodieMetastoreTableDescriptor.inputFormatClassName(HoodieTableType.MERGE_ON_READ, true));
  }

  @Test
  void theSchemaStringLengthThresholdMatchesTheHiveSyncDefault() {
    // Keeping these equal is what makes a table registered through the descriptor byte-identical to
    // a hive-synced one, rather than merely readable.
    assertEquals(
        HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.defaultValue().intValue(),
        HoodieMetastoreTableDescriptor.DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD);
  }
}
