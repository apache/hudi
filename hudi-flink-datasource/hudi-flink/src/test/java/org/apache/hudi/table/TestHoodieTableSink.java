/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.DataModificationInfos;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieTableSink}.
 */
class TestHoodieTableSink {

  @TempDir
  File tempFile;

  @Test
  void testGetSinkRuntimeProviderRejectsInsertOverwriteWithNonBlockingConcurrencyControl() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());

    HoodieTableSink sink = new HoodieTableSink(conf, TestConfigurations.TABLE_SCHEMA);
    // The invalid combination must be rejected up front, before the provider lambda runs
    // StreamerUtil.initTableFromClientIfNecessary and creates the table on disk.
    HoodieException exception = assertThrows(HoodieException.class,
        () -> sink.getSinkRuntimeProvider(null));
    assertTrue(exception.getMessage()
        .contains(WriteConcurrencyMode.INSERT_OVERWRITE_NOT_SUPPORTED_ERROR));
    assertFalse(new File(tempFile, HoodieTableMetaClient.METAFOLDER_NAME).exists(),
        "The table should not be initialized when the write operation is rejected");
  }

  @Test
  void testGetSinkRuntimeProviderRejectsInsertOverwriteInjectedViaApplyOverwrite() {
    // Mirrors the SQL planner flow: INSERT OVERWRITE reaches the sink through applyOverwrite /
    // applyStaticPartition rather than an explicit FlinkOptions.OPERATION setting. The guard must
    // still reject the combination with non-blocking concurrency control.
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());

    HoodieTableSink sink = new HoodieTableSink(conf, TestConfigurations.TABLE_SCHEMA);
    // INSERT OVERWRITE TABLE injects INSERT_OVERWRITE_TABLE.
    sink.applyOverwrite(true);
    assertEquals(
        WriteOperationType.INSERT_OVERWRITE_TABLE.value(),
        conf.get(FlinkOptions.OPERATION));

    HoodieException exception = assertThrows(HoodieException.class,
        () -> sink.getSinkRuntimeProvider(null));
    assertTrue(exception.getMessage()
        .contains(WriteConcurrencyMode.INSERT_OVERWRITE_NOT_SUPPORTED_ERROR));
    assertFalse(new File(tempFile, HoodieTableMetaClient.METAFOLDER_NAME).exists(),
        "The table should not be initialized when the write operation is rejected");

    // INSERT OVERWRITE with static partitions injects INSERT_OVERWRITE; it must be rejected too.
    sink.applyStaticPartition(Collections.singletonMap("partition", "p1"));
    assertEquals(
        WriteOperationType.INSERT_OVERWRITE.value(),
        conf.get(FlinkOptions.OPERATION));

    exception = assertThrows(HoodieException.class,
        () -> sink.getSinkRuntimeProvider(null));
    assertTrue(exception.getMessage()
        .contains(WriteConcurrencyMode.INSERT_OVERWRITE_NOT_SUPPORTED_ERROR));
    assertFalse(new File(tempFile, HoodieTableMetaClient.METAFOLDER_NAME).exists(),
        "The table should not be initialized when the write operation is rejected");
  }

  @Test
  void testChangelogModeAndCopy() {
    Configuration conf = new Configuration();
    HoodieTableSink sink = new HoodieTableSink(conf, TestConfigurations.TABLE_SCHEMA);

    assertEquals(ChangelogModes.UPSERT, sink.getChangelogMode(ChangelogMode.all()));
    conf.set(FlinkOptions.CHANGELOG_ENABLED, true);
    assertEquals(ChangelogModes.FULL, sink.getChangelogMode(ChangelogMode.insertOnly()));
    assertEquals("HoodieTableSink", sink.asSummaryString());

    HoodieTableSink copied = (HoodieTableSink) sink.copy();
    assertNotSame(sink, copied);
    assertNotSame(conf, copied.getConf());
    assertEquals(ChangelogModes.FULL, copied.getChangelogMode(ChangelogMode.insertOnly()));

    copied.applyRowLevelDelete(null);
    assertEquals(
        WriteOperationType.DELETE.value(),
        copied.getConf().get(FlinkOptions.OPERATION));
    assertEquals(WriteOperationType.UPSERT.value(), conf.get(FlinkOptions.OPERATION));
  }

  @Test
  void testOverwriteAndRowLevelOperations() {
    Configuration conf = new Configuration();
    HoodieTableSink sink = new HoodieTableSink(conf, TestConfigurations.TABLE_SCHEMA);

    sink.applyOverwrite(true);
    assertEquals(
        WriteOperationType.INSERT_OVERWRITE_TABLE.value(),
        conf.get(FlinkOptions.OPERATION));
    sink.applyStaticPartition(Collections.singletonMap("partition", "p1"));
    assertEquals(
        WriteOperationType.INSERT_OVERWRITE.value(),
        conf.get(FlinkOptions.OPERATION));

    conf.set(FlinkOptions.WRITE_PARTITION_OVERWRITE_MODE, "DYNAMIC");
    sink.applyOverwrite(true);
    assertEquals(
        WriteOperationType.INSERT_OVERWRITE.value(),
        conf.get(FlinkOptions.OPERATION));

    assertSame(
        DataModificationInfos.DEFAULT_DELETE_INFO,
        sink.applyRowLevelDelete(null));
    assertEquals(WriteOperationType.DELETE.value(), conf.get(FlinkOptions.OPERATION));
    assertSame(
        DataModificationInfos.DEFAULT_UPDATE_INFO,
        sink.applyRowLevelUpdate(Collections.emptyList(), null));
    assertEquals(WriteOperationType.UPSERT.value(), conf.get(FlinkOptions.OPERATION));
  }
}
