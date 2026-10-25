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

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.DataModificationInfos;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link HoodieTableSink}.
 */
class TestHoodieTableSink {

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
