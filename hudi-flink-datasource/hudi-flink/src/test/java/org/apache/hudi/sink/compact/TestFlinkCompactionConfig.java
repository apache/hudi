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

package org.apache.hudi.sink.compact;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.configuration.FlinkOptions;

import com.beust.jcommander.JCommander;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkCompactionConfig}.
 */
class TestFlinkCompactionConfig {

  @TempDir
  Path tempDir;

  @Test
  void testParseAndDeriveFlinkConfiguration() {
    FlinkCompactionConfig config = new FlinkCompactionConfig();
    JCommander.newBuilder().addObject(config).build().parse(
        "--path", tempDir.toString(),
        "--compaction-trigger-strategy", FlinkCompactionConfig.NUM_OR_TIME,
        "--disable-file-group-reader",
        "--compaction-delta-commits", "8",
        "--compaction-delta-seconds", "900",
        "--clean-async-enabled",
        "--compaction-max-memory", "256",
        "--compaction-target-io", "2048",
        "--compaction-tasks", "4",
        "--schedule",
        "--spillable_map_path", tempDir.resolve("spill").toString(),
        "--hoodie-conf", "hoodie.test.compaction.option=from-cli");

    Configuration conf = FlinkCompactionConfig.toFlinkConfig(config);

    assertEquals(tempDir.toString(), conf.get(FlinkOptions.PATH));
    assertEquals(FlinkCompactionConfig.NUM_OR_TIME, conf.get(FlinkOptions.COMPACTION_TRIGGER_STRATEGY));
    assertEquals(8, conf.get(FlinkOptions.COMPACTION_DELTA_COMMITS));
    assertEquals(900, conf.get(FlinkOptions.COMPACTION_DELTA_SECONDS));
    assertEquals(256, conf.get(FlinkOptions.COMPACTION_MAX_MEMORY));
    assertEquals(256, conf.get(FlinkOptions.WRITE_MERGE_MAX_MEMORY));
    assertEquals(2048L, conf.get(FlinkOptions.COMPACTION_TARGET_IO));
    assertEquals(4, conf.get(FlinkOptions.COMPACTION_TASKS));
    assertTrue(conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED));
    assertFalse(conf.get(FlinkOptions.COMPACTION_OPERATION_EXECUTE_ASYNC_ENABLED));
    assertTrue(conf.get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED));
    assertEquals(tempDir.resolve("spill").toString(),
        conf.getString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), null));
    assertEquals("false", conf.getString(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), null));
    assertEquals("from-cli", conf.getString("hoodie.test.compaction.option", null));
  }
}
