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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import com.beust.jcommander.JCommander;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkClusteringConfig}.
 */
class TestFlinkClusteringConfig {

  @TempDir
  Path tempDir;

  @Test
  void testParseAndDeriveFlinkConfiguration() throws Exception {
    Configuration tableConf = TestConfigurations.getDefaultConf(tempDir.toString());
    tableConf.set(FlinkOptions.URL_ENCODE_PARTITIONING, true);
    tableConf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    StreamerUtil.initTableIfNotExists(tableConf);

    FlinkClusteringConfig config = new FlinkClusteringConfig();
    JCommander.newBuilder().addObject(config).build().parse(
        "--path", tempDir.toString(),
        "--clustering-delta-commits", "6",
        "--clustering-tasks", "4",
        "--clean-retain-commits", "12",
        "--clean-retain-hours", "36",
        "--clean-retain-file-versions", "7",
        "--archive-min-commits", "25",
        "--archive-max-commits", "40",
        "--schedule",
        "--clean-async-enabled",
        "--plan-partition-filter-mode", "RECENT_DAYS",
        "--target-file-max-bytes", "1048576",
        "--small-file-limit", "524288",
        "--skip-from-latest-partitions", "2",
        "--sort-columns", "event_ts,order_id",
        "--sort-memory", "256",
        "--max-num-groups", "10",
        "--target-partitions", "5",
        "--cluster-begin-partition", "2026-01-01",
        "--cluster-end-partition", "2026-01-31",
        "--partition-regex-pattern", "2026-01-.*",
        "--partition-selected", "2026-01-01,2026-01-02",
        "--hoodie-conf", "hoodie.test.clustering.option=from-cli");

    Configuration conf = FlinkClusteringConfig.toFlinkConfig(config);

    assertEquals(tempDir.toString(), conf.get(FlinkOptions.PATH));
    assertEquals(6, conf.get(FlinkOptions.CLUSTERING_DELTA_COMMITS));
    assertEquals(4, conf.get(FlinkOptions.CLUSTERING_TASKS));
    assertEquals(12, conf.get(FlinkOptions.CLEAN_RETAIN_COMMITS));
    assertEquals(36, conf.get(FlinkOptions.CLEAN_RETAIN_HOURS));
    assertEquals(7, conf.get(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS));
    assertEquals(25, conf.get(FlinkOptions.ARCHIVE_MIN_COMMITS));
    assertEquals(40, conf.get(FlinkOptions.ARCHIVE_MAX_COMMITS));
    assertEquals("RECENT_DAYS", conf.get(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME));
    assertEquals(1048576L, conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES));
    assertEquals(524288L, conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT));
    assertEquals(2, conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST));
    assertEquals("event_ts,order_id", conf.get(FlinkOptions.CLUSTERING_SORT_COLUMNS));
    assertEquals(256, conf.get(FlinkOptions.WRITE_SORT_MEMORY));
    assertEquals(10, conf.get(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS));
    assertEquals(5, conf.get(FlinkOptions.CLUSTERING_TARGET_PARTITIONS));
    assertEquals("2026-01-01",
        conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION));
    assertEquals("2026-01-31",
        conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION));
    assertEquals("2026-01-.*",
        conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN));
    assertEquals("2026-01-01,2026-01-02",
        conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_SELECTED));
    assertTrue(conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED));
    assertFalse(conf.get(FlinkOptions.CLUSTERING_ASYNC_ENABLED));
    assertTrue(conf.get(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED));
    assertTrue(conf.get(FlinkOptions.URL_ENCODE_PARTITIONING));
    assertTrue(conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING));
    assertEquals("from-cli", conf.getString("hoodie.test.clustering.option", null));
  }
}
