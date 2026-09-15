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

package org.apache.hudi.streamer;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkStreamerConfig}.
 */
class TestFlinkStreamerConfig {

  @TempDir
  Path tempDir;

  @Test
  void testParseAndDeriveFlinkConfiguration() {
    FlinkStreamerConfig config = parse(
        "--kafka-topic", "orders",
        "--kafka-group-id", "flink-writers",
        "--kafka-bootstrap-servers", "broker:9092",
        "--target-base-path", tempDir.toString(),
        "--target-table", "orders_hudi",
        "--table-type", "merge_on_read",
        "--op", "INSERT",
        "--record-key-field", "order_id",
        "--partition-path-field", "order_date",
        "--source-ordering-fields", "event_ts,seq_no",
        "--instant-retry-times", "7",
        "--instant-retry-interval", "2",
        "--filter-dupes",
        "--commit-on-errors",
        "--metadata-enabled",
        "--write-rate-limit", "500",
        "--write-task-num", "6",
        "--bucket-assign-num", "5",
        "--index-bootstrap-num", "4",
        "--source-avro-schema-path", "file:///tmp/source.avsc",
        "--source-avro-schema", "{\"type\":\"record\",\"name\":\"order\",\"fields\":[]}",
        "--compaction-tasks", "3",
        "--clustering-tasks", "2",
        "--hive-sync-enable",
        "--hive-sync-db", "analytics",
        "--hive-sync-table", "orders",
        "--hoodie-conf", "hoodie.datasource.write.drop.partition.columns=true");

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(config);

    assertEquals(tempDir.toString(), conf.get(FlinkOptions.PATH));
    assertEquals("orders_hudi", conf.get(FlinkOptions.TABLE_NAME));
    assertEquals("MERGE_ON_READ", conf.get(FlinkOptions.TABLE_TYPE));
    assertEquals(WriteOperationType.INSERT.value(), conf.get(FlinkOptions.OPERATION));
    assertEquals("order_id", conf.get(FlinkOptions.RECORD_KEY_FIELD));
    assertEquals("order_date", conf.get(FlinkOptions.PARTITION_PATH_FIELD));
    assertEquals("event_ts,seq_no", conf.get(FlinkOptions.ORDERING_FIELDS));
    assertEquals(7, conf.get(FlinkOptions.RETRY_TIMES));
    assertEquals(2_000L, conf.get(FlinkOptions.RETRY_INTERVAL_MS));
    assertTrue(conf.get(FlinkOptions.PRE_COMBINE));
    assertTrue(conf.get(FlinkOptions.IGNORE_FAILED));
    assertTrue(conf.get(FlinkOptions.METADATA_ENABLED));
    assertEquals(500L, conf.get(FlinkOptions.WRITE_RATE_LIMIT));
    assertEquals(6, conf.get(FlinkOptions.WRITE_TASKS));
    assertEquals(5, conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
    assertEquals(4, conf.get(FlinkOptions.INDEX_BOOTSTRAP_TASKS));
    assertEquals("file:///tmp/source.avsc", conf.get(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH));
    assertEquals(3, conf.get(FlinkOptions.COMPACTION_TASKS));
    assertEquals(2, conf.get(FlinkOptions.CLUSTERING_TASKS));
    assertTrue(conf.get(FlinkOptions.HIVE_SYNC_ENABLED));
    assertEquals("analytics", conf.get(FlinkOptions.HIVE_SYNC_DB));
    assertEquals("orders", conf.get(FlinkOptions.HIVE_SYNC_TABLE));
    assertEquals("true",
        conf.getString("hoodie.datasource.write.drop.partition.columns", null));
  }

  @Test
  void testCustomKeyGeneratorTakesPrecedence() {
    FlinkStreamerConfig config = parseRequiredOptions(
        "--keygen-class", "org.example.CustomKeyGenerator",
        "--keygen-type", "COMPLEX");

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(config);

    assertEquals("org.example.CustomKeyGenerator", conf.get(FlinkOptions.KEYGEN_CLASS_NAME));
    assertFalse(conf.contains(FlinkOptions.KEYGEN_TYPE));
  }

  @Test
  void testRequiredOptionsAndNumericValuesAreValidated() {
    FlinkStreamerConfig missingRequired = new FlinkStreamerConfig();
    assertThrows(ParameterException.class,
        () -> JCommander.newBuilder().addObject(missingRequired).build().parse("--kafka-topic", "orders"));

    FlinkStreamerConfig invalidRetry = parseRequiredOptions("--instant-retry-times", "not-a-number");
    assertThrows(NumberFormatException.class, () -> FlinkStreamerConfig.toFlinkConfig(invalidRetry));
  }

  private FlinkStreamerConfig parseRequiredOptions(String... additionalArgs) {
    String[] requiredArgs = {
        "--kafka-topic", "orders",
        "--kafka-group-id", "flink-writers",
        "--kafka-bootstrap-servers", "broker:9092",
        "--target-base-path", tempDir.toString(),
        "--target-table", "orders_hudi",
        "--table-type", "copy_on_write"
    };
    String[] args = new String[requiredArgs.length + additionalArgs.length];
    System.arraycopy(requiredArgs, 0, args, 0, requiredArgs.length);
    System.arraycopy(additionalArgs, 0, args, requiredArgs.length, additionalArgs.length);
    return parse(args);
  }

  private static FlinkStreamerConfig parse(String... args) {
    FlinkStreamerConfig config = new FlinkStreamerConfig();
    JCommander.newBuilder().addObject(config).build().parse(args);
    return config;
  }
}
