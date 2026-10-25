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

package org.apache.hudi.sink.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkValidatorUtils}.
 */
public class TestFlinkValidatorUtils {

  private static Configuration confWithValidator(String validatorClassName) {
    Configuration conf = new Configuration();
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), validatorClassName);
    return conf;
  }

  private static WriteStatus buildWriteStatus(String partitionPath, long numInserts, long numUpdates) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(partitionPath);
    stat.setNumInserts(numInserts);
    stat.setNumUpdateWrites(numUpdates);

    WriteStatus ws = new WriteStatus(false, 0.0);
    ws.setStat(stat);
    return ws;
  }

  // ========== Tests ==========

  @Test
  public void testNoValidatorsConfigured() {
    Configuration conf = new Configuration();
    // No validator class names set
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses,
        new HashMap<>(), () -> Option.empty()));
  }

  @Test
  public void testEmptyValidatorString() {
    Configuration conf = new Configuration();
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), "");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses,
        new HashMap<>(), () -> Option.empty()));
  }

  @Test
  public void testValidValidatorRunsSuccessfully() {
    // FlinkKafkaOffsetValidator with first commit (no previous) should pass
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:100");

    // First commit (no previous metadata) — validator should skip and pass
    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.empty()));
  }

  @Test
  public void testValidatorPassesWithMatchingCounts() {
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    // 100 records written, offset diff = 100
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:200");

    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:100");

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.of(prevMeta)));
  }

  @Test
  public void testValidatorFailsWithMismatchedCounts() {
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    // Only 10 records written but offset diff = 1000
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 10, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:1000");

    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:0");

    assertThrows(HoodieValidationException.class,
        () -> FlinkValidatorUtils.runValidators(
            conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.of(prevMeta)));
  }

  @Test
  public void testInvalidValidatorClassThrows() {
    Configuration conf = confWithValidator("com.nonexistent.FakeValidator");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertThrows(HoodieValidationException.class,
        () -> FlinkValidatorUtils.runValidators(
            conf, "20260320120000000", writeStatuses, new HashMap<>(), () -> Option.empty()));
  }

  @Test
  public void testMultipleValidators() {
    // Two validators comma-separated, both FlinkKafkaOffsetValidator (will both skip on first commit)
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator,"
            + "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:100");

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.empty()));
  }

  @Test
  public void testValidatorWithWhitespaceInClassNames() {
    // Class names with extra whitespace and trailing comma
    Configuration conf = confWithValidator(
        "  org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator  , ");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, new HashMap<>(), () -> Option.empty()));
  }

  @Test
  public void testBuildCommitMetadataWithNullExtraMetadata() {
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    // null extraMetadata should be handled gracefully
    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, null, () -> Option.empty()));
  }

  @Test
  public void testMultipleWriteStatusesAggregated() {
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    // Two write statuses: 60 inserts + 40 inserts = 100 total
    List<WriteStatus> writeStatuses = new ArrayList<>();
    writeStatuses.add(buildWriteStatus("p1", 60, 0));
    writeStatuses.add(buildWriteStatus("p2", 40, 0));

    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:200");

    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:100");

    // Offset diff = 100, records = 100 — should pass
    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.of(prevMeta)));
  }

  @Test
  public void testEmptyWriteStatuses() {
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    // Empty write statuses with matching empty offsets
    List<WriteStatus> writeStatuses = Collections.emptyList();
    Map<String, String> extraMeta = new HashMap<>();
    String checkpoint = "kafka_metadata%3Aevents%3A0:100";
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY, checkpoint);

    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerUtil.HOODIE_METADATA_KEY, checkpoint);

    // Both offset diff and records are 0 — should skip validation
    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.of(prevMeta)));
  }

  @Test
  public void testValidationExceptionPreservedAcrossValidators() {
    // First validator: valid, second: invalid class
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator,"
            + "com.nonexistent.FakeValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> FlinkValidatorUtils.runValidators(
            conf, "20260320120000000", writeStatuses, new HashMap<>(), () -> Option.empty()));
    assertTrue(ex.getMessage().contains("FakeValidator"));
  }

  @Test
  public void testConfigPropertiesPassedToValidator() {
    // Set a high tolerance so even with mismatch, it passes
    Configuration conf = confWithValidator(
        "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator");
    conf.setString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "100.0");
    conf.setString(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");

    // 0 records written but 1000 offset diff — 100% deviation, but tolerance is 100%
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 0, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:1000");

    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerUtil.HOODIE_METADATA_KEY,
        "kafka_metadata%3Aevents%3A0:0");

    assertDoesNotThrow(() -> FlinkValidatorUtils.runValidators(
        conf, "20260320120000000", writeStatuses, extraMeta, () -> Option.of(prevMeta)));
  }
}
