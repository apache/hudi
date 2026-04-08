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

package org.apache.hudi.utilities.streamer.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SparkStreamerValidatorUtils}.
 *
 * <p>Tests cover orchestration logic (class loading, config passing, error handling)
 * as well as end-to-end offset validation using a two-commit timeline to verify
 * the real comparison path is exercised.</p>
 */
public class TestSparkStreamerValidatorUtils {

  @TempDir
  Path tempDir;

  private static TypedProperties propsWithValidator(String validatorClassName) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), validatorClassName);
    props.setProperty(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");
    return props;
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

  private HoodieTableMetaClient createMetaClient() throws IOException {
    return HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
  }

  // ========== Tests ==========

  @Test
  public void testNoValidatorsConfigured() throws IOException {
    TypedProperties props = new TypedProperties();
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses,
        new HashMap<>(), createMetaClient()));
  }

  @Test
  public void testEmptyValidatorString() throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), "");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses,
        new HashMap<>(), createMetaClient()));
  }

  @Test
  public void testValidValidatorFirstCommitPasses() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:100");

    // First commit (no previous metadata on timeline) — validator should skip and pass
    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, extraMeta, createMetaClient()));
  }

  @Test
  public void testInvalidValidatorClassThrows() throws IOException {
    TypedProperties props = propsWithValidator("com.nonexistent.FakeValidator");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertThrows(HoodieValidationException.class,
        () -> SparkStreamerValidatorUtils.runValidators(
            props, "20260320120000000", writeStatuses, new HashMap<>(), createMetaClient()));
  }

  @Test
  public void testMultipleValidators() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator,"
            + "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:100");

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, extraMeta, createMetaClient()));
  }

  @Test
  public void testValidatorWithWhitespaceInClassNames() throws IOException {
    TypedProperties props = propsWithValidator(
        "  org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator  , ");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, new HashMap<>(), createMetaClient()));
  }

  @Test
  public void testNullExtraMetadataHandled() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, null, createMetaClient()));
  }

  @Test
  public void testMultipleWriteStatusesAggregated() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    List<WriteStatus> writeStatuses = new ArrayList<>();
    writeStatuses.add(buildWriteStatus("p1", 60, 0));
    writeStatuses.add(buildWriteStatus("p2", 40, 0));

    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:100");

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, extraMeta, createMetaClient()));
  }

  @Test
  public void testEmptyWriteStatuses() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    List<WriteStatus> writeStatuses = Collections.emptyList();
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:100");

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, extraMeta, createMetaClient()));
  }

  @Test
  public void testValidationExceptionPreservedAcrossValidators() throws IOException {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator,"
            + "com.nonexistent.FakeValidator");

    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> SparkStreamerValidatorUtils.runValidators(
            props, "20260320120000000", writeStatuses, new HashMap<>(), createMetaClient()));
    assertTrue(ex.getMessage().contains("FakeValidator"));
  }

  @Test
  public void testSecondCommitMatchingOffsetsPasses() throws Exception {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    // Create table with a previous committed instant: offset 0 -> 500
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:500");
    HoodieTestTable.of(metaClient).addCommit("20260320110000000", Option.of(prevMeta));

    // Second commit: offset 500 -> 600, 100 records written — matches diff exactly
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:600");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 100, 0));

    assertDoesNotThrow(() -> SparkStreamerValidatorUtils.runValidators(
        props, "20260320120000000", writeStatuses, extraMeta, metaClient));
  }

  @Test
  public void testSecondCommitDataLossDetected() throws Exception {
    TypedProperties props = propsWithValidator(
        "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator");

    // Create table with a previous committed instant: offset 0 -> 1000
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:1000");
    HoodieTestTable.of(metaClient).addCommit("20260320110000000", Option.of(prevMeta));

    // Second commit: offset 1000 -> 2000 (diff=1000) but only 500 records written — data loss
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:2000");
    List<WriteStatus> writeStatuses = Collections.singletonList(buildWriteStatus("p1", 500, 0));

    assertThrows(HoodieValidationException.class,
        () -> SparkStreamerValidatorUtils.runValidators(
            props, "20260320120000000", writeStatuses, extraMeta, metaClient));
  }
}
