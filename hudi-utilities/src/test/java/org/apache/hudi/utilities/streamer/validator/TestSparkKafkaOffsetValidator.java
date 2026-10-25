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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link SparkKafkaOffsetValidator}.
 */
public class TestSparkKafkaOffsetValidator {

  // ========== Helper methods ==========

  private static TypedProperties defaultConfig() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(), "0.0");
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");
    return props;
  }

  private static TypedProperties configWithTolerance(double tolerance) {
    TypedProperties props = defaultConfig();
    props.setProperty(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(),
        String.valueOf(tolerance));
    return props;
  }

  private static TypedProperties configWithWarnPolicy() {
    TypedProperties props = defaultConfig();
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "WARN_LOG");
    return props;
  }

  /**
   * Build a Spark Kafka checkpoint string.
   * Format: topic,partition:offset,partition:offset,...
   */
  private static String buildSparkKafkaCheckpoint(String topic, int[] partitions, long[] offsets) {
    StringBuilder sb = new StringBuilder();
    sb.append(topic);
    for (int i = 0; i < partitions.length; i++) {
      sb.append(",").append(partitions[i]).append(":").append(offsets[i]);
    }
    return sb.toString();
  }

  private static HoodieCommitMetadata buildMetadata(String checkpointValue) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    if (checkpointValue != null) {
      metadata.addMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, checkpointValue);
    }
    return metadata;
  }

  private static List<HoodieWriteStat> buildWriteStats(long numInserts, long numUpdates) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setNumInserts(numInserts);
    stat.setNumUpdateWrites(numUpdates);
    stat.setPartitionPath("partition1");
    return Collections.singletonList(stat);
  }

  private static SparkValidationContext buildContext(
      String instantTime,
      HoodieCommitMetadata currentMetadata,
      List<HoodieWriteStat> writeStats,
      HoodieCommitMetadata previousMetadata) {
    return new SparkValidationContext(
        instantTime,
        Option.of(currentMetadata),
        Option.of(writeStats),
        previousMetadata != null ? Option.of(previousMetadata) : Option.empty());
  }

  // ========== Tests ==========

  @Test
  public void testExactMatchPasses() {
    // Previous: partition 0 at offset 100, partition 1 at offset 200
    // Current: partition 0 at offset 200, partition 1 at offset 300
    // Diff = (200-100) + (300-200) = 200. Records written = 200.
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0, 1}, new long[]{100, 200});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0, 1}, new long[]{200, 300});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(200, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testDataLossDetected() {
    // Diff = 1000 but only 500 records written -> 50% deviation
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(500, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertThrows(HoodieValidationException.class, () -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testWithinTolerancePasses() {
    // Diff = 1000, records = 950 -> 5% deviation, tolerance = 10%
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(950, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(configWithTolerance(10.0));
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testWarnPolicyDoesNotThrow() {
    // Data loss but WARN_LOG policy
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(0, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(configWithWarnPolicy());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testSkipsFirstCommit() {
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    // No previous commit
    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(buildMetadata(currCheckpoint)),
        Option.of(buildWriteStats(500, 0)),
        Option.empty());

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testSkipsWhenNoCheckpointKey() {
    // Current metadata has no checkpoint key
    HoodieCommitMetadata currentMeta = new HoodieCommitMetadata();
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{100});

    SparkValidationContext ctx = buildContext("20260320120000000",
        currentMeta,
        buildWriteStats(500, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testMultiPartitionValidation() {
    // 4 partitions, each advancing by 250 = total diff 1000
    String prevCheckpoint = buildSparkKafkaCheckpoint("events",
        new int[]{0, 1, 2, 3}, new long[]{0, 0, 0, 0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events",
        new int[]{0, 1, 2, 3}, new long[]{250, 250, 250, 250});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(800, 200),  // 800 inserts + 200 updates = 1000
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testEmptyCommitSkipsValidation() {
    // Both offsets same and no records written
    String checkpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{100});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(checkpoint),
        buildWriteStats(0, 0),
        buildMetadata(checkpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testPreviousCheckpointMissingSkipsValidation() {
    // Previous metadata exists but has no checkpoint key
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();

    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(500, 0),
        prevMeta);

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testOvercountingDetected() {
    // More records written than offset diff
    // Diff = 100, records = 200 -> |100-200|/100 = 100% deviation
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{100});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(200, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertThrows(HoodieValidationException.class, () -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testExactToleranceBoundaryPasses() {
    // Diff = 1000, records = 900 -> 10% deviation, tolerance = 10%
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(900, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(configWithTolerance(10.0));
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testJustOverToleranceFails() {
    // Diff = 1000, records = 899 -> 10.1% deviation, tolerance = 10%
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(899, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(configWithTolerance(10.0));
    assertThrows(HoodieValidationException.class, () -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testOnlyInsertsNoUpdates() {
    // Pure insert workload
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0, 1}, new long[]{0, 0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0, 1}, new long[]{500, 500});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(1000, 0),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testUpdatesCountedInRecordTotal() {
    // Diff = 1000. 600 inserts + 400 updates = 1000 total
    String prevCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{0});
    String currCheckpoint = buildSparkKafkaCheckpoint("events", new int[]{0}, new long[]{1000});

    SparkValidationContext ctx = buildContext("20260320120000000",
        buildMetadata(currCheckpoint),
        buildWriteStats(600, 400),
        buildMetadata(prevCheckpoint));

    SparkKafkaOffsetValidator validator = new SparkKafkaOffsetValidator(defaultConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }
}
