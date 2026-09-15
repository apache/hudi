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

package org.apache.hudi.client.validator;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig.ValidationFailurePolicy;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for StreamingOffsetValidator, ValidationContext, and BasePreCommitValidator.
 */
public class TestStreamingOffsetValidator {

  private static final String CHECKPOINT_KEY = "test.checkpoint.key";

  /**
   * Mock implementation of StreamingOffsetValidator for testing.
   */
  private static class MockOffsetValidator extends StreamingOffsetValidator {
    public MockOffsetValidator(TypedProperties config) {
      super(config, CHECKPOINT_KEY, CheckpointFormat.SPARK_KAFKA);
    }

    // Expose protected method for testing
    public void testValidateOffsetConsistency(long offsetDiff, long recordsWritten,
                                                String current, String previous) {
      validateOffsetConsistency(offsetDiff, recordsWritten, 0L, current, previous);
    }

    // Expose validateWithMetadata for testing
    public void testValidateWithMetadata(ValidationContext context) {
      validateWithMetadata(context);
    }
  }

  /**
   * Simple test implementation of ValidationContext for testing.
   */
  private static class TestValidationContext implements ValidationContext {
    private final String instantTime;
    private final Option<HoodieCommitMetadata> commitMetadata;
    private final Option<List<HoodieWriteStat>> writeStats;
    private final boolean firstCommit;
    private final Option<HoodieCommitMetadata> previousCommitMetadata;

    TestValidationContext(String instantTime,
                          Option<HoodieCommitMetadata> commitMetadata,
                          Option<List<HoodieWriteStat>> writeStats,
                          boolean firstCommit,
                          Option<HoodieCommitMetadata> previousCommitMetadata) {
      this.instantTime = instantTime;
      this.commitMetadata = commitMetadata;
      this.writeStats = writeStats;
      this.firstCommit = firstCommit;
      this.previousCommitMetadata = previousCommitMetadata;
    }

    @Override
    public String getInstantTime() {
      return instantTime;
    }

    @Override
    public Option<HoodieCommitMetadata> getCommitMetadata() {
      return commitMetadata;
    }

    @Override
    public Option<List<HoodieWriteStat>> getWriteStats() {
      return writeStats;
    }

    @Override
    public HoodieActiveTimeline getActiveTimeline() {
      return null; // Not needed for these tests
    }

    @Override
    public Option<HoodieInstant> getPreviousCommitInstant() {
      if (firstCommit) {
        return Option.empty();
      }
      return Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, "commit",
          "20260309110000", InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR));
    }

    @Override
    public Option<HoodieCommitMetadata> getPreviousCommitMetadata() {
      return previousCommitMetadata;
    }
  }

  private MockOffsetValidator createValidator(double tolerance, ValidationFailurePolicy policy) {
    TypedProperties config = new TypedProperties();
    config.setProperty(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(),
        String.valueOf(tolerance));
    config.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(),
        policy.name());
    return new MockOffsetValidator(config);
  }

  private HoodieWriteStat createWriteStat(long numInserts, long numUpdateWrites) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setNumInserts(numInserts);
    stat.setNumUpdateWrites(numUpdateWrites);
    stat.setNumWrites(numInserts + numUpdateWrites + 50); // numWrites includes small file overhead
    return stat;
  }

  // ========== validateOffsetConsistency tests ==========

  @Test
  public void testExactMatchValidation() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 1000, "cur", "prev"));
  }

  @Test
  public void testStrictModeFailure() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 900, "cur", "prev"));
  }

  @Test
  public void testValidationWithTolerance() {
    MockOffsetValidator validator = createValidator(10.0, ValidationFailurePolicy.FAIL);

    // 5% deviation - within 10% tolerance
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 950, "cur", "prev"));

    // 10% deviation - exactly at tolerance
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 900, "cur", "prev"));

    // 15% deviation - exceeds tolerance
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 850, "cur", "prev"));
  }

  @Test
  public void testWarnLogPolicy() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.WARN_LOG);

    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 500, "cur", "prev"));
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 0, "cur", "prev"));
  }

  @Test
  public void testEdgeCaseZeroBoth() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(0, 0, "cur", "prev"));
  }

  @Test
  public void testEdgeCaseOneZero() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(0, 1000, "cur", "prev"));
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 0, "cur", "prev"));
  }

  @Test
  public void testDefaultConfiguration() {
    TypedProperties config = new TypedProperties();
    MockOffsetValidator validator = new MockOffsetValidator(config);

    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 1000, "cur", "prev"));
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 999, "cur", "prev"));
  }

  // ========== validateWithMetadata end-to-end tests ==========

  @Test
  public void testValidateWithMetadataSkipsFirstCommit() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.empty(),
        Option.empty(),
        true, // first commit
        Option.empty());

    assertTrue(context.isFirstCommit());
    // Should not throw - skips validation for first commit
    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsMissingCurrentCheckpoint() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    // Current commit has no checkpoint metadata
    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    // No checkpoint key added

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.empty(),
        false,
        Option.empty());

    assertFalse(context.isFirstCommit());
    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsInvalidCurrentCheckpoint() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "invalid_format");

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.empty(),
        false,
        Option.empty());

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsMissingPreviousCheckpoint() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:300");

    // Previous metadata exists but has no checkpoint key
    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.empty(),
        false,
        Option.of(prevMetadata));

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsNoPreviousMetadata() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:300");

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.empty(),
        false,
        Option.empty()); // no previous commit metadata at all

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsInvalidPreviousCheckpoint() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:300");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "bad_format");

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.empty(),
        false,
        Option.of(prevMetadata));

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataSkipsEmptyCommit() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    // Same checkpoint = 0 offset diff, 0 records written
    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.of(Collections.emptyList()), // no write stats
        false,
        Option.of(prevMetadata));

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataPassesValidCommit() {
    MockOffsetValidator validator = createValidator(5.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:400");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    // Offset diff = (200-100) + (400-200) = 300
    // Records: 150 inserts + 150 updates = 300
    HoodieWriteStat stat1 = createWriteStat(150, 0);
    HoodieWriteStat stat2 = createWriteStat(0, 150);
    List<HoodieWriteStat> stats = Arrays.asList(stat1, stat2);

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.of(stats),
        false,
        Option.of(prevMetadata));

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataFailsOnMismatch() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:400");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    // Offset diff = 300, but only 100 records written
    HoodieWriteStat stat = createWriteStat(100, 0);
    List<HoodieWriteStat> stats = Collections.singletonList(stat);

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.of(stats),
        false,
        Option.of(prevMetadata));

    assertThrows(HoodieValidationException.class,
        () -> validator.testValidateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataWarnsOnMismatchWithWarnPolicy() {
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.WARN_LOG);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:200,1:400");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    // Large mismatch but WARN_LOG policy
    HoodieWriteStat stat = createWriteStat(50, 0);

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.of(Collections.singletonList(stat)),
        false,
        Option.of(prevMetadata));

    assertDoesNotThrow(() -> validator.testValidateWithMetadata(context));
  }

  // ========== ValidationContext default method tests ==========

  @Test
  public void testValidationContextGetExtraMetadata() {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("key1", "value1");
    metadata.addMetadata("key2", "value2");

    TestValidationContext context = new TestValidationContext(
        "20260309120000", Option.of(metadata), Option.empty(),
        true, Option.empty());

    assertEquals("value1", context.getExtraMetadata("key1").get());
    assertEquals("value2", context.getExtraMetadata("key2").get());
    assertFalse(context.getExtraMetadata("missing").isPresent());

    assertEquals(2, context.getExtraMetadata().size());
  }

  @Test
  public void testValidationContextGetExtraMetadataEmpty() {
    TestValidationContext context = new TestValidationContext(
        "20260309120000", Option.empty(), Option.empty(),
        true, Option.empty());

    assertTrue(context.getExtraMetadata().isEmpty());
    assertFalse(context.getExtraMetadata("any").isPresent());
  }

  @Test
  public void testValidationContextRecordCounts() {
    HoodieWriteStat stat1 = new HoodieWriteStat();
    stat1.setNumInserts(100);
    stat1.setNumUpdateWrites(20);
    stat1.setNumWrites(200);

    HoodieWriteStat stat2 = new HoodieWriteStat();
    stat2.setNumInserts(50);
    stat2.setNumUpdateWrites(30);
    stat2.setNumWrites(150);

    TestValidationContext context = new TestValidationContext(
        "20260309120000", Option.empty(),
        Option.of(Arrays.asList(stat1, stat2)),
        true, Option.empty());

    // getTotalRecordsWritten uses numInserts + numUpdateWrites (not numWrites)
    // stat1: 100 + 20 = 120, stat2: 50 + 30 = 80, total = 200
    assertEquals(200, context.getTotalRecordsWritten());
    assertEquals(150, context.getTotalInsertRecordsWritten());
    assertEquals(50, context.getTotalUpdateRecordsWritten());
  }

  @Test
  public void testValidationContextRecordCountsEmpty() {
    TestValidationContext context = new TestValidationContext(
        "20260309120000", Option.empty(), Option.empty(),
        true, Option.empty());

    assertEquals(0, context.getTotalRecordsWritten());
    assertEquals(0, context.getTotalInsertRecordsWritten());
    assertEquals(0, context.getTotalUpdateRecordsWritten());
  }

  @Test
  public void testValidationContextIsFirstCommit() {
    TestValidationContext firstCommit = new TestValidationContext(
        "20260309120000", Option.empty(), Option.empty(),
        true, Option.empty());
    assertTrue(firstCommit.isFirstCommit());

    TestValidationContext nonFirstCommit = new TestValidationContext(
        "20260309120000", Option.empty(), Option.empty(),
        false, Option.empty());
    assertFalse(nonFirstCommit.isFirstCommit());
  }

  // ========== BasePreCommitValidator tests ==========

  @Test
  public void testBasePreCommitValidatorConfig() {
    TypedProperties config = new TypedProperties();
    config.setProperty("test.key", "test.value");

    MockOffsetValidator validator = new MockOffsetValidator(config);
    // config is a protected field accessible from subclasses
    assertNotNull(validator.config);
    assertEquals("test.value", validator.config.getString("test.key"));
  }

  @Test
  public void testBasePreCommitValidatorDefaultNoOp() {
    TypedProperties config = new TypedProperties();
    BasePreCommitValidator validator = new BasePreCommitValidator(config) {};

    TestValidationContext context = new TestValidationContext(
        "20260309120000", Option.empty(), Option.empty(),
        true, Option.empty());

    // Default implementation is no-op and should not throw
    assertDoesNotThrow(() -> validator.validateWithMetadata(context));
  }

  @Test
  public void testValidateWithMetadataOffsetDiffZeroRecordsNonZero() {
    // Covers the branch where offsetDiff == 0 but recordsWritten > 0
    MockOffsetValidator validator = createValidator(0.0, ValidationFailurePolicy.FAIL);

    HoodieCommitMetadata currentMetadata = new HoodieCommitMetadata();
    currentMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    HoodieCommitMetadata prevMetadata = new HoodieCommitMetadata();
    prevMetadata.addMetadata(CHECKPOINT_KEY, "topic,0:100,1:200");

    // Same offsets (diff=0) but records were written — should fail
    HoodieWriteStat stat = createWriteStat(100, 0);

    TestValidationContext context = new TestValidationContext(
        "20260309120000",
        Option.of(currentMetadata),
        Option.of(Collections.singletonList(stat)),
        false,
        Option.of(prevMetadata));

    assertThrows(HoodieValidationException.class,
        () -> validator.testValidateWithMetadata(context));
  }
}
