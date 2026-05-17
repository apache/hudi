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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SparkWriteErrorValidator}.
 */
public class TestSparkWriteErrorValidator {

  private static final String INSTANT = "20260520120000000";

  // ========== Helpers ==========

  private static TypedProperties failConfig() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "FAIL");
    return props;
  }

  private static TypedProperties warnConfig() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(), "WARN_LOG");
    return props;
  }

  private static HoodieWriteStat stat(String partition, long numInserts, long numUpdates, long writeErrors) {
    HoodieWriteStat s = new HoodieWriteStat();
    s.setPartitionPath(partition);
    s.setNumInserts(numInserts);
    s.setNumUpdateWrites(numUpdates);
    s.setTotalWriteErrors(writeErrors);
    return s;
  }

  private static SparkValidationContext context(List<HoodieWriteStat> writeStats) {
    return new SparkValidationContext(
        INSTANT,
        Option.of(new HoodieCommitMetadata()),
        Option.of(writeStats),
        Option.empty());
  }

  // ========== Tests ==========

  @Test
  public void testNoErrorsPasses() {
    // 1000 records written, no errors -> passes
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 1000, 0, 0)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testErrorsWithFailPolicyThrows() {
    // 500 written + 50 errors -> fails under FAIL policy (mirrors commitOnErrors=false)
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 500, 0, 50)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> validator.validateWithMetadata(ctx));
    assertTrue(ex.getMessage().contains("Errors: 50"), "message should report error count");
    assertTrue(ex.getMessage().contains("Total: 550"), "message should report total record count");
  }

  @Test
  public void testErrorsWithWarnPolicyDoesNotThrow() {
    // 500 written + 50 errors -> passes under WARN_LOG (mirrors commitOnErrors=true)
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 500, 0, 50)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(warnConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testEmptyCommitPasses() {
    // 0 records, 0 errors -> empty commit, validation is skipped
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 0, 0, 0)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testNoWriteStatsTreatedAsEmpty() {
    SparkValidationContext ctx = new SparkValidationContext(
        INSTANT,
        Option.of(new HoodieCommitMetadata()),
        Option.empty(),
        Option.empty());

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    assertDoesNotThrow(() -> validator.validateWithMetadata(ctx));
  }

  @Test
  public void testErrorsAcrossMultiplePartitionsAreSummed() {
    // p1: 100 written + 5 errors. p2: 200 written + 10 errors. Total errors > 0 -> fail.
    SparkValidationContext ctx = context(Arrays.asList(
        stat("p1", 100, 0, 5),
        stat("p2", 200, 0, 10)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> validator.validateWithMetadata(ctx));
    assertTrue(ex.getMessage().contains("Errors: 15"),
        "errors should be summed across partitions, got: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("Total: 315"),
        "total should be inserts + updates + errors across partitions, got: " + ex.getMessage());
  }

  @Test
  public void testUpdatesCountedTowardTotal() {
    // 0 inserts, 100 updates, 1 error -> 1/101 -> fail under FAIL
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 0, 100, 1)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(failConfig());
    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> validator.validateWithMetadata(ctx));
    assertTrue(ex.getMessage().contains("Total: 101"),
        "updates should count toward total, got: " + ex.getMessage());
  }

  @Test
  public void testDefaultPolicyIsFail() {
    // No failure.policy set -> default is FAIL
    TypedProperties props = new TypedProperties();
    SparkValidationContext ctx = context(Collections.singletonList(stat("p1", 10, 0, 1)));

    SparkWriteErrorValidator validator = new SparkWriteErrorValidator(props);
    assertThrows(HoodieValidationException.class, () -> validator.validateWithMetadata(ctx));
  }
}
