/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSource#checkBoundedCommitRangeUnchanged}: a bounded read enumerates its
 * splits once at job start and reuses them on restore, so resuming a checkpoint taken under a
 * different {@code read.start-commit} / {@code read.end-commit} range would read the old range and
 * silently ignore the configured one.
 *
 * <p>Detection returns the failure message rather than throwing; it is raised from the enumerator's
 * {@code start()} — see {@code TestHoodieStaticSplitEnumerator#testStartFailsJobWhenCommitRangeChanged}.
 */
public class TestHoodieSourceCommitRangeGuard {

  private static final String START = "20260226000000";
  private static final String END = "20260227000000";

  @Test
  public void testUnchangedRangeResumes() {
    assertFalse(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.of(START), Option.of(END)), Option.of(START), Option.of(END))
        .isPresent());
  }

  @Test
  public void testChangedStartCommitReportsActionableFailure() {
    // The motivating case: a bounded read resubmitted from a retained checkpoint with a new range.
    Option<String> failure = HoodieSource.checkBoundedCommitRangeUnchanged(
        "table",
        stateWithRange(Option.of(START), Option.of(END)),
        Option.of("20260225000000"),
        Option.of(END));

    assertTrue(failure.isPresent(), "A changed start commit must be reported");
    assertTrue(failure.get().contains(START), "Message should show the checkpoint range");
    assertTrue(failure.get().contains("20260225000000"), "Message should show the configured range");
    assertTrue(failure.get().contains("checkpoint directory"),
        "Message should explain how to run the new range");
  }

  @Test
  public void testChangedEndCommitReportsFailure() {
    assertTrue(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table",
        stateWithRange(Option.of(START), Option.of(END)),
        Option.of(START),
        Option.of("20260228000000"))
        .isPresent());
  }

  @Test
  public void testNewlyConfiguredBoundReportsFailure() {
    // The checkpoint recorded the range as unset (empty string, not an absent Option). Adding a
    // start commit afterwards is still a range change and must be caught.
    assertTrue(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table",
        stateWithRange(Option.of(""), Option.of("")),
        Option.of(START),
        Option.of(""))
        .isPresent());
  }

  @Test
  public void testUnsetRangeOnBothSidesResumes() {
    // A bounded snapshot read configures neither bound; both sides record the empty string and match.
    assertEquals(Option.empty(), HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.of(""), Option.of("")), Option.of(""), Option.of("")));
  }

  @Test
  public void testLegacyCheckpointWithoutRangeResumes() {
    // Checkpoints written by serializer VERSION 1 carry no range at all, so nothing can be verified.
    // They must still restore, otherwise the guard would break every existing bounded job.
    assertFalse(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.empty(), Option.empty()), Option.of(START), Option.of(END))
        .isPresent());
  }

  @Test
  public void testEarliestSentinelIsMatchedCaseInsensitively() {
    // OptionsResolver matches the `earliest` sentinel with equalsIgnoreCase everywhere else, so
    // `earliest` and `EARLIEST` select the same data. Failing the job over the difference in case
    // would be a false positive that no restart can clear.
    assertFalse(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.of("earliest"), Option.of("")), Option.of("EARLIEST"), Option.of(""))
        .isPresent());
  }

  @Test
  public void testSurroundingWhitespaceDoesNotCountAsAChange() {
    assertFalse(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.of(START), Option.of("")), Option.of(" " + START + " "), Option.of(""))
        .isPresent());
  }

  @Test
  public void testEarliestToExplicitCommitIsStillAChange() {
    // Normalization must not blunt the guard: `earliest` and a real instant are different scopes.
    assertTrue(HoodieSource.checkBoundedCommitRangeUnchanged(
        "table", stateWithRange(Option.of("earliest"), Option.of("")), Option.of(START), Option.of(""))
        .isPresent());
  }

  private static HoodieSplitEnumeratorState stateWithRange(
      Option<String> readStartCommit, Option<String> readEndCommit) {
    return new HoodieSplitEnumeratorState(
        Collections.emptyList(), Option.empty(), Option.empty(), readStartCommit, readEndCommit);
  }
}
