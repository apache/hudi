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

package org.apache.hudi.utilities.util;

import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ValidationFailures#discardForRecompute}, which both dimensions' {@code run()} catch blocks
 * call to decide whether a caught failure is a finding a recompute made unattributable, or a
 * failure that must still be classified and rethrown.
 *
 * <p>This pins the ordering the whole class exists to get right: an infrastructure-classified
 * failure must win over a recompute, even when phase 1 genuinely recomputed, because a lost
 * executor is simultaneously the commonest cause of a recompute and exactly what {@link
 * ValidationFailures} exists to tell apart from real data corruption. Getting this backwards would
 * make a preempted node report success with {@code MDT_VALIDATOR_RECOMPUTE_SKIPPED} instead of failing
 * as infrastructure -- the regression this test guards against.
 */
class TestValidationFailures {

  @Test
  void infrastructureFailureIsNotDiscardedEvenWhenPhaseOneRecomputed() {
    RuntimeException executorLost =
        new RuntimeException("Job aborted due to stage failure: ExecutorLostFailure (executor 3 "
            + "exited caused by one of the running tasks) Reason: Container from a bad node");

    assertFalse(ValidationFailures.discardForRecompute(executorLost, true),
        "an infrastructure-classified failure must win over a recompute, even when phase 1 "
            + "genuinely recomputed -- it must be classified and rethrown as infrastructure, not "
            + "silently discarded as a finding the recompute made unattributable");
  }

  @Test
  void realFindingIsDiscardedWhenPhaseOneRecomputed() {
    HoodieValidationException duplicateFinding =
        new HoodieValidationException("Duplicate base file detected for file group fg-1");

    assertTrue(ValidationFailures.discardForRecompute(duplicateFinding, true),
        "a failure that does not match an infrastructure signature is exactly the case the "
            + "recompute discard exists for");
  }

  @Test
  void nothingIsDiscardedWhenPhaseOneNeverRecomputed() {
    HoodieValidationException duplicateFinding =
        new HoodieValidationException("Duplicate base file detected for file group fg-1");

    assertFalse(ValidationFailures.discardForRecompute(duplicateFinding, false),
        "with no recompute to blame, the failure must be classified and rethrown as it always "
            + "was");
  }

  @Test
  void infrastructureSignatureAnywhereInTheCauseChainStillWins() {
    RuntimeException wrapped =
        new RuntimeException("task failed", new RuntimeException("Executor heartbeat timed out"));

    assertFalse(ValidationFailures.discardForRecompute(wrapped, true),
        "matchInfraFailure searches the whole cause chain, and discardForRecompute must not narrow "
            + "that to the top-level message");
  }
}
