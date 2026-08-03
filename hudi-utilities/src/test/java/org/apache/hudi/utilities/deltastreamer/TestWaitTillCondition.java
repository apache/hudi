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

package org.apache.hudi.utilities.deltastreamer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@code HoodieDeltaStreamerTestBase.TestHelpers#waitTillCondition}, the helper every
 * continuous-mode deltastreamer test waits on.
 *
 * <p>HUDI-6843 is a flaky timeout in that wait whose only output was
 * {@code java.util.concurrent.TimeoutException} at this method, with no indication of which assertion in
 * the condition never held - the condition's error was logged at debug and discarded. That is why every
 * report of the flake looks the same and none of them is actionable.
 */
class TestWaitTillCondition {

  /** A deltastreamer future that never finishes, as a continuous-mode job would be. */
  private static final Future<?> RUNNING = new CompletableFuture<>();

  @Test
  void timeoutFailureNamesTheLastConditionFailure() {
    String assertionText = "assertAtleastNDeltaCommits: expected at least 3 delta commits but got 2";

    AssertionError error = assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
            ignored -> {
              throw new AssertionError(assertionText);
            }, RUNNING, 3));

    assertTrue(error.getMessage().contains("was not met within 3 seconds"),
        () -> "The failure should say the condition timed out, but was: " + error.getMessage());
    assertTrue(error.getMessage().contains(assertionText),
        () -> "The failure should carry the condition's own error, which is the only clue to why the "
            + "wait timed out, but was: " + error.getMessage());
  }

  @Test
  void satisfiedConditionReturnsNormally() {
    assertDoesNotThrow(() -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
        ignored -> true, RUNNING, 30));
  }

  /**
   * When the streamer finishes first the wait returns rather than failing, and the caller
   * ({@code deltaStreamerTestRunner}) surfaces the streamer's own outcome. Pinned so the timeout handling
   * above does not turn this into a failure.
   */
  @Test
  void finishedStreamerEndsTheWaitWithoutFailing() {
    Future<?> finished = CompletableFuture.completedFuture(null);

    assertDoesNotThrow(() -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
        ignored -> false, finished, 30));
  }
}
