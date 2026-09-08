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

import org.apache.hudi.common.testutils.JavaTestUtils;
import org.apache.hudi.utilities.streamer.NoNewDataTerminationStrategy;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the deltastreamer test helpers every continuous-mode test runs on: the wait in
 * {@code HoodieDeltaStreamerTestBase.TestHelpers} and the runner in {@code TestHoodieDeltaStreamer}.
 *
 * <p>The wait used to fail with a bare {@code TimeoutException} naming only the helper, with the
 * condition's own error logged at debug and discarded, so a timeout said nothing about which assertion
 * never held (HUDI-6843).
 */
class TestDeltaStreamerTestHelpers {

  /** A deltastreamer future that never finishes, as a continuous-mode job would be. */
  private static final Future<?> RUNNING = new CompletableFuture<>();

  /**
   * The helper polls every 2s, so the timeout has to leave room for at least one evaluation to be recorded.
   * 5s is enough for that, and keeps the tests in this class from spending half a minute asleep in the
   * shared utilities job.
   */
  private static final int CONDITION_TIMEOUT_SECS = 5;

  /** For the cases that are not meant to time out: they finish long before this, so it is never reached. */
  private static final int NEVER_REACHED_TIMEOUT_SECS = 30;

  @Test
  void timeoutFailureNamesTheLastConditionFailure() {
    String assertionText = "assertAtleastNDeltaCommits: expected at least 3 delta commits but got 2";

    AssertionError error = assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
            ignored -> {
              throw new AssertionError(assertionText);
            }, RUNNING, CONDITION_TIMEOUT_SECS));

    assertTrue(error.getMessage().contains("was not met within " + CONDITION_TIMEOUT_SECS + " seconds"),
        () -> "The failure should say the condition timed out, but was: " + error.getMessage());
    assertTrue(error.getMessage().contains(assertionText),
        () -> "The failure should carry the condition's own error, which is the only clue to why the "
            + "wait timed out, but was: " + error.getMessage());
    assertTrue(error.getMessage().contains("evaluations completed"),
        () -> "The failure should say how many evaluations completed, which separates a condition that "
            + "kept failing from one that never finished an evaluation, but was: " + error.getMessage());
  }

  /**
   * {@code shutdownNow} interrupts the polling thread, but {@code Thread.sleep} clears the interrupt flag
   * when it throws, so a catch-all around the sleep would swallow it and keep polling for the life of the
   * JVM. This pins that the worker actually stops.
   */
  @Test
  void pollingStopsOnceTheWaitHasGivenUp() throws Exception {
    AtomicInteger polls = new AtomicInteger();

    assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
            ignored -> {
              polls.incrementAndGet();
              throw new AssertionError("never true");
            }, RUNNING, CONDITION_TIMEOUT_SECS));

    int pollsWhenItGaveUp = polls.get();
    assertTrue(pollsWhenItGaveUp > 0,
        "the condition should have been evaluated at least once before the wait gave up, otherwise the "
            + "comparison below passes trivially");
    Thread.sleep(2 * HoodieDeltaStreamerTestBase.TestHelpers.POLL_INTERVAL_MS);
    assertEquals(pollsWhenItGaveUp, polls.get(),
        "the polling thread should have stopped when the wait gave up, not carried on in the background");
  }

  /**
   * A condition that hangs part-way through its first evaluation is a different failure from one that keeps
   * returning false, and the report has to say which: with no completed evaluation there is no last error,
   * and claiming the condition "returned false without throwing" would assert the wrong thing.
   */
  @Test
  void timeoutDistinguishesAConditionThatNeverCompletedAnEvaluation() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
            ignored -> {
              try {
                Thread.sleep(60_000);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
              return true;
            }, RUNNING, CONDITION_TIMEOUT_SECS));

    assertTrue(error.getMessage().contains("No evaluation of the condition completed"),
        () -> "a condition still running its first evaluation should be reported as such, but was: "
            + error.getMessage());
    assertFalse(JavaTestUtils.checkNestedExceptionContains(error, "no such text"),
        "walking the cause chain has to tolerate the null-message TimeoutException this path attaches, "
            + "which is what the multi-writer test hits when its ingestion wait times out");
  }

  /**
   * Conditions in the continuous-mode tests catch their own failures and return false rather than throwing,
   * so this is the branch a real timeout reports. It has to say how many evaluations ran, since that is the
   * only signal separating it from a condition that never completed one.
   */
  @Test
  void timeoutReportsEvaluationsThatReturnedFalse() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
            ignored -> false, RUNNING, CONDITION_TIMEOUT_SECS));

    assertTrue(error.getMessage().contains("returned false without throwing"),
        () -> "a condition that kept returning false should be reported as such, but was: " + error.getMessage());
  }

  /**
   * The bound exists so a hung poll cannot run for the life of the JVM. Both production callers of waitFor
   * are currently disabled (HUDI-8951), so this is the only thing exercising it.
   */
  @Test
  void waitForGivesUpAtItsBound() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> HoodieDeltaStreamerTestBase.TestHelpers.waitFor(() -> false, 1));

    assertTrue(error.getMessage().contains("did not hold within 1 seconds"),
        () -> "the bound should name itself in the failure, but was: " + error.getMessage());
  }

  /**
   * When a streamer configured with a post-write termination strategy dies, the wait returns because the
   * future is done, and {@code deltaStreamerTestRunner} has to surface that failure. Without the
   * {@code dsFuture.isDone()} guard it would instead call {@code awaitDeltaStreamerShutdown} and report the
   * misleading "Deltastreamer should have shutdown by now" two minutes later - here, on a mock with no
   * ingestion service, it would NPE.
   */
  @Test
  void dyingStreamerWithTerminationStrategyIsSurfacedNotWaitedOut() throws Exception {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    Mockito.doThrow(new IllegalStateException("source is unreachable")).when(ds).sync();
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    cfg.postWriteTerminationStrategyClass = NoNewDataTerminationStrategy.class.getName();

    ExecutionException failure = assertThrows(ExecutionException.class,
        () -> TestHoodieDeltaStreamer.deltaStreamerTestRunner(ds, cfg, ignored -> false, "dying_ds_job"));

    assertTrue(JavaTestUtils.checkNestedExceptionContains(failure, "source is unreachable"),
        () -> "the streamer's own failure should be surfaced, but was: " + failure);
  }

  @Test
  void satisfiedConditionReturnsNormally() {
    assertDoesNotThrow(() -> HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition(
        ignored -> true, RUNNING, NEVER_REACHED_TIMEOUT_SECS));
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
        ignored -> false, finished, NEVER_REACHED_TIMEOUT_SECS));
  }
}
