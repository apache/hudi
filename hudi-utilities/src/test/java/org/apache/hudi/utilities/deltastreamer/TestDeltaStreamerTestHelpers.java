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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.describeTimeout;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.waitFor;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
   * The poll interval these tests drive the helper at, so the class does not spend the production 2s cadence
   * asleep.
   */
  private static final long FAST_POLL_INTERVAL_MS = 50;

  /**
   * With the fast poll above, one second still leaves room for many evaluations to be recorded, which is what
   * the timeout report needs.
   */
  private static final int CONDITION_TIMEOUT_SECS = 1;

  /** For the cases that are not meant to time out: they finish long before this, so it is never reached. */
  private static final int NEVER_REACHED_TIMEOUT_SECS = 30;

  @Test
  void timeoutFailureNamesTheLastConditionFailure() {
    String assertionText = "assertAtleastNDeltaCommits: expected at least 3 delta commits but got 2";

    AssertionError error = assertThrows(AssertionError.class,
        () -> waitTillCondition(
            ignored -> {
              throw new AssertionError(assertionText);
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    assertTrue(error.getMessage().contains("was not met within " + CONDITION_TIMEOUT_SECS + " seconds"),
        () -> "The failure should say the condition timed out, but was: " + error.getMessage());
    assertTrue(error.getMessage().contains(assertionText),
        () -> "The failure should carry the condition's own error, which is the only clue to why the "
            + "wait timed out, but was: " + error.getMessage());
    assertFalse(error.getMessage().contains("returned false without throwing"),
        () -> "The failure should carry the condition's error, not the 'kept returning false' branch, "
            + "but was: " + error.getMessage());
    assertInstanceOf(TimeoutException.class, error.getSuppressed()[0],
        "the timeout should stay attached as a suppressed exception once the condition's error becomes the cause");
  }

  /**
   * {@code shutdownNow} interrupts the polling thread, but {@code Thread.sleep} clears the interrupt flag
   * when it throws, so a catch-all around the sleep would swallow it and keep polling for the life of the
   * JVM. This pins that the worker actually stops.
   */
  @Test
  void pollingStopsOnceTheWaitHasGivenUp() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    AtomicReference<Thread> poller = new AtomicReference<>();

    assertThrows(AssertionError.class,
        () -> waitTillCondition(
            ignored -> {
              poller.set(Thread.currentThread());
              polls.incrementAndGet();
              throw new AssertionError("never true");
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    int pollsWhenItGaveUp = polls.get();
    assertTrue(pollsWhenItGaveUp > 0,
        "the condition should have been evaluated at least once before the wait gave up, otherwise the "
            + "comparison below passes trivially");
    poller.get().join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(poller.get().isAlive(),
        "the polling thread should have exited once the wait gave up, not still be running after the join");
    assertEquals(pollsWhenItGaveUp, polls.get(),
        "the polling thread should have stopped when the wait gave up, not carried on in the background");
  }

  /**
   * The interrupt from {@code shutdownNow} is delivered once, and {@code Thread.sleep} clears the flag when it
   * throws, so a condition that swallows it without restoring it leaves the loop with no interrupt to see. The
   * {@code executor.isShutdown()} guard is what stops the worker in that case.
   */
  @Test
  void pollingStopsEvenWhenTheConditionSwallowsTheInterrupt() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    AtomicReference<Thread> poller = new AtomicReference<>();

    assertThrows(AssertionError.class,
        () -> waitTillCondition(
            ignored -> {
              poller.set(Thread.currentThread());
              polls.incrementAndGet();
              try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(60));
              } catch (InterruptedException interrupted) {
                // The missing Thread.currentThread().interrupt() is the point of the test: a condition that
                // swallows the interrupt is exactly what the isShutdown() guard exists for, so do not "fix"
                // this catch.
              }
              return false;
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    int pollsWhenItGaveUp = polls.get();
    assertTrue(pollsWhenItGaveUp > 0,
        "the condition should have been evaluated at least once before the wait gave up, otherwise the "
            + "comparison below passes trivially");
    poller.get().join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(poller.get().isAlive(),
        "the isShutdown() guard should have stopped the polling thread even though the condition swallowed "
            + "the interrupt without restoring the flag");
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
        () -> waitTillCondition(
            ignored -> {
              try {
                Thread.sleep(60_000);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
              return true;
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

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
        () -> waitTillCondition(ignored -> false, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

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
        () -> waitFor(() -> false, 1));

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
    assertDoesNotThrow(() -> waitTillCondition(
        ignored -> true, RUNNING, NEVER_REACHED_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));
  }

  /**
   * When the streamer finishes first the wait returns rather than failing, and the caller
   * ({@code deltaStreamerTestRunner}) surfaces the streamer's own outcome. Pinned so the timeout handling
   * above does not turn this into a failure.
   */
  @Test
  void finishedStreamerEndsTheWaitWithoutFailing() {
    Future<?> finished = CompletableFuture.completedFuture(null);

    assertDoesNotThrow(() -> waitTillCondition(
        ignored -> false, finished, NEVER_REACHED_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));
  }

  /**
   * Unreachable through the helper, which reads the evaluation counter before the last error and so can see a
   * recorded error alongside a count of zero; pinned here because nothing else can produce that combination.
   */
  @Test
  void describeTimeoutReportsAnErrorEvenWithNoCompletedEvaluation() {
    String message = describeTimeout(new AssertionError("boom"), 0, CONDITION_TIMEOUT_SECS);

    assertTrue(message.contains("boom"),
        () -> "an error recorded before the counter caught up should still be reported, but was: " + message);
    assertFalse(message.contains("No evaluation of the condition completed"),
        () -> "a recorded error should not be reported as no evaluation having completed, but was: " + message);
  }
}
