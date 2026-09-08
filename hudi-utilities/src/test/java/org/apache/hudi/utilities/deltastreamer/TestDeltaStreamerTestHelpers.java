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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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

  /**
   * Slow enough that the poll spends almost all of its time asleep, so a directly delivered interrupt lands in
   * the sleep rather than in the condition.
   */
  private static final long SLOW_POLL_INTERVAL_MS = 500;

  /** Generous, so that "ended well before the timeout" is an unambiguous signal rather than a near miss. */
  private static final int INTERRUPT_TIMEOUT_SECS = 10;

  /** The stop bound the stop-path cases drive, instead of the production 60s. */
  private static final long FAST_STOP_TIMEOUT_SECS = 1;

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
    assertEquals(1, error.getSuppressed().length,
        () -> "the timeout should stay attached as a suppressed exception once the condition's error becomes "
            + "the cause, but the suppressed list was: " + Arrays.toString(error.getSuppressed()));
    assertInstanceOf(TimeoutException.class, error.getSuppressed()[0],
        "the suppressed exception should be the timeout the wait gave up on");
  }

  /**
   * Pins that the worker actually stops once the wait has given up, whichever of the two guards fires. The
   * {@code InterruptedException} branch on its own is covered by
   * {@link #directInterruptEndsTheWaitWithoutRunningToTheTimeout()}, since the {@code executor.isShutdown()}
   * guard would stop the worker here even without it.
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
                Thread.sleep(TimeUnit.SECONDS.toMillis(60));
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
              return true;
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    assertTrue(error.getMessage().contains("No evaluation of the condition completed"),
        () -> "a condition still running its first evaluation should be reported as such, but was: "
            + error.getMessage());
    assertInstanceOf(TimeoutException.class, error.getCause(),
        "with no error recorded the timeout itself should be the cause");
    assertNull(error.getCause().getMessage(),
        "the cause is a message-less TimeoutException, which is the shape that made the null check in "
            + "JavaTestUtils.checkNestedExceptionContains necessary");
  }

  /**
   * The branch for a condition that swallows its own failure and returns false, as {@code testHoodieIndexer}
   * does. The HUDI-6843 condition is not one of those: it only ever throws, so a real timeout there takes the
   * last-error branch instead. This one has to say how many evaluations ran, since that is the only signal
   * separating it from a condition that never completed one.
   */
  @Test
  void timeoutReportsEvaluationsThatReturnedFalse() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> waitTillCondition(ignored -> false, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    assertTrue(error.getMessage().contains("returned false without throwing"),
        () -> "a condition that kept returning false should be reported as such, but was: " + error.getMessage());
    assertFalse(error.getMessage().contains("0 evaluations completed"),
        () -> "the count should be the real number of evaluations, not a constant, but was: "
            + error.getMessage());
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
   * Not deterministically reproducible through the helper: it reads the evaluation counter before the last
   * error, so it can see a recorded error alongside a count of zero, but only on an interleaving a test cannot
   * force. Pinned here by calling the report builder directly.
   */
  @Test
  void describeTimeoutReportsAnErrorEvenWithNoCompletedEvaluation() {
    String message = describeTimeout(new AssertionError("boom"), 0, CONDITION_TIMEOUT_SECS);

    assertTrue(message.contains("boom"),
        () -> "an error recorded before the counter caught up should still be reported, but was: " + message);
    assertFalse(message.contains("No evaluation of the condition completed"),
        () -> "a recorded error should not be reported as no evaluation having completed, but was: " + message);
  }

  /**
   * The {@code InterruptedException} branch on its own: an interrupt delivered while the poll is sleeping has
   * to end the wait, rather than be recorded as a condition failure and polled through. A catch-all around the
   * sleep would clear the flag and keep polling until the timeout, which is what this discriminates.
   *
   * <p>The slow poll makes the sleep the overwhelmingly likely place for the interrupt to land. Landing outside
   * it is also a pass, since the loop guard then ends the wait, so this cannot flake either way.
   */
  @Test
  void directInterruptEndsTheWaitWithoutRunningToTheTimeout() throws Exception {
    AtomicReference<Thread> poller = new AtomicReference<>();
    CountDownLatch polling = new CountDownLatch(1);
    Thread interrupter = new Thread(() -> {
      try {
        if (polling.await(5, TimeUnit.SECONDS)) {
          poller.get().interrupt();
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    });
    interrupter.start();

    long startedAt = System.nanoTime();
    assertDoesNotThrow(() -> waitTillCondition(
        ignored -> {
          poller.set(Thread.currentThread());
          polling.countDown();
          return false;
        }, RUNNING, INTERRUPT_TIMEOUT_SECS, SLOW_POLL_INTERVAL_MS));
    long elapsedSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedAt);
    interrupter.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(elapsedSecs < INTERRUPT_TIMEOUT_SECS,
        () -> "the interrupt should have ended the wait well before the timeout, but it took "
            + elapsedSecs + "s of " + INTERRUPT_TIMEOUT_SECS + "s");
  }

  /**
   * A stop that throws does not excuse leaving the ingest task running: the wait falls through to the join,
   * which times out, and the task is force-stopped and cancelled rather than left reading into the next test.
   */
  @Test
  void stopThatThrowsStillCancelsTheIngestTask() throws Exception {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    Mockito.doThrow(new IllegalStateException("stop blew up")).when(ds).shutdownGracefully();

    ExecutorService ingest = Executors.newSingleThreadExecutor();
    try {
      Future<?> dsFuture = ingest.submit(() -> {
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        return null;
      });

      TestHoodieDeltaStreamer.stopLeakedStreamer(ds, dsFuture, FAST_STOP_TIMEOUT_SECS);

      assertTrue(dsFuture.isCancelled(),
          "a stop that threw should still leave the ingest task cancelled, since that leak is what the "
              + "helper exists to close");
    } finally {
      ingest.shutdownNow();
    }
  }

  /**
   * The same outcome when the stop hangs instead of throwing, which is the case the bound exists for:
   * {@code shutdownGracefully} can await the ingest executor for up to 24 hours.
   */
  @Test
  void stopThatHangsIsBoundedAndCancelsTheIngestTask() throws Exception {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    Mockito.doAnswer(invocation -> {
      Thread.sleep(TimeUnit.MINUTES.toMillis(10));
      return null;
    }).when(ds).shutdownGracefully();

    ExecutorService ingest = Executors.newSingleThreadExecutor();
    try {
      Future<?> dsFuture = ingest.submit(() -> {
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        return null;
      });

      TestHoodieDeltaStreamer.stopLeakedStreamer(ds, dsFuture, FAST_STOP_TIMEOUT_SECS);

      assertTrue(dsFuture.isCancelled(),
          "a stop that hung past its bound should still leave the ingest task cancelled");
    } finally {
      ingest.shutdownNow();
    }
  }
}
