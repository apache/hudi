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
import org.apache.hudi.utilities.ingestion.HoodieIngestionService;
import org.apache.hudi.utilities.streamer.NoNewDataTerminationStrategy;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.describeTimeout;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.waitFor;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase.TestHelpers.waitTillCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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

  /**
   * Generous, so a catch-all that polled through the interrupt fails by throwing at the timeout rather than
   * by a near miss.
   */
  private static final int INTERRUPT_TIMEOUT_SECS = 10;

  /** The stop bound the stop-path cases drive, instead of the production one. */
  private static final long FAST_STOP_TIMEOUT_SECS = 1;

  /**
   * At most two of the stop's bounds can run in sequence (a stop that times out skips the join, and a stop that
   * returns leaves nothing for the close-wait), so two seconds of this is bound and the rest is slack. A stop that
   * lost its bound would take the ten minutes the mock sleeps.
   */
  private static final long STOP_PATH_CEILING_SECS = 5;

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

    assertTrue(polls.get() > 0,
        "the condition should have been entered at least once, otherwise there is no polling thread to join");
    poller.get().join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(poller.get().isAlive(),
        "the isShutdown() guard should have stopped the polling thread even though the condition swallowed "
            + "the interrupt without restoring the flag");
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
    AtomicInteger polls = new AtomicInteger();

    AssertionError error = assertThrows(AssertionError.class,
        () -> waitTillCondition(
            ignored -> {
              polls.incrementAndGet();
              return false;
            }, RUNNING, CONDITION_TIMEOUT_SECS, FAST_POLL_INTERVAL_MS));

    assertTrue(error.getMessage().contains("returned false without throwing"),
        () -> "a condition that kept returning false should be reported as such, but was: " + error.getMessage());
    Matcher count = Pattern.compile("(\\d+) evaluations completed").matcher(error.getMessage());
    assertTrue(count.find(),
        () -> "the report should carry an evaluation count, but was: " + error.getMessage());
    // The worker records an evaluation only after the condition returns, so at most one can be in flight when
    // the timeout reads the counter, and none can follow it because the loop then sees the shutdown. The tally
    // the condition kept is therefore the reported count or exactly one more.
    int reported = Integer.parseInt(count.group(1));
    int observed = polls.get();
    assertTrue(reported >= 1 && reported <= observed && observed - reported <= 1,
        () -> "the count should be the real number of completed evaluations, but the report said " + reported
            + " while the condition ran " + observed + " times: " + error.getMessage());
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
    AtomicBoolean interruptSent = new AtomicBoolean();
    Thread interrupter = new Thread(() -> {
      try {
        if (polling.await(5, TimeUnit.SECONDS)) {
          poller.get().interrupt();
          interruptSent.set(true);
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    });
    interrupter.start();

    assertDoesNotThrow(() -> waitTillCondition(
        ignored -> {
          poller.set(Thread.currentThread());
          polling.countDown();
          return false;
        }, RUNNING, INTERRUPT_TIMEOUT_SECS, SLOW_POLL_INTERVAL_MS));
    interrupter.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(interruptSent.get(),
        "the interrupt was never sent because the first poll did not complete within 5s, so this run says "
            + "nothing about the interrupt path");
  }

  /**
   * A stop that throws does not excuse leaving the ingest task running: the wait falls through to the join,
   * which times out, and the task is force-stopped and cancelled rather than left reading into the next test.
   */
  @Test
  void stopThatThrowsStillCancelsTheIngestTask() {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    Mockito.doThrow(new IllegalStateException("stop blew up")).when(ds).shutdownGracefully();

    assertStopEndsWithTheIngestTaskCancelled(ds);
  }

  /**
   * The same outcome when the stop hangs instead of throwing, which is the case the bound exists for:
   * {@code shutdownGracefully} can await the ingest executor for up to 24 hours. The preemptive ceiling in the
   * helper is what pins the bound here.
   */
  @Test
  void stopThatHangsIsBoundedAndCancelsTheIngestTask() {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    Mockito.doAnswer(invocation -> {
      Thread.sleep(TimeUnit.MINUTES.toMillis(10));
      return null;
    }).when(ds).shutdownGracefully();

    assertStopEndsWithTheIngestTaskCancelled(ds);
  }

  /**
   * The branch the bound exists for. When the stop is still running after its bound, the ingestion service is
   * force-stopped and the ingest task cancelled, and the close-wait gives up after its own bound and lets the
   * stop run on rather than block the next test.
   */
  @Test
  void stopThatOutlivesItsBoundIsForceStoppedAndLetRunOn() {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    HoodieIngestionService service = Mockito.mock(HoodieIngestionService.class);
    Mockito.when(ds.getIngestionService()).thenReturn(service);
    CountDownLatch release = new CountDownLatch(1);
    stubStopThatOutlivesTheInterrupt(ds, release);

    try {
      long startedAt = System.nanoTime();
      assertStopEndsWithTheIngestTaskCancelled(ds);
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

      Mockito.verify(service).shutdown(true);
      // The stop's own bound expires first, then the close-wait runs for its bound before giving up, so the
      // branch that lets the stop run on costs at least two bounds. A lower bound cannot flake under load.
      assertTrue(elapsedMs >= TimeUnit.SECONDS.toMillis(2 * FAST_STOP_TIMEOUT_SECS),
          () -> "the close-wait should have run for its bound after the stop's bound expired, which is the branch "
              + "that lets the stop run on, but the helper returned after " + elapsedMs + "ms");
    } finally {
      release.countDown();
    }
  }

  /**
   * A caller that arrives already interrupted must not skip the close-wait: the flag would make
   * {@code awaitTermination} throw at once. The wait runs for its bound and the interrupt is restored
   * afterwards.
   */
  @Test
  void interruptedCallerStillGetsTheBoundedCloseWait() {
    HoodieDeltaStreamer ds = Mockito.mock(HoodieDeltaStreamer.class);
    CountDownLatch release = new CountDownLatch(1);
    stubStopThatOutlivesTheInterrupt(ds, release);

    try {
      assertTimeoutPreemptively(Duration.ofSeconds(STOP_PATH_CEILING_SECS), () -> {
        Thread.currentThread().interrupt();
        long startedAt = System.nanoTime();

        TestHoodieDeltaStreamer.stopLeakedStreamer(ds, null, FAST_STOP_TIMEOUT_SECS);

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        assertTrue(Thread.interrupted(),
            "the caller's interrupt should be restored after the wait, not lost");
        assertTrue(elapsedMs >= TimeUnit.SECONDS.toMillis(FAST_STOP_TIMEOUT_SECS),
            () -> "the close-wait should have run for its bound instead of being skipped because the caller "
                + "was interrupted, but returned after " + elapsedMs + "ms");
      });
    } finally {
      release.countDown();
    }
  }

  /** Runs the stop against a ten-minute ingest task and asserts it ends, within the ceiling, with the task cancelled. */
  private static void assertStopEndsWithTheIngestTaskCancelled(HoodieDeltaStreamer ds) {
    ExecutorService ingest = Executors.newSingleThreadExecutor();
    try {
      Future<?> dsFuture = ingest.submit(() -> {
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        return null;
      });

      assertTimeoutPreemptively(Duration.ofSeconds(STOP_PATH_CEILING_SECS),
          () -> TestHoodieDeltaStreamer.stopLeakedStreamer(ds, dsFuture, FAST_STOP_TIMEOUT_SECS),
          "the stop should be bounded, not wait out the ten minutes the mock sleeps");

      assertTrue(dsFuture.isCancelled(),
          "the ingest task should have been cancelled, since that leak is what the helper exists to close");
    } finally {
      ingest.shutdownNow();
    }
  }

  /**
   * A stop that sleeps through the interrupt, as {@code HoodieAsyncService.shutdown(false)} does, so the stopper
   * thread outlives the close-wait until the test releases it.
   */
  private static void stubStopThatOutlivesTheInterrupt(HoodieDeltaStreamer ds, CountDownLatch release) {
    Mockito.doAnswer(invocation -> {
      boolean released = false;
      while (!released) {
        try {
          released = release.await(10, TimeUnit.MINUTES);
        } catch (InterruptedException swallowed) {
          // deliberately swallowed without restoring the flag: that is the shape being modelled
        }
      }
      return null;
    }).when(ds).shutdownGracefully();
  }
}
