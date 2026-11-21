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

package org.apache.hudi.client.common;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class TestJavaParallelTaskRunner {

  private static final long WAIT_SECONDS = 30;
  private static final Runnable NOTHING = () -> { };

  private static String uniquePrefix() {
    return "runner-test-" + UUID.randomUUID();
  }

  private static List<Thread> liveThreads(String prefix) {
    List<Thread> threads = Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix)).collect(Collectors.toList());
    for (Thread thread : threads) {
      try {
        thread.join(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return threads.stream().filter(Thread::isAlive).collect(Collectors.toList());
  }

  private static List<Integer> range(int n) {
    return IntStream.range(0, n).boxed().collect(Collectors.toList());
  }

  @Test
  void testResultsKeepInputOrderWhenTasksCompleteOutOfOrder() throws Exception {
    List<Integer> items = range(8);
    CountDownLatch lastItemDone = new CountDownLatch(1);
    List<Integer> results = JavaParallelTaskRunner.mapInOrder(items, i -> {
      if (i == items.size() - 1) {
        lastItemDone.countDown();
      } else if (i == 0) {
        await(lastItemDone);
      }
      return i * 10;
    }, 8, uniquePrefix());
    assertEquals(items.stream().map(i -> i * 10).collect(Collectors.toList()), results);
  }

  @Test
  void testEmptyAndSingleItemRunOnTheCallingThread() {
    String prefix = uniquePrefix();
    Thread caller = Thread.currentThread();
    assertEquals(Collections.emptyList(), JavaParallelTaskRunner.mapInOrder(Collections.<Integer>emptyList(), i -> i, 4, prefix));
    AtomicReference<Thread> ranOn = new AtomicReference<>();
    assertEquals(Collections.singletonList(7), JavaParallelTaskRunner.mapInOrder(Collections.singletonList(7), i -> {
      ranOn.set(Thread.currentThread());
      return i;
    }, 4, prefix));
    assertSame(caller, ranOn.get());
  }

  @Test
  void testParallelismOneRunsOnTheCallingThread() {
    Set<Thread> threads = ConcurrentHashMap.newKeySet();
    List<Integer> results = JavaParallelTaskRunner.mapInOrder(range(5), i -> {
      threads.add(Thread.currentThread());
      return i;
    }, 1, uniquePrefix());
    assertEquals(range(5), results);
    assertEquals(Collections.singleton(Thread.currentThread()), threads);
  }

  @Test
  void testFewerItemsThanParallelismUseOneThreadPerItem() {
    String prefix = uniquePrefix();
    int items = 3;
    CyclicBarrier allRunning = new CyclicBarrier(items);
    Set<String> threadNames = ConcurrentHashMap.newKeySet();
    JavaParallelTaskRunner.mapInOrder(range(items), i -> {
      threadNames.add(Thread.currentThread().getName());
      await(allRunning);
      return i;
    }, 16, prefix);
    assertEquals(items, threadNames.size());
    assertTrue(threadNames.stream().allMatch(name -> name.startsWith(prefix)));
  }

  @Test
  void testRunsAtMostParallelismTasksAtOnce() {
    int parallelism = 4;
    AtomicInteger active = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    CyclicBarrier firstWave = new CyclicBarrier(parallelism);
    AtomicInteger started = new AtomicInteger();
    JavaParallelTaskRunner.mapInOrder(range(parallelism * 5 + 1), i -> {
      int now = active.incrementAndGet();
      peak.accumulateAndGet(now, Math::max);
      if (started.getAndIncrement() < parallelism) {
        await(firstWave);
      }
      active.decrementAndGet();
      return i;
    }, parallelism, uniquePrefix());
    assertEquals(parallelism, peak.get());
  }

  @Test
  void testNoItemStartsAfterAFailureAndRunningItemsFinishFirst() throws Exception {
    String prefix = uniquePrefix();
    CountDownLatch slowItemStarted = new CountDownLatch(1);
    CountDownLatch stopRecorded = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Set<Integer> started = ConcurrentHashMap.newKeySet();
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    RuntimeException failure = new RuntimeException("bucket failed");
    Function<Integer, Integer> task = i -> {
      started.add(i);
      if (i == 0) {
        slowItemStarted.countDown();
        await(stopRecorded);
        await(release);
        events.add("item 0 finished");
        return i;
      }
      if (i == 1) {
        await(slowItemStarted);
        throw failure;
      }
      return i;
    };
    AtomicReference<Throwable> outcome = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        JavaParallelTaskRunner.mapInOrder(range(20), task, 2, prefix, daemonThreads(prefix), NOTHING, stopRecorded::countDown);
      } catch (Throwable t) {
        outcome.set(t);
      }
      events.add("call returned");
    });
    try {
      caller.start();
      await(stopRecorded);
      assertTrue(caller.isAlive(), "The call waits for the item still running when the failure was recorded");
    } finally {
      release.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    }
    assertFalse(caller.isAlive());
    assertSame(failure, outcome.get());
    assertEquals(Arrays.asList("item 0 finished", "call returned"), events);
    assertEquals(new HashSet<>(Arrays.asList(0, 1)), started, "No item starts after the failure");
    assertTrue(liveThreads(prefix).isEmpty());
  }

  @Test
  void testAThreadAboutToTakeAnItemTakesNoneOnceAFailureIsRecorded() {
    String prefix = uniquePrefix();
    AtomicInteger takes = new AtomicInteger();
    CountDownLatch secondThreadAboutToTake = new CountDownLatch(1);
    CountDownLatch stopRecorded = new CountDownLatch(1);
    Runnable beforeTake = () -> {
      if (takes.incrementAndGet() == 2) {
        secondThreadAboutToTake.countDown();
        await(stopRecorded);
      }
    };
    Set<Integer> started = ConcurrentHashMap.newKeySet();
    RuntimeException failure = new RuntimeException("first item failed");
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> JavaParallelTaskRunner.mapInOrder(range(10), i -> {
      started.add(i);
      if (i == 0) {
        await(secondThreadAboutToTake);
        throw failure;
      }
      return i;
    }, 2, prefix, daemonThreads(prefix), beforeTake, stopRecorded::countDown));
    assertSame(failure, thrown);
    assertEquals(Collections.singleton(0), started, "No item is taken once the failure is recorded");
    assertTrue(liveThreads(prefix).isEmpty());
  }

  @Test
  void testFailuresAreThrownAsOnePrimaryWithTheOthersSuppressed() {
    int parallelism = 3;
    CyclicBarrier allFailing = new CyclicBarrier(parallelism);
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> JavaParallelTaskRunner.mapInOrder(range(parallelism), i -> {
      await(allFailing);
      throw new IllegalStateException("failure " + i);
    }, parallelism, uniquePrefix()));
    Set<String> messages = new HashSet<>();
    messages.add(thrown.getMessage());
    Arrays.stream(thrown.getSuppressed()).forEach(t -> messages.add(t.getMessage()));
    assertEquals(new HashSet<>(Arrays.asList("failure 0", "failure 1", "failure 2")), messages);
    assertEquals(parallelism - 1, thrown.getSuppressed().length);
  }

  @Test
  void testTheSameFailureThrownByTwoTasksIsThrownUnchanged() {
    CyclicBarrier bothFailing = new CyclicBarrier(2);
    RuntimeException shared = new IllegalStateException("shared failure");
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> JavaParallelTaskRunner.mapInOrder(range(2), i -> {
      await(bothFailing);
      throw shared;
    }, 2, uniquePrefix()));
    assertSame(shared, thrown);
    assertEquals(0, thrown.getSuppressed().length);
  }

  @Test
  void testErrorsPassThroughUnwrapped() {
    AssertionError error = new AssertionError("fatal");
    AssertionError thrown = assertThrows(AssertionError.class, () -> JavaParallelTaskRunner.mapInOrder(range(4), i -> {
      if (i == 2) {
        throw error;
      }
      return i;
    }, 2, uniquePrefix()));
    assertSame(error, thrown);
  }

  @Test
  void testCheckedFailuresArriveWrappedInHoodieException() {
    IOException checked = new IOException("io failure");
    HoodieException thrown = assertThrows(HoodieException.class, () -> JavaParallelTaskRunner.mapInOrder(range(4), i -> {
      if (i == 3) {
        sneakyThrow(checked);
      }
      return i;
    }, 2, uniquePrefix()));
    assertSame(checked, thrown.getCause());
  }

  @Test
  void testCallerInterruptWaitsForRunningItemsAndKeepsTheInterruptFlag() throws Exception {
    String prefix = uniquePrefix();
    CountDownLatch bothRunning = new CountDownLatch(2);
    CountDownLatch stopRecorded = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger finished = new AtomicInteger();
    Set<Integer> started = ConcurrentHashMap.newKeySet();
    AtomicReference<Throwable> outcome = new AtomicReference<>();
    AtomicReference<Boolean> interruptFlag = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        JavaParallelTaskRunner.mapInOrder(range(10), i -> {
          started.add(i);
          bothRunning.countDown();
          await(release);
          finished.incrementAndGet();
          return i;
        }, 2, prefix, daemonThreads(prefix), NOTHING, stopRecorded::countDown);
      } catch (Throwable t) {
        outcome.set(t);
      }
      interruptFlag.set(Thread.currentThread().isInterrupted());
    });
    try {
      caller.start();
      await(bothRunning);
      caller.interrupt();
      await(stopRecorded);
      assertTrue(caller.isAlive(), "The caller waits for the running items");
    } finally {
      release.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    }
    assertFalse(caller.isAlive());
    assertInstanceOf(HoodieException.class, outcome.get());
    assertTrue(interruptFlag.get());
    assertEquals(2, finished.get());
    assertEquals(new HashSet<>(Arrays.asList(0, 1)), started);
    assertTrue(liveThreads(prefix).isEmpty());
  }

  @Test
  void testASubmissionFailureWaitsForRunningItemsAndKeepsTheInterruptFlag() throws Exception {
    String prefix = uniquePrefix();
    IllegalStateException cannotCreateThread = new IllegalStateException("cannot create another thread");
    CountDownLatch itemRunning = new CountDownLatch(1);
    AtomicInteger created = new AtomicInteger();
    ThreadFactory secondThreadFails = runnable -> {
      if (created.incrementAndGet() > 1) {
        await(itemRunning);
        throw cannotCreateThread;
      }
      Thread thread = new Thread(runnable, prefix + "-1");
      thread.setDaemon(true);
      return thread;
    };
    AtomicInteger stops = new AtomicInteger();
    CountDownLatch submissionFailureRecorded = new CountDownLatch(1);
    CountDownLatch interruptRecorded = new CountDownLatch(1);
    Runnable afterStopRequest = () -> (stops.incrementAndGet() == 1 ? submissionFailureRecorded : interruptRecorded).countDown();
    CountDownLatch release = new CountDownLatch(1);
    Set<Integer> started = ConcurrentHashMap.newKeySet();
    AtomicReference<Throwable> outcome = new AtomicReference<>();
    AtomicReference<Boolean> interruptFlag = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        JavaParallelTaskRunner.mapInOrder(range(6), i -> {
          started.add(i);
          itemRunning.countDown();
          await(release);
          return i;
        }, 3, prefix, secondThreadFails, NOTHING, afterStopRequest);
      } catch (Throwable t) {
        outcome.set(t);
      }
      interruptFlag.set(Thread.currentThread().isInterrupted());
    });
    try {
      caller.start();
      await(submissionFailureRecorded);
      caller.interrupt();
      await(interruptRecorded);
      assertTrue(caller.isAlive(), "The caller waits for the running item after the submission failure");
    } finally {
      release.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    }
    assertFalse(caller.isAlive());
    assertSame(cannotCreateThread, outcome.get());
    assertTrue(interruptFlag.get(), "The interrupt flag is kept when the submission fails");
    assertEquals(Collections.singleton(0), started);
    assertTrue(liveThreads(prefix).isEmpty());
  }

  @Test
  void testASubmissionFailureIsThrownWithTheTaskFailuresSuppressed() {
    String prefix = uniquePrefix();
    RuntimeException taskFailure = new RuntimeException("first item failed");
    IllegalStateException cannotCreateThread = new IllegalStateException("cannot create another thread");
    CountDownLatch taskFailureRecorded = new CountDownLatch(1);
    AtomicInteger created = new AtomicInteger();
    ThreadFactory secondThreadFailsAfterTheTaskFailure = runnable -> {
      if (created.incrementAndGet() > 1) {
        await(taskFailureRecorded);
        throw cannotCreateThread;
      }
      Thread thread = new Thread(runnable, prefix + "-1");
      thread.setDaemon(true);
      return thread;
    };
    Set<Integer> started = ConcurrentHashMap.newKeySet();
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> JavaParallelTaskRunner.mapInOrder(range(3), i -> {
      started.add(i);
      throw taskFailure;
    }, 2, prefix, secondThreadFailsAfterTheTaskFailure, NOTHING, taskFailureRecorded::countDown));
    assertSame(cannotCreateThread, thrown);
    assertArrayEquals(new Throwable[] {taskFailure}, thrown.getSuppressed());
    assertEquals(Collections.singleton(0), started);
    assertTrue(liveThreads(prefix).isEmpty());
  }

  @Test
  void testWorkersUseTheCallersContextClassLoader() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader callerLoader = new URLClassLoader(new URL[0], original);
    Thread.currentThread().setContextClassLoader(callerLoader);
    try {
      List<ClassLoader> loaders = JavaParallelTaskRunner.mapInOrder(range(4),
          i -> Thread.currentThread().getContextClassLoader(), 4, uniquePrefix());
      ClassLoader[] expected = new ClassLoader[4];
      Arrays.fill(expected, callerLoader);
      assertArrayEquals(expected, loaders.toArray(new ClassLoader[0]));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testNoPoolThreadIsLeftAfterTheCall() {
    String prefix = uniquePrefix();
    List<Integer> results = JavaParallelTaskRunner.mapInOrder(range(50), i -> i + 1, 8, prefix);
    assertEquals(range(50).stream().map(i -> i + 1).collect(Collectors.toList()), results);
    assertTrue(liveThreads(prefix).isEmpty());
    assertThrows(IllegalStateException.class, () -> JavaParallelTaskRunner.mapInOrder(range(50), i -> {
      if (i == 25) {
        throw new IllegalStateException("failed");
      }
      return i;
    }, 8, prefix));
    assertTrue(liveThreads(prefix).isEmpty());
  }

  private static ThreadFactory daemonThreads(String prefix) {
    AtomicInteger created = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + created.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(WAIT_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(WAIT_SECONDS, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }
}
