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

package org.apache.hudi.client.transaction.lock.models;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.hudi.client.transaction.lock.models.LockProviderHeartbeatManager.DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestLockProviderHeartbeatManager {

  private ScheduledExecutorService mockScheduler;
  private Logger mockLogger;
  private ScheduledFuture<?> mockFuture;
  private HeartbeatManager manager;
  private static final String LOGGER_ID = "test-owner";
  private ScheduledExecutorService actualExecutorService;

  @BeforeEach
  void setUp() {
    mockScheduler = mock(ScheduledExecutorService.class);
    mockLogger = mock(Logger.class);
    mockFuture = mock(ScheduledFuture.class);
    actualExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "Heartbeat-Test-Thread");
      t.setDaemon(true);
      return t;
    });
  }

  @AfterEach
  void tearDown() throws Exception {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    actualExecutorService.shutdownNow();
  }

  @Test
  void testStartHeartbeatSuccess() {
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), eq(100L), eq(100L), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> mockFuture);
    manager = createDefaultManagerWithMocks(() -> true);
    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
  }

  @Test
  void testStartHeartbeatAlreadyRunning() {
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> mockFuture);

    manager = createDefaultManagerWithMocks(() -> true);

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    assertFalse(manager.startHeartbeatForThread(Thread.currentThread()));
    verify(mockLogger).warn("Owner {}: Heartbeat is already running.", LOGGER_ID);
  }

  @Test
  void testStartHeartbeatSchedulerException() {
    doThrow(new RejectedExecutionException("Scheduler failure"))
            .when(mockScheduler)
            .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

    manager = createDefaultManagerWithMocks(() -> true);

    assertFalse(manager.startHeartbeatForThread(Thread.currentThread()));
    verify(mockLogger).error(eq("Owner {}: Unable to schedule heartbeat task. {}"), eq(LOGGER_ID), any(RejectedExecutionException.class));
  }

  @Test
  void testStopHeartbeatNeverStarted() {
    manager = createDefaultManagerWithMocks(() -> true);

    assertFalse(manager.stopHeartbeat(true));
    verify(mockLogger).warn("Owner {}: No active heartbeat task to stop.", LOGGER_ID);
  }

  @Test
  void testStopHeartbeatAlreadyRequested() {
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> mockFuture);

    manager = createDefaultManagerWithMocks(() -> true);
    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));

    when(mockFuture.cancel(true)).thenReturn(true);
    when(mockFuture.isDone()).thenReturn(false).thenReturn(true);

    assertTrue(manager.stopHeartbeat(true));

    // Call stop again
    assertFalse(manager.stopHeartbeat(true));
    verify(mockLogger).warn("Owner {}: No active heartbeat task to stop.", LOGGER_ID);
  }

  @Test
  void testHeartbeatUnableToAcquireSemaphore() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Thread> t = new AtomicReference<>();
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> {
              Runnable task = invocation.getArgument(0);
              t.set(new Thread(() -> {
                task.run();
                latch.countDown();
              }));
              return mockFuture;
            });

    when(mockFuture.cancel(true)).thenReturn(true);
    Semaphore semaphore = mock(Semaphore.class);

    // Stub the tryAcquire() method to return false (for the heartbeat) and true (for stop)
    when(semaphore.tryAcquire()).thenReturn(false);
    when(semaphore.tryAcquire(eq(DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    manager = new LockProviderHeartbeatManager(
            LOGGER_ID,
            mockScheduler,
            100L,
            DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS,
            () -> true,
            semaphore,
            mockLogger);
    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    t.get().start();
    assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(manager.stopHeartbeat(true));

    verify(mockLogger).error("Owner {}: Heartbeat semaphore should be acquirable at the start of every heartbeat!", LOGGER_ID);
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testStopHeartbeatMockSuccessfulCancel() {
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> mockFuture);
    when(mockFuture.cancel(true)).thenReturn(true);

    manager = createDefaultManagerWithMocks(() -> true);
    manager.startHeartbeatForThread(Thread.currentThread());

    when(mockFuture.isDone()).thenReturn(false).thenReturn(true);
    assertTrue(manager.stopHeartbeat(true));
  }

  @Test
  void testHeartbeatTaskHandlesInterrupt() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Thread> t = new AtomicReference<>();
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> {
              Runnable task = invocation.getArgument(0);
              t.set(new Thread(() -> {
                task.run();
                latch.countDown();
              }));
              return mockFuture;
            });

    when(mockFuture.cancel(true)).thenReturn(true);

    // Initialize heartbeat manager with a function that always returns false (renewal failure)
    manager = createDefaultManagerWithMocks(() -> false);
    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    t.get().start();
    t.get().interrupt();

    assertTrue(latch.await(500, TimeUnit.MILLISECONDS), "Heartbeat task did not run in time");

    // This call will wait for heartbeat task to stop itself, as the semaphore has already been acquired by the heartbeat task.
    assertFalse(manager.stopHeartbeat(true));

    verify(mockLogger).warn("Owner {}: No active heartbeat task to stop.", LOGGER_ID);
    verify(mockLogger).debug(
            "Owner {}: Heartbeat started with interval: {} ms",
            "test-owner",
            100L
    );
    verify(mockLogger).info("Owner {}: Requested termination of heartbeat task. Cancellation returned {}.", LOGGER_ID, true);
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testHeartbeatTaskNullWriter() {
    manager = createDefaultManagerWithMocks(() -> true);
    assertThrows(IllegalArgumentException.class, () -> manager.startHeartbeatForThread(null));
  }

  @Test
  void testHeartbeatTaskImmediateDeadMonitoringThread() throws InterruptedException {
    // Use a real thread that will terminate immediately.
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Thread> t = new AtomicReference<>();
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> {
              Runnable task = invocation.getArgument(0);
              t.set(new Thread(() -> {
                task.run();
                latch.countDown();
              }));
              return mockFuture;
            });

    when(mockFuture.cancel(false)).thenReturn(false);
    Thread deadThread = new Thread(() -> {
    });
    deadThread.start();
    deadThread.join();
    manager = createDefaultManagerWithMocks(() -> true);

    assertTrue(manager.startHeartbeatForThread(deadThread));
    t.get().start();

    assertTrue(latch.await(500, TimeUnit.MILLISECONDS), "Heartbeat task did not run in time");
    verify(mockLogger).warn("Owner {}: Monitored thread is no longer alive.", LOGGER_ID);
    verify(mockLogger).info("Owner {}: Requested termination of heartbeat task. Cancellation returned {}.", LOGGER_ID, false);
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testHeartbeatTaskRenewalException() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Thread> t = new AtomicReference<>();
    when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> {
              Runnable task = invocation.getArgument(0);
              t.set(new Thread(() -> {
                task.run();
                latch.countDown();
              }));
              return mockFuture;
            });
    manager = createDefaultManagerWithMocks(() -> {
      throw new RuntimeException("Renewal error");
    });

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    t.get().start();
    assertTrue(latch.await(500, TimeUnit.MILLISECONDS), "Heartbeat task did not run in time");
    verify(mockLogger).error(
            eq("Owner {}: Heartbeat function threw exception {}"),
            eq(LOGGER_ID),
            any(RuntimeException.class));
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testHeartbeatStopWaitsForHeartbeatTaskToFinish() throws InterruptedException {
    // Use a real thread
    CountDownLatch stopHeartbeatTaskLatch = new CountDownLatch(1);
    manager = createDefaultManagerWithRealExecutor(() -> {
      try {
        // This will freeze the heartbeat task.
        assertTrue(stopHeartbeatTaskLatch.await(500, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    });

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    CountDownLatch finishStopHeartbeatLatch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      assertTrue(manager.stopHeartbeat(false));
      finishStopHeartbeatLatch.countDown();
    });
    t.start();
    // Unblock the heartbeat task.
    stopHeartbeatTaskLatch.countDown();
    assertTrue(finishStopHeartbeatLatch.await(500, TimeUnit.MILLISECONDS), "Stop heartbeat task did not finish.");
    assertFalse(manager.hasActiveHeartbeat());
    verify(mockLogger).debug("Owner {}: Heartbeat task successfully terminated.", LOGGER_ID);
  }

  @Test
  void testHeartbeatUnableToStopHeartbeatTask() throws InterruptedException {
    CountDownLatch stopHeartbeatTaskLatch = new CountDownLatch(1);
    CountDownLatch heartbeatStartedLatch = new CountDownLatch(1);
    // Set stop heartbeat timeout to 5000ms
    manager = new LockProviderHeartbeatManager(LOGGER_ID, actualExecutorService, 100L, 5000L, () -> {
      try {
        // Tells us that the heartbeat has started
        heartbeatStartedLatch.countDown();
        // This will freeze the heartbeat task.
        assertTrue(stopHeartbeatTaskLatch.await(10000, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      // Regardless of whether we return true or false the future executions will be cancelled.
      return true;
    }, new Semaphore(1), mockLogger);

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    CountDownLatch stopHeartbeatLatch = new CountDownLatch(1);
    Thread stopHeartbeatThread = new Thread(() -> {
      // Try to stop the heartbeat (this should hang for 15 seconds)
      assertFalse(manager.stopHeartbeat(false));
      stopHeartbeatLatch.countDown();
    });
    assertTrue(heartbeatStartedLatch.await(500, TimeUnit.MILLISECONDS), "Heartbeat task did not start.");
    stopHeartbeatThread.start();
    assertTrue(stopHeartbeatLatch.await(7000, TimeUnit.MILLISECONDS), "Stop heartbeat task did not finish.");
    assertTrue(manager.hasActiveHeartbeat());
    verify(mockLogger).error("Owner {}: Heartbeat is still in flight!", LOGGER_ID);
    // Unblock the heartbeat task.
    stopHeartbeatTaskLatch.countDown();
  }

  @Test
  void testHeartbeatInterruptStopHeartbeatTask() throws InterruptedException {
    CountDownLatch stopHeartbeatTaskLatch = new CountDownLatch(1);
    CountDownLatch heartbeatStartedLatch = new CountDownLatch(1);
    // Set stop heartbeat timeout to 5000ms
    manager = new LockProviderHeartbeatManager(LOGGER_ID, actualExecutorService, 100L, 5000L, () -> {
      try {
        // Tells us that the heartbeat has started
        heartbeatStartedLatch.countDown();
        // This will freeze the heartbeat task.
        assertTrue(stopHeartbeatTaskLatch.await(10000, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      // Regardless of whether we return true or false the future executions will be cancelled.
      return true;
    }, new Semaphore(1), mockLogger);

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
    CountDownLatch stopHeartbeatLatch = new CountDownLatch(1);
    Thread stopHeartbeatThread = new Thread(() -> {
      // Try to stop the heartbeat (this should hang for 15 seconds)
      assertFalse(manager.stopHeartbeat(false));
      stopHeartbeatLatch.countDown();
    });
    assertTrue(heartbeatStartedLatch.await(500, TimeUnit.MILLISECONDS), "Heartbeat task did not start.");
    stopHeartbeatThread.start();
    stopHeartbeatThread.interrupt();
    assertTrue(stopHeartbeatLatch.await(7000, TimeUnit.MILLISECONDS), "Stop heartbeat task did not finish.");
    assertTrue(manager.hasActiveHeartbeat());
    verify(mockLogger).warn("Owner {}: Interrupted while waiting for heartbeat termination.", LOGGER_ID);
    // Unblock the heartbeat task.
    stopHeartbeatTaskLatch.countDown();
  }

  @Test
  void testHeartbeatTaskValidateStop() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);

    manager = createDefaultManagerWithRealExecutor(() -> {
      latch.countDown();
      return true;
    });

    assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));

    // Wait until at least two heartbeat renewals have occurred
    assertTrue(latch.await(2000, TimeUnit.MILLISECONDS), "Heartbeat did not renew twice in time");

    assertEquals(0, latch.getCount(), "Heartbeat did not execute exactly twice");

    assertTrue(manager.hasActiveHeartbeat());
    assertTrue(manager.stopHeartbeat(false));
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testDefaultManagerRapidStartStop1Ms() {
    manager = new LockProviderHeartbeatManager(LOGGER_ID, 1, () -> true);

    for (int i = 0; i < 100; i++) {
      assertTrue(manager.startHeartbeatForThread(Thread.currentThread()));
      assertTrue(manager.hasActiveHeartbeat());
      assertTrue(manager.stopHeartbeat(true));
      assertFalse(manager.hasActiveHeartbeat());
    }
  }

  @Test
  void testClose() throws Exception {
    manager = createDefaultManagerWithMocks(() -> true);
    manager.close();
    assertFalse(manager.hasActiveHeartbeat());
  }

  @Test
  void testClose_StopsHeartbeatAndShutsDownScheduler() throws Exception {
    when(mockScheduler.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
    manager = createDefaultManagerWithMocks(() -> true);

    manager.close();

    verify(mockScheduler).shutdown();
    verify(mockScheduler, never()).shutdownNow();
  }

  @Test
  void testClose_ForceShutdownWhenTerminationTimesOut() throws Exception {
    when(mockScheduler.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(false);
    manager = createDefaultManagerWithMocks(() -> true);

    manager.close();

    verify(mockScheduler).shutdown();
    verify(mockScheduler).shutdownNow();
  }

  @Test
  void testClose_HandlesInterruptedException() throws Exception {
    when(mockScheduler.awaitTermination(5, TimeUnit.SECONDS)).thenThrow(new InterruptedException());
    manager = createDefaultManagerWithMocks(() -> true);

    manager.close();

    verify(mockScheduler).shutdown();
    verify(mockScheduler).shutdownNow();
    assertTrue(Thread.currentThread().isInterrupted(), "Thread should be interrupted after exception handling");
  }

  private LockProviderHeartbeatManager createDefaultManagerWithMocks(Supplier<Boolean> heartbeatFunc) {
    return new LockProviderHeartbeatManager(
            LOGGER_ID,
            mockScheduler,
            100L,
            DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS,
            heartbeatFunc,
            new Semaphore(1),
            mockLogger);
  }

  private LockProviderHeartbeatManager createDefaultManagerWithRealExecutor(Supplier<Boolean> heartbeatFunc) {
    return new LockProviderHeartbeatManager(
            LOGGER_ID,
            actualExecutorService,
            100L,
            DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS,
            heartbeatFunc,
            new Semaphore(1),
            mockLogger);
  }
}
