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

import org.apache.hudi.common.util.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * LockProviderHeartbeatManager is a helper class which handles the scheduling and stopping of heartbeat
 * tasks. This is intended for use with the conditional write lock provider, which requires
 * a separate thread to spawn and renew the lock repeatedly.
 * It should be responsible for the entire lifecycle of the heartbeat task.
 * Importantly, a new instance should be created for each lock provider.
 */
public class LockProviderHeartbeatManager implements HeartbeatManager {
  public static long DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS = 15_000L;
  private final ScheduledExecutorService scheduler;

  private final String ownerId;
  private final Logger logger;
  private final long heartbeatTimeMs;
  private final Supplier<Boolean> heartbeatFuncToExec;
  private ScheduledFuture<?> scheduledFuture;

  // We use this variable with a try finally to track whether the heartbeat task is executing.
  // Since we only have one thread in our executor service, even if the heartbeat
  // task runs way longer than the heartbeat time, it will block the next execution
  // and if we run scheduledFuture.cancel(), then those blocked executions will be cancelled.
  // Only the current task will continue to run.
  private final Semaphore heartbeatSemaphore;
  private final long stopHeartbeatTimeoutMs;

  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(LockProviderHeartbeatManager.class);

  /**
   * Initializes a heartbeat manager.
   * @param ownerId The identifier for logging of who owns this heartbeat manager.
   * @param heartbeatTimeMs The time between heartbeat executions.
   *                        The first heartbeat will execute after this amount of time elapses.
   * @param heartbeatFuncToExec The function to execute on each heartbeat. This should handle interrupts.
   */
  public LockProviderHeartbeatManager(String ownerId,
                                      long heartbeatTimeMs,
                                      Supplier<Boolean> heartbeatFuncToExec) {
    this(
        ownerId,
        createDaemonThreadScheduler((ownerId != null && ownerId.length() >= 6) ? ownerId.substring(0, 6) : ""),
        heartbeatTimeMs,
        DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS,
        heartbeatFuncToExec,
        new Semaphore(1),
        DEFAULT_LOGGER);
  }

  @VisibleForTesting
  LockProviderHeartbeatManager(String ownerId,
                               ScheduledExecutorService scheduler,
                               long heartbeatTimeMs,
                               long stopHeartbeatTimeoutMs,
                               Supplier<Boolean> heartbeatFuncToExec,
                               Semaphore heartbeatSemaphore,
                               Logger testLogger) {
    this.ownerId = ownerId;
    this.heartbeatTimeMs = heartbeatTimeMs;
    this.heartbeatFuncToExec = heartbeatFuncToExec;
    this.logger = testLogger;
    this.scheduler = scheduler;
    this.heartbeatSemaphore = heartbeatSemaphore;
    this.stopHeartbeatTimeoutMs = stopHeartbeatTimeoutMs;
  }

  /**
   * Creates a new daemon thread scheduler for heartbeat execution.
   */
  private static ScheduledExecutorService createDaemonThreadScheduler(String shortUuid) {
    return Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "LockProvider-HeartbeatManager-Thread-" + shortUuid);
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * {@inheritDoc}
   */
  public synchronized boolean startHeartbeatForThread(Thread threadToMonitor) {
    if (threadToMonitor == null) {
      throw new IllegalArgumentException("threadToMonitor cannot be null.");
    }

    if (this.hasActiveHeartbeat()) {
      logger.warn("Owner {}: Heartbeat is already running.", ownerId);
      return false;
    }
    try {
      scheduledFuture = scheduler.scheduleAtFixedRate(() -> heartbeatTask(threadToMonitor), heartbeatTimeMs, heartbeatTimeMs, TimeUnit.MILLISECONDS);
      logger.debug("Owner {}: Heartbeat started with interval: {} ms", ownerId, heartbeatTimeMs);
      return true;
    } catch (Exception e) {
      logger.error("Owner {}: Unable to schedule heartbeat task. {}", ownerId, e);
      return false;
    }
  }

  /**
   * Executes the heartbeat task. Will handle failures gracefully.
   * @param threadToMonitor The thread to monitor. If we detect that
   *                        this thread has stopped we should end the heartbeat.
   */
  private void heartbeatTask(Thread threadToMonitor) {
    if (!heartbeatSemaphore.tryAcquire()) {
      // It's undesirable let execution continue while someone else has the semaphore.
      // Therefore, we will exit quickly, like with interrupt.
      logger.error("Owner {}: Heartbeat semaphore should be acquirable at the start of every heartbeat!", ownerId);
      return;
    }
    try {

      if (!threadToMonitor.isAlive()) {
        logger.warn("Owner {}: Monitored thread is no longer alive.", ownerId);
      } else {
        // The thread we are tracking is still active so proceed as normal
        try {
          if (heartbeatFuncToExec.get()) {
            // Successfully executed the heartbeat, return.
            return;
          } else {
            logger.warn("Owner {}: Heartbeat function did not succeed.", ownerId);
          }
        } catch (Exception e) {
          logger.error("Owner {}: Heartbeat function threw exception {}", ownerId, e);
        }
      }

      // We only check for interruption here.
      if (Thread.currentThread().isInterrupted()) {
        logger.warn("Owner {}: Heartbeat task was interrupted. Exiting gracefully.", ownerId);
        return;
      }

      if (threadToMonitor.isAlive()) {
        // This is very bad, if we are unable to renew but the monitored thread is still alive
        logger.error("Owner {}: Monitored thread is still alive!", this.ownerId);
      }

      // Do not interrupt this current task.
      // This will cancel all future invocations.
      if (scheduledFuture != null) {
        logger.info("Owner {}: Cancelling future heartbeat invocations.", this.ownerId);
        boolean cancellationSuccessful = scheduledFuture.cancel(false);
        logger.info("Owner {}: Requested termination of heartbeat task. Cancellation returned {}.", this.ownerId, cancellationSuccessful);
        scheduledFuture = null;
      }

    } finally {
      heartbeatSemaphore.release();
    }
  }

  /**
   * {@inheritDoc}
   */
  public synchronized boolean stopHeartbeat(boolean mayInterruptIfRunning) {
    if (!this.hasActiveHeartbeat()) {
      logger.warn("Owner {}: No active heartbeat task to stop.", ownerId);
      return false;
    }

    // Attempt to cancel the scheduled future
    // More than likely this will be false.
    boolean cancellationSuccessful = scheduledFuture.cancel(mayInterruptIfRunning);
    logger.debug("Owner {}: Requested termination of heartbeat task. Cancellation returned {}", ownerId, cancellationSuccessful);

    // Wait for up to 15 seconds for the currently executing heartbeat task to complete.
    // It is assumed that the heartbeat task, when finishing its execution,
    // sets heartbeatIsExecuting to false and calls notifyAll() on this object.
    try {
      // Try to acquire a permit for up to stopHeartbeatTimeoutMs milliseconds.
      if (!heartbeatSemaphore.tryAcquire(stopHeartbeatTimeoutMs, TimeUnit.MILLISECONDS)) {
        logger.warn("Owner {}: Timed out while waiting for heartbeat termination.", ownerId);
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Owner {}: Interrupted while waiting for heartbeat termination.", ownerId);
      return false;
    }

    heartbeatSemaphore.release();
    logger.debug("Owner {}: Heartbeat task successfully terminated.", ownerId);
    scheduledFuture = null;
    return true;
  }

  @Override
  public synchronized boolean hasActiveHeartbeat() {
    return scheduledFuture != null;
  }

  @Override
  public synchronized void close() throws Exception {
    stopHeartbeat(true);
    scheduler.shutdown();

    try {
      if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
