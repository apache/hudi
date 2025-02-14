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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

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
@ThreadSafe
public class LockProviderHeartbeatManager implements HeartbeatManager {
  public static long DEFAULT_STOP_HEARTBEAT_TIMEOUT_MS = 15_000L;
  @GuardedBy("this")
  private final ScheduledExecutorService scheduler;

  // Constant does not need multi-threading protections.
  private final String ownerId;
  private final Logger logger;
  private final long heartbeatTimeMs;
  // The function passed to the heartbeat manager should exhaust all options before returning false.
  // In the case of the lock provider, this means retrying transient errors and only returning false
  // on truly fatal cases. Consider returning true if expiration is far out, which will give the
  // heartbeat task another opportunity to renew the lock.
  // Returning false will result in immediate termination of the heartbeat
  // and may negatively impact the original caller thread which started the heartbeat.
  // IE: if the caller thread is a writer which assumes it has a lock
  // using this heartbeat manager, then it may proceed to write without the lock, which is very bad!
  private final Supplier<Boolean> heartbeatFuncToExec;
  private final long stopHeartbeatTimeoutMs;

  // We ensure within the context of LockProviderHeartbeatManager, heartbeatFuncToExec only execute in a single thread periodically.
  @GuardedBy("this")
  private ScheduledFuture<?> scheduledFuture;

  // !!!!!!! Important !!!!!!!
  // We use this variable with a try finally to track whether the heartbeat task is executing.
  // Since we only have one thread in our executor service, even if the heartbeat
  // task runs way longer than the heartbeat time, it will block the next execution
  // and if we run scheduledFuture.cancel(), then those blocked executions will be cancelled.
  // Only the current task will continue to run.
  // This is mutually exclusive to synchronize(this). Never sync on "this" while the thread is holding a heartbeatSemaphore
  // !!!!!!! Important !!!!!!!
  private final Semaphore heartbeatSemaphore;

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
      scheduledFuture = scheduler.scheduleAtFixedRate(() -> heartbeatTaskRunner(threadToMonitor), heartbeatTimeMs, heartbeatTimeMs, TimeUnit.MILLISECONDS);
      logger.debug("Owner {}: Heartbeat started with interval: {} ms", ownerId, heartbeatTimeMs);
      return true;
    } catch (Exception e) {
      logger.error("Owner {}: Unable to schedule heartbeat task. {}", ownerId, e);
      return false;
    }
  }

  /**
   * Responsible for managing the execution and result of the heartbeat task.
   * Maintains a semaphore which ensures thread safety for determining the state
   * of the heartbeat (is the heartbeat executing or not).
   * @param threadToMonitor The thread to monitor. Required by heartbeat execution.
   */
  private void heartbeatTaskRunner(Thread threadToMonitor) {
    if (!heartbeatSemaphore.tryAcquire()) {
      // This will trigger an alert
      logger.error("Owner {}: Heartbeat semaphore should be acquirable at the start of every heartbeat!", ownerId);
      return;
    }

    boolean heartbeatExecutionSuccessful;
    try {
      heartbeatExecutionSuccessful = executeHeartbeat(threadToMonitor);
    } finally {
      heartbeatSemaphore.release();
    }

    // Call synchronized method after releasing the semaphore
    if (!heartbeatExecutionSuccessful) {
      // Unschedule self from further execution if heartbeat was unsuccessful.
      heartbeatTaskUnscheduleItself();
    }
  }

  /**
   * Executes the heartbeat task.
   * @param threadToMonitor The thread to monitor. If we detect that
   *                        this thread has stopped we should end the heartbeat.
   * @return Whether the heartbeat task successfully ran.
   */
  private boolean executeHeartbeat(Thread threadToMonitor) {
    // Check if monitored thread is dead
    if (!threadToMonitor.isAlive()) {
      logger.warn("Owner {}: Monitored thread is no longer alive.", ownerId);
      return false;
    }

    // Execute heartbeat function
    try {
      if (heartbeatFuncToExec.get()) {
        return true;
      }
      logger.warn("Owner {}: Heartbeat function did not succeed.", ownerId);
    } catch (Exception e) {
      logger.error("Owner {}: Heartbeat function threw exception {}", ownerId, e);
    }

    // Check for interruption after failed heartbeat
    if (Thread.currentThread().isInterrupted()) {
      logger.warn("Owner {}: Heartbeat task was interrupted. Exiting gracefully.", ownerId);
      return false;
    }

    // Final check if monitored thread is still alive after failed heartbeat.
    if (threadToMonitor.isAlive()) {
      // This will trigger an alert
      logger.error("Owner {}: Monitored thread is still alive!", this.ownerId);
    }
    return false;
  }

  /**
   * This prevents further scheduling of the heartbeat task. Intended to be used by heartbeat task itself.
   */
  private synchronized void heartbeatTaskUnscheduleItself() {
    // Do not interrupt this current task.
    // This will cancel all future invocations.
    if (scheduledFuture != null) {
      logger.info("Owner {}: Cancelling future heartbeat invocations.", this.ownerId);
      boolean cancellationSuccessful = scheduledFuture.cancel(false);
      logger.info("Owner {}: Requested termination of heartbeat task. Cancellation returned {}.", this.ownerId, cancellationSuccessful);
      scheduledFuture = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean stopHeartbeat(boolean mayInterruptIfRunning) {
    if (cancelRecurringHeartbeatTask(mayInterruptIfRunning)) {
      return false;
    }

    // If we requested to stop heartbeat, here we ensure the cancel request results in heartbeat task
    // exiting synchronously.
    boolean heartbeatStillInflight = syncWaitInflightHeartbeatTaskToFinish();
    if (heartbeatStillInflight) {
      // This will trigger an alert
      logger.error("Owner {}: Heartbeat is still in flight!", ownerId);
      return false;
    }

    // We have stopped the heartbeat, now clean up any leftover states.
    synchronized (this) {
      logger.debug("Owner {}: Heartbeat task successfully terminated.", ownerId);
      scheduledFuture = null;
    }
    return true;
  }

  /**
   * Cancels the recurring heartbeat task.
   * @param mayInterruptIfRunning Whether to interrupt the heartbeat task if it is currently running.
   * @return True if the heartbeat task did not need to be stopped.
   */
  private synchronized boolean cancelRecurringHeartbeatTask(boolean mayInterruptIfRunning) {
    if (!this.hasActiveHeartbeat()) {
      logger.warn("Owner {}: No active heartbeat task to stop.", ownerId);
      return true;
    }

    // Attempt to cancel the scheduled future
    boolean cancellationSuccessful = scheduledFuture.cancel(mayInterruptIfRunning);
    logger.debug("Owner {}: Requested termination of heartbeat task. Cancellation returned {}", ownerId, cancellationSuccessful);
    return false;
  }

  private boolean syncWaitInflightHeartbeatTaskToFinish() {
    // Wait for up to 15 seconds for the currently executing heartbeat task to complete.
    // It is assumed that the heartbeat task, when finishing its execution,
    // sets heartbeatIsExecuting to false and calls notifyAll() on this object.
    boolean heartbeatStillInflight = true;
    try {
      // Semaphore successfully acquired here excludes the heart execution task. tryAcquire with timeout
      // means we wait any inflight task execution to finish synchronously.
      heartbeatStillInflight = !heartbeatSemaphore.tryAcquire(stopHeartbeatTimeoutMs, TimeUnit.MILLISECONDS);
      if (heartbeatStillInflight) {
        logger.warn("Owner {}: Timed out while waiting for heartbeat termination.", ownerId);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Owner {}: Interrupted while waiting for heartbeat termination.", ownerId);
    }
    // If we successfully acquired the semaphore before, return it here.
    heartbeatSemaphore.release(heartbeatStillInflight ? 0 : 1);
    return heartbeatStillInflight;
  }

  @Override
  public synchronized boolean hasActiveHeartbeat() {
    return scheduledFuture != null;
  }

  @Override
  public synchronized void close() throws Exception {
    if (hasActiveHeartbeat()) {
      stopHeartbeat(true);
    }
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
