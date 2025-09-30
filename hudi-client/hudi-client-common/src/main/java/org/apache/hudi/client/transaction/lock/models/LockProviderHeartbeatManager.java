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
 * tasks. This is intended for use with the storage based lock provider, which requires
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

  /**
   * Contract for the heartbeat function execution.
   *
   * <p>Behavior of the heartbeat manager (consumer):
   * <ul>
   *   <li>Executes heartBeatFuncToExec every heartbeatTimeMs when:
   *     <ul>
   *       <li>heartBeatFuncToExec returns true</li>
   *     </ul>
   *   </li>
   *   <li>Stops executing heartBeatFuncToExec when:
   *     <ul>
   *       <li>heartBeatFuncToExec returns false</li>
   *       <li>heartBeatFuncToExec throws an exception</li>
   *       <li>heart beat manager calls stopHeartbeat, which will interrupt any inflight execution
   *           and prevent further recurring executions</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <p>Requirements for heartBeatFuncToExec implementation:
   * <ul>
   *   <li>Should perform the logic of renewing lock lease</li>
   *   <li>Should be super light-weight, typically runs within 1 second</li>
   *   <li>Should handle thread interruptions so that the stopHeartbeat function will not wait long for any
   *       inflight execution to complete</li>
   *   <li>Should almost always return true in cases like:
   *     <ul>
   *       <li>Successfully extending the lock lease</li>
   *       <li>Transient failures (network partition, remote service errors) to allow automatic retry</li>
   *     </ul>
   *   </li>
   *   <li>Should return false only in specific cases:
   *     <ul>
   *       <li>When the lock is already expired (no point in extending an expired lock)</li>
   *       <li>When the writer thread does not hold any lock</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <p>Warning: Returning false stops all future lock renewal attempts. If the writer thread
   * is still running, it will execute with a lock that can expire at any time, potentially
   * leading to corrupted data.
   */
  private final Supplier<Boolean> heartbeatFuncToExec;
  private final long stopHeartbeatTimeoutMs;

  // We ensure within the context of LockProviderHeartbeatManager, heartbeatFuncToExec only execute in a single thread periodically.
  @GuardedBy("this")
  private ScheduledFuture<?> scheduledFuture;

  /**
   * Semaphore for managing heartbeat task execution synchronization.
   *
   * <p><strong>IMPORTANT: Thread Safety Warning</strong>
   * This semaphore is mutually exclusive with {@code synchronized(this)}. Never synchronize
   * on {@code this} while the thread is holding the heartbeatSemaphore.
   *
   * <p>Execution flow:
   * <ul>
   *   <li>Heartbeat task always attempts to acquire the semaphore before proceeding and
   *       releases it before finishing the current round of execution</li>
   *   <li>The heartbeat manager acquires the semaphore when it needs to:
   *     <ul>
   *       <li>Drain any inflight execution</li>
   *       <li>Prevent further execution of the heartbeat task</li>
   *     </ul>
   *   </li>
   * </ul>
   */
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
            createThreadScheduler((ownerId != null && ownerId.length() >= 6) ? ownerId.substring(0, 6) : ""),
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
   * Creates a new thread scheduler for heartbeat execution.
   */
  private static ScheduledExecutorService createThreadScheduler(String shortUuid) {
    return Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "LockProvider-HeartbeatManager-Thread-" + shortUuid));
  }

  /**
   * {@inheritDoc}
   */
  public synchronized boolean startHeartbeatForThread(Thread threadToMonitor) {
    if (threadToMonitor == null) {
      throw new IllegalArgumentException("threadToMonitor cannot be null.");
    }

    if (this.hasActiveHeartbeat()) {
      logger.info("Owner {}: Heartbeat is already running.", ownerId);
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
      logger.info("Owner {}: Heartbeat function did not succeed.", ownerId);
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
      logger.info("Owner {}: Monitored thread is no longer alive.", ownerId);
      return false;
    }

    // Execute heartbeat function
    try {
      return heartbeatFuncToExec.get();
    } catch (Exception e) {
      logger.error("Owner {}: Heartbeat function threw exception {}", ownerId, e);
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
      boolean cancellationSuccessful = scheduledFuture.cancel(true);
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
      // If waiting for cancellation was interrupted, do not log an error.
      if (Thread.currentThread().isInterrupted()) {
        logger.info("Owner {}: Heartbeat is still in flight due to interruption!", ownerId);
      } else {
        logger.error("Owner {}: Heartbeat is still in flight!", ownerId);
      }
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
      logger.info("Owner {}: No active heartbeat task to stop.", ownerId);
      return true;
    }

    // Attempt to cancel the scheduled future
    boolean cancellationSuccessful = scheduledFuture.cancel(mayInterruptIfRunning);
    logger.debug("Owner {}: Requested termination of heartbeat task. Cancellation returned {}", ownerId, cancellationSuccessful);
    return false;
  }

  private boolean syncWaitInflightHeartbeatTaskToFinish() {
    // Wait for up to stopHeartbeatTimeoutMs for the currently executing heartbeat task to complete.
    // It is assumed that the heartbeat task, when finishing its execution,
    // sets heartbeatIsExecuting to false and calls notifyAll() on this object.
    boolean heartbeatStillInflight = true;
    try {
      // Semaphore successfully acquired here excludes the heart execution task. tryAcquire with timeout
      // means we wait any inflight task execution to finish synchronously.
      heartbeatStillInflight = !heartbeatSemaphore.tryAcquire(stopHeartbeatTimeoutMs, TimeUnit.MILLISECONDS);
      if (heartbeatStillInflight) {
        logger.info("Owner {}: Timed out while waiting for heartbeat termination.", ownerId);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Owner {}: Interrupted while waiting for heartbeat termination.", ownerId);
    }
    // If we successfully acquired the semaphore before, return it here.
    heartbeatSemaphore.release(heartbeatStillInflight ? 0 : 1);
    return heartbeatStillInflight;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized boolean hasActiveHeartbeat() {
    return scheduledFuture != null;
  }

  /**
   * {@inheritDoc}
   */
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
