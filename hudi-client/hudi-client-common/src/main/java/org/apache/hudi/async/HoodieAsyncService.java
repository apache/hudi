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

package org.apache.hudi.async;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Base Class for running clean/delta-sync/compaction/clustering in separate thread and controlling their life-cycle.
 */
public abstract class HoodieAsyncService implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieAsyncService.class);

  // Flag to track if the service is started.
  private boolean started;
  // Flag indicating shutdown is externally requested
  private boolean shutdownRequested;
  // Flag indicating the service is shutdown
  private volatile boolean shutdown;
  // Executor Service for running delta-sync/compaction
  private transient ExecutorService executor;
  // Future tracking delta-sync/compaction
  private transient CompletableFuture future;
  // Run in daemon mode
  private final boolean runInDaemonMode;
  // Queue to hold pending compaction/clustering instants
  private transient BlockingQueue<HoodieInstant> pendingInstants = new LinkedBlockingQueue<>();
  // Mutex lock for synchronized access to pendingInstants queue
  private transient ReentrantLock queueLock = new ReentrantLock();
  // Condition instance to use with the queueLock
  private transient Condition consumed = queueLock.newCondition();

  protected HoodieAsyncService() {
    this(false);
  }

  protected HoodieAsyncService(boolean runInDaemonMode) {
    shutdownRequested = false;
    this.runInDaemonMode = runInDaemonMode;
  }

  protected boolean isShutdownRequested() {
    return shutdownRequested;
  }

  protected boolean isShutdown() {
    return shutdown;
  }

  /**
   * Wait till the service shutdown. If the service shutdown with exception, it will be thrown
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void waitForShutdown() throws ExecutionException, InterruptedException {
    try {
      future.get();
    } catch (ExecutionException ex) {
      LOG.error("Service shutdown with error", ex);
      throw ex;
    }
  }

  /**
   * Request shutdown either forcefully or gracefully. Graceful shutdown allows the service to finish up the current
   * round of work and shutdown. For graceful shutdown, it waits till the service is shutdown
   * 
   * @param force Forcefully shutdown
   */
  public void shutdown(boolean force) {
    if (!shutdownRequested || force) {
      shutdownRequested = true;
      if (executor != null) {
        if (force) {
          executor.shutdownNow();
        } else {
          executor.shutdown();
          try {
            // Wait for some max time after requesting shutdown
            executor.awaitTermination(24, TimeUnit.HOURS);
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while waiting for shutdown", ie);
          }
        }
      }
    }
  }

  /**
   * Start the service. Runs the service in a different thread and returns. Also starts a monitor thread to
   * run-callbacks in case of shutdown
   * 
   * @param onShutdownCallback
   */
  public void start(Function<Boolean, Boolean> onShutdownCallback) {
    Pair<CompletableFuture, ExecutorService> res = startService();
    future = res.getKey();
    executor = res.getValue();
    started = true;
    monitorThreads(onShutdownCallback);
  }

  /**
   * Service implementation.
   * 
   * @return
   */
  protected abstract Pair<CompletableFuture, ExecutorService> startService();

  /**
   * A monitor thread is started which would trigger a callback if the service is shutdown.
   * 
   * @param onShutdownCallback
   */
  private void monitorThreads(Function<Boolean, Boolean> onShutdownCallback) {
    LOG.info("Submitting monitor thread !!");
    Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "Monitor Thread");
      t.setDaemon(isRunInDaemonMode());
      return t;
    }).submit(() -> {
      boolean error = false;
      try {
        LOG.info("Monitoring thread(s) !!");
        future.get();
      } catch (ExecutionException ex) {
        LOG.error("Monitor noticed one or more threads failed. Requesting graceful shutdown of other threads", ex);
        error = true;
      } catch (InterruptedException ie) {
        LOG.error("Got interrupted Monitoring threads", ie);
        error = true;
      } finally {
        // Mark as shutdown
        shutdown = true;
        if (null != onShutdownCallback) {
          onShutdownCallback.apply(error);
        }
        shutdown(false);
      }
    });
  }

  public boolean isRunInDaemonMode() {
    return runInDaemonMode;
  }

  /**
   * Wait till outstanding pending compaction/clustering reduces to the passed in value.
   *
   * @param numPending Maximum pending compactions/clustering allowed
   * @throws InterruptedException
   */
  public void waitTillPendingAsyncServiceInstantsReducesTo(int numPending) throws InterruptedException {
    try {
      queueLock.lock();
      while (!isShutdown() && (pendingInstants.size() > numPending)) {
        consumed.await();
      }
    } finally {
      queueLock.unlock();
    }
  }

  /**
   * Enqueues new pending clustering instant.
   * @param instant {@link HoodieInstant} to enqueue.
   */
  public void enqueuePendingAsyncServiceInstant(HoodieInstant instant) {
    LOG.info("Enqueuing new pending clustering instant: " + instant.getTimestamp());
    pendingInstants.add(instant);
  }

  /**
   * Fetch next pending compaction/clustering instant if available.
   *
   * @return {@link HoodieInstant} corresponding to the next pending compaction/clustering.
   * @throws InterruptedException
   */
  HoodieInstant fetchNextAsyncServiceInstant() throws InterruptedException {
    LOG.info("Waiting for next instant upto 10 seconds");
    HoodieInstant instant = pendingInstants.poll(10, TimeUnit.SECONDS);
    if (instant != null) {
      try {
        queueLock.lock();
        // Signal waiting thread
        consumed.signal();
      } finally {
        queueLock.unlock();
      }
    }
    return instant;
  }
}
