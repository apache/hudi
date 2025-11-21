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

import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Base Class for running archive/clean/delta-sync/compaction/clustering in separate thread and controlling their life-cycles.
 */
public abstract class HoodieAsyncService implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAsyncService.class);
  private static final long POLLING_SECONDS = 10;

  // Flag indicating whether an error is incurred in the service
  protected boolean hasError;
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
  private transient BlockingQueue<String> pendingInstants = new LinkedBlockingQueue<>();
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

  public boolean isStarted() {
    return started;
  }

  public boolean isShutdownRequested() {
    return shutdownRequested;
  }

  public boolean isShutdown() {
    return shutdown;
  }

  public boolean hasError() {
    return hasError;
  }

  /**
   * Wait till the service shutdown. If the service shutdown with exception, it will be thrown
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void waitForShutdown() throws ExecutionException, InterruptedException {
    if (future == null) {
      return;
    }
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
      shutdown = true;
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
    if (started) {
      LOG.info("The async service already started.");
      return;
    }
    Pair<CompletableFuture, ExecutorService> res = startService();
    future = res.getKey();
    executor = res.getValue();
    started = true;
    shutdownCallback(onShutdownCallback);
  }

  /**
   * Service implementation.
   */
  protected abstract Pair<CompletableFuture, ExecutorService> startService();

  /**
   * Add shutdown callback for the completable future.
   * 
   * @param callback The callback
   */
  @SuppressWarnings("unchecked")
  private void shutdownCallback(Function<Boolean, Boolean> callback) {
    if (future == null) {
      return;
    }
    future.whenComplete((resp, error) -> {
      if (null != callback) {
        callback.apply(null != error);
      }
      this.started = false;
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
      while (!isShutdown() && !hasError() && (pendingInstants.size() > numPending)) {
        consumed.await(POLLING_SECONDS, TimeUnit.SECONDS);
      }
    } finally {
      queueLock.unlock();
    }
  }

  /**
   * Enqueues new pending table service instant.
   * @param instantTime {@link String} to enqueue.
   */
  public void enqueuePendingAsyncServiceInstant(String instantTime) {
    LOG.info("Enqueuing new pending table service instant: " + instantTime);
    pendingInstants.add(instantTime);
  }

  /**
   * Fetch next pending compaction/clustering instant if available.
   *
   * @return {@link String} corresponding to the next pending compaction/clustering.
   * @throws InterruptedException
   */
  String fetchNextAsyncServiceInstant() throws InterruptedException {
    LOG.info("Waiting for next instant up to {} seconds", POLLING_SECONDS);
    String instantTime = pendingInstants.poll(POLLING_SECONDS, TimeUnit.SECONDS);
    if (instantTime != null) {
      try {
        queueLock.lock();
        // Signal waiting thread
        consumed.signal();
      } finally {
        queueLock.unlock();
      }
    }
    return instantTime;
  }
}
