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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Base Class for running delta-sync/compaction in separate thread and controlling their life-cyle.
 */
public abstract class AbstractDeltaStreamerService implements Serializable {

  protected static volatile Logger log = LogManager.getLogger(AbstractDeltaStreamerService.class);

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

  AbstractDeltaStreamerService() {
    shutdownRequested = false;
  }

  boolean isShutdownRequested() {
    return shutdownRequested;
  }

  boolean isShutdown() {
    return shutdown;
  }

  /**
   * Wait till the service shutdown. If the service shutdown with exception, it will be thrown
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void waitForShutdown() throws ExecutionException, InterruptedException {
    try {
      future.get();
    } catch (ExecutionException ex) {
      log.error("Service shutdown with error", ex);
      throw ex;
    }
  }

  /**
   * Request shutdown either forcefully or gracefully. Graceful shutdown allows the service to finish up the current
   * round of work and shutdown. For graceful shutdown, it waits till the service is shutdown
   * 
   * @param force Forcefully shutdown
   */
  void shutdown(boolean force) {
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
            log.error("Interrupted while waiting for shutdown", ie);
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
    log.info("Submitting monitor thread !!");
    Executors.newSingleThreadExecutor().submit(() -> {
      boolean error = false;
      try {
        log.info("Monitoring thread(s) !!");
        future.get();
      } catch (ExecutionException ex) {
        log.error("Monitor noticed one or more threads failed." + " Requesting graceful shutdown of other threads", ex);
        error = true;
        shutdown(false);
      } catch (InterruptedException ie) {
        log.error("Got interrupted Monitoring threads", ie);
        error = true;
        shutdown(false);
      } finally {
        // Mark as shutdown
        shutdown = true;
        onShutdownCallback.apply(error);
      }
    });
  }
}
