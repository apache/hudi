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

package org.apache.hudi.timeline.service.handlers.marker;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.HoodieTimer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A runnable for scheduling batch processing of marker creation requests.
 */
public class MarkerCreationDispatchingRunnable implements Runnable {
  public static final Logger LOG = LogManager.getLogger(MarkerCreationDispatchingRunnable.class);

  // Marker directory states, {markerDirPath -> MarkerDirState instance}
  private final Map<String, MarkerDirState> markerDirStateMap;
  private final Registry metricsRegistry;
  private final ExecutorService executorService;
  // Batch process interval in milliseconds
  private final long batchIntervalMs;
  private boolean isRunning = false;

  public MarkerCreationDispatchingRunnable(
      Map<String, MarkerDirState> markerDirStateMap, Registry metricsRegistry,
      int batchNumThreads, long batchIntervalMs) {
    this.markerDirStateMap = markerDirStateMap;
    this.metricsRegistry = metricsRegistry;
    this.batchIntervalMs = batchIntervalMs;
    this.executorService = Executors.newFixedThreadPool(batchNumThreads);
    this.isRunning = true;
  }

  public void stop() {
    this.isRunning = false;
  }

  @Override
  public void run() {
    while (isRunning) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      Map<String, List<MarkerCreationCompletableFuture>> futureMap =
          markerDirStateMap.entrySet().stream().collect(
              Collectors.toMap(Map.Entry::getKey,
                  e -> e.getValue().fetchPendingMarkerCreationRequests()));
      executorService.execute(
          new MarkerCreationBatchingRunnable(markerDirStateMap, metricsRegistry, futureMap));

      try {
        Thread.sleep(Math.max(batchIntervalMs - timer.endTimer(), 0L));
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException in MarkerCreationDispatchingRunnable", e);
      }
    }
  }
}
