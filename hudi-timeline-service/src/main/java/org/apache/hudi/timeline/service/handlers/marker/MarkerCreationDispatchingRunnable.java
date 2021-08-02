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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A runnable for scheduling batch processing of marker creation requests.
 */
public class MarkerCreationDispatchingRunnable implements Runnable {
  public static final Logger LOG = LogManager.getLogger(MarkerCreationDispatchingRunnable.class);

  // Marker directory states, {markerDirPath -> MarkerDirState instance}
  private final Map<String, MarkerDirState> markerDirStateMap;
  private final ExecutorService executorService;

  public MarkerCreationDispatchingRunnable(
      Map<String, MarkerDirState> markerDirStateMap, int batchNumThreads) {
    this.markerDirStateMap = markerDirStateMap;
    this.executorService = Executors.newFixedThreadPool(batchNumThreads);
  }

  @Override
  public void run() {
    Map<String, MarkerDirRequestContext> requestContextMap = new HashMap<>();

    // Only fetch pending marker creation requests that can be processed,
    // i.e., that markers can be written to a underlying file
    for (String markerDir : markerDirStateMap.keySet()) {
      MarkerDirState markerDirState = markerDirStateMap.get(markerDir);
      int fileIndex = markerDirState.getNextFileIndexToUse();
      if (fileIndex < 0) {
        LOG.debug("All marker files are busy, skip batch processing of create marker requests in " + markerDir);
        continue;
      }
      List<MarkerCreationCompletableFuture> futures = markerDirState.fetchPendingMarkerCreationRequests();
      if (futures.isEmpty()) {
        markerDirState.markFileAvailable(fileIndex);
        continue;
      }
      requestContextMap.put(markerDir, new MarkerDirRequestContext(futures, fileIndex));
    }

    if (requestContextMap.size() > 0) {
      executorService.execute(
          new MarkerCreationBatchingRunnable(markerDirStateMap, requestContextMap));
    }
  }
}
