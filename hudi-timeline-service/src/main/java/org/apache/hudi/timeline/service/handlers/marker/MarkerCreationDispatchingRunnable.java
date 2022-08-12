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

import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * A runnable that performs periodic, batched creation of markers for write operations.
 */
public class MarkerCreationDispatchingRunnable implements Runnable {
  public static final Logger LOG = LogManager.getLogger(MarkerCreationDispatchingRunnable.class);

  // Marker directory states, {markerDirPath -> MarkerDirState instance}
  private final Map<String, MarkerDirState> markerDirStateMap;
  private final ExecutorService executorService;

  public MarkerCreationDispatchingRunnable(
      Map<String, MarkerDirState> markerDirStateMap, ExecutorService executorService) {
    this.markerDirStateMap = markerDirStateMap;
    this.executorService = executorService;
  }

  /**
   * Dispatches the marker creation requests that can be process to a worker thread of batch
   * processing the requests.
   *
   * For each marker directory, goes through the following steps:
   * (1) find the next available file index for writing.  If no file index is available,
   *   skip the processing of this marker directory;
   * (2) fetch the pending marker creation requests for this marker directory.  If there is
   *   no request, skip this marker directory;
   * (3) put the marker directory, marker dir state, list of requests futures, and the file index
   *   to a {@code MarkerDirRequestContext} instance and add the instance to the request context list.
   *
   * If the request context list is not empty, spins up a worker thread, {@code MarkerCreationBatchingRunnable},
   * and pass all the request context to the thread for batch processing.  The thread is responsible
   * for responding to the request futures directly.
   */
  @Override
  public void run() {
    List<BatchedMarkerCreationContext> requestContextList = new ArrayList<>();

    // Only fetch pending marker creation requests that can be processed,
    // i.e., that markers can be written to a underlying file
    for (Map.Entry<String, MarkerDirState> entry : markerDirStateMap.entrySet()) {
      String markerDir = entry.getKey();
      MarkerDirState markerDirState = entry.getValue();
      Option<Integer> fileIndex = markerDirState.getNextFileIndexToUse();
      if (!fileIndex.isPresent()) {
        LOG.debug("All marker files are busy, skip batch processing of create marker requests in " + markerDir);
        continue;
      }
      List<MarkerCreationFuture> futures = markerDirState.fetchPendingMarkerCreationRequests();
      if (futures.isEmpty()) {
        markerDirState.markFileAsAvailable(fileIndex.get());
        continue;
      }
      requestContextList.add(
          new BatchedMarkerCreationContext(markerDir, markerDirState, futures, fileIndex.get()));
    }

    if (requestContextList.size() > 0) {
      executorService.execute(
          new BatchedMarkerCreationRunnable(requestContextList));
    }
  }
}
