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
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.timeline.service.RequestHandler.jsonifyResult;

/**
 * A runnable for batch processing marker creation requests.
 */
public class MarkerCreationBatchingRunnable implements Runnable {
  private static final Logger LOG = LogManager.getLogger(MarkerCreationBatchingRunnable.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<String, MarkerDirState> markerDirStateMap;
  private final Registry metricsRegistry;
  private final Map<String, List<MarkerCreationCompletableFuture>> futureMap;

  public MarkerCreationBatchingRunnable(
      Map<String, MarkerDirState> markerDirStateMap, Registry metricsRegistry,
      Map<String, List<MarkerCreationCompletableFuture>> futureMap) {
    this.markerDirStateMap = markerDirStateMap;
    this.metricsRegistry = metricsRegistry;
    this.futureMap = futureMap;
  }

  @Override
  public void run() {
    LOG.debug("Start processing create marker requests");
    HoodieTimer timer = new HoodieTimer().startTimer();
    List<MarkerCreationCompletableFuture> futuresToRemove = new ArrayList<>();

    for (String markerDir : futureMap.keySet()) {
      MarkerDirState markerDirState = markerDirStateMap.get(markerDir);

      if (markerDirState == null) {
        LOG.error("MarkerDirState of " + markerDir + " does not exist!");
        continue;
      }

      futuresToRemove.addAll(
          markerDirState.processMarkerCreationRequests(futureMap.get(markerDir)));
    }

    for (MarkerCreationCompletableFuture future : futuresToRemove) {
      try {
        synchronized (metricsRegistry) {
          future.complete(jsonifyResult(future.getContext(), future.getResult(), metricsRegistry, OBJECT_MAPPER, LOG));
        }
      } catch (JsonProcessingException e) {
        throw new HoodieException("Failed to JSON encode the value", e);
      }
    }
    LOG.debug("Finish batch processing of create marker requests in " + timer.endTimer() + " ms");
  }
}
