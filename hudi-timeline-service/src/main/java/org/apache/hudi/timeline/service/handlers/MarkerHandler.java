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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.conflict.detection.TimelineServerBasedDetectionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.timeline.service.handlers.marker.MarkerCreationDispatchingRunnable;
import org.apache.hudi.timeline.service.handlers.marker.MarkerCreationFuture;
import org.apache.hudi.timeline.service.handlers.marker.MarkerDirState;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.timeline.service.RequestHandler.jsonifyResult;

/**
 * REST Handler servicing marker requests.
 *
 * The marker creation requests are handled asynchronous, while other types of requests
 * are handled synchronous.
 *
 * Marker creation requests are batch processed periodically by a thread.  Each batch
 * processing thread adds new markers to a marker file.  Given that marker file operation
 * can take time, multiple concurrent threads can run at the same, while they operate
 * on different marker files storing mutually exclusive marker entries.  At any given
 * time, a marker file is touched by at most one thread to guarantee consistency.
 * Below is an example of running batch processing threads.
 *
 *                  |-----| batch interval
 * Worker Thread 1  |-------------------------->| writing to MARKERS0
 * Worker Thread 2        |-------------------------->| writing to MARKERS1
 * Worker Thread 3               |-------------------------->| writing to MARKERS2
 */
public class MarkerHandler extends Handler {
  private static final Logger LOG = LoggerFactory.getLogger(MarkerHandler.class);

  private final Registry metricsRegistry;
  // a scheduled executor service to schedule dispatching of marker creation requests
  private final ScheduledExecutorService dispatchingExecutorService;
  // an executor service to schedule the worker threads of batch processing marker creation requests
  private final ExecutorService batchingExecutorService;
  // Parallelism for reading and deleting marker files
  private final int parallelism;
  // Marker directory states, {markerDirPath -> MarkerDirState instance}
  // Use ConcurrentHashMap to ensure thread safety in dispatchingExecutorService
  private final Map<String, MarkerDirState> markerDirStateMap = new ConcurrentHashMap<>();
  // A thread to dispatch marker creation requests to batch processing threads
  private final MarkerCreationDispatchingRunnable markerCreationDispatchingRunnable;
  private final Object firstCreationRequestSeenLock = new Object();
  private final Object earlyConflictDetectionLock = new Object();
  private transient HoodieEngineContext hoodieEngineContext;
  private ScheduledFuture<?> dispatchingThreadFuture;
  private boolean firstCreationRequestSeen;
  private String currentMarkerDir = null;
  private TimelineServerBasedDetectionStrategy earlyConflictDetectionStrategy;

  public MarkerHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                       HoodieEngineContext hoodieEngineContext, HoodieStorage storage,
                       FileSystemViewManager viewManager, Registry metricsRegistry) throws IOException {
    super(conf, timelineServiceConfig, storage, viewManager);
    LOG.debug("MarkerHandler FileSystem: " + this.storage.getScheme());
    LOG.debug("MarkerHandler batching params: batchNumThreads=" + timelineServiceConfig.markerBatchNumThreads
        + " batchIntervalMs=" + timelineServiceConfig.markerBatchIntervalMs + "ms");
    this.hoodieEngineContext = hoodieEngineContext;
    this.metricsRegistry = metricsRegistry;
    this.parallelism = timelineServiceConfig.markerParallelism;
    this.dispatchingExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.batchingExecutorService = Executors.newFixedThreadPool(timelineServiceConfig.markerBatchNumThreads);
    this.markerCreationDispatchingRunnable =
        new MarkerCreationDispatchingRunnable(markerDirStateMap, batchingExecutorService);
    this.firstCreationRequestSeen = false;
  }

  /**
   * Stops the dispatching of marker creation requests.
   */
  public void stop() {
    if (dispatchingThreadFuture != null) {
      dispatchingThreadFuture.cancel(true);
    }
    dispatchingExecutorService.shutdownNow();
    batchingExecutorService.shutdownNow();
    if (earlyConflictDetectionStrategy != null) {
      earlyConflictDetectionStrategy.stop();
    }
  }

  /**
   * @param markerDir marker directory path
   * @return all marker paths in the marker directory
   */
  public Set<String> getAllMarkers(String markerDir) {
    MarkerDirState markerDirState = getMarkerDirState(markerDir);
    return markerDirState.getAllMarkers();
  }

  /**
   * @param markerDir marker directory path.
   * @return Pending markers from the requests to process.
   */
  public Set<String> getPendingMarkersToProcess(String markerDir) {
    if (markerDirStateMap.containsKey(markerDir)) {
      MarkerDirState markerDirState = getMarkerDirState(markerDir);
      return markerDirState.getPendingMarkerCreationRequests(false).stream()
          .map(MarkerCreationFuture::getMarkerName).collect(Collectors.toSet());
    }
    return Collections.emptySet();
  }

  /**
   * @param markerDir marker directory path
   * @return all marker paths of write IO type "CREATE" and "MERGE"
   */
  public Set<String> getCreateAndMergeMarkers(String markerDir) {
    return getAllMarkers(markerDir).stream()
        .filter(markerName -> !markerName.endsWith(IOType.APPEND.name()))
        .collect(Collectors.toSet());
  }

  /**
   * @param markerDir marker directory path
   * @return all marker paths of write IO type "APPEND"
   */
  public Set<String> getAppendMarkers(String markerDir) {
    return getAllMarkers(markerDir).stream()
        .filter(markerName -> markerName.endsWith(IOType.APPEND.name()))
        .collect(Collectors.toSet());
  }

  /**
   * @param markerDir  marker directory path
   * @return {@code true} if the marker directory exists; {@code false} otherwise.
   */
  public boolean doesMarkerDirExist(String markerDir) {
    MarkerDirState markerDirState = getMarkerDirState(markerDir);
    return markerDirState.exists();
  }

  /**
   * Generates a future for an async marker creation request
   *
   * The future is added to the marker creation future list and waits for the next batch processing
   * of marker creation requests.
   *
   * @param context Javalin app context
   * @param markerDir marker directory path
   * @param markerName marker name
   * @return the {@code CompletableFuture} instance for the request
   */
  public CompletableFuture<String> createMarker(Context context, String markerDir, String markerName, String basePath) {
    // Step1 do early conflict detection if enable
    if (timelineServiceConfig.earlyConflictDetectionEnable) {
      try {
        synchronized (earlyConflictDetectionLock) {
          if (earlyConflictDetectionStrategy == null) {
            String strategyClassName = timelineServiceConfig.earlyConflictDetectionStrategy;
            if (!ReflectionUtils.isSubClass(strategyClassName, TimelineServerBasedDetectionStrategy.class)) {
              LOG.warn("Cannot use " + strategyClassName + " for timeline-server-based markers.");
              strategyClassName = "org.apache.hudi.timeline.service.handlers.marker.AsyncTimelineServerBasedDetectionStrategy";
              LOG.warn("Falling back to " + strategyClassName);
            }

            earlyConflictDetectionStrategy =
                (TimelineServerBasedDetectionStrategy) ReflectionUtils.loadClass(
                    strategyClassName, basePath, markerDir, markerName, timelineServiceConfig.checkCommitConflict);
          }

          // markerDir => $base_path/.hoodie/.temp/$instant_time
          // If markerDir is changed like move to the next instant action, we need to fresh this earlyConflictDetectionStrategy.
          // For specific instant related create marker action, we only call this check/fresh once
          // instead of starting the conflict detector for every request
          if (!markerDir.equalsIgnoreCase(currentMarkerDir)) {
            this.currentMarkerDir = markerDir;
            Set<String> actions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION);
            Set<HoodieInstant> completedCommits = new HashSet<>(
                viewManager.getFileSystemView(basePath)
                    .getTimeline()
                    .filterCompletedInstants()
                    .filter(instant -> actions.contains(instant.getAction()))
                    .getInstants());

            earlyConflictDetectionStrategy.startAsyncDetection(
                timelineServiceConfig.asyncConflictDetectorInitialDelayMs,
                timelineServiceConfig.asyncConflictDetectorPeriodMs,
                markerDir, basePath, timelineServiceConfig.maxAllowableHeartbeatIntervalInMs,
                storage, this, completedCommits);
          }
        }

        earlyConflictDetectionStrategy.detectAndResolveConflictIfNecessary();

      } catch (HoodieEarlyConflictDetectionException he) {
        LOG.warn("Detected the write conflict due to a concurrent writer, "
            + "failing the marker creation as the early conflict detection is enabled", he);
        return finishCreateMarkerFuture(context, markerDir, markerName);
      } catch (Exception e) {
        LOG.warn("Failed to execute early conflict detection." + e.getMessage());
        // When early conflict detection fails to execute, we still allow the marker creation
        // to continue
        return addMarkerCreationRequestForAsyncProcessing(context, markerDir, markerName);
      }
    }

    // Step 2 create marker
    return addMarkerCreationRequestForAsyncProcessing(context, markerDir, markerName);
  }

  private MarkerCreationFuture addMarkerCreationRequestForAsyncProcessing(
      Context context, String markerDir, String markerName) {
    LOG.info("Request: create marker: " + markerName);
    MarkerCreationFuture future = new MarkerCreationFuture(context, markerDir, markerName);
    // Add the future to the list
    MarkerDirState markerDirState = getMarkerDirState(markerDir);
    markerDirState.addMarkerCreationFuture(future);
    if (!firstCreationRequestSeen) {
      synchronized (firstCreationRequestSeenLock) {
        if (!firstCreationRequestSeen) {
          dispatchingThreadFuture = dispatchingExecutorService.scheduleAtFixedRate(markerCreationDispatchingRunnable,
              timelineServiceConfig.markerBatchIntervalMs, timelineServiceConfig.markerBatchIntervalMs,
              TimeUnit.MILLISECONDS);
          firstCreationRequestSeen = true;
        }
      }
    }
    return future;
  }

  private CompletableFuture<String> finishCreateMarkerFuture(Context context, String markerDir, String markerName) {
    MarkerCreationFuture future = new MarkerCreationFuture(context, markerDir, markerName);
    try {
      future.complete(jsonifyResult(
          future.getContext(), future.isSuccessful(), metricsRegistry, new ObjectMapper(), LOG));
    } catch (JsonProcessingException e) {
      throw new HoodieException("Failed to JSON encode the value", e);
    }
    return future;
  }

  /**
   * Deletes markers in the directory.
   *
   * @param markerDir marker directory path
   * @return {@code true} if successful; {@code false} otherwise.
   */
  public Boolean deleteMarkers(String markerDir) {
    boolean result = getMarkerDirState(markerDir).deleteAllMarkers();
    markerDirStateMap.remove(markerDir);
    return result;
  }

  private MarkerDirState getMarkerDirState(String markerDir) {
    MarkerDirState markerDirState = markerDirStateMap.get(markerDir);
    if (markerDirState == null) {
      synchronized (markerDirStateMap) {
        if (markerDirStateMap.get(markerDir) == null) {
          Option<TimelineServerBasedDetectionStrategy> strategy =
              timelineServiceConfig.earlyConflictDetectionEnable
                  && earlyConflictDetectionStrategy != null
                  ? Option.of(earlyConflictDetectionStrategy) : Option.empty();
          markerDirState = new MarkerDirState(
              markerDir, timelineServiceConfig.markerBatchNumThreads,
              strategy, storage, metricsRegistry, hoodieEngineContext, parallelism);
          markerDirStateMap.put(markerDir, markerDirState);
        } else {
          markerDirState = markerDirStateMap.get(markerDir);
        }
      }
    }
    return markerDirState;
  }
}
