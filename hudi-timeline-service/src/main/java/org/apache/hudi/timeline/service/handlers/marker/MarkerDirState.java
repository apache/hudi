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

import org.apache.hudi.common.conflict.detection.TimelineServerBasedDetectionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.FileIOUtils.closeQuietly;
import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.apache.hudi.timeline.service.RequestHandler.jsonifyResult;

/**
 * Stores the state of a marker directory.
 *
 * The operations inside this class is designed to be thread-safe.
 */
public class MarkerDirState implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MarkerDirState.class);
  // Marker directory
  private final StoragePath markerDirPath;
  private final HoodieStorage storage;
  private final Registry metricsRegistry;
  // A cached copy of all markers in memory
  private final Set<String> allMarkers = new HashSet<>();
  // A cached copy of marker entries in each marker file, stored in StringBuilder
  // for efficient appending
  // Mapping: {markerFileIndex -> markers}
  private final Map<Integer, StringBuilder> fileMarkersMap = new HashMap<>();
  // A list of use status of underlying files storing markers by a thread.
  // {@code true} means the file is in use by a {@code BatchCreateMarkerRunnable}.
  // Index of the list is used for the filename, i.e., "1" -> "MARKERS1"
  private final List<Boolean> threadUseStatus;
  // A list of pending futures from async marker creation requests
  private final List<MarkerCreationFuture> markerCreationFutures = new ArrayList<>();
  private final int parallelism;
  private final Object markerCreationProcessingLock = new Object();
  // Early conflict detection strategy if enabled
  private final Option<TimelineServerBasedDetectionStrategy> conflictDetectionStrategy;
  private transient HoodieEngineContext hoodieEngineContext;
  // Last underlying file index used, for finding the next file index
  // in a round-robin fashion
  private int lastFileIndexUsed = -1;
  private boolean isMarkerTypeWritten = false;

  public MarkerDirState(String markerDirPath, int markerBatchNumThreads,
                        Option<TimelineServerBasedDetectionStrategy> conflictDetectionStrategy,
                        HoodieStorage storage, Registry metricsRegistry, int parallelism) {
    this.markerDirPath = new StoragePath(markerDirPath);
    this.storage = storage;
    this.metricsRegistry = metricsRegistry;
    this.hoodieEngineContext = new HoodieLocalEngineContext(storage.getConf());
    this.parallelism = parallelism;
    this.threadUseStatus =
        Stream.generate(() -> false).limit(markerBatchNumThreads).collect(Collectors.toList());
    this.conflictDetectionStrategy = conflictDetectionStrategy;
    // Lazy initialization of markers by reading MARKERS* files on the file system
    syncMarkersFromFileSystem();
  }

  /**
   * @return {@code true} if the marker directory exists in the system.
   */
  public boolean exists() {
    try {
      return storage.exists(markerDirPath);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * @return all markers in the marker directory.
   */
  public Set<String> getAllMarkers() {
    return allMarkers;
  }

  /**
   * Adds a {@code MarkerCreationCompletableFuture} instance from a marker
   * creation request to the queue.
   *
   * @param future  {@code MarkerCreationCompletableFuture} instance.
   */
  public void addMarkerCreationFuture(MarkerCreationFuture future) {
    synchronized (markerCreationFutures) {
      markerCreationFutures.add(future);
    }
  }

  /**
   * @return the next file index to use in a round-robin fashion,
   * or empty if no file is available.
   */
  public Option<Integer> getNextFileIndexToUse() {
    int fileIndex = -1;
    synchronized (markerCreationProcessingLock) {
      // Scans for the next free file index to use after {@code lastFileIndexUsed}
      for (int i = 0; i < threadUseStatus.size(); i++) {
        int index = (lastFileIndexUsed + 1 + i) % threadUseStatus.size();
        if (!threadUseStatus.get(index)) {
          fileIndex = index;
          threadUseStatus.set(index, true);
          break;
        }
      }

      if (fileIndex >= 0) {
        lastFileIndexUsed = fileIndex;
        return Option.of(fileIndex);
      }
    }
    return Option.empty();
  }

  /**
   * Marks the file as available to use again.
   *
   * @param fileIndex file index
   */
  public void markFileAsAvailable(int fileIndex) {
    synchronized (markerCreationProcessingLock) {
      threadUseStatus.set(fileIndex, false);
    }
  }

  /**
   * @return futures of pending marker creation requests and removes them from the list.
   */
  public List<MarkerCreationFuture> fetchPendingMarkerCreationRequests() {
    return getPendingMarkerCreationRequests(true);
  }

  /**
   * @param shouldClear Should clear the internal request list or not.
   * @return futures of pending marker creation requests.
   */
  public List<MarkerCreationFuture> getPendingMarkerCreationRequests(boolean shouldClear) {
    List<MarkerCreationFuture> pendingFutures;
    synchronized (markerCreationFutures) {
      if (markerCreationFutures.isEmpty()) {
        return new ArrayList<>();
      }
      pendingFutures = new ArrayList<>(markerCreationFutures);
      if (shouldClear) {
        markerCreationFutures.clear();
      }
    }
    return pendingFutures;
  }

  /**
   * Processes pending marker creation requests.
   *
   * @param pendingMarkerCreationFutures futures of pending marker creation requests
   * @param fileIndex file index to use to write markers
   */
  public void processMarkerCreationRequests(
      final List<MarkerCreationFuture> pendingMarkerCreationFutures, int fileIndex) {
    if (pendingMarkerCreationFutures.isEmpty()) {
      markFileAsAvailable(fileIndex);
      return;
    }

    LOG.debug("timeMs={} markerDirPath={} numRequests={} fileIndex={}",
        System.currentTimeMillis(), markerDirPath, pendingMarkerCreationFutures.size(), fileIndex);
    boolean shouldFlushMarkers = false;
    
    synchronized (markerCreationProcessingLock) {
      for (MarkerCreationFuture future : pendingMarkerCreationFutures) {
        String markerName = future.getMarkerName();
        boolean exists = allMarkers.contains(markerName);
        if (!exists) {
          if (conflictDetectionStrategy.isPresent()) {
            try {
              conflictDetectionStrategy.get().detectAndResolveConflictIfNecessary();
            } catch (HoodieEarlyConflictDetectionException he) {
              LOG.warn("Detected the write conflict due to a concurrent writer, "
                  + "failing the marker creation as the early conflict detection is enabled", he);
              future.setResult(false);
              continue;
            } catch (Exception e) {
              LOG.warn("Failed to execute early conflict detection.", e);
              // When early conflict detection fails to execute, we still allow the marker creation
              // to continue
              addMarkerToMap(fileIndex, markerName);
              future.setResult(true);
              shouldFlushMarkers = true;
              continue;
            }
          }
          addMarkerToMap(fileIndex, markerName);
          shouldFlushMarkers = true;
        }
        future.setResult(!exists);
      }

      if (!isMarkerTypeWritten) {
        // Create marker directory and write marker type to MARKERS.type
        writeMarkerTypeToFile();
        isMarkerTypeWritten = true;
      }
    }
    if (shouldFlushMarkers) {
      flushMarkersToFile(fileIndex);
    }
    markFileAsAvailable(fileIndex);

    for (MarkerCreationFuture future : pendingMarkerCreationFutures) {
      try {
        future.complete(jsonifyResult(
            future.getContext(), future.isSuccessful(), metricsRegistry));
      } catch (JsonProcessingException e) {
        throw new HoodieException("Failed to JSON encode the value", e);
      }
    }
  }

  /**
   * Deletes markers in the directory.
   *
   * @return {@code true} if successful; {@code false} otherwise.
   */
  public boolean deleteAllMarkers() {
    boolean result = FSUtils.deleteDir(hoodieEngineContext, storage, markerDirPath, parallelism);
    allMarkers.clear();
    fileMarkersMap.clear();
    return result;
  }

  /**
   * Syncs all markers maintained in the underlying files under the marker directory in the file system.
   */
  private void syncMarkersFromFileSystem() {
    Map<String, Set<String>> fileMarkersSetMap = MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
        markerDirPath.toString(), storage, hoodieEngineContext, parallelism);
    for (String markersFilePathStr : fileMarkersSetMap.keySet()) {
      Set<String> fileMarkers = fileMarkersSetMap.get(markersFilePathStr);
      if (!fileMarkers.isEmpty()) {
        int index = parseMarkerFileIndex(markersFilePathStr);
        if (index >= 0) {
          fileMarkersMap.put(index, new StringBuilder(StringUtils.join(",", fileMarkers)));
          allMarkers.addAll(fileMarkers);
        }
      }
    }

    try {
      if (MarkerUtils.doesMarkerTypeFileExist(storage, markerDirPath)) {
        isMarkerTypeWritten = true;
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  /**
   * Adds a new marker to the in-memory map.
   *
   * @param fileIndex  Marker file index number.
   * @param markerName Marker name.
   */
  private void addMarkerToMap(int fileIndex, String markerName) {
    allMarkers.add(markerName);
    StringBuilder stringBuilder = fileMarkersMap.computeIfAbsent(fileIndex, k -> new StringBuilder(16384));
    stringBuilder.append(markerName);
    stringBuilder.append('\n');
  }

  /**
   * Writes marker type, "TIMELINE_SERVER_BASED", to file.
   */
  private void writeMarkerTypeToFile() {
    try {
      if (!MarkerUtils.doesMarkerTypeFileExist(storage, markerDirPath)) {
        // There is no existing marker directory, create a new directory and write marker type
        storage.createDirectory(markerDirPath);
        MarkerUtils.writeMarkerTypeToFile(MarkerType.TIMELINE_SERVER_BASED, storage, markerDirPath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to write marker type file in " + markerDirPath
          + ": " + e.getMessage(), e);
    }
  }

  /**
   * Parses the marker file index from the marker file path.
   * <p>
   * E.g., if the marker file path is /tmp/table/.hoodie/.temp/000/MARKERS3, the index returned is 3.
   *
   * @param markerFilePathStr full path of marker file
   * @return the marker file index
   */
  private int parseMarkerFileIndex(String markerFilePathStr) {
    String markerFileName = new StoragePath(markerFilePathStr).getName();
    int prefixIndex = markerFileName.indexOf(MARKERS_FILENAME_PREFIX);
    if (prefixIndex < 0) {
      return -1;
    }
    try {
      return Integer.parseInt(markerFileName.substring(prefixIndex + MARKERS_FILENAME_PREFIX.length()));
    } catch (NumberFormatException nfe) {
      LOG.error("Failed to parse marker file index from " + markerFilePathStr);
      throw new HoodieException(nfe.getMessage(), nfe);
    }
  }

  /**
   * Flushes markers to the underlying file.
   *
   * @param markerFileIndex  file index to use.
   */
  private void flushMarkersToFile(int markerFileIndex) {
    LOG.debug("Write to {}/{}{}", markerDirPath, MARKERS_FILENAME_PREFIX, markerFileIndex);
    HoodieTimer timer = HoodieTimer.start();
    StoragePath markersFilePath = new StoragePath(
        markerDirPath, MARKERS_FILENAME_PREFIX + markerFileIndex);
    OutputStream outputStream = null;
    BufferedWriter bufferedWriter = null;
    try {
      outputStream = storage.create(markersFilePath);
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
      bufferedWriter.write(fileMarkersMap.get(markerFileIndex).toString());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to overwrite marker file " + markersFilePath, e);
    } finally {
      closeQuietly(bufferedWriter);
      closeQuietly(outputStream);
    }
    LOG.debug("{} written in {} ms", markersFilePath, timer.endTimer());
  }
}