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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FileIOUtils.closeQuietly;

/**
 * Stores the state of a marker directory.
 *
 * The operations inside this class is designed to be thread-safe.
 */
public class MarkerDirState implements Serializable {
  public static final String MARKERS_FILENAME_PREFIX = "MARKERS";
  private static final Logger LOG = LogManager.getLogger(MarkerDirState.class);
  // Marker directory
  private final String markerDirPath;
  private final FileSystem fileSystem;
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
  private final List<MarkerCreationCompletableFuture> markerCreationFutures = new ArrayList<>();
  private final int parallelism;
  private transient HoodieEngineContext hoodieEngineContext;
  // Last underlying file index used, for finding the next file index
  // in a round-robin fashion
  private int lastFileIndex = 0;
  private boolean hasFirstRequest = false;

  public MarkerDirState(String markerDirPath, int markerBatchNumThreads,
                        FileSystem fileSystem, HoodieEngineContext hoodieEngineContext, int parallelism) {
    this.markerDirPath = markerDirPath;
    this.fileSystem = fileSystem;
    this.hoodieEngineContext = hoodieEngineContext;
    this.parallelism = parallelism;

    List<Boolean> isMarkerFileInUseList = new ArrayList<>(markerBatchNumThreads);
    for (int i = 0; i < markerBatchNumThreads; i++) {
      isMarkerFileInUseList.add(false);
    }
    this.threadUseStatus = Collections.synchronizedList(isMarkerFileInUseList);
  }

  /**
   * @return  {@code true} if the marker directory exists in the system.
   */
  public boolean exists() {
    try {
      return fileSystem.exists(new Path(markerDirPath));
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * @return all markers in the marker directory.
   */
  public Set<String> getAllMarkers() {
    maybeSyncOnFirstRequest();
    return allMarkers;
  }

  /**
   * Adds a {@code MarkerCreationCompletableFuture} instance from a marker
   * creation request to the queue.
   *
   * @param future  {@code MarkerCreationCompletableFuture} instance.
   */
  public void addMarkerCreationFuture(MarkerCreationCompletableFuture future) {
    synchronized (markerCreationFutures) {
      markerCreationFutures.add(future);
    }
  }

  /**
   * Processes pending marker creation requests.
   *
   * @return A list of processed futures
   */
  public List<MarkerCreationCompletableFuture> processMarkerCreationRequests() {
    List<MarkerCreationCompletableFuture> futuresToRemove = new ArrayList<>();
    if (markerCreationFutures.isEmpty()) {
      return futuresToRemove;
    }
    maybeSyncOnFirstRequest();

    int fileIndex = getNextFileIndexToUse();
    if (fileIndex < 0) {
      LOG.debug("All marker files are busy, skip batch processing of create marker requests in " + markerDirPath);
      return futuresToRemove;
    }
    LOG.debug("timeMs=" + System.currentTimeMillis() + " fileIndex=" + fileIndex);

    synchronized (markerCreationFutures) {
      for (MarkerCreationCompletableFuture future : markerCreationFutures) {
        String markerName = future.getMarkerName();
        boolean exists = allMarkers.contains(markerName);
        if (!exists) {
          allMarkers.add(markerName);
          StringBuilder stringBuilder = fileMarkersMap.computeIfAbsent(fileIndex, k -> new StringBuilder(16384));
          stringBuilder.append(markerName);
          stringBuilder.append('\n');
        }
        future.setResult(!exists);
        futuresToRemove.add(future);
      }
      markerCreationFutures.removeAll(futuresToRemove);
    }
    flushMarkersToFile(fileIndex);

    synchronized (threadUseStatus) {
      threadUseStatus.set(fileIndex, false);
    }
    return futuresToRemove;
  }

  /**
   * Deletes markers in the directory.
   *
   * @return {@code true} if successful; {@code false} otherwise.
   */
  public boolean deleteAllMarkers() {
    Path dirPath = new Path(markerDirPath);
    boolean result = false;
    try {
      if (fileSystem.exists(dirPath)) {
        FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
            .map(fileStatus -> fileStatus.getPath().toString())
            .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fileSystem.getConf());
          int actualParallelism = Math.min(markerDirSubPaths.size(), parallelism);
          hoodieEngineContext.foreach(markerDirSubPaths, subPathStr -> {
            Path subPath = new Path(subPathStr);
            FileSystem fileSystem = subPath.getFileSystem(conf.get());
            fileSystem.delete(subPath, true);
          }, actualParallelism);
        }

        result = fileSystem.delete(dirPath, true);
        LOG.info("Removing marker directory at " + dirPath);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    allMarkers.clear();
    fileMarkersMap.clear();
    return result;
  }

  /**
   * Syncs the markers from the underlying files for the first request.
   */
  private void maybeSyncOnFirstRequest() {
    if (!hasFirstRequest) {
      syncAllMarkersFromFile();
      hasFirstRequest = true;
    }
  }

  /**
   * @return the next file index to use in a round-robin fashion
   */
  private int getNextFileIndexToUse() {
    int fileIndex = -1;
    synchronized (threadUseStatus) {
      for (int i = 0; i < threadUseStatus.size(); i++) {
        int index = (lastFileIndex + 1 + i) % threadUseStatus.size();
        if (!threadUseStatus.get(index)) {
          fileIndex = index;
          threadUseStatus.set(index, true);
          break;
        }
      }
      if (fileIndex >= 0) {
        lastFileIndex = fileIndex;
      }
    }
    return fileIndex;
  }

  /**
   * Syncs all markers maintained in the underlying files from the marker directory in the file system.
   */
  private void syncAllMarkersFromFile() {
    Path dirPath = new Path(markerDirPath);
    try {
      if (fileSystem.exists(dirPath)) {
        FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
            .map(fileStatus -> fileStatus.getPath().toString())
            .filter(pathStr -> pathStr.contains(MARKERS_FILENAME_PREFIX))
            .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fileSystem.getConf());
          int actualParallelism = Math.min(markerDirSubPaths.size(), parallelism);
          Map<String, Set<String>> fileMarkersSetMap =
              hoodieEngineContext.mapToPair(markerDirSubPaths, markersFilePathStr -> {
                Path markersFilePath = new Path(markersFilePathStr);
                FileSystem fileSystem = markersFilePath.getFileSystem(conf.get());
                FSDataInputStream fsDataInputStream = null;
                BufferedReader bufferedReader = null;
                Set<String> markers = new HashSet<>();
                try {
                  LOG.debug("Read marker file: " + markersFilePathStr);
                  fsDataInputStream = fileSystem.open(markersFilePath);
                  bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));
                  markers = bufferedReader.lines().collect(Collectors.toSet());
                  bufferedReader.close();
                  fsDataInputStream.close();
                } catch (IOException e) {
                  throw new HoodieIOException("Failed to read MARKERS file " + markerDirPath, e);
                } finally {
                  closeQuietly(bufferedReader);
                  closeQuietly(fsDataInputStream);
                }
                return new ImmutablePair<>(markersFilePathStr, markers);
              }, actualParallelism);

          for (String markersFilePathStr: fileMarkersSetMap.keySet()) {
            Set<String> fileMarkers = fileMarkersSetMap.get(markersFilePathStr);
            if (!fileMarkers.isEmpty()) {
              int index = getMarkerFileIndex(markersFilePathStr);

              if (index >= 0) {
                fileMarkersMap.put(index, new StringBuilder(StringUtils.join(",", fileMarkers)));
                allMarkers.addAll(fileMarkers);
              }
            }
          }
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Gets the marker file index from the marker file path.
   *
   * E.g., if the marker file path is /tmp/table/.hoodie/.temp/000/MARKERS3, the index returned is 3.
   *
   * @param markerFilePathStr full path of marker file
   * @return the marker file index
   */
  private int getMarkerFileIndex(String markerFilePathStr) {
    String markerFileName = new Path(markerFilePathStr).getName();
    int prefixIndex = markerFileName.indexOf(MARKERS_FILENAME_PREFIX);
    if (prefixIndex < 0) {
      return -1;
    }
    try {
      return Integer.parseInt(markerFileName.substring(prefixIndex + MARKERS_FILENAME_PREFIX.length()));
    } catch (NumberFormatException nfe) {
      nfe.printStackTrace();
    }
    return -1;
  }

  /**
   * Flushes markers to the underlying file.
   *
   * @param markerFileIndex  file index to use.
   */
  private void flushMarkersToFile(int markerFileIndex) {
    LOG.debug("Write to " + markerDirPath + "/" + MARKERS_FILENAME_PREFIX + markerFileIndex);
    HoodieTimer timer = new HoodieTimer().startTimer();
    Path markersFilePath = new Path(markerDirPath, MARKERS_FILENAME_PREFIX + markerFileIndex);
    Path dirPath = markersFilePath.getParent();
    try {
      if (!fileSystem.exists(dirPath)) {
        fileSystem.mkdirs(dirPath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + dirPath, e);
    }
    FSDataOutputStream fsDataOutputStream = null;
    BufferedWriter bufferedWriter = null;
    try {
      fsDataOutputStream = fileSystem.create(markersFilePath);
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
      bufferedWriter.write(fileMarkersMap.get(markerFileIndex).toString());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to overwrite marker file " + markersFilePath, e);
    } finally {
      closeQuietly(bufferedWriter);
      closeQuietly(fsDataOutputStream);
    }
    LOG.debug(markersFilePath.toString() + " written in " + timer.endTimer() + " ms");
  }
}