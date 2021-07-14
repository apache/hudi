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

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.timeline.service.RequestHandler.jsonifyResult;

public class MarkerHandler extends Handler {
  public static final String MARKERS_FILENAME_PREFIX = "MARKERS";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LogManager.getLogger(MarkerHandler.class);
  private static final long MARGIN_TIME_MS = 10L;

  private final Registry metricsRegistry;
  private final ScheduledExecutorService executorService;
  // {markerDirPath -> all markers}
  private final Map<String, Set<String>> allMarkersMap = new HashMap<>();
  // {markerDirPath -> {markerFileIndex -> markers}}
  private final Map<String, Map<Integer, StringBuilder>> fileMarkersMap = new HashMap<>();
  private final List<CreateMarkerCompletableFuture> createMarkerFutures = new ArrayList<>();
  private final List<Boolean> markerFilesUseStatus;
  private final long batchIntervalMs;
  private final int parallelism;
  private volatile Object createMarkerRequestlockObject = new Object();
  private long nextBatchProcessTimeMs = 0L;

  public MarkerHandler(Configuration conf, FileSystem fileSystem, FileSystemViewManager viewManager, Registry metricsRegistry,
                       int batchNumThreads, long batchIntervalMs, int parallelism) throws IOException {
    super(conf, fileSystem, viewManager);
    LOG.info("*** MarkerHandler FileSystem: " + this.fileSystem.getScheme());
    LOG.info("*** MarkerHandler Params: batchNumThreads=" + batchNumThreads + " batchIntervalMs=" + batchIntervalMs + "ms");
    this.metricsRegistry = metricsRegistry;
    this.batchIntervalMs = batchIntervalMs;
    this.parallelism = parallelism;
    this.executorService = Executors.newScheduledThreadPool(batchNumThreads);
    List<Boolean> isMarkerFileInUseList = new ArrayList<>(batchNumThreads);
    for (int i = 0; i < batchNumThreads; i++) {
      isMarkerFileInUseList.add(false);
    }
    this.markerFilesUseStatus = Collections.synchronizedList(isMarkerFileInUseList);
  }

  public Set<String> getAllMarkers(String markerDirPath) {
    return allMarkersMap.getOrDefault(markerDirPath, new HashSet<>());
  }

  public Set<String> getCreateAndMergeMarkers(String markerDirPath) {
    return allMarkersMap.getOrDefault(markerDirPath, new HashSet<>()).stream()
        .filter(markerName -> !markerName.endsWith(IOType.APPEND.name()))
        .collect(Collectors.toSet());
  }

  public CompletableFuture<String> createMarker(Context context, String markerDirPath, String markerName) {
    LOG.info("Request: create marker " + markerDirPath + " " + markerName);
    CreateMarkerCompletableFuture future = new CreateMarkerCompletableFuture(context, markerDirPath, markerName);
    synchronized (createMarkerRequestlockObject) {
      createMarkerFutures.add(future);
      long currTimeMs = System.currentTimeMillis();
      if (currTimeMs >= nextBatchProcessTimeMs - MARGIN_TIME_MS) {
        nextBatchProcessTimeMs += batchIntervalMs * (Math.max((currTimeMs - nextBatchProcessTimeMs) / batchIntervalMs + 1L, 1L));

        long waitMs = nextBatchProcessTimeMs - System.currentTimeMillis();
        executorService.schedule(
            new BatchCreateMarkerRunnable(), Math.max(0L, waitMs), TimeUnit.MILLISECONDS);
        LOG.info("Wait for " + waitMs + " ms, next batch time: " + nextBatchProcessTimeMs);
      }
    }
    return future;
  }

  public Boolean deleteMarkers(String markerDir) {
    Path markerDirPath = new Path(markerDir);
    try {
      if (fileSystem.exists(markerDirPath)) {
        FileStatus[] fileStatuses = fileSystem.listStatus(markerDirPath);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
            .map(fileStatus -> fileStatus.getPath().toString())
            .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          for (String subPathStr: markerDirSubPaths) {
            fileSystem.delete(new Path(subPathStr), true);
          }
        }

        boolean result = fileSystem.delete(markerDirPath, true);
        LOG.info("Removing marker directory at " + markerDirPath);
        return result;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return false;
  }

  private Set<String> getAllMarkersFromFile(String markerDirPath) {
    LOG.info("Get all markers from " + markerDirPath);
    Path markersFilePath = new Path(markerDirPath, MARKERS_FILENAME_PREFIX);
    Set<String> markers = new HashSet<>();
    try {
      if (fileSystem.exists(markersFilePath)) {
        LOG.info("Marker file exists: " + markersFilePath.toString());
        FSDataInputStream fsDataInputStream = fileSystem.open(markersFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));
        markers = bufferedReader.lines().collect(Collectors.toSet());
        bufferedReader.close();
        fsDataInputStream.close();
      } else {
        LOG.info("Marker file not exist: " + markersFilePath.toString());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read MARKERS file " + markerDirPath, e);
    }
    return markers;
  }

  private class CreateMarkerCompletableFuture extends CompletableFuture<String> {
    private final Context context;
    private final String markerDirPath;
    private final String markerName;
    private boolean result;
    private final long startTimeMs;

    public CreateMarkerCompletableFuture(Context context, String markerDirPath, String markerName) {
      super();
      this.startTimeMs = System.currentTimeMillis();
      this.context = context;
      this.markerDirPath = markerDirPath;
      this.markerName = markerName;
      this.result = false;
    }

    public Context getContext() {
      return context;
    }

    public String getMarkerDirPath() {
      return markerDirPath;
    }

    public String getMarkerName() {
      return markerName;
    }

    public boolean getResult() {
      return result;
    }

    public void setResult(boolean result) {
      LOG.info("*** Request queued for " + (System.currentTimeMillis() - startTimeMs) + " ms");
      this.result = result;
    }
  }

  private class BatchCreateMarkerRunnable implements Runnable {

    @Override
    public void run() {
      LOG.info("Start processing create marker requests");
      long startTimeMs = System.currentTimeMillis();
      List<CreateMarkerCompletableFuture> futuresToRemove = new ArrayList<>();
      Set<String> updatedMarkerDirPaths = new HashSet<>();
      int markerFileIndex = -1;
      synchronized (markerFilesUseStatus) {
        for (int i = 0; i < markerFilesUseStatus.size(); i++) {
          if (!markerFilesUseStatus.get(i)) {
            markerFileIndex = i;
            markerFilesUseStatus.set(i, true);
            break;
          }
        }
        if (markerFileIndex < 0) {
          LOG.info("All marker files are busy, skip batch processing of create marker requests in " + (System.currentTimeMillis() - startTimeMs) + " ms");
          return;
        }
      }

      LOG.info("timeMs=" + System.currentTimeMillis() + " markerFileIndex=" + markerFileIndex);
      synchronized (createMarkerRequestlockObject) {
        LOG.info("Iterating through existing requests, size=" + createMarkerFutures.size());
        for (CreateMarkerCompletableFuture future : createMarkerFutures) {
          String markerDirPath = future.getMarkerDirPath();
          String markerName = future.getMarkerName();
          LOG.info("--- markerDirPath=" + markerDirPath + " markerName=" + markerName);
          Set<String> allMarkers = allMarkersMap.computeIfAbsent(markerDirPath, k -> new HashSet<>());
          StringBuilder stringBuilder = fileMarkersMap.computeIfAbsent(markerDirPath, k -> new HashMap<>())
              .computeIfAbsent(markerFileIndex, k -> new StringBuilder(16384));
          boolean exists = allMarkers.contains(markerName);
          if (!exists) {
            allMarkers.add(markerName);
            stringBuilder.append(markerName);
            stringBuilder.append('\n');
            updatedMarkerDirPaths.add(markerDirPath);
          }
          future.setResult(!exists);
          futuresToRemove.add(future);
        }
        createMarkerFutures.removeAll(futuresToRemove);
      }
      LOG.info("Flush to MARKERS file .. ");
      flushMarkersToFile(updatedMarkerDirPaths, markerFileIndex);
      markerFilesUseStatus.set(markerFileIndex, false);
      LOG.info("Resolve request futures .. ");
      for (CreateMarkerCompletableFuture future : futuresToRemove) {
        try {
          future.complete(jsonifyResult(future.getContext(), future.getResult(), metricsRegistry, OBJECT_MAPPER, LOG));
        } catch (JsonProcessingException e) {
          throw new HoodieException("Failed to JSON encode the value", e);
        }
      }
      LOG.info("Finish batch processing of create marker requests in " + (System.currentTimeMillis() - startTimeMs) + " ms");
    }

    private void flushMarkersToFile(Set<String> updatedMarkerDirPaths, int markerFileIndex) {
      long flushStartTimeMs = System.currentTimeMillis();
      for (String markerDirPath : updatedMarkerDirPaths) {
        LOG.info("Write to " + markerDirPath);
        long startTimeMs = System.currentTimeMillis();
        Path markersFilePath = new Path(markerDirPath, MARKERS_FILENAME_PREFIX + markerFileIndex);
        Path dirPath = markersFilePath.getParent();
        try {
          if (!fileSystem.exists(dirPath)) {
            fileSystem.mkdirs(dirPath);
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to make dir " + dirPath, e);
        }
        try {
          LOG.info("Create " + markersFilePath.toString());
          FSDataOutputStream fsDataOutputStream = fileSystem.create(markersFilePath);

          BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
          bufferedWriter.write(fileMarkersMap.get(markerDirPath).get(markerFileIndex).toString());
          bufferedWriter.close();
          fsDataOutputStream.close();
        } catch (IOException e) {
          throw new HoodieIOException("Failed to overwrite marker file " + markersFilePath, e);
        }
        LOG.info("MARKERS written in " + (System.currentTimeMillis() - startTimeMs) + " ms");
      }
      LOG.info("All MARKERS flushed in " + (System.currentTimeMillis() - flushStartTimeMs) + " ms");
    }
  }
}
