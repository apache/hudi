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

import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MarkerCheckerRunnable implements Runnable {
  private static final Logger LOG = LogManager.getLogger(MarkerCheckerRunnable.class);

  private MarkerHandler markerHandler;
  private String markerDir;
  private String basePath;
  private HoodieEngineContext hoodieEngineContext;
  private int parallelism;
  private FileSystem fs;
  private AtomicBoolean hasConflict;

  public MarkerCheckerRunnable(AtomicBoolean hasConflict, MarkerHandler markerHandler, String markerDir, String basePath,
                               HoodieEngineContext hoodieEngineContext, int parallelism, FileSystem fileSystem) {
    this.markerHandler = markerHandler;
    this.markerDir = markerDir;
    this.basePath = basePath;
    this.hoodieEngineContext = hoodieEngineContext;
    this.parallelism = parallelism;
    this.fs = fileSystem;
    this.hasConflict = hasConflict;
  }

  @Override
  public void run() {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Set<String> currentInstantAllMarkers = markerHandler.getAllMarkers(markerDir);

    Path tempPath = new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME);
    try {
      List<Path> instants = Arrays.stream(fs.listStatus(tempPath)).map(FileStatus::getPath).collect(Collectors.toList());
      // TODO clean instants
      Set<String> tableMarkers = instants.stream().map(Path::toString)
          .filter(dir -> !dir.contains(markerDir) && !dir.equalsIgnoreCase(markerDir)).flatMap(instant -> {
            return readTimelineServerBasedMarkersFromFileSystem(instant, fs).stream();
          }).collect(Collectors.toSet());

      Set<String> currentFileIDs = currentInstantAllMarkers.stream().map(this::makerToFileID).collect(Collectors.toSet());
      Set<String> tableFilesIDs = tableMarkers.stream().map(this::makerToFileID).collect(Collectors.toSet());

      currentFileIDs.retainAll(tableFilesIDs);

      if (!currentFileIDs.isEmpty()) {
        LOG.info("Conflict writing detected based on markers!\n"
        + "Conflict markers: " + currentInstantAllMarkers + "\n"
        + "Table markers: " + tableMarkers);
        hasConflict.compareAndSet(false, true);
      }
      LOG.info("Finish batch marker checker in " + timer.endTimer() + " ms");

    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }
  }

  /**
   * Get fileID from full marker path, for example:
   * 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0_85-15-1390_20220620181735781.parquet.marker.MERGE
   *    ==> get 932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0
   * @param marker
   * @return
   */
  private String makerToFileID(String marker) {
    if (marker.contains(Path.SEPARATOR) && marker.contains("_")) {
      String[] splits = marker.split(Path.SEPARATOR);
      String fileName = splits[splits.length - 1];
      String[] ele = fileName.split("_");
      return ele[0];
    } else {
      return "";
    }
  }

  private Set<String> readTimelineServerBasedMarkersFromFileSystem(String markerDir, FileSystem fileSystem) {
    Path dirPath = new Path(markerDir);
    try {
      if (fileSystem.exists(dirPath)) {
        Predicate<FileStatus> prefixFilter = fileStatus ->
            fileStatus.getPath().getName().startsWith(MARKERS_FILENAME_PREFIX);
        Predicate<FileStatus> markerTypeFilter = fileStatus ->
            !fileStatus.getPath().getName().equals(MARKER_TYPE_FILENAME);

        CopyOnWriteArraySet<String> result = new CopyOnWriteArraySet<>();
        FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);
        List<String> subPaths = Arrays.stream(fileStatuses)
            .filter(prefixFilter.and(markerTypeFilter))
            .map(fileStatus -> fileStatus.getPath().toString())
            .collect(Collectors.toList());

        if (subPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fileSystem.getConf());
          subPaths.stream().parallel().forEach(subPath -> {
            result.addAll(MarkerUtils.readMarkersFromFile(new Path(subPath), conf, true));
          });
        }
        return result;
      }
      return new HashSet<>();
    } catch (Exception ioe) {
      LOG.warn("IOException occurs during read TimelineServer based markers from fileSystem", ioe);
      return new HashSet<>();
    }
  }
}
