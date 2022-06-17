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

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class MarkerCollectorRunnable implements Runnable {
  private static final Logger LOG = LogManager.getLogger(MarkerCollectorRunnable.class);

  private MarkerHandler markerHandler;
  private String markerDir;
  private String basePath;
  private HoodieEngineContext hoodieEngineContext;
  private int parallelism;
  private FileSystem fs;
  // Marker directory -> markers under current directory
  private ConcurrentHashMap<String, MarkerInfo> instant2MarkerInfo;

  public MarkerCollectorRunnable(MarkerHandler markerHandler, String markerDir, String basePath,
                                 HoodieEngineContext hoodieEngineContext, int parallelism, FileSystem fileSystem,
                                 ConcurrentHashMap<String, MarkerInfo> instant2MarkerInfo) {
    this.markerHandler = markerHandler;
    this.markerDir = markerDir;
    this.basePath = basePath;
    this.hoodieEngineContext = hoodieEngineContext;
    this.parallelism = parallelism;
    this.fs = fileSystem;
    this.instant2MarkerInfo = instant2MarkerInfo;
  }

  @Override
  public void run() {
    Path tempPath = new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME);
    try {
      List<Path> instants = Arrays.stream(fs.listStatus(tempPath)).map(FileStatus::getPath).collect(Collectors.toList());

      cleanUp(instants);
      instants.forEach(instant -> {
        if (instant2MarkerInfo.containsKey(instant.toString())) {
          refreshMarkerInfo(instant, instant2MarkerInfo.get(instant.toString()));
        } else {
          MarkerInfo markerInfo = initialMarkerInfo(instant);
          instant2MarkerInfo.put(instant.toString(), markerInfo);
        }
      });
    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }
  }

  private void refreshMarkerInfo(Path instant, MarkerInfo markerInfo) {
    try {
      List<Path> markerFiles = Arrays.stream(fs.listStatus(instant)).map(FileStatus::getPath).collect(Collectors.toList());
      HashSet<String> allMarkerFilesInMarkerInfo = markerInfo.getAllMarkerFiles();
      List<Path> newMarkerFile = markerFiles.stream().filter(makerFile -> {
        return !allMarkerFilesInMarkerInfo.contains(makerFile.toString());
      }).collect(Collectors.toList());

      newMarkerFile.forEach(markerFile -> {
        CopyOnWriteArraySet<String> markers = new CopyOnWriteArraySet<>();
        Tailer tailer = Tailer.create(new File(markerFile.toString()), new FileMonitor(markers), 500);
        markerInfo.add(markerFile.toString(), markers, tailer);
      });

    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }
  }

  /**
   * Removed finished marker or dead marker
   * only get active instant
   * @param instants
   */
  private void cleanUp(List<Path> instants) {
    // TODO remove dead instant based on heart-beat
    List<String> currentInstantFromFS = instants.stream().map(Path::toString).collect(Collectors.toList());

    // pick up all the instant2MarkerInfo's instants which are not existed on fs.
    List<String> outOfDateInstantInMap = instant2MarkerInfo.keySet().stream()
        .filter(instant -> !currentInstantFromFS.contains(instant)).collect(Collectors.toList());
    // remove all these offline instant.
    outOfDateInstantInMap.forEach(offInstant -> {
      MarkerInfo removed = instant2MarkerInfo.remove(offInstant);
      removed.close();
    });
  }

  private MarkerInfo initialMarkerInfo(Path instant) {
    // marker0 -> markers
    ConcurrentHashMap<String, CopyOnWriteArraySet<String>> markerFile2markers = new ConcurrentHashMap<>();
    // marker0 -> tail
    ConcurrentHashMap<String, Tailer> markerFile2Tailer = new ConcurrentHashMap<>();

    try {
      List<Path> markerFiles = Arrays.stream(fs.listStatus(instant)).map(FileStatus::getPath).collect(Collectors.toList());

      markerFiles.stream().parallel().forEach(markerFile -> {

        CopyOnWriteArraySet<String> markers = new CopyOnWriteArraySet<>();
        Tailer tailer = Tailer.create(new File(markerFile.toString()), new FileMonitor(markers), 500);
        markerFile2markers.put(markerFile.toString(), markers);
        markerFile2Tailer.put(markerFile.toString(), tailer);
      });
    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }

    return new MarkerInfo(instant.toString(), markerFile2markers, markerFile2Tailer);
  }

  class FileMonitor extends TailerListenerAdapter {
    private CopyOnWriteArraySet<String> markers;

    FileMonitor(CopyOnWriteArraySet<String> markers) {
      this.markers = markers;
    }

    @Override
    public void handle(String line) {
      markers.add(line);
    }
  }
}
