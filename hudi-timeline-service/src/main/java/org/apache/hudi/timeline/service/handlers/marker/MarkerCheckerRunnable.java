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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private long maxAllowableHeartbeatIntervalInMs;

  public MarkerCheckerRunnable(AtomicBoolean hasConflict, MarkerHandler markerHandler, String markerDir, String basePath,
                               HoodieEngineContext hoodieEngineContext, int parallelism, FileSystem fileSystem, long maxAllowableHeartbeatIntervalInMs) {
    this.markerHandler = markerHandler;
    this.markerDir = markerDir;
    this.basePath = basePath;
    this.hoodieEngineContext = hoodieEngineContext;
    this.parallelism = parallelism;
    this.fs = fileSystem;
    this.hasConflict = hasConflict;
    this.maxAllowableHeartbeatIntervalInMs = maxAllowableHeartbeatIntervalInMs;
  }

  @Override
  public void run() {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Set<String> currentInstantAllMarkers = markerHandler.getAllMarkers(markerDir);

    Path tempPath = new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME);
    try {
      List<Path> instants = MarkerUtils.getAllMarkerDir(tempPath, fs);
      List<String> candidate = getCandidateInstants(instants, markerToInstantTime(markerDir));
      Set<String> tableMarkers = candidate.stream().flatMap(instant -> {
        return MarkerUtils.readTimelineServerBasedMarkersFromFileSystemLocally(instant, fs).stream();
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
   * Get Candidate Instant to do conflict checking:
   * 1. Skip current writer related instant(currentInstantTime)
   * 2. Skip all instants after currentInstantTime
   * 3. Skip dead writers related instants based on heart-beat
   * @param instants
   * @return
   */
  private List<String> getCandidateInstants(List<Path> instants, String currentInstantTime) {
    return instants.stream().map(Path::toString).filter(instantPath -> {
      String instantTime = markerToInstantTime(instantPath);
      return instantTime.compareToIgnoreCase(currentInstantTime) < 0;
    }).filter(instantPath -> {
      try {
        return !isHeartbeatExpired(markerToInstantTime(instantPath));
      } catch (IOException e) {
        return false;
      }
    }).collect(Collectors.toList());
  }

  /**
   * Get fileID from full marker path, for example:
   * 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0_85-15-1390_20220620181735781.parquet.marker.MERGE
   *    ==> get 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0
   * @param marker
   * @return
   */
  private String makerToFileID(String marker) {
    String[] ele = marker.split("_");
    return ele[0];
  }

  /**
   * Get instantTime from full marker path, for example:
   * 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0_85-15-1390_20220620181735781.parquet.marker.MERGE
   *    ==> 20220620181735781
   * @param marker
   * @return
   */
  private String markerToInstantTime(String marker) {
    String[] ele = marker.split("_");
    String[] splits = ele[ele.length - 1].split("\\.");
    return splits[0];
  }

  /**
   * Use modification time as last heart beat time
   * @param fs
   * @param basePath
   * @param instantTime
   * @return
   * @throws IOException
   */
  public Long getLastHeartbeatTime(FileSystem fs, String basePath, String instantTime) throws IOException {
    Path heartbeatFilePath = new Path(HoodieTableMetaClient.getHeartbeatFolderPath(basePath) + Path.SEPARATOR + instantTime);
    if (fs.exists(heartbeatFilePath)) {
      return fs.getFileStatus(heartbeatFilePath).getModificationTime();
    } else {
      // NOTE : This can happen when a writer is upgraded to use lazy cleaning and the last write had failed
      return 0L;
    }
  }

  public boolean isHeartbeatExpired(String instantTime) throws IOException {
    Long currentTime = System.currentTimeMillis();
    Long lastHeartbeatTime = getLastHeartbeatTime(fs, basePath, instantTime);
    if (currentTime - lastHeartbeatTime > this.maxAllowableHeartbeatIntervalInMs) {
      LOG.warn("Heartbeat expired, for instant: " + instantTime);
      return true;
    }
    return false;
  }
}
