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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MarkerBasedEarlyConflictDetectionRunnable implements Runnable {
  private static final Logger LOG = LogManager.getLogger(MarkerBasedEarlyConflictDetectionRunnable.class);

  private MarkerHandler markerHandler;
  private String markerDir;
  private String basePath;
  private FileSystem fs;
  private AtomicBoolean hasConflict;
  private long maxAllowableHeartbeatIntervalInMs;
  private Set<HoodieInstant> completedCommits;
  private final boolean checkCommitConflict;

  public MarkerBasedEarlyConflictDetectionRunnable(AtomicBoolean hasConflict, MarkerHandler markerHandler, String markerDir,
                                                   String basePath, FileSystem fileSystem, long maxAllowableHeartbeatIntervalInMs,
                                                   Set<HoodieInstant> completedCommits, boolean checkCommitConflict) {
    this.markerHandler = markerHandler;
    this.markerDir = markerDir;
    this.basePath = basePath;
    this.fs = fileSystem;
    this.hasConflict = hasConflict;
    this.maxAllowableHeartbeatIntervalInMs = maxAllowableHeartbeatIntervalInMs;
    this.completedCommits = completedCommits;
    this.checkCommitConflict = checkCommitConflict;
  }

  @Override
  public void run() {
    // If a conflict among multiple writers is already detected,
    // there is no need to run the detection again.
    if (hasConflict.get()) {
      return;
    }

    try {
      Set<String> pendingMarkers = markerHandler.getPendingMarkersToProcess(markerDir);

      if (!fs.exists(new Path(markerDir)) && pendingMarkers.isEmpty()) {
        return;
      }

      HoodieTimer timer = HoodieTimer.start();
      Set<String> currentInstantAllMarkers = new HashSet<>();
      // We need to check both the markers already written to the storage
      // and the markers from the requests pending processing.
      currentInstantAllMarkers.addAll(markerHandler.getAllMarkers(markerDir));
      currentInstantAllMarkers.addAll(pendingMarkers);
      Path tempPath = new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME);

      List<Path> instants = MarkerUtils.getAllMarkerDir(tempPath, fs);

      HoodieTableMetaClient metaClient =
          HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(basePath)
              .setLoadActiveTimelineOnLoad(true).build();
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

      List<String> candidate = MarkerUtils.getCandidateInstants(activeTimeline, instants,
          MarkerUtils.markerDirToInstantTime(markerDir), maxAllowableHeartbeatIntervalInMs, fs, basePath);
      Set<String> tableMarkers = candidate.stream().flatMap(instant -> {
        return MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(instant, fs, new HoodieLocalEngineContext(new Configuration()), 100)
            .values().stream().flatMap(Collection::stream);
      }).collect(Collectors.toSet());

      Set<String> currentFileIDs = currentInstantAllMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet());
      Set<String> tableFilesIDs = tableMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet());

      currentFileIDs.retainAll(tableFilesIDs);
      if (!currentFileIDs.isEmpty()
          || (checkCommitConflict && MarkerUtils.hasCommitConflict(activeTimeline,
          currentInstantAllMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet()), completedCommits))) {
        LOG.warn("Conflict writing detected based on markers!\n"
            + "Conflict markers: " + currentInstantAllMarkers + "\n"
            + "Table markers: " + tableMarkers);
        hasConflict.compareAndSet(false, true);
      }
      LOG.info("Finish batching marker-based conflict detection in " + timer.endTimer() + " ms");

    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }
  }
}
