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
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MarkerBasedEarlyConflictDetectionRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MarkerBasedEarlyConflictDetectionRunnable.class);

  private final MarkerHandler markerHandler;
  private final String markerDir;
  private final String basePath;
  private final HoodieStorage storage;
  private final AtomicBoolean hasConflict;
  private final long maxAllowableHeartbeatIntervalInMs;
  private final Set<HoodieInstant> completedCommits;
  private final boolean checkCommitConflict;

  public MarkerBasedEarlyConflictDetectionRunnable(AtomicBoolean hasConflict, MarkerHandler markerHandler,
                                                   String markerDir,
                                                   String basePath, HoodieStorage storage,
                                                   long maxAllowableHeartbeatIntervalInMs,
                                                   Set<HoodieInstant> completedCommits, boolean checkCommitConflict) {
    this.markerHandler = markerHandler;
    this.markerDir = markerDir;
    this.basePath = basePath;
    this.storage = storage;
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

      if (!storage.exists(new StoragePath(markerDir)) && pendingMarkers.isEmpty()) {
        return;
      }

      HoodieTimer timer = HoodieTimer.start();
      Set<String> currentInstantAllMarkers = new HashSet<>();
      // We need to check both the markers already written to the storage
      // and the markers from the requests pending processing.
      currentInstantAllMarkers.addAll(markerHandler.getAllMarkers(markerDir));
      currentInstantAllMarkers.addAll(pendingMarkers);
      StoragePath tempPath = new StoragePath(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);

      List<StoragePath> instants = MarkerUtils.getAllMarkerDir(tempPath, storage);

      HoodieTableMetaClient metaClient =
          HoodieTableMetaClient.builder().setConf(storage.getConf().newInstance()).setBasePath(basePath)
              .setLoadActiveTimelineOnLoad(true).build();
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

      List<String> candidate = MarkerUtils.getCandidateInstants(activeTimeline, instants,
          MarkerUtils.markerDirToInstantTime(markerDir), maxAllowableHeartbeatIntervalInMs,
          storage, basePath);
      Set<String> tableMarkers = candidate.stream().flatMap(instant -> {
        return MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(instant, storage,
                new HoodieLocalEngineContext(storage.getConf().newInstance()), 100)
            .values().stream().flatMap(Collection::stream);
      }).collect(Collectors.toSet());

      Set<String> currentFileIDs = currentInstantAllMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet());
      Set<String> tableFilesIDs = tableMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet());

      currentFileIDs.retainAll(tableFilesIDs);
      if (!currentFileIDs.isEmpty()
          || (checkCommitConflict && MarkerUtils.hasCommitConflict(activeTimeline,
          currentInstantAllMarkers.stream().map(MarkerUtils::makerToPartitionAndFileID).collect(Collectors.toSet()), completedCommits))) {
        LOG.error("Conflict writing detected based on markers!\nConflict markers: {}\nTable markers: {}", currentInstantAllMarkers, tableMarkers);
        hasConflict.compareAndSet(false, true);
      }
      LOG.info("Finish batching marker-based conflict detection in {} ms", timer.endTimer());

    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker conflict");
    }
  }
}
