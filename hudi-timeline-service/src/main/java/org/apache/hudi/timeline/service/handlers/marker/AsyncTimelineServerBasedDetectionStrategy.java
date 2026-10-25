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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;

import lombok.extern.slf4j.Slf4j;

import java.util.ConcurrentModificationException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This abstract strategy is used for writers using timeline-server-based markers,
 * trying to do early conflict detection by asynchronously and periodically checking
 * write conflict among multiple writers based on the timeline-server-based markers.
 */
@Slf4j
public class AsyncTimelineServerBasedDetectionStrategy extends TimelineServerBasedDetectionStrategy {

  private final AtomicBoolean hasConflict = new AtomicBoolean(false);
  private ScheduledExecutorService asyncDetectorExecutor;

  public AsyncTimelineServerBasedDetectionStrategy(String basePath, String markerDir, String markerName, Boolean checkCommitConflict) {
    super(basePath, markerDir, markerName, checkCommitConflict);
  }

  @Override
  public boolean hasMarkerConflict() {
    return hasConflict.get();
  }

  @Override
  public void resolveMarkerConflict(String basePath, String markerDir, String markerName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }

  @Override
  public void startAsyncDetection(Long initialDelayMs, Long periodMs, String markerDir,
                                  String basePath, Long maxAllowableHeartbeatIntervalInMs,
                                  HoodieStorage storage, Object markerHandler,
                                  Set<HoodieInstant> completedCommits) {
    if (asyncDetectorExecutor != null) {
      asyncDetectorExecutor.shutdown();
    }
    hasConflict.set(false);
    asyncDetectorExecutor = Executors.newSingleThreadScheduledExecutor();
    asyncDetectorExecutor.scheduleAtFixedRate(
        new MarkerBasedEarlyConflictDetectionRunnable(
            hasConflict, (MarkerHandler) markerHandler, markerDir, basePath,
            storage, maxAllowableHeartbeatIntervalInMs, completedCommits, checkCommitConflict),
        initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    if (hasMarkerConflict()) {
      resolveMarkerConflict(basePath, markerDir, markerName);
    }
  }

  public void stop() {
    if (asyncDetectorExecutor != null) {
      asyncDetectorExecutor.shutdown();
    }
  }
}
