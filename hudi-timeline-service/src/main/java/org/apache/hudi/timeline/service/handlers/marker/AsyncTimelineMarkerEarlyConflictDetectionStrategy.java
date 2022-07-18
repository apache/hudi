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
import org.apache.hudi.common.conflict.detection.HoodieTimelineServerBasedEarlyConflictDetectionStrategy;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ConcurrentModificationException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncTimelineMarkerEarlyConflictDetectionStrategy extends HoodieTimelineServerBasedEarlyConflictDetectionStrategy {
  private static final Logger LOG = LogManager.getLogger(AsyncTimelineMarkerEarlyConflictDetectionStrategy.class);
  private AtomicBoolean hasConflict = new AtomicBoolean(false);
  private ScheduledExecutorService markerChecker;

  @Override
  public boolean hasMarkerConflict() {
    return hasConflict.get();
  }

  @Override
  public void resolveMarkerConflict(String basePath, String markerDir, String markerName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }

  public void fresh(String batchInterval, String period, String markerDir, String basePath,
                    String maxAllowableHeartbeatIntervalInMs, FileSystem fileSystem, Object markerHandler, Set<HoodieInstant> oldInstants) {
    if (markerChecker != null) {
      markerChecker.shutdown();
    }
    hasConflict.compareAndSet(true, false);
    markerChecker = Executors.newSingleThreadScheduledExecutor();
    markerChecker.scheduleAtFixedRate(new MarkerCheckerRunnable(hasConflict, (MarkerHandler) markerHandler, markerDir, basePath,
        fileSystem, Long.parseLong(maxAllowableHeartbeatIntervalInMs), oldInstants), Long.parseLong(batchInterval), Long.parseLong(period), TimeUnit.MILLISECONDS);
  }
}
