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

package org.apache.hudi.common.conflict.detection;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.storage.HoodieStorage;

import java.util.Set;

/**
 * This abstract strategy is used for writers using timeline-server-based markers,
 * trying to do early conflict detection.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class TimelineServerBasedDetectionStrategy implements EarlyConflictDetectionStrategy {

  protected final String basePath;
  protected final String markerDir;
  protected final String markerName;
  protected final boolean checkCommitConflict;

  public TimelineServerBasedDetectionStrategy(String basePath, String markerDir, String markerName, Boolean checkCommitConflict) {
    this.basePath = basePath;
    this.markerDir = markerDir;
    this.markerName = markerName;
    this.checkCommitConflict = checkCommitConflict;
  }

  /**
   * Starts the async conflict detection thread.
   *
   * @param initialDelayMs                    Initial delay in milliseconds.
   * @param periodMs                          Scheduling period in milliseconds.
   * @param markerDir                         Marker directory.
   * @param basePath                          Base path of the table.
   * @param maxAllowableHeartbeatIntervalInMs Heartbeat timeout.
   * @param storage                        {@link HoodieStorage} instance.
   * @param markerHandler                     Marker handler.
   * @param completedCommits                  Completed Hudi commits.
   */
  public abstract void startAsyncDetection(Long initialDelayMs, Long periodMs, String markerDir,
                                           String basePath, Long maxAllowableHeartbeatIntervalInMs,
                                           HoodieStorage storage, Object markerHandler,
                                           Set<HoodieInstant> completedCommits);

  public abstract void stop();
}
