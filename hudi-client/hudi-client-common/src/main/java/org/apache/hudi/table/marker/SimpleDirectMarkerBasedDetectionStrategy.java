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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file
 * conflict, without any locking.
 */
public class SimpleDirectMarkerBasedDetectionStrategy extends DirectMarkerBasedDetectionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDirectMarkerBasedDetectionStrategy.class);
  private final String basePath;
  private final boolean checkCommitConflict;
  private final Set<HoodieInstant> completedCommitInstants;
  private final long maxAllowableHeartbeatIntervalInMs;

  public SimpleDirectMarkerBasedDetectionStrategy(HoodieStorage storage, String partitionPath, String fileId, String instantTime,
                                                  HoodieActiveTimeline activeTimeline, HoodieWriteConfig config) {
    super(storage, partitionPath, fileId, instantTime, activeTimeline, config);
    this.basePath = config.getBasePath();
    this.checkCommitConflict = config.earlyConflictDetectionCheckCommitConflict();
    this.completedCommitInstants = new HashSet<>(activeTimeline.getCommitsTimeline().filterCompletedInstants().getInstants());
    this.maxAllowableHeartbeatIntervalInMs = config.getHoodieClientHeartbeatIntervalInMs() * config.getHoodieClientHeartbeatTolerableMisses();

  }

  @Override
  public boolean hasMarkerConflict() {
    try {
      return checkMarkerConflict(basePath, maxAllowableHeartbeatIntervalInMs)
          || (checkCommitConflict && MarkerUtils.hasCommitConflict(activeTimeline, Stream.of(fileId).collect(Collectors.toSet()), completedCommitInstants));
    } catch (IOException e) {
      LOG.error("Exception occurs during create marker file in eager conflict detection mode.", e);
      throw new HoodieIOException("Exception occurs during create marker file in eager conflict detection mode.", e);
    }
  }

  @Override
  public void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    if (hasMarkerConflict()) {
      resolveMarkerConflict(basePath, partitionPath, fileId);
    }
  }
}
