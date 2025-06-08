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

import java.io.IOException;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * REST Handler servicing timeline requests.
 */
public class TimelineHandler extends Handler {

  public TimelineHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                         FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
  }

  public List<InstantDTO> getLastInstant(String basePath) {
    return viewManager.getFileSystemView(basePath).getLastInstant().map(InstantDTO::fromInstant)
        .map(Arrays::asList).orElse(Collections.emptyList());
  }

  public TimelineDTO getTimeline(String basePath) {
    return TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }

  public org.apache.hudi.common.table.timeline.dto.v2.TimelineDTO getTimelineV2(String basePath) {
    return org.apache.hudi.common.table.timeline.dto.v2.TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }

  public Object getInstantDetails(String basePath, String requestedTime, String action, String state)
      throws IOException {
    HoodieTimeline hoodieTimeline = viewManager.getFileSystemView(basePath).getTimeline();
    try {
      HoodieInstant requestedInstant = new HoodieInstant(
          HoodieInstant.State.valueOf(state), action, requestedTime,
          InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);

      switch (requestedInstant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          return hoodieTimeline.readCommitMetadata(requestedInstant);
        default:
          // TODO: implement other actions
          return null;
      }
    } catch (Exception e) {
      // Input parameters might be invalid
      return null;
    }
  }
}
