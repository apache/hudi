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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * REST Handler servicing timeline requests.
 */
public class TimelineHandler extends Handler {
  private static final Logger LOG = LoggerFactory.getLogger(TimelineHandler.class);

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

  public String getTimelineHash(String basePath) {
    return viewManager.getFileSystemView(basePath).getTimeline().getTimelineHash();
  }

  public boolean initializeTimeline(String basePath, TimelineDTO timelineDTO) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(conf)
        .setBasePath(basePath)
        .build();
    HoodieTimeline hoodieTimeline = TimelineDTO.toTimeline(timelineDTO, metaClient);
    // Check if we already have the right view.
    if (viewManager.doesFileSystemViewExists(basePath)
        && hoodieTimeline.getTimelineHash().equals(
        viewManager.getFileSystemView(basePath).getTimeline().getTimelineHash())) {
      LOG.debug("Timeline hashes match returning view for basePath {}", basePath);
      return true;
    }
    // Lock on the viewManager.
    synchronized (viewManager) {
      SyncableFileSystemView viewInServer = viewManager.getFileSystemView(metaClient, hoodieTimeline);
      if (!hoodieTimeline.getTimelineHash().equals(viewInServer.getTimeline().getTimelineHash())) {
        LOG.info("Clearing and Re-creating view as timeline hashes don't match for basePath {}", basePath);
        viewManager.clearFileSystemView(basePath);
        viewManager.getFileSystemView(metaClient, hoodieTimeline);
      }
      return true;
    }
  }
}
