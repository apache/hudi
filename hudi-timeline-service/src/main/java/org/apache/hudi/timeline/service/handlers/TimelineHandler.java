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

import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
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
}
