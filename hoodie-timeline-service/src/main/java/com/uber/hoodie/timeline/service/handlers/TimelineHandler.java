/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.timeline.service.handlers;

import com.uber.hoodie.common.table.timeline.dto.InstantDTO;
import com.uber.hoodie.common.table.timeline.dto.TimelineDTO;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * REST Handler servicing timeline requests
 */
public class TimelineHandler extends Handler {

  public TimelineHandler(Configuration conf,
      FileSystemViewManager viewManager) throws IOException {
    super(conf, viewManager);
  }

  public List<InstantDTO> getLastInstant(String basePath) {
    return viewManager.getFileSystemView(basePath).getLastInstant()
        .map(InstantDTO::fromInstant).map(dto -> Arrays.asList(dto)).orElse(new ArrayList<>());
  }

  public TimelineDTO getTimeline(String basePath) {
    return TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }
}
