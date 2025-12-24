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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * DTO utils to hold batch apis.
 */
public class DTOUtils {

  public static List<FileGroupDTO> fileGroupDTOsfromFileGroups(List<HoodieFileGroup> fileGroups) {
    if (fileGroups.isEmpty()) {
      return Collections.emptyList();
    } else if (fileGroups.size() == 1) {
      return Collections.singletonList(FileGroupDTO.fromFileGroup(fileGroups.get(0), true));
    } else {
      List<FileGroupDTO> fileGroupDTOS = new ArrayList<>();
      fileGroupDTOS.add(FileGroupDTO.fromFileGroup(fileGroups.get(0), true));
      fileGroupDTOS.addAll(fileGroups.subList(1, fileGroups.size()).stream()
          .map(fg -> FileGroupDTO.fromFileGroup(fg, false)).collect(Collectors.toList()));
      return fileGroupDTOS;
    }
  }

  public static Stream<HoodieFileGroup> fileGroupDTOsToFileGroups(List<FileGroupDTO> dtos, HoodieTableMetaClient metaClient) {
    if (dtos.isEmpty()) {
      return Stream.empty();
    }

    // Timeline exists only in the first file group DTO. Optimisation to reduce payload size.
    checkState(dtos.get(0).timeline != null, "Timeline is expected to be set for the first FileGroupDTO");
    HoodieTimeline timeline = TimelineDTO.toTimeline(dtos.get(0).timeline, metaClient);
    if (metaClient.getTableConfig().isLSMBasedLogFormat()) {
      return dtos.stream().map(dto -> FileGroupDTO.toLSMFileGroup(dto, timeline));
    } else {
      return dtos.stream().map(dto -> FileGroupDTO.toFileGroup(dto, timeline));
    }
  }
}
