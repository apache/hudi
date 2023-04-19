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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The data transfer object of file group.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileGroupDTO {

  @JsonProperty("partition")
  String partition;

  @JsonProperty("fileId")
  String id;

  @JsonProperty("slices")
  List<FileSliceDTO> slices;

  @JsonProperty("timeline")
  TimelineDTO timeline;

  public static FileGroupDTO fromFileGroup(HoodieFileGroup fileGroup) {
    return fromFileGroup(fileGroup, true);
  }

  public static List<FileGroupDTO> fromFileGroup(List<HoodieFileGroup> fileGroups) {
    if (fileGroups.isEmpty()) {
      return Collections.emptyList();
    }

    List<FileGroupDTO> fileGroupDTOs = fileGroups.stream()
        .map(fg -> FileGroupDTO.fromFileGroup(fg, false)).collect(Collectors.toList());
    // Timeline exists only in the first file group DTO. Optimisation to reduce payload size.
    fileGroupDTOs.set(0, FileGroupDTO.fromFileGroup(fileGroups.get(0), true));
    return fileGroupDTOs;
  }

  public static HoodieFileGroup toFileGroup(FileGroupDTO dto, HoodieTableMetaClient metaClient) {
    return toFileGroup(dto, metaClient, null);
  }

  public static Stream<HoodieFileGroup> toFileGroup(List<FileGroupDTO> dtos, HoodieTableMetaClient metaClient) {
    if (dtos.isEmpty()) {
      return Stream.empty();
    }

    // Timeline exists only in the first file group DTO. Optimisation to reduce payload size.
    HoodieTimeline timeline = toFileGroup(dtos.get(0), metaClient).getTimeline();
    return dtos.stream().map(dto -> toFileGroup(dto, metaClient, timeline));
  }

  private static FileGroupDTO fromFileGroup(HoodieFileGroup fileGroup, boolean includeTimeline) {
    FileGroupDTO dto = new FileGroupDTO();
    dto.partition = fileGroup.getPartitionPath();
    dto.id = fileGroup.getFileGroupId().getFileId();
    dto.slices = fileGroup.getAllRawFileSlices().map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
    if (includeTimeline) {
      dto.timeline = TimelineDTO.fromTimeline(fileGroup.getTimeline());
    }
    return dto;
  }

  private static HoodieFileGroup toFileGroup(FileGroupDTO dto, HoodieTableMetaClient metaClient, HoodieTimeline inputTimeline) {
    HoodieTimeline fgTimeline = inputTimeline == null ? TimelineDTO.toTimeline(dto.timeline, metaClient) : inputTimeline;
    HoodieFileGroup fileGroup =
        new HoodieFileGroup(dto.partition, dto.id, fgTimeline);
    dto.slices.stream().map(FileSliceDTO::toFileSlice).forEach(fileSlice -> fileGroup.addFileSlice(fileSlice));
    return fileGroup;
  }
}
