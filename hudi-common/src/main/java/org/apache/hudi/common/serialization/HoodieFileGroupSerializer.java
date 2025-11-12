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

package org.apache.hudi.common.serialization;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Serializes {@link HoodieFileGroup} but keeps the timeline in memory since file groups
 * for a single view will share the same {@link HoodieTimeline} instance.
 * Deserialization will create a new instance so recreating timeline objects can lead to unnecessary bloat.
 * IMPORTANT: This serializer can only be used in cases where the data is kept on the local machine and cannot be used
 * when serializing between spark driver and executor.
 */
public class HoodieFileGroupSerializer implements CustomSerializer<List<HoodieFileGroup>> {
  private final Map<HoodieFileGroupId, HoodieTimeline> fileGroupIdHoodieTimelineMap;

  public HoodieFileGroupSerializer() {
    this.fileGroupIdHoodieTimelineMap = new ConcurrentHashMap<>();
  }

  @Override
  public byte[] serialize(List<HoodieFileGroup> input) throws IOException {
    List<HoodieFileGroupLite> fileGroupLites = input.stream().map(fileGroup -> {
      if (fileGroup.getTimeline() == null) {
        throw new IllegalStateException("Timeline is null for FileGroup: '" + fileGroup.getFileGroupId().toString() + "'. All filegroup states: ["
            + input.stream().map(fg -> fg.getFileGroupId().toString() + ":" + ((fg.getTimeline() == null) ? "NULL" : "OK")).collect(Collectors.joining(",")) + "]");
      }
      fileGroupIdHoodieTimelineMap.put(fileGroup.getFileGroupId(), fileGroup.getTimeline());
      return new HoodieFileGroupLite(fileGroup.getAllRawFileSlices().collect(Collectors.toList()));
    }).collect(Collectors.toList());
    return SerializationUtils.serialize(fileGroupLites);
  }

  @Override
  public List<HoodieFileGroup> deserialize(byte[] bytes) {
    List<HoodieFileGroupLite> fileGroupLites = SerializationUtils.deserialize(bytes);
    return fileGroupLites.stream().map(fileGroupLite -> {
      HoodieTimeline timeline = fileGroupIdHoodieTimelineMap.get(fileGroupLite.getFileGroupId());
      if (timeline == null) {
        throw new IllegalStateException("Timeline for fileGroupId: '" + fileGroupLite.getFileGroupId() + "' was not found in the map. Available fileGroupId in map: ["
            + fileGroupIdHoodieTimelineMap.keySet().stream().map(HoodieFileGroupId::toString).collect(Collectors.joining(",")) + "]. FileGroupId that we are deserializing: ["
            + fileGroupLites.stream().map(fg -> fg.getFileGroupId().toString()).collect(Collectors.joining(",")) + "]");
      }
      HoodieFileGroup fileGroup = new HoodieFileGroup(fileGroupLite.getFileGroupId(), timeline);
      fileGroupLite.fileSlices.forEach(fileGroup::addFileSlice);
      return fileGroup;
    }).collect(Collectors.toList());
  }

  private static class HoodieFileGroupLite implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<FileSlice> fileSlices;

    HoodieFileGroupLite(List<FileSlice> fileSlices) {
      this.fileSlices = fileSlices;
    }

    private HoodieFileGroupId getFileGroupId() {
      return fileSlices.get(0).getFileGroupId();
    }
  }
}
