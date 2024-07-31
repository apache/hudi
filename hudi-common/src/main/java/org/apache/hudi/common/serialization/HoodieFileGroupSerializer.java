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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serializes {@link org.apache.hudi.common.model.HoodieFileGroup} but keeps the timeline in memory since file groups
 * for a single view will share the same {@link org.apache.hudi.common.table.timeline.HoodieTimeline} instance.
 * Deserialization will create a new instance so recreating timeline objects can lead to unnecessary bloat.
 * IMPORTANT: This serializer can only be used in cases where the data is kept on the local machine and cannot be used
 * when serializing between spark driver and executor.
 */
public class HoodieFileGroupSerializer implements CustomSerializer<List<HoodieFileGroup>> {
  private final Map<HoodieFileGroupId, HoodieTimeline> fileGroupIdHoodieTimelineMap;

  public HoodieFileGroupSerializer() {
    this.fileGroupIdHoodieTimelineMap = new HashMap<>();
  }

  @Override
  public byte[] serialize(List<HoodieFileGroup> input) throws IOException {
    List<HoodieFileGroupLite> fileGroupLites = input.stream().map(fileGroup -> {
      fileGroupIdHoodieTimelineMap.put(fileGroup.getFileGroupId(), fileGroup.getTimeline());
      return new HoodieFileGroupLite(fileGroup.getAllRawFileSlices().collect(Collectors.toList()), fileGroup.getFileGroupId());
    }).collect(Collectors.toList());
    return SerializationUtils.serialize(fileGroupLites);
  }

  @Override
  public List<HoodieFileGroup> deserialize(byte[] bytes) {
    List<HoodieFileGroupLite> fileGroupLites = SerializationUtils.deserialize(bytes);
    return fileGroupLites.stream().map(fileGroupLite -> {
      HoodieFileGroup fileGroup = new HoodieFileGroup(fileGroupLite.fileGroupId, fileGroupIdHoodieTimelineMap.get(fileGroupLite.fileGroupId));
      fileGroupLite.fileSlices.forEach(fileGroup::addFileSlice);
      return fileGroup;
    }).collect(Collectors.toList());
  }

  private static class HoodieFileGroupLite implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<FileSlice> fileSlices;
    private final HoodieFileGroupId fileGroupId;

    HoodieFileGroupLite(List<FileSlice> fileSlices, HoodieFileGroupId fileGroupId) {
      this.fileSlices = fileSlices;
      this.fileGroupId = fileGroupId;
    }
  }
}
