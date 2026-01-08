/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class to assist with finding an index for a given file group in MDT partition.
 * Most partitions in MDT are flat or one level where all file groups are numbered from 1 to N
 * and we can determine the index just based on file Id.
 * This is the default class to be used in most of such cases
 */
public class CustomMetadataTableFileGroupIndexParser
    implements MetadataTableFileGroupIndexParser {

  private final Map<String, Integer> partitionFileGroupCountMap;
  private final Map<String, Integer> partitionOffsets;
  private final int numFileGroups;

  public CustomMetadataTableFileGroupIndexParser(
      Map<String, Integer> partitionFileGroupCountMap) {

    this.partitionFileGroupCountMap = new HashMap<>(partitionFileGroupCountMap);
    this.partitionOffsets = new HashMap<>();

    // Sort partitions deterministically
    List<String> sortedPartitions = partitionFileGroupCountMap.keySet()
        .stream()
        .sorted()
        .collect(Collectors.toList());

    int runningOffset = 0;
    for (String partition : sortedPartitions) {
      partitionOffsets.put(partition, runningOffset);
      runningOffset += partitionFileGroupCountMap.get(partition);
    }

    this.numFileGroups = runningOffset;
  }

  @Override
  public int getFileGroupIndex(String fileID) {
    return HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileID);
  }

  @Override
  public int getFileGroupIndex(String partitionPath, int fileIndexInPartition) {
    Integer offset = partitionOffsets.get(partitionPath);

    ValidationUtils.checkArgument(
        offset != null,
        "Unknown partition path: " + partitionPath
    );

    return offset + fileIndexInPartition;
  }

  @Override
  public int getNumberOfFileGroups() {
    return numFileGroups;
  }
}

