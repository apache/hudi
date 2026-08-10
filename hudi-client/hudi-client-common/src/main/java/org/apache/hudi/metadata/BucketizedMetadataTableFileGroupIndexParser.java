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

import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Uses sorting order to determine file numbering. Used during the initialization of the partitioned record index
 * <p>
 * Each data table partition p_i corresponds to n_i number of filegroups in the partitioned rli
 * <p>
 * The file id stores both the data table partition name and the index of the filegroup
 * to get the overall index of the file, we sort the data table  partitions by name
 * and then p_0 will map to indexes 0 to n_0 - 1
 * p_1 will map to indexes n_0 to n_0 + n_1 - 1
 * p_2 will map to indexes n_0 + n_1 to n_0 + n_1 + n_2 - 1
 * p_i will map to indexes (sum(k from 0 to i - 1) n_k) to (sum(k from 0 to i) n_k) - 1
 * <p>
 * for example, we have file names
 * record_index_p1_fg1
 * record_index_p1_fg2
 * <p>
 * record_index_p2_fg1
 * record_index_p2_fg2
 * record_index_p2_fg3
 * record_index_p2_fg4
 * <p>
 * record_index_p3_fg1
 * record_index_p3_fg2
 * record_index_p3_fg3
 * <p>
 * so the indexes would be
 * record_index_p1_fg1: 0
 * record_index_p1_fg2: 1
 * record_index_p2_fg1: 2
 * record_index_p2_fg2: 3
 * record_index_p2_fg3: 4
 * record_index_p2_fg4: 5
 * record_index_p3_fg1: 6
 * record_index_p3_fg2: 7
 * record_index_p3_fg3: 8
 * <p>
 * partitionSizes will be
 * p1 -> 2
 * p2 -> 4
 * p3 -> 3
 * <p>
 * and partitionToBaseIndexOffset will be
 * p1 -> 0
 * p2 -> 2
 * p3 -> 6
 * <p>
 * then to calculate the index for a record, lets say in partition 2, we hash between 0 and 3 because p2 in partitionSizes is 4
 * then we add 2 because that is what p2 maps to in partitionToBaseIndexOffset
 */
public class BucketizedMetadataTableFileGroupIndexParser implements MetadataTableFileGroupIndexParser {
  private final Map<String, Integer> partitionToBaseIndexOffset;
  @Getter
  private final int numberOfFileGroups;

  public BucketizedMetadataTableFileGroupIndexParser(Map<String, Integer> partitionSizes) {
    numberOfFileGroups = calculateNumberOfFileGroups(partitionSizes);
    this.partitionToBaseIndexOffset = generatePartitionToBaseIndexOffsets(partitionSizes);
  }

  /**
   * Create a map between the partition names, and their spark partition offset
   *
   * For example, if the input is
   * p1 -> 2
   * p2 -> 4
   * p3 -> 3
   * p4 -> 5
   * p5 -> 2
   * <p>
   * we will return
   * p1 -> 0
   * p2 -> 2
   * p3 -> 6
   * p4 -> 9
   * p5 -> 14
   */
  public static Map<String, Integer> generatePartitionToBaseIndexOffsets(Map<String, Integer> partitionSizes) {
    Map<String, Integer> partitionIndexOffsets = new HashMap<>(partitionSizes.size());
    int runningSum = 0;
    String[] sortedPartitionNames = partitionSizes.keySet().toArray(new String[0]);
    Arrays.sort(sortedPartitionNames);
    for (String partitionName : sortedPartitionNames) {
      partitionIndexOffsets.put(partitionName, runningSum);
      runningSum += partitionSizes.get(partitionName);
    }
    return partitionIndexOffsets;
  }

  public static int calculateNumberOfFileGroups(Map<String, Integer> partitionSizes) {
    return partitionSizes.values().stream().mapToInt(Integer::intValue).sum();
  }

  @Override
  public int getFileGroupIndex(String fileID) {
    String partitionName = HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileID);
    return partitionToBaseIndexOffset.get(partitionName) + HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileID);
  }
}
