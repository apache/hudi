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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SpecificPartitionCompactionStrategy extends CompactionStrategy {

  /**
   * All the partition level filter can be added here.
   * @param allPartitionPaths List of all the partitions in the table
   * @return Final list of partitions paths.
   */
  @Override
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    String value = getPartitionsConfig(writeConfig);
    if (StringUtils.isNullOrEmpty(value)) {
      return allPartitionPaths;
    }
    Set<String> partitionsToCompact = Arrays.stream(value.split("\\.")).collect(Collectors.toSet());
    return allPartitionPaths.stream()
        .filter(partitionsToCompact::contains)
        .collect(Collectors.toList());
  }

  protected String getPartitionsConfig(HoodieWriteConfig writeConfig) {
    return writeConfig.getPartitionsForCompaction();
  }
}
