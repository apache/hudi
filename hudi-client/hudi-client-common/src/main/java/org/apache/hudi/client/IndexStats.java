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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieRecordDelegate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to hold all index stats required to generate Metadata records for all enabled partitions.
 * Supported stats are record level index stats and secondary index stats.
 */
public class IndexStats implements Serializable {
  private static final long serialVersionUID = 1L;

  private List<HoodieRecordDelegate> writtenRecordDelegates = new ArrayList<>();
  private Map<String, List<SecondaryIndexStats>> secondaryIndexStats = new HashMap<>();

  void addHoodieRecordDelegate(HoodieRecordDelegate hoodieRecordDelegate) {
    this.writtenRecordDelegates.add(hoodieRecordDelegate);
  }

  public List<HoodieRecordDelegate> getWrittenRecordDelegates() {
    return writtenRecordDelegates;
  }

  public void initSecondaryIndexStats(String secondaryIndexPartitionPath) {
    secondaryIndexStats.put(secondaryIndexPartitionPath, new ArrayList<>());
  }

  public void addSecondaryIndexStats(String secondaryIndexPartitionPath, String recordKey, String secondaryIndexValue, boolean isDeleted) {
    secondaryIndexStats.computeIfAbsent(secondaryIndexPartitionPath, k -> new ArrayList<>())
        .add(new SecondaryIndexStats(recordKey, secondaryIndexValue, isDeleted));
  }

  public Map<String, List<SecondaryIndexStats>> getSecondaryIndexStats() {
    return secondaryIndexStats;
  }

  public void setWrittenRecordDelegates(List<HoodieRecordDelegate> writtenRecordDelegates) {
    this.writtenRecordDelegates = writtenRecordDelegates;
  }

  public void setSecondaryIndexStats(Map<String, List<SecondaryIndexStats>> secondaryIndexStats) {
    this.secondaryIndexStats = secondaryIndexStats;
  }
}
