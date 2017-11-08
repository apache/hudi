/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Place holder for the compaction specific meta-data, uses all the details used in a normal HoodieCommitMetadata
 */
public class HoodieCompactionMetadata extends HoodieCommitMetadata {
  private static volatile Logger log = LogManager.getLogger(HoodieCompactionMetadata.class);
  protected HashMap<String, List<CompactionWriteStat>> partitionToCompactionWriteStats;
  protected long compactionCommit;

  public HoodieCompactionMetadata() {
    partitionToCompactionWriteStats = new HashMap<>();
  }

  public void addWriteStat(String partitionPath, CompactionWriteStat stat) {
    addWriteStat(partitionPath, stat.getHoodieWriteStat());
    if (!partitionToCompactionWriteStats.containsKey(partitionPath)) {
      partitionToCompactionWriteStats.put(partitionPath, new ArrayList<>());
    }
    partitionToCompactionWriteStats.get(partitionPath).add(stat);
  }

  public void setCompactionCommit(long compactionCommit) {
    this.compactionCommit = compactionCommit;
  }

  public long getCompactionCommit() {
    return this.compactionCommit;
  }

  public List<CompactionWriteStat> getCompactionWriteStats(String partitionPath) {
    return partitionToCompactionWriteStats.get(partitionPath);
  }

  public Map<String, List<CompactionWriteStat>> getPartitionToCompactionWriteStats() {
    return partitionToCompactionWriteStats;
  }

  public Long getTotalLogRecordsCompacted() {
    Long totalLogRecords = 0L;
    for(Map.Entry<String, List<CompactionWriteStat>> entry : partitionToCompactionWriteStats.entrySet()) {
      for (CompactionWriteStat cWriteStat : entry.getValue()) {
        totalLogRecords += cWriteStat.getTotalLogRecords();
      }
    }
    return totalLogRecords;
  }

  public Long getTotalLogFilesCompacted() {
    Long totalLogFiles = 0L;
    for(Map.Entry<String, List<CompactionWriteStat>> entry : partitionToCompactionWriteStats.entrySet()) {
      for (CompactionWriteStat cWriteStat : entry.getValue()) {
        totalLogFiles += cWriteStat.getTotalLogFiles();
      }
    }
    return totalLogFiles;
  }

  public Long getTotalCompactedRecordsToBeUpdated() {
    Long totalUpdateRecords = 0L;
    for (Map.Entry<String, List<CompactionWriteStat>> entry : partitionToCompactionWriteStats.entrySet()) {
      for (CompactionWriteStat cWriteStat : entry.getValue()) {
        totalUpdateRecords += cWriteStat.getTotalRecordsToBeUpdate();
      }
    }
    return totalUpdateRecords;
  }

  public String toJsonString() throws IOException {
    if(partitionToCompactionWriteStats.containsKey(null)) {
      log.info("partition path is null for " + partitionToCompactionWriteStats.get(null));
      partitionToCompactionWriteStats.remove(null);
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper.defaultPrettyPrintingWriter().writeValueAsString(this);
  }

  public static HoodieCompactionMetadata fromJsonString(String jsonStr) throws IOException {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return new HoodieCompactionMetadata();
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper.readValue(jsonStr, HoodieCompactionMetadata.class);
  }

  public static HoodieCompactionMetadata fromBytes(byte[] bytes) throws IOException {
    return fromJsonString(new String(bytes, Charset.forName("utf-8")));
  }

}
