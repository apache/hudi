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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.FSUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All the metadata that gets stored along with a commit.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieCommitMetadata implements Serializable {

  public static final String SCHEMA_KEY = "schema";
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCommitMetadata.class);
  protected Map<String, List<HoodieWriteStat>> partitionToWriteStats;
  protected Boolean compacted;

  private Map<String, String> extraMetadataMap;

  // for ser/deser
  public HoodieCommitMetadata() {
    this(false);
  }

  public HoodieCommitMetadata(boolean compacted) {
    extraMetadataMap = new HashMap<>();
    partitionToWriteStats = new HashMap<>();
    this.compacted = compacted;
  }

  public void addWriteStat(String partitionPath, HoodieWriteStat stat) {
    if (!partitionToWriteStats.containsKey(partitionPath)) {
      partitionToWriteStats.put(partitionPath, new ArrayList<>());
    }
    partitionToWriteStats.get(partitionPath).add(stat);
  }

  public void addMetadata(String metaKey, String value) {
    extraMetadataMap.put(metaKey, value);
  }

  public List<HoodieWriteStat> getWriteStats(String partitionPath) {
    return partitionToWriteStats.get(partitionPath);
  }

  public Map<String, String> getExtraMetadata() {
    return extraMetadataMap;
  }

  public Map<String, List<HoodieWriteStat>> getPartitionToWriteStats() {
    return partitionToWriteStats;
  }

  public String getMetadata(String metaKey) {
    return extraMetadataMap.get(metaKey);
  }

  public Boolean getCompacted() {
    return compacted;
  }

  public void setCompacted(Boolean compacted) {
    this.compacted = compacted;
  }

  public HashMap<String, String> getFileIdAndRelativePaths() {
    HashMap<String, String> filePaths = new HashMap<>();
    // list all partitions paths
    for (Map.Entry<String, List<HoodieWriteStat>> entry : getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        filePaths.put(stat.getFileId(), stat.getPath());
      }
    }
    return filePaths;
  }

  public HashMap<String, String> getFileIdAndFullPaths(String basePath) {
    HashMap<String, String> fullPaths = new HashMap<>();
    for (Map.Entry<String, String> entry : getFileIdAndRelativePaths().entrySet()) {
      String fullPath =
          (entry.getValue() != null) ? (FSUtils.getPartitionPath(basePath, entry.getValue())).toString() : null;
      fullPaths.put(entry.getKey(), fullPath);
    }
    return fullPaths;
  }

  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      LOG.info("partition path is null for {}", partitionToWriteStats.get(null));
      partitionToWriteStats.remove(null);
    }
    return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return clazz.newInstance();
    }
    return getObjectMapper().readValue(jsonStr, clazz);
  }

  // Here the functions are named "fetch" instead of "get", to get avoid of the json conversion.
  public long fetchTotalPartitionsWritten() {
    return partitionToWriteStats.size();
  }

  public long fetchTotalFilesInsert() {
    long totalFilesInsert = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit() != null && stat.getPrevCommit().equalsIgnoreCase("null")) {
          totalFilesInsert++;
        }
      }
    }
    return totalFilesInsert;
  }

  public long fetchTotalFilesUpdated() {
    long totalFilesUpdated = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit() != null && !stat.getPrevCommit().equalsIgnoreCase("null")) {
          totalFilesUpdated++;
        }
      }
    }
    return totalFilesUpdated;
  }

  public long fetchTotalUpdateRecordsWritten() {
    long totalUpdateRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalUpdateRecordsWritten += stat.getNumUpdateWrites();
      }
    }
    return totalUpdateRecordsWritten;
  }

  public long fetchTotalInsertRecordsWritten() {
    long totalInsertRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit() != null && stat.getPrevCommit().equalsIgnoreCase("null")) {
          totalInsertRecordsWritten += stat.getNumInserts();
        }
      }
    }
    return totalInsertRecordsWritten;
  }

  public long fetchTotalRecordsWritten() {
    long totalRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalRecordsWritten += stat.getNumWrites();
      }
    }
    return totalRecordsWritten;
  }

  public long fetchTotalBytesWritten() {
    long totalBytesWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalBytesWritten += stat.getTotalWriteBytes();
      }
    }
    return totalBytesWritten;
  }

  public long fetchTotalWriteErrors() {
    long totalWriteErrors = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalWriteErrors += stat.getTotalWriteErrors();
      }
    }
    return totalWriteErrors;
  }

  public long getTotalRecordsDeleted() {
    long totalDeletes = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalDeletes += stat.getNumDeletes();
      }
    }
    return totalDeletes;
  }

  public Long getTotalLogRecordsCompacted() {
    Long totalLogRecords = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogRecords += writeStat.getTotalLogRecords();
      }
    }
    return totalLogRecords;
  }

  public Long getTotalLogFilesCompacted() {
    Long totalLogFiles = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogFiles += writeStat.getTotalLogFilesCompacted();
      }
    }
    return totalLogFiles;
  }

  public Long getTotalCompactedRecordsUpdated() {
    Long totalUpdateRecords = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalUpdateRecords += writeStat.getTotalUpdatedRecordsCompacted();
      }
    }
    return totalUpdateRecords;
  }

  public Long getTotalLogFilesSize() {
    Long totalLogFilesSize = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogFilesSize += writeStat.getTotalLogSizeCompacted();
      }
    }
    return totalLogFilesSize;
  }

  public Long getTotalScanTime() {
    Long totalScanTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalScanTime += writeStat.getRuntimeStats().getTotalScanTime();
        }
      }
    }
    return totalScanTime;
  }

  public Long getTotalCreateTime() {
    Long totalCreateTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalCreateTime += writeStat.getRuntimeStats().getTotalCreateTime();
        }
      }
    }
    return totalCreateTime;
  }

  public Long getTotalUpsertTime() {
    Long totalUpsertTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalUpsertTime += writeStat.getRuntimeStats().getTotalUpsertTime();
        }
      }
    }
    return totalUpsertTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieCommitMetadata that = (HoodieCommitMetadata) o;

    if (!partitionToWriteStats.equals(that.partitionToWriteStats)) {
      return false;
    }
    return compacted.equals(that.compacted);

  }

  @Override
  public int hashCode() {
    int result = partitionToWriteStats.hashCode();
    result = 31 * result + compacted.hashCode();
    return result;
  }

  public static <T> T fromBytes(byte[] bytes, Class<T> clazz) throws IOException {
    try {
      return fromJsonString(new String(bytes, Charset.forName("utf-8")), clazz);
    } catch (Exception e) {
      throw new IOException("unable to read commit metadata", e);
    }
  }

  protected static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper;
  }

  @Override
  public String toString() {
    return "HoodieCommitMetadata{partitionToWriteStats=" + partitionToWriteStats + ", compacted=" + compacted
        + ", extraMetadataMap=" + extraMetadataMap + '}';
  }
}
