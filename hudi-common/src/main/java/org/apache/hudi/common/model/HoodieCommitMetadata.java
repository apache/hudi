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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * All the metadata that gets stored along with a commit.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieCommitMetadata implements Serializable {

  public static final String SCHEMA_KEY = "schema";
  private static final Logger LOG = LogManager.getLogger(HoodieCommitMetadata.class);
  protected Map<String, List<HoodieWriteStat>> partitionToWriteStats;
  protected Boolean compacted;

  protected Map<String, String> extraMetadata;

  protected WriteOperationType operationType = WriteOperationType.UNKNOWN;

  // for ser/deser
  public HoodieCommitMetadata() {
    this(false);
  }

  public HoodieCommitMetadata(boolean compacted) {
    extraMetadata = new HashMap<>();
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
    extraMetadata.put(metaKey, value);
  }

  public List<HoodieWriteStat> getWriteStats(String partitionPath) {
    return partitionToWriteStats.get(partitionPath);
  }

  public Map<String, String> getExtraMetadata() {
    return extraMetadata;
  }

  public Map<String, List<HoodieWriteStat>> getPartitionToWriteStats() {
    return partitionToWriteStats;
  }

  public String getMetadata(String metaKey) {
    return extraMetadata.get(metaKey);
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
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      for (HoodieWriteStat stat : stats) {
        filePaths.put(stat.getFileId(), stat.getPath());
      }
    }
    return filePaths;
  }

  public void setOperationType(WriteOperationType type) {
    this.operationType = type;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
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

  public Map<HoodieFileGroupId, String> getFileGroupIdAndFullPaths(String basePath) {
    Map<HoodieFileGroupId, String> fileGroupIdToFullPaths = new HashMap<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId(stat.getPartitionPath(), stat.getFileId());
        Path fullPath = new Path(basePath, stat.getPath());
        fileGroupIdToFullPaths.put(fileGroupId, fullPath.toString());
      }
    }
    return fileGroupIdToFullPaths;
  }

  /**
   * Extract the file status of all affected files from the commit metadata. If a file has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit.
   *
   * @param basePath The base path
   * @return the file full path to file status mapping
   */
  public Map<String, FileStatus> getFullPathToFileStatus(String basePath) {
    Map<String, FileStatus> fullPathToFileStatus = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        Path fullPath = relativeFilePath != null ? FSUtils.getPartitionPath(basePath, relativeFilePath) : null;
        if (fullPath != null) {
          FileStatus fileStatus = new FileStatus(stat.getFileSizeInBytes(), false, 0, 0,
              0, fullPath);
          fullPathToFileStatus.put(fullPath.getName(), fileStatus);
        }
      }
    }
    return fullPathToFileStatus;
  }

  /**
   * Extract the file status of all affected files from the commit metadata. If a file has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit by file group ID.
   *
   * <p>Note: different with {@link #getFullPathToFileStatus(String)},
   * only the latest commit file for a file group is returned,
   * this is an optimization for COPY_ON_WRITE table to eliminate legacy files for filesystem view.
   *
   * @param basePath The base path
   * @return the file ID to file status mapping
   */
  public Map<String, FileStatus> getFileIdToFileStatus(String basePath) {
    Map<String, FileStatus> fileIdToFileStatus = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        Path fullPath = relativeFilePath != null ? FSUtils.getPartitionPath(basePath, relativeFilePath) : null;
        if (fullPath != null) {
          FileStatus fileStatus = new FileStatus(stat.getFileSizeInBytes(), false, 0, 0,
              0, fullPath);
          fileIdToFileStatus.put(stat.getFileId(), fileStatus);
        }
      }
    }
    return fileIdToFileStatus;
  }

  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToWriteStats.get(null));
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
        // determine insert rows in every file
        if (stat.getPrevCommit() != null) {
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

  public Pair<Option<Long>, Option<Long>> getMinAndMaxEventTime() {
    long minEventTime = Long.MAX_VALUE;
    long maxEventTime = Long.MIN_VALUE;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        minEventTime = writeStat.getMinEventTime() != null ? Math.min(writeStat.getMinEventTime(), minEventTime) : minEventTime;
        maxEventTime = writeStat.getMaxEventTime() != null ? Math.max(writeStat.getMaxEventTime(), maxEventTime) : maxEventTime;
      }
    }
    return Pair.of(
        minEventTime == Long.MAX_VALUE ? Option.empty() : Option.of(minEventTime),
        maxEventTime == Long.MIN_VALUE ? Option.empty() : Option.of(maxEventTime));
  }

  public HashSet<String> getWritePartitionPaths() {
    return new HashSet<>(partitionToWriteStats.keySet());
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
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), clazz);
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
    return "HoodieCommitMetadata{" + "partitionToWriteStats=" + partitionToWriteStats
        + ", compacted=" + compacted
        + ", extraMetadata=" + extraMetadata
        + ", operationType=" + operationType + '}';
  }
}
