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
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadata;

/**
 * All the metadata that gets stored along with a commit.
 * ******** IMPORTANT ********
 * For any newly added/removed data fields, make sure we have the same definition in
 * src/main/avro/HoodieCommitMetadata.avsc file!!!!!
 *
 * For any newly added subclass, make sure we add corresponding handler in
 * org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2#deserialize method.
 * ***************************
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@EqualsAndHashCode
@ToString
@Slf4j
public class HoodieCommitMetadata implements Serializable {

  public static final String SCHEMA_KEY = "schema";
  protected Map<String, List<HoodieWriteStat>> partitionToWriteStats;
  @Setter
  protected Boolean compacted;

  @EqualsAndHashCode.Exclude
  protected Map<String, String> extraMetadata;

  @Setter
  @EqualsAndHashCode.Exclude
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

  public List<HoodieWriteStat> getWriteStats() {
    return partitionToWriteStats.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  public String getMetadata(String metaKey) {
    return extraMetadata.get(metaKey);
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

  public HashMap<String, String> getFileIdAndFullPaths(StoragePath basePath) {
    HashMap<String, String> fullPaths = new HashMap<>();
    for (Map.Entry<String, String> entry : getFileIdAndRelativePaths().entrySet()) {
      String fullPath = entry.getValue() != null
          ? FSUtils.constructAbsolutePath(basePath, entry.getValue()).toString()
          : null;
      fullPaths.put(entry.getKey(), fullPath);
    }
    return fullPaths;
  }

  public List<String> getFullPathsByPartitionPath(String basePath, String partitionPath) {
    HashSet<String> fullPaths = new HashSet<>();
    if (getPartitionToWriteStats().get(partitionPath) != null) {
      for (HoodieWriteStat stat : getPartitionToWriteStats().get(partitionPath)) {
        if ((stat.getFileId() != null)) {
          String fullPath = FSUtils.constructAbsolutePath(basePath, stat.getPath()).toString();
          fullPaths.add(fullPath);
        }
      }
    }
    return new ArrayList<>(fullPaths);
  }

  public Map<HoodieFileGroupId, String> getFileGroupIdAndFullPaths(String basePath) {
    Map<HoodieFileGroupId, String> fileGroupIdToFullPaths = new HashMap<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId(stat.getPartitionPath(), stat.getFileId());
        StoragePath fullPath = new StoragePath(basePath, stat.getPath());
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
   * @param storage     {@link HoodieStorage} instance.
   * @param basePath    The base path
   * @return the file full path to file status mapping
   */
  public Map<String, StoragePathInfo> getFullPathToInfo(HoodieStorage storage,
                                                        String basePath) {
    Map<String, StoragePathInfo> fullPathToInfoMap = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        StoragePath fullPath = relativeFilePath != null
            ? FSUtils.constructAbsolutePath(basePath, relativeFilePath) : null;
        if (fullPath != null) {
          long blockSize = storage.getDefaultBlockSize(fullPath);
          StoragePathInfo pathInfo = new StoragePathInfo(
              fullPath, stat.getFileSizeInBytes(), false, (short) 0, blockSize, 0);
          fullPathToInfoMap.put(fullPath.toString(), pathInfo);
        }
      }
    }
    return fullPathToInfoMap;
  }

  /**
   * Extract the file status of all affected files from the commit metadata. If a file has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit by file group ID.
   *
   * <p>Note: different with {@link #getFullPathToInfo(HoodieStorage, String)},
   * only the latest commit file for a file group is returned,
   * this is an optimization for COPY_ON_WRITE table to eliminate legacy files for filesystem view.
   *
   * @param basePath    The base path
   * @return the file ID to file status mapping
   */
  public Map<String, StoragePathInfo> getFileIdToInfo(String basePath) {
    Map<String, StoragePathInfo> fileIdToInfoMap = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        StoragePath fullPath =
            relativeFilePath != null ? FSUtils.constructAbsolutePath(basePath,
                relativeFilePath) : null;
        if (fullPath != null) {
          StoragePathInfo pathInfo =
              new StoragePathInfo(fullPath, stat.getFileSizeInBytes(), false, (short) 0, 0, 0);
          fileIdToInfoMap.put(stat.getFileId(), pathInfo);
        }
      }
    }
    return fileIdToInfoMap;
  }

  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      log.info("partition path is null for " + partitionToWriteStats.get(null));
      partitionToWriteStats.remove(null);
    }
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  /**
   * For a mor log file, get the completed previous file slice from the related commit metadata.
   * This file slice will be used when we extract the change data from this mor log file.
   */
  public static Option<Pair<String, List<String>>> getDependentFileSliceForFileGroupFromDeltaCommit(
      InputStream inputStream, HoodieFileGroupId fileGroupId, String currentLogFile) {
    try {
      org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadata = deserializeAvroMetadata(inputStream, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
      Map<String,List<org.apache.hudi.avro.model.HoodieWriteStat>> partitionToWriteStatsMap =
              commitMetadata.getPartitionToWriteStats();
      List<org.apache.hudi.avro.model.HoodieWriteStat> targetWriteStats = new ArrayList<>();
      for (Map.Entry<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> partitionToWriteStat: partitionToWriteStatsMap.entrySet()) {
        for (org.apache.hudi.avro.model.HoodieWriteStat writeStat: partitionToWriteStat.getValue()) {
          HoodieFileGroupId fgId = new HoodieFileGroupId(partitionToWriteStat.getKey(), writeStat.getFileId());
          if (fgId.equals(fileGroupId)) {
            targetWriteStats.add(writeStat);
          }
        }
      }
      if (targetWriteStats.isEmpty()) {
        return Option.empty();
      } else if (targetWriteStats.size() == 1) {
        org.apache.hudi.avro.model.HoodieWriteStat writeStat = targetWriteStats.get(0);
        return Option.of(Pair.of(writeStat.getBaseFile() == null ? "" : writeStat.getBaseFile(),
            writeStat.getLogFiles().stream().filter(logFile -> !logFile.equals(currentLogFile)).collect(Collectors.toList())));
      } else {
        // There maybe multiple write stats for the same file group id, e.g., when the write buffer exceeds the limit,
        // flink writer will eager flushing records into the log file with increasing file versions inside one checkpoint.
        String baseFile = "";
        List<String> logFiles = new ArrayList<>();
        for (org.apache.hudi.avro.model.HoodieWriteStat writeStat: targetWriteStats) {
          baseFile = writeStat.getBaseFile() == null ? "" : writeStat.getBaseFile();
          logFiles.addAll(writeStat.getLogFiles());
        }
        return Option.of(Pair.of(baseFile, logFiles.stream().filter(f -> f.compareTo(currentLogFile) < 0).distinct().sorted().collect(Collectors.toList())));
      }
    } catch (Exception e) {
      throw new HoodieException("Fail to parse the base file and log files from DeltaCommit", e);
    }
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

  public long fetchTotalFiles() {
    return partitionToWriteStats.values().stream().mapToLong(List::size).sum();
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

  public Long getTotalLogBlocksCompacted() {
    Long totalLogBlocks = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogBlocks += writeStat.getTotalLogBlocks();
      }
    }
    return totalLogBlocks;
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

  public Long getTotalCorruptLogBlocks() {
    Long totalCorruptedLogBlocks = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalCorruptedLogBlocks += writeStat.getTotalCorruptLogBlock();
      }
    }
    return totalCorruptedLogBlocks;
  }

  public Long getTotalRollbackLogBlocks() {
    Long totalRollbackLogBlocks = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalRollbackLogBlocks += writeStat.getTotalRollbackBlocks();
      }
    }
    return totalRollbackLogBlocks;
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
}
