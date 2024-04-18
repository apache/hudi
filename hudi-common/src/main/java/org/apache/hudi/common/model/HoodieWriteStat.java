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

import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.storage.StoragePath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/**
 * Statistics about a single Hoodie write operation.
 */
public class HoodieWriteStat implements Serializable {

  public static final String NULL_COMMIT = "null";

  /**
   * Id of the file being written.
   */
  private String fileId;

  /**
   * Relative path to the file from the base path.
   */
  private String path;

  /**
   * Relative CDC file path that store the CDC data and its size.
   */
  private Map<String, Long> cdcStats;

  /**
   * The previous version of the file. (null if this is the first version. i.e insert)
   */
  private String prevCommit;

  /**
   * Total number of records written for this file. - for updates, its the entire number of records in the file - for
   * inserts, its the actual number of records inserted.
   */
  private long numWrites;

  /**
   * Total number of records deleted.
   */
  private long numDeletes;

  /**
   * Total number of records actually changed. (0 for inserts)
   */
  private long numUpdateWrites;

  /**
   * Total number of insert records or converted to updates (for small file handling).
   */
  private long numInserts;

  /**
   * Total number of bytes written.
   */
  private long totalWriteBytes;

  /**
   * Total number of records, that were n't able to be written due to errors.
   */
  private long totalWriteErrors;

  /**
   * Relative path to the temporary file from the base path.
   */
  @Nullable
  private String tempPath;

  /**
   * Following properties are associated only with the result of a Compaction Operation
   */

  /**
   * Partition Path associated with this writeStat.
   */
  @Nullable
  private String partitionPath;

  /**
   * Total number of log records that were compacted by a compaction operation.
   */
  @Nullable
  private long totalLogRecords;

  /**
   * Total number of log files compacted for a file slice with this base fileid.
   */
  @Nullable
  private long totalLogFilesCompacted;

  /**
   * Total size of all log files for a file slice with this base fileid.
   */
  @Nullable
  private long totalLogSizeCompacted;

  /**
   * Total number of records updated by a compaction operation.
   */
  @Nullable
  private long totalUpdatedRecordsCompacted;

  /**
   * Total number of log blocks seen in a compaction operation.
   */
  @Nullable
  private long totalLogBlocks;

  /**
   * Total number of corrupt blocks seen in a compaction operation.
   */
  @Nullable
  private long totalCorruptLogBlock;

  /**
   * Total number of rollback blocks seen in a compaction operation.
   */
  @Nullable
  private long totalRollbackBlocks;

  /**
   * File Size as of close.
   */
  private long fileSizeInBytes;

  /**
   * The earliest of incoming records' event times (Epoch ms) for calculating latency.
   */
  @Nullable
  private Long minEventTime;

  /**
   * The latest of incoming records' event times (Epoch ms) for calculating freshness.
   */
  @Nullable
  private Long maxEventTime;

  @Nullable
  private RuntimeStats runtimeStats;

  public HoodieWriteStat() {
    // called by jackson json lib
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setPrevCommit(String prevCommit) {
    this.prevCommit = prevCommit;
  }

  public void setNumWrites(long numWrites) {
    this.numWrites = numWrites;
  }

  public void setNumDeletes(long numDeletes) {
    this.numDeletes = numDeletes;
  }

  public void setNumUpdateWrites(long numUpdateWrites) {
    this.numUpdateWrites = numUpdateWrites;
  }

  public void setNumInserts(long numInserts) {
    this.numInserts = numInserts;
  }

  public long getTotalWriteBytes() {
    return totalWriteBytes;
  }

  public void setTotalWriteBytes(long totalWriteBytes) {
    this.totalWriteBytes = totalWriteBytes;
  }

  public long getTotalWriteErrors() {
    return totalWriteErrors;
  }

  public void setTotalWriteErrors(long totalWriteErrors) {
    this.totalWriteErrors = totalWriteErrors;
  }

  public String getPrevCommit() {
    return prevCommit;
  }

  public long getNumWrites() {
    return numWrites;
  }

  public long getNumDeletes() {
    return numDeletes;
  }

  public long getNumUpdateWrites() {
    return numUpdateWrites;
  }

  public long getNumInserts() {
    return numInserts;
  }

  public String getFileId() {
    return fileId;
  }

  public String getPath() {
    return path;
  }

  @Nullable
  public Map<String, Long> getCdcStats() {
    return cdcStats;
  }

  public void setCdcStats(Map<String, Long> cdcStats) {
    this.cdcStats = cdcStats;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public long getTotalLogRecords() {
    return totalLogRecords;
  }

  public void setTotalLogRecords(long totalLogRecords) {
    this.totalLogRecords = totalLogRecords;
  }

  public long getTotalLogFilesCompacted() {
    return totalLogFilesCompacted;
  }

  public void setTotalLogFilesCompacted(long totalLogFilesCompacted) {
    this.totalLogFilesCompacted = totalLogFilesCompacted;
  }

  public long getTotalUpdatedRecordsCompacted() {
    return totalUpdatedRecordsCompacted;
  }

  public void setTotalUpdatedRecordsCompacted(long totalUpdatedRecordsCompacted) {
    this.totalUpdatedRecordsCompacted = totalUpdatedRecordsCompacted;
  }

  public void setTempPath(String tempPath) {
    this.tempPath = tempPath;
  }

  public String getTempPath() {
    return this.tempPath;
  }

  public long getTotalLogSizeCompacted() {
    return totalLogSizeCompacted;
  }

  public void setTotalLogSizeCompacted(long totalLogSizeCompacted) {
    this.totalLogSizeCompacted = totalLogSizeCompacted;
  }

  public long getTotalLogBlocks() {
    return totalLogBlocks;
  }

  public void setTotalLogBlocks(long totalLogBlocks) {
    this.totalLogBlocks = totalLogBlocks;
  }

  public long getTotalCorruptLogBlock() {
    return totalCorruptLogBlock;
  }

  public void setTotalCorruptLogBlock(long totalCorruptLogBlock) {
    this.totalCorruptLogBlock = totalCorruptLogBlock;
  }

  public long getTotalRollbackBlocks() {
    return totalRollbackBlocks;
  }

  public void setTotalRollbackBlocks(long totalRollbackBlocks) {
    this.totalRollbackBlocks = totalRollbackBlocks;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public void setFileSizeInBytes(long fileSizeInBytes) {
    this.fileSizeInBytes = fileSizeInBytes;
  }

  public Long getMinEventTime() {
    return minEventTime;
  }

  public void setMinEventTime(Long minEventTime) {
    if (this.minEventTime == null) {
      this.minEventTime = minEventTime;
    } else {
      this.minEventTime = Math.min(minEventTime, this.minEventTime);
    }
  }

  public Long getMaxEventTime() {
    return maxEventTime;
  }

  public void setMaxEventTime(Long maxEventTime) {
    if (this.maxEventTime == null) {
      this.maxEventTime = maxEventTime;
    } else {
      this.maxEventTime = Math.max(maxEventTime, this.maxEventTime);
    }
  }

  @Nullable
  public RuntimeStats getRuntimeStats() {
    return runtimeStats;
  }

  public void setRuntimeStats(@Nullable RuntimeStats runtimeStats) {
    this.runtimeStats = runtimeStats;
  }

  /**
   * Set path and tempPath relative to the given basePath.
   */
  public void setPath(StoragePath basePath, StoragePath path) {
    this.path = path.toString().replace(basePath + "/", "");
  }

  @Override
  public String toString() {
    return "HoodieWriteStat{fileId='" + fileId + '\'' + ", path='" + path + '\'' + ", prevCommit='" + prevCommit
        + '\'' + ", numWrites=" + numWrites + ", numDeletes=" + numDeletes + ", numUpdateWrites=" + numUpdateWrites
        + ", totalWriteBytes=" + totalWriteBytes + ", totalWriteErrors=" + totalWriteErrors + ", tempPath='" + tempPath
        + '\'' + ", cdcStats='" + JsonUtils.toString(cdcStats)
        + '\'' + ", partitionPath='" + partitionPath + '\'' + ", totalLogRecords=" + totalLogRecords
        + ", totalLogFilesCompacted=" + totalLogFilesCompacted + ", totalLogSizeCompacted=" + totalLogSizeCompacted
        + ", totalUpdatedRecordsCompacted=" + totalUpdatedRecordsCompacted + ", totalLogBlocks=" + totalLogBlocks
        + ", totalCorruptLogBlock=" + totalCorruptLogBlock + ", totalRollbackBlocks=" + totalRollbackBlocks + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieWriteStat that = (HoodieWriteStat) o;
    if (!path.equals(that.path)) {
      return false;
    }
    return prevCommit.equals(that.prevCommit);

  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + prevCommit.hashCode();
    return result;
  }

  /**
   * The runtime stats for writing operation.
   */
  public static class RuntimeStats implements Serializable {

    /**
     * Total time taken to read and merge logblocks in a log file.
     */
    private long totalScanTime;

    /**
     * Total time taken by a Hoodie Merge for an existing file.
     */
    private long totalUpsertTime;

    /**
     * Total time taken by a Hoodie Insert to a file.
     */
    private long totalCreateTime;

    public long getTotalScanTime() {
      return totalScanTime;
    }

    public void setTotalScanTime(long totalScanTime) {
      this.totalScanTime = totalScanTime;
    }

    public long getTotalUpsertTime() {
      return totalUpsertTime;
    }

    public void setTotalUpsertTime(long totalUpsertTime) {
      this.totalUpsertTime = totalUpsertTime;
    }

    public long getTotalCreateTime() {
      return totalCreateTime;
    }

    public void setTotalCreateTime(long totalCreateTime) {
      this.totalCreateTime = totalCreateTime;
    }
  }
}
