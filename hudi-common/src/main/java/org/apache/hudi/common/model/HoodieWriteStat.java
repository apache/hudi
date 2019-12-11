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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Statistics about a single Hoodie write operation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
   * Total size of file written.
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
  private long totalRollbackBlocks;

  /**
   * File Size as of close.
   */
  private long fileSizeInBytes;

  @Nullable
  @JsonIgnore
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

  public void setTotalRollbackBlocks(Long totalRollbackBlocks) {
    this.totalRollbackBlocks = totalRollbackBlocks;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public void setFileSizeInBytes(long fileSizeInBytes) {
    this.fileSizeInBytes = fileSizeInBytes;
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
  public void setPath(Path basePath, Path path) {
    this.path = path.toString().replace(basePath + "/", "");
  }

  @Override
  public String toString() {
    return "HoodieWriteStat{fileId='" + fileId + '\'' + ", path='" + path + '\'' + ", prevCommit='" + prevCommit
        + '\'' + ", numWrites=" + numWrites + ", numDeletes=" + numDeletes + ", numUpdateWrites=" + numUpdateWrites
        + ", totalWriteBytes=" + totalWriteBytes + ", totalWriteErrors=" + totalWriteErrors + ", tempPath='" + tempPath
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
    @Nullable
    private long totalScanTime;

    /**
     * Total time taken by a Hoodie Merge for an existing file.
     */
    @Nullable
    private long totalUpsertTime;

    /**
     * Total time taken by a Hoodie Insert to a file.
     */
    @Nullable
    private long totalCreateTime;

    @Nullable
    public long getTotalScanTime() {
      return totalScanTime;
    }

    public void setTotalScanTime(@Nullable long totalScanTime) {
      this.totalScanTime = totalScanTime;
    }

    @Nullable
    public long getTotalUpsertTime() {
      return totalUpsertTime;
    }

    public void setTotalUpsertTime(@Nullable long totalUpsertTime) {
      this.totalUpsertTime = totalUpsertTime;
    }

    @Nullable
    public long getTotalCreateTime() {
      return totalCreateTime;
    }

    public void setTotalCreateTime(@Nullable long totalCreateTime) {
      this.totalCreateTime = totalCreateTime;
    }
  }
}
