/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.annotation.Nullable;
import java.io.Serializable;
import org.apache.hadoop.fs.Path;

/**
 * Statistics about a single Hoodie write operation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieWriteStat implements Serializable {

  public static final String NULL_COMMIT = "null";

  /**
   * Id of the file being written
   */
  private String fileId;

  /**
   * Relative path to the file from the base path
   */
  private String path;

  /**
   * The previous version of the file. (null if this is the first version. i.e insert)
   */
  private String prevCommit;

  /**
   * Total number of records written for this file. - for updates, its the entire number of records
   * in the file - for inserts, its the actual number of records inserted.
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
   * Total size of file written
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
   * Partition Path associated with this writeStat
   */
  @Nullable
  private String partitionPath;

  /**
   * Total number of log records that were compacted by a compaction operation
   */
  @Nullable
  private Long totalLogRecords;

  /**
   * Total number of log files that were compacted by a compaction operation
   */
  @Nullable
  private Long totalLogFiles;

  /**
   * Total number of records updated by a compaction operation
   */
  @Nullable
  private Long totalRecordsToBeUpdate;

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

  public Long getTotalLogRecords() {
    return totalLogRecords;
  }

  public void setTotalLogRecords(Long totalLogRecords) {
    this.totalLogRecords = totalLogRecords;
  }

  public Long getTotalLogFiles() {
    return totalLogFiles;
  }

  public void setTotalLogFiles(Long totalLogFiles) {
    this.totalLogFiles = totalLogFiles;
  }

  public Long getTotalRecordsToBeUpdate() {
    return totalRecordsToBeUpdate;
  }

  public void setTotalRecordsToBeUpdate(Long totalRecordsToBeUpdate) {
    this.totalRecordsToBeUpdate = totalRecordsToBeUpdate;
  }

  public void setTempPath(String tempPath) {
    this.tempPath = tempPath;
  }

  public String getTempPath() {
    return this.tempPath;
  }

  /**
   * Set path and tempPath relative to the given basePath.
   */
  public void setPaths(Path basePath, Path path, Path tempPath) {
    this.path = path.toString().replace(basePath + "/", "");
    if (tempPath != null) {
      this.tempPath = tempPath.toString().replace(basePath + "/", "");
    }
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("HoodieWriteStat {")
        .append("path=" + path)
        .append(", tempPath=" + tempPath)
        .append(", prevCommit='" + prevCommit + '\'')
        .append(", numWrites=" + numWrites)
        .append(", numDeletes=" + numDeletes)
        .append(", numUpdateWrites=" + numUpdateWrites)
        .append(", numWriteBytes=" + totalWriteBytes)
        .append('}')
        .toString();
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
}
