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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hoodie SourceSplit implementation for source V2. It has the same semantic to the {@link org.apache.hudi.table.format.mor.MergeOnReadInputSplit}.
 */
@Getter
public class HoodieSourceSplit implements SourceSplit, Serializable {
  public static AtomicInteger SPLIT_COUNTER = new AtomicInteger(0);
  private static final long serialVersionUID = 1L;
  private static final long NUM_NO_CONSUMPTION = 0L;

  // the split number
  private final int splitNum;
  // the base file of a file slice
  private final Option<String> basePath;
  // change log files of a file slice
  private final Option<List<String>> logPaths;
  // the base table path
  private final String tablePath;
  // partition path
  private final String partitionPath;
  // source merge type
  private final String mergeType;
  // the latest commit instant time
  private final String latestCommit;
  // file id of file splice
  @Setter
  protected String fileId;

  // for streaming reader to record the consumed offset,
  // which is the start of next round reading.
  private long consumed = NUM_NO_CONSUMPTION;

  // for failure recovering
  private int fileOffset;
  private long recordOffset;

  public HoodieSourceSplit(
      int splitNum,
      @Nullable String basePath,
      Option<List<String>> logPaths,
      String tablePath,
      String partitionPath,
      String latestCommit,
      String mergeType,
      String fileId) {
    this.splitNum = splitNum;
    this.basePath = Option.ofNullable(basePath);
    this.logPaths = logPaths;
    this.tablePath = tablePath;
    this.partitionPath = partitionPath;
    this.latestCommit = latestCommit;
    this.mergeType = mergeType;
    this.fileId = fileId;
    this.fileOffset = 0;
    this.recordOffset = 0L;
  }

  @Override
  public String splitId() {
    return toString();
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public Option<String> getBasePath() {
    return basePath;
  }

  public Option<List<String>> getLogPaths() {
    return logPaths;
  }

  public String getTablePath() {
    return tablePath;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getLatestCommit() {
    return latestCommit;
  }

  public String getMergeType() {
    return mergeType;
  }

  public void consume() {
    this.consumed += 1L;
  }

  public boolean isConsumed() {
    return this.consumed != NUM_NO_CONSUMPTION;
  }

  public void updatePosition(int newFileOffset, long newRecordOffset) {
    fileOffset = newFileOffset;
    recordOffset = newRecordOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieSourceSplit that = (HoodieSourceSplit) o;
    return splitNum == that.splitNum && consumed == that.consumed && fileOffset == that.fileOffset && recordOffset == that.recordOffset && Objects.equals(basePath, that.basePath)
        && Objects.equals(logPaths, that.logPaths) && Objects.equals(tablePath, that.tablePath) && Objects.equals(partitionPath, that.partitionPath)
        && Objects.equals(mergeType, that.mergeType) && Objects.equals(latestCommit, that.latestCommit) && Objects.equals(fileId, that.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitNum, basePath, logPaths, tablePath, partitionPath, mergeType, latestCommit, fileId, consumed, fileOffset, recordOffset);
  }

  @Override
  public String toString() {
    return "HoodieSourceSplit{"
        + "splitNum=" + splitNum
        + ", basePath=" + basePath
        + ", logPaths=" + logPaths
        + ", tablePath='" + tablePath + '\''
        + ", partitionPath='" + partitionPath + '\''
        + ", lastCommit='" + latestCommit + '\''
        + ", mergeType='" + mergeType + '\''
        + '}';
  }
}
