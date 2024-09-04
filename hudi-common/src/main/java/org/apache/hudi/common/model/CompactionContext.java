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

import org.apache.hudi.common.util.Option;

import java.util.List;

/**
 * Context in compaction operation.
 */
public class CompactionContext {

  /* ------------ basic compaction context ------------ */
  private String compactionInstantTime;
  private long maxMemoryForCompaction;
  private boolean metadataTableCompaction;

  /* ------------ sort merge join compaction context ------------ */
  private boolean sortMergeCompaction;
  private long maxMemoryForLogScanner;

  /* ------------ base file context ------------ */
  private Option<String> baseFilePath = Option.empty();
  private long baseFileRealSize;
  private long estimatedBaseFileSize;
  private boolean baseFileSorted;

  /* ------------ log files context ------------ */
  private long estimatedLogFileSize;
  private List<String> logFilePaths;
  private long logFilesRealSize;

  public void setLogFilesContext(List<String> logFilePaths, long logFilesRealSize, long estimatedLogFileSize) {
    this.logFilePaths = logFilePaths;
    this.logFilesRealSize = logFilesRealSize;
    this.estimatedLogFileSize = estimatedLogFileSize;
  }

  public void setBaseFileContext(String baseFilePath, long baseFileRealSize, long estimatedBaseFileSize, boolean baseFileSorted) {
    this.baseFilePath = Option.of(baseFilePath);
    this.baseFileRealSize = baseFileRealSize;
    this.estimatedBaseFileSize = estimatedBaseFileSize;
    this.baseFileSorted = baseFileSorted;
  }

  public void setBasicCompactionContext(long maxMemoryForCompaction, String compactionInstantTime, boolean metadataTableCompaction) {
    this.maxMemoryForCompaction = maxMemoryForCompaction;
    this.compactionInstantTime = compactionInstantTime;
    this.metadataTableCompaction = metadataTableCompaction;
  }

  public void setSortMergeJoinCompactionContext(boolean sortMergeCompaction, long maxMemoryForLogScanner) {
    this.sortMergeCompaction = sortMergeCompaction;
    this.maxMemoryForLogScanner = maxMemoryForLogScanner;
  }

  public List<String> getLogFilePaths() {
    return logFilePaths;
  }

  public void setLogFilePaths(List<String> logFilePaths) {
    this.logFilePaths = logFilePaths;
  }

  public boolean hasBaseFile() {
    return baseFilePath != null && baseFilePath.isPresent();
  }

  public Option<String> getBaseFilePath() {
    return baseFilePath;
  }

  public void setBaseFilePath(Option<String> baseFilePath) {
    this.baseFilePath = baseFilePath;
  }

  public String getCompactionInstantTime() {
    return compactionInstantTime;
  }

  public void setCompactionInstantTime(String compactionInstantTime) {
    this.compactionInstantTime = compactionInstantTime;
  }

  public void setMaxMemoryForLogScanner(long maxMemoryForLogScanner) {
    this.maxMemoryForLogScanner = maxMemoryForLogScanner;
  }

  public void setSortMergeCompaction(boolean sortMergeCompaction) {
    this.sortMergeCompaction = sortMergeCompaction;
  }

  public void setBaseFileSorted(boolean baseFileSorted) {
    this.baseFileSorted = baseFileSorted;
  }

  public void setMaxMemoryForCompaction(long maxMemoryForCompaction) {
    this.maxMemoryForCompaction = maxMemoryForCompaction;
  }

  public long getMaxMemoryForCompaction() {
    return maxMemoryForCompaction;
  }

  public boolean isMetadataTableCompaction() {
    return metadataTableCompaction;
  }

  public boolean isBaseFileSorted() {
    return baseFileSorted;
  }

  public long getBaseFileRealSize() {
    return baseFileRealSize;
  }

  public long getLogFilesRealSize() {
    return logFilesRealSize;
  }

  public long getEstimatedBaseFileSize() {
    return estimatedBaseFileSize;
  }

  public long getEstimatedLogFileSize() {
    return estimatedLogFileSize;
  }

  public long getMaxMemoryForLogScanner() {
    return maxMemoryForLogScanner;
  }

  public boolean isSortMergeCompaction() {
    return sortMergeCompaction;
  }

  @Override
  public String toString() {
    return "CompactionContext{"
        + "compactionInstantTime='" + compactionInstantTime + '\''
        + ", maxMemoryForCompaction=" + maxMemoryForCompaction
        + ", metadataTableCompaction=" + metadataTableCompaction
        + ", sortMergeCompaction=" + sortMergeCompaction
        + ", maxMemoryForLogScanner=" + maxMemoryForLogScanner
        + ", baseFilePath=" + baseFilePath
        + ", baseFileRealSize=" + baseFileRealSize
        + ", estimatedBaseFileSize=" + estimatedBaseFileSize
        + ", baseFileSorted=" + baseFileSorted
        + ", estimatedLogFileSize=" + estimatedLogFileSize
        + ", logFilePaths=" + logFilePaths
        + ", logFilesRealSize=" + logFilesRealSize
        + '}';
  }
}
