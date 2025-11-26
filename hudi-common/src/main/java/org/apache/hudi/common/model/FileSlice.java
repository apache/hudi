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

import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Within a file group, a slice is a combination of data file written at a commit time and list of log files, containing
 * changes to the data file from that commit time.
 */
public class FileSlice implements Serializable {

  /**
   * File Group Id of the Slice.
   */
  private final HoodieFileGroupId fileGroupId;

  /**
   * Point in the timeline, at which the slice was created.
   */
  private final String baseInstantTime;

  /**
   * data file, with the compacted data, for this slice.
   */
  private HoodieBaseFile baseFile;

  /**
   * List of appendable log files with real time data - Sorted with greater log version first - Always empty for
   * copy_on_write storage.
   */
  private final TreeSet<HoodieLogFile> logFiles;

  public FileSlice(FileSlice fileSlice) {
    this(fileSlice, true);
  }

  private FileSlice(FileSlice fileSlice, boolean includeLogFiles) {
    this.baseInstantTime = fileSlice.baseInstantTime;
    this.baseFile = fileSlice.baseFile != null ? new HoodieBaseFile(fileSlice.baseFile) : null;
    this.fileGroupId = fileSlice.fileGroupId;
    this.logFiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
    if (includeLogFiles) {
      fileSlice.logFiles.forEach(lf -> this.logFiles.add(new HoodieLogFile(lf)));
    }
  }

  public FileSlice(String partitionPath, String baseInstantTime, String fileId) {
    this(new HoodieFileGroupId(partitionPath, fileId), baseInstantTime);
  }

  public FileSlice(HoodieFileGroupId fileGroupId, String baseInstantTime) {
    this.fileGroupId = fileGroupId;
    this.baseInstantTime = baseInstantTime;
    this.baseFile = null;
    this.logFiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
  }

  public FileSlice(HoodieFileGroupId fileGroupId, String baseInstantTime,
                   HoodieBaseFile baseFile, List<HoodieLogFile> logFiles) {
    this.fileGroupId = fileGroupId;
    this.baseInstantTime = baseInstantTime;
    this.baseFile = baseFile;
    this.logFiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
    this.logFiles.addAll(logFiles);
  }

  public void setBaseFile(HoodieBaseFile baseFile) {
    this.baseFile = baseFile;
  }

  public void addLogFile(HoodieLogFile logFile) {
    this.logFiles.add(logFile);
  }

  public FileSlice withLogFiles(boolean includeLogFiles) {
    if (includeLogFiles || !hasLogFiles()) {
      return this;
    } else {
      return new FileSlice(this, false);
    }
  }

  public FileSlice filterLogFiles(SerializableFunctionUnchecked<HoodieLogFile, Boolean> filter) {
    List<HoodieLogFile> filtered = logFiles.stream().filter(filter::apply).collect(Collectors.toList());
    if (filtered.size() == logFiles.size()) {
      // nothing filtered, returns this directly.
      return this;
    }
    return new FileSlice(fileGroupId, baseInstantTime, baseFile, filtered);
  }

  public boolean hasBootstrapBase() {
    return getBaseFile().isPresent() && getBaseFile().get().getBootstrapBaseFile().isPresent();
  }

  public boolean hasLogFiles() {
    return !logFiles.isEmpty();
  }

  public Stream<HoodieLogFile> getLogFiles() {
    return logFiles.stream();
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public String getPartitionPath() {
    return fileGroupId.getPartitionPath();
  }

  public String getFileId() {
    return fileGroupId.getFileId();
  }

  public HoodieFileGroupId getFileGroupId() {
    return fileGroupId;
  }

  public Option<HoodieBaseFile> getBaseFile() {
    return Option.ofNullable(baseFile);
  }

  public Option<HoodieLogFile> getLatestLogFile() {
    return Option.fromJavaOptional(logFiles.stream().findFirst());
  }

  /**
   * Return file names list of base file and log files.
   */
  public List<String> getAllFileNames() {
    List<String> fileList = new ArrayList<>();
    getBaseFile().ifPresent(hoodieBaseFile -> fileList.add(hoodieBaseFile.getFileName()));
    getLogFiles().forEach(hoodieLogFile -> fileList.add(hoodieLogFile.getFileName()));
    return fileList;
  }

  public long getTotalFileSize() {
    return getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L)
        + getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();
  }

  /**
   * Returns the latest instant time of the file slice.
   */
  public String getLatestInstantTime() {
    Option<String> latestDeltaCommitTime = getLatestLogFile().map(HoodieLogFile::getDeltaCommitTime);
    return latestDeltaCommitTime.isPresent() ? InstantComparison.maxInstant(latestDeltaCommitTime.get(), getBaseInstantTime()) : getBaseInstantTime();
  }

  /**
   * Returns true if there is no data file and no log files. Happens as part of pending compaction.
   */
  public boolean isEmpty() {
    return (baseFile == null) && (logFiles.isEmpty());
  }

  @Override
  public String toString() {
    return "FileSlice {" + "fileGroupId=" + fileGroupId
        + ", baseCommitTime=" + baseInstantTime
        + ", baseFile='" + baseFile + '\''
        + ", logFiles='" + logFiles + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileSlice slice = (FileSlice) o;
    return Objects.equals(fileGroupId, slice.fileGroupId) && Objects.equals(baseInstantTime, slice.baseInstantTime)
        && Objects.equals(baseFile, slice.baseFile) && Objects.equals(logFiles, slice.logFiles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileGroupId, baseInstantTime);
  }

  /**
   * Get the total file size of a file slice similar on the base file.
   * For the log file, we need to convert its size to the estimated size similar on the base file in a certain proportion
   */
  public long getTotalFileSizeAsParquetFormat(double logFileFraction) {
    long logFileSize = convertLogFilesSizeToExpectedParquetSize(logFileFraction);
    return getBaseFile().isPresent() ? getBaseFile().get().getFileSize() + logFileSize : logFileSize;
  }

  private long convertLogFilesSizeToExpectedParquetSize(double logFileFraction) {
    long totalSizeOfLogFiles =
        logFiles.stream()
            .map(HoodieLogFile::getFileSize)
            .filter(size -> size > 0)
            .reduce(Long::sum)
            .orElse(0L);
    // Here we assume that if there is no base parquet file, all log files contain only inserts.
    // We can then just get the parquet equivalent size of these log files, compare that with
    // {@link config.getParquetMaxFileSize()} and decide if there is scope to insert more rows
    return (long) (totalSizeOfLogFiles * logFileFraction);
  }
}
