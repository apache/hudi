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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
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
  private final TreeSet<HoodieLogFileEntry> logFiles;

  public FileSlice(FileSlice fileSlice) {
    this(fileSlice, true);
  }

  private FileSlice(FileSlice fileSlice, boolean includeLogFiles) {
    this.baseInstantTime = fileSlice.baseInstantTime;
    this.baseFile = fileSlice.baseFile != null ? new HoodieBaseFile(fileSlice.baseFile) : null;
    this.fileGroupId = fileSlice.fileGroupId;
    this.logFiles = new TreeSet<>(HoodieLogFileEntry.getLogFileEntryComparator());
    if (includeLogFiles) {
      fileSlice.logFiles.forEach(logFileEntry -> this.logFiles.add(new HoodieLogFileEntry(logFileEntry)));
    }
  }

  public FileSlice(String partitionPath, String baseInstantTime, String fileId) {
    this(new HoodieFileGroupId(partitionPath, fileId), baseInstantTime);
  }

  public FileSlice(HoodieFileGroupId fileGroupId, String baseInstantTime) {
    this.fileGroupId = fileGroupId;
    this.baseInstantTime = baseInstantTime;
    this.baseFile = null;
    this.logFiles = new TreeSet<>(HoodieLogFileEntry.getLogFileEntryComparator());
  }

  public FileSlice(HoodieFileGroupId fileGroupId, String baseInstantTime,
                   HoodieBaseFile baseFile, List<HoodieLogFile> logFiles, HoodieTableMetaClient metaClient) {
    this.fileGroupId = fileGroupId;
    this.baseInstantTime = baseInstantTime;
    this.baseFile = baseFile;
    this.logFiles = new TreeSet<>(HoodieLogFileEntry.getLogFileEntryComparator());
    if (metaClient.getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      CompletionTimeQueryView completionTimeQueryView = metaClient.getTimelineLayout().getTimelineFactory().createCompletionTimeQueryView(metaClient);
      logFiles.forEach(lf -> this.logFiles.add(new HoodieLogFileEntry(lf, completionTimeQueryView.getCompletionTime(baseInstantTime, lf.getDeltaCommitTime()))));
    } else {
      logFiles.forEach(lf -> this.logFiles.add(new HoodieLogFileEntry(lf, Option.empty())));
    }
  }

  public void setBaseFile(HoodieBaseFile baseFile) {
    this.baseFile = baseFile;
  }

  public void addLogFile(HoodieLogFile logFile, Option<String> completionTime) {
    this.logFiles.add(new HoodieLogFileEntry(logFile, completionTime));
  }

  public FileSlice withLogFiles(boolean includeLogFiles) {
    if (includeLogFiles || !hasLogFiles()) {
      return this;
    } else {
      return new FileSlice(this, false);
    }
  }

  public boolean hasBootstrapBase() {
    return getBaseFile().isPresent() && getBaseFile().get().getBootstrapBaseFile().isPresent();
  }

  public boolean hasLogFiles() {
    return !logFiles.isEmpty();
  }

  public Stream<HoodieLogFile> getLogFiles() {
    return logFiles.stream().map(HoodieLogFileEntry::getLogFile);
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
    return Option.fromJavaOptional(logFiles.stream().map(HoodieLogFileEntry::getLogFile).findFirst());
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
        logFiles.stream().map(HoodieLogFileEntry::getLogFile)
            .map(HoodieLogFile::getFileSize)
            .filter(size -> size > 0)
            .reduce(Long::sum)
            .orElse(0L);
    // Here we assume that if there is no base parquet file, all log files contain only inserts.
    // We can then just get the parquet equivalent size of these log files, compare that with
    // {@link config.getParquetMaxFileSize()} and decide if there is scope to insert more rows
    return (long) (totalSizeOfLogFiles * logFileFraction);
  }

  private static class HoodieLogFileEntry {
    HoodieLogFile logFile;
    Option<String> completionTime;

    private HoodieLogFileEntry(HoodieLogFile logFile, Option<String> completionTime) {
      this.logFile = logFile;
      this.completionTime = completionTime;
    }

    public HoodieLogFileEntry(HoodieLogFileEntry logFileEntry) {
      this.logFile = new HoodieLogFile(logFileEntry.logFile);
      this.completionTime = logFileEntry.completionTime;
    }

    private static Comparator<HoodieLogFileEntry> getLogFileEntryComparator() {
      return (entry1, entry2) -> {
        ValidationUtils.checkArgument(entry1.completionTime.isPresent() == entry2.completionTime.isPresent(),
            String.format("We expect either all log files or no log file to have completion time %s %s", entry1, entry2));
        if (entry1.completionTime.isPresent()) {
          String completionTime1 = entry1.completionTime.get();
          String completionTime2 = entry2.completionTime.get();
          int comparisonResult = completionTime1.compareTo(completionTime2);
          return comparisonResult != 0 ? comparisonResult : HoodieLogFile.getReverseLogFileComparator().compare(entry1.logFile, entry2.logFile);
        } else {
          return HoodieLogFile.getReverseLogFileComparator().compare(entry1.logFile, entry2.logFile);
        }
      };
    }

    private HoodieLogFile getLogFile() {
      return logFile;
    }

    @Override
    public String toString() {
      return String.format("{log file: %s, completionTime: %s}", logFile.toString(), completionTime);
    }
  }
}
