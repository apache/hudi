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

import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * A set of data/base files + set of log files, that make up an unit for all operations.
 */
public class HoodieFileGroup implements Serializable {

  public static Comparator<String> getReverseCommitTimeComparator() {
    return Comparator.reverseOrder();
  }

  /**
   * file group id.
   */
  private final HoodieFileGroupId fileGroupId;

  /**
   * Slices of files in this group, sorted with greater commit first.
   */
  private final TreeMap<String, FileSlice> fileSlices;

  /**
   * Timeline, based on which all getter work.
   */
  private final HoodieTimeline timeline;

  /**
   * The last completed instant, that acts as a high watermark for all getters.
   */
  private final Option<HoodieInstant> lastInstant;

  public HoodieFileGroup(String partitionPath, String id, HoodieTimeline timeline) {
    this(new HoodieFileGroupId(partitionPath, id), timeline);
  }

  public HoodieFileGroup(HoodieFileGroupId fileGroupId, HoodieTimeline timeline) {
    this.fileGroupId = fileGroupId;
    this.fileSlices = new TreeMap<>(HoodieFileGroup.getReverseCommitTimeComparator());
    this.timeline = timeline;
    this.lastInstant = timeline.lastInstant();
  }

  /**
   * Potentially add a new file-slice by adding base-instant time A file-slice without any data-file and log-files can
   * exist (if a compaction just got requested).
   */
  public void addNewFileSliceAtInstant(String baseInstantTime) {
    if (!fileSlices.containsKey(baseInstantTime)) {
      fileSlices.put(baseInstantTime, new FileSlice(fileGroupId, baseInstantTime));
    }
  }

  /**
   * Add a new datafile into the file group.
   */
  public void addDataFile(HoodieDataFile dataFile) {
    if (!fileSlices.containsKey(dataFile.getCommitTime())) {
      fileSlices.put(dataFile.getCommitTime(), new FileSlice(fileGroupId, dataFile.getCommitTime()));
    }
    fileSlices.get(dataFile.getCommitTime()).setDataFile(dataFile);
  }

  /**
   * Add a new log file into the group.
   */
  public void addLogFile(HoodieLogFile logFile) {
    if (!fileSlices.containsKey(logFile.getBaseCommitTime())) {
      fileSlices.put(logFile.getBaseCommitTime(), new FileSlice(fileGroupId, logFile.getBaseCommitTime()));
    }
    fileSlices.get(logFile.getBaseCommitTime()).addLogFile(logFile);
  }

  public String getPartitionPath() {
    return fileGroupId.getPartitionPath();
  }

  public HoodieFileGroupId getFileGroupId() {
    return fileGroupId;
  }

  /**
   * A FileSlice is considered committed, if one of the following is true - There is a committed data file - There are
   * some log files, that are based off a commit or delta commit.
   */
  private boolean isFileSliceCommitted(FileSlice slice) {
    String maxCommitTime = lastInstant.get().getTimestamp();
    return timeline.containsOrBeforeTimelineStarts(slice.getBaseInstantTime())
        && HoodieTimeline.compareTimestamps(slice.getBaseInstantTime(), maxCommitTime, HoodieTimeline.LESSER_OR_EQUAL);

  }

  /**
   * Get all the the file slices including in-flight ones as seen in underlying file-system.
   */
  public Stream<FileSlice> getAllFileSlicesIncludingInflight() {
    return fileSlices.entrySet().stream().map(Map.Entry::getValue);
  }

  /**
   * Get latest file slices including in-flight ones.
   */
  public Option<FileSlice> getLatestFileSlicesIncludingInflight() {
    return Option.fromJavaOptional(getAllFileSlicesIncludingInflight().findFirst());
  }

  /**
   * Provides a stream of committed file slices, sorted reverse base commit time.
   */
  public Stream<FileSlice> getAllFileSlices() {
    if (!timeline.empty()) {
      return fileSlices.entrySet().stream().map(Map.Entry::getValue).filter(this::isFileSliceCommitted);
    }
    return Stream.empty();
  }

  /**
   * Gets the latest slice - this can contain either.
   * <p>
   * - just the log files without data file - (or) data file with 0 or more log files
   */
  public Option<FileSlice> getLatestFileSlice() {
    // there should always be one
    return Option.fromJavaOptional(getAllFileSlices().findFirst());
  }

  /**
   * Gets the latest data file.
   */
  public Option<HoodieDataFile> getLatestDataFile() {
    return Option.fromJavaOptional(getAllDataFiles().findFirst());
  }

  /**
   * Obtain the latest file slice, upto a commitTime i.e <= maxCommitTime.
   */
  public Option<FileSlice> getLatestFileSliceBeforeOrOn(String maxCommitTime) {
    return Option.fromJavaOptional(getAllFileSlices().filter(slice -> HoodieTimeline
        .compareTimestamps(slice.getBaseInstantTime(), maxCommitTime, HoodieTimeline.LESSER_OR_EQUAL)).findFirst());
  }

  /**
   * Obtain the latest file slice, upto a commitTime i.e < maxInstantTime.
   * 
   * @param maxInstantTime Max Instant Time
   * @return
   */
  public Option<FileSlice> getLatestFileSliceBefore(String maxInstantTime) {
    return Option.fromJavaOptional(getAllFileSlices().filter(
        slice -> HoodieTimeline.compareTimestamps(slice.getBaseInstantTime(), maxInstantTime, HoodieTimeline.LESSER))
        .findFirst());
  }

  public Option<FileSlice> getLatestFileSliceInRange(List<String> commitRange) {
    return Option.fromJavaOptional(
        getAllFileSlices().filter(slice -> commitRange.contains(slice.getBaseInstantTime())).findFirst());
  }

  /**
   * Stream of committed data files, sorted reverse commit time.
   */
  public Stream<HoodieDataFile> getAllDataFiles() {
    return getAllFileSlices().filter(slice -> slice.getDataFile().isPresent()).map(slice -> slice.getDataFile().get());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieFileGroup {");
    sb.append("id=").append(fileGroupId);
    sb.append(", fileSlices='").append(fileSlices).append('\'');
    sb.append(", lastInstant='").append(lastInstant).append('\'');
    sb.append('}');
    return sb.toString();
  }

  public void addFileSlice(FileSlice slice) {
    fileSlices.put(slice.getBaseInstantTime(), slice);
  }

  public Stream<FileSlice> getAllRawFileSlices() {
    return fileSlices.values().stream();
  }

  public HoodieTimeline getTimeline() {
    return timeline;
  }
}
