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

import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * A set of data/base files + set of log files, that make up a unit for all operations.
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

  public HoodieFileGroup(HoodieFileGroup fileGroup) {
    this.timeline = fileGroup.timeline;
    this.fileGroupId = fileGroup.fileGroupId;
    this.fileSlices = new TreeMap<>(fileGroup.fileSlices);
    this.lastInstant = fileGroup.lastInstant;
  }

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
  public void addBaseFile(HoodieBaseFile dataFile) {
    if (!fileSlices.containsKey(dataFile.getCommitTime())) {
      fileSlices.put(dataFile.getCommitTime(), new FileSlice(fileGroupId, dataFile.getCommitTime()));
    }
    fileSlices.get(dataFile.getCommitTime()).setBaseFile(dataFile);
  }

  /**
   * Add a new log file into the group.
   *
   * <p>CAUTION: the log file must be added in sequence of the delta commit time.
   */
  public void addLogFile(CompletionTimeQueryView completionTimeQueryView, HoodieLogFile logFile) {
    String baseInstantTime = getBaseInstantTime(completionTimeQueryView, logFile);
    if (!fileSlices.containsKey(baseInstantTime)) {
      fileSlices.put(baseInstantTime, new FileSlice(fileGroupId, baseInstantTime));
    }
    fileSlices.get(baseInstantTime).addLogFile(logFile);
  }

  @VisibleForTesting
  public String getBaseInstantTime(CompletionTimeQueryView completionTimeQueryView, HoodieLogFile logFile) {
    if (fileSlices.isEmpty()) {
      // no base file in the file group, use the log file delta commit time.
      return logFile.getDeltaCommitTime();
    }
    Option<String> completionTimeOpt = completionTimeQueryView.getCompletionTime(fileSlices.firstKey(), logFile.getDeltaCommitTime());
    if (completionTimeOpt.isPresent()) {
      for (String commitTime : fileSlices.keySet()) {
        // find the largest commit time that is smaller than the log delta commit completion time
        if (compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, commitTime)) {
          return commitTime;
        }
      }
      // no base file that starts earlier than the log delta commit completion time,
      // use the log file delta commit time.
      return logFile.getDeltaCommitTime();
    }
    // the log file is still pending, it always belongs to the latest file slice.
    return fileSlices.firstKey();
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
    if (!compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, lastInstant.get().requestedTime())) {
      return false;
    }

    return timeline.containsOrBeforeTimelineStarts(slice.getBaseInstantTime());
  }

  /**
   * Get all the file slices including in-flight ones as seen in underlying file system.
   */
  public Stream<FileSlice> getAllFileSlicesIncludingInflight() {
    return fileSlices.values().stream();
  }

  /**
   * Get the latest file slices including inflight ones.
   */
  public Option<FileSlice> getLatestFileSlicesIncludingInflight() {
    return Option.fromJavaOptional(getAllFileSlicesIncludingInflight().findFirst());
  }

  /**
   * Provides a stream of committed file slices, sorted reverse base commit time.
   */
  public Stream<FileSlice> getAllFileSlices() {
    if (!timeline.empty()) {
      return fileSlices.values().stream().filter(this::isFileSliceCommitted);
    }
    return Stream.empty();
  }

  public Stream<FileSlice> getAllFileSlicesBeforeOn(String maxInstantTime) {
    return getAllFileSlices().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, maxInstantTime));
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
  public Option<HoodieBaseFile> getLatestDataFile() {
    return Option.fromJavaOptional(getAllBaseFiles().findFirst());
  }

  /**
   * Obtain the latest file slice, upto a instantTime i.e <= maxInstantTime.
   */
  public Option<FileSlice> getLatestFileSliceBeforeOrOn(String maxInstantTime) {
    return Option.fromJavaOptional(getAllFileSlices().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, maxInstantTime)).findFirst());
  }

  /**
   * Obtain the latest file slice, upto an instantTime i.e < maxInstantTime.
   * 
   * @param maxInstantTime Max Instant Time
   * @return the latest file slice
   */
  public Option<FileSlice> getLatestFileSliceBefore(String maxInstantTime) {
    return Option.fromJavaOptional(getAllFileSlices().filter(
        slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN, maxInstantTime))
        .findFirst());
  }

  public Option<FileSlice> getLatestFileSliceInRange(List<String> commitRange) {
    return Option.fromJavaOptional(
        getAllFileSlices().filter(slice -> commitRange.contains(slice.getBaseInstantTime())).findFirst());
  }

  /**
   * Stream of committed data files, sorted reverse commit time.
   */
  public Stream<HoodieBaseFile> getAllBaseFiles() {
    return getAllFileSlices().filter(slice -> slice.getBaseFile().isPresent()).map(slice -> slice.getBaseFile().get());
  }

  @Override
  public String toString() {
    return "HoodieFileGroup {" + "id=" + fileGroupId
        + ", fileSlices='" + fileSlices + '\''
        + ", lastInstant='" + lastInstant + '\''
        + '}';
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieFileGroup fileGroup = (HoodieFileGroup) o;
    return Objects.equals(fileGroupId, fileGroup.fileGroupId)
        && Objects.equals(fileSlices, fileGroup.fileSlices)
        && Objects.equals(lastInstant, fileGroup.lastInstant);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileGroupId, fileSlices, lastInstant);
  }
}
