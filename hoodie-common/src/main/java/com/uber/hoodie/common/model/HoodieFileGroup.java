/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * A set of data/base files + set of log files, that make up an unit for all operations
 */
public class HoodieFileGroup implements Serializable {

  public static Comparator<String> getReverseCommitTimeComparator() {
    return (o1, o2) -> {
      // reverse the order
      return o2.compareTo(o1);
    };
  }

  /**
   * Partition containing the file group.
   */
  private final String partitionPath;

  /**
   * uniquely identifies the file group
   */
  private final String id;

  /**
   * Slices of files in this group, sorted with greater commit first.
   */
  private final TreeMap<String, FileSlice> fileSlices;

  /**
   * Timeline, based on which all getter work
   */
  private final HoodieTimeline timeline;

  /**
   * The last completed instant, that acts as a high watermark for all getters
   */
  private final Optional<HoodieInstant> lastInstant;

  public HoodieFileGroup(String partitionPath, String id, HoodieTimeline timeline) {
    this.partitionPath = partitionPath;
    this.id = id;
    this.fileSlices = new TreeMap<>(HoodieFileGroup.getReverseCommitTimeComparator());
    this.timeline = timeline;
    this.lastInstant = timeline.lastInstant();
  }

  /**
   * Add a new datafile into the file group
   */
  public void addDataFile(HoodieDataFile dataFile) {
    if (!fileSlices.containsKey(dataFile.getCommitTime())) {
      fileSlices.put(dataFile.getCommitTime(), new FileSlice(dataFile.getCommitTime(), id));
    }
    fileSlices.get(dataFile.getCommitTime()).setDataFile(dataFile);
  }

  /**
   * Add a new log file into the group
   */
  public void addLogFile(HoodieLogFile logFile) {
    if (!fileSlices.containsKey(logFile.getBaseCommitTime())) {
      fileSlices.put(logFile.getBaseCommitTime(), new FileSlice(logFile.getBaseCommitTime(), id));
    }
    fileSlices.get(logFile.getBaseCommitTime()).addLogFile(logFile);
  }

  public String getId() {
    return id;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  /**
   * A FileSlice is considered committed, if one of the following is true - There is a committed
   * data file - There are some log files, that are based off a commit or delta commit
   */
  private boolean isFileSliceCommitted(FileSlice slice) {
    String maxCommitTime = lastInstant.get().getTimestamp();
    return timeline.containsOrBeforeTimelineStarts(slice.getBaseCommitTime())
        && HoodieTimeline.compareTimestamps(slice.getBaseCommitTime(),
        maxCommitTime,
        HoodieTimeline.LESSER_OR_EQUAL);

  }

  /**
   * Provides a stream of committed file slices, sorted reverse base commit time.
   */
  public Stream<FileSlice> getAllFileSlices() {
    if (!timeline.empty()) {
      return fileSlices.entrySet().stream()
          .map(sliceEntry -> sliceEntry.getValue())
          .filter(slice -> isFileSliceCommitted(slice));
    }
    return Stream.empty();
  }

  /**
   * Gets the latest slice - this can contain either
   * <p>
   * - just the log files without data file - (or) data file with 0 or more log files
   */
  public Optional<FileSlice> getLatestFileSlice() {
    // there should always be one
    return getAllFileSlices().findFirst();
  }

  /**
   * Obtain the latest file slice, upto a commitTime i.e <= maxCommitTime
   */
  public Optional<FileSlice> getLatestFileSliceBeforeOrOn(String maxCommitTime) {
    return getAllFileSlices()
        .filter(slice ->
            HoodieTimeline.compareTimestamps(slice.getBaseCommitTime(),
                maxCommitTime,
                HoodieTimeline.LESSER_OR_EQUAL))
        .findFirst();
  }

  public Optional<FileSlice> getLatestFileSliceInRange(List<String> commitRange) {
    return getAllFileSlices()
        .filter(slice -> commitRange.contains(slice.getBaseCommitTime()))
        .findFirst();
  }

  /**
   * Stream of committed data files, sorted reverse commit time
   */
  public Stream<HoodieDataFile> getAllDataFiles() {
    return getAllFileSlices()
        .filter(slice -> slice.getDataFile().isPresent())
        .map(slice -> slice.getDataFile().get());
  }

  /**
   * Get the latest committed data file
   */
  public Optional<HoodieDataFile> getLatestDataFile() {
    return getAllDataFiles().findFirst();
  }

  /**
   * Get the latest data file, that is <=  max commit time
   */
  public Optional<HoodieDataFile> getLatestDataFileBeforeOrOn(String maxCommitTime) {
    return getAllDataFiles()
        .filter(dataFile ->
            HoodieTimeline.compareTimestamps(dataFile.getCommitTime(),
                maxCommitTime,
                HoodieTimeline.LESSER_OR_EQUAL))
        .findFirst();
  }

  /**
   * Get the latest data file, that is contained within the provided commit range.
   */
  public Optional<HoodieDataFile> getLatestDataFileInRange(List<String> commitRange) {
    return getAllDataFiles()
        .filter(dataFile -> commitRange.contains(dataFile.getCommitTime()))
        .findFirst();
  }

  /**
   * Obtain the latest log file (based on latest committed data file), currently being appended to
   *
   * @return logfile if present, empty if no log file has been opened already.
   */
  public Optional<HoodieLogFile> getLatestLogFile() {
    Optional<FileSlice> latestSlice = getLatestFileSlice();
    if (latestSlice.isPresent() && latestSlice.get().getLogFiles().count() > 0) {
      return latestSlice.get().getLogFiles().findFirst();
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieFileGroup {");
    sb.append("id=").append(id);
    sb.append(", fileSlices='").append(fileSlices).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
