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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * A set of data/base files + set of log files, that make up an unit for all operations.
 */
public class HoodieFileGroup implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieFileGroup.class);

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
  private TreeMap<String, FileSlice> fileSlices;

  /**
   * Timeline, based on which all getter work.
   */
  private final HoodieTimeline timeline;

  /**
   * The last completed instant, that acts as a high watermark for all getters.
   */
  private final Option<HoodieInstant> lastInstant;

  // todo zhangyue143 isLSMBasedLogFormat 能否去掉
  private final boolean isLSMBasedLogFormat;

  public HoodieFileGroup(HoodieFileGroup fileGroup) {
    this.timeline = fileGroup.timeline;
    this.fileGroupId = fileGroup.fileGroupId;
    this.fileSlices = new TreeMap<>(fileGroup.fileSlices);
    this.lastInstant = fileGroup.lastInstant;
    this.isLSMBasedLogFormat = false;
  }

  public HoodieFileGroup(String partitionPath, String id, HoodieTimeline timeline) {
    this(new HoodieFileGroupId(partitionPath, id), timeline, false);
  }

  public HoodieFileGroup(HoodieFileGroupId fileGroupId, HoodieTimeline timeline) {
    this.fileGroupId = fileGroupId;
    this.fileSlices = new TreeMap<>(HoodieFileGroup.getReverseCommitTimeComparator());
    this.timeline = timeline;
    this.lastInstant = timeline.lastInstant();
    this.isLSMBasedLogFormat = false;
  }

  public HoodieFileGroup(HoodieFileGroupId fileGroupId, HoodieTimeline timeline, boolean isLSMBasedLogFormat) {
    this.fileGroupId = fileGroupId;
    this.fileSlices = new TreeMap<>(HoodieFileGroup.getReverseCommitTimeComparator());
    this.timeline = timeline;
    this.lastInstant = timeline.lastInstant();
    this.isLSMBasedLogFormat = isLSMBasedLogFormat;
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
    FileSlice fileSlice = fileSlices.get(dataFile.getCommitTime());
    Option<HoodieBaseFile> oldBaseFileOpt = fileSlice.getBaseFile();
    if (!oldBaseFileOpt.isPresent()) {
      fileSlice.setBaseFile(dataFile);
    } else {
      if (oldBaseFileOpt.get().getModifyTime() > 0 && dataFile.getModifyTime() > 0) {
        // 若base文件发生冲突，优先选择modify time小的那个文件
        if (dataFile.getModifyTime() < oldBaseFileOpt.get().getModifyTime()) {
          LOG.warn("Modify Time Compare ==>  Replace base file of file slice " + fileSlice + " from " + oldBaseFileOpt.get() + " to " +  dataFile);
          fileSlice.setBaseFile(dataFile);
        } else {
          LOG.warn("Modify Time Compare ==>  Skip tp replace base file of file slice " + fileSlice + " from " + oldBaseFileOpt.get() + " to " +  dataFile);
        }
      } else {
        // 若base文件发生冲突，优先选择length大的那个文件的那个文件
        if (oldBaseFileOpt.get().getFileLen() <= dataFile.getFileLen()) {
          LOG.warn("File Length Compare ==>  Replace base file of file slice " + fileSlice + " from " + oldBaseFileOpt.get() + " to " +  dataFile);
          fileSlice.setBaseFile(dataFile);
        } else {
          LOG.warn("File Length Compare ==>  Skip tp replace base file of file slice " + fileSlice + " from " + oldBaseFileOpt.get() + " to " +  dataFile);
        }
      }
    }
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
    if (!compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, lastInstant.get().getTimestamp())) {
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
    return isLSMBasedLogFormat ? getAllFileSlices().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, maxInstantTime)) :
        fileSlices.values().stream().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, maxInstantTime));
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

  public void initFileSlices(TreeMap<String, FileSlice> fileSlices) {
    this.fileSlices = fileSlices;
  }

  public Stream<FileSlice> getAllRawFileSlices() {
    return fileSlices.values().stream();
  }

  public HoodieTimeline getTimeline() {
    return timeline;
  }
}
