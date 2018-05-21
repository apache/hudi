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

import java.io.Serializable;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Within a file group, a slice is a combination of data file written at a commit time and list of
 * log files, containing changes to the data file from that commit time.
 * File Slice represents a collection of files that when merged will give the snapshot view of the file-group as
 * of the latest instant in that file-slice. With asynchronous compaction, the latest file slice will also include
 * the set of log files written after the compaction request instant.
 */
public class FileSlice implements Serializable {

  /**
   * id of the slice
   */
  private String fileId;

  /**
   * Point in the timeline, at which the slice was created
   */
  private String baseCommitTime;

  /**
   * data file, with the compacted data, for this slice
   */
  private HoodieDataFile dataFile;

  /**
   * Outstanding Compaction request instant time
   * For a given file-slice, only one compaction request can be outstanding.
   */
  private String outstandingCompactionInstant;

  /**
   * List of appendable log files with real time data - Sorted with greater log version first -
   * Always empty for copy_on_write storage.
   */
  private final TreeSet<HoodieLogFile> logFiles;

  /**
   * Construct a file-slice from fileId only
   * @param fileId
   */
  public FileSlice(String fileId) {
    this(null, fileId);
  }

  public FileSlice(String baseCommitTime, String fileId) {
    this.fileId = fileId;
    this.baseCommitTime = baseCommitTime;
    this.dataFile = null;
    this.outstandingCompactionInstant = null;
    this.logFiles = new TreeSet<>(HoodieLogFile.getBaseInstantAndLogVersionComparator());
  }

  public void setDataFile(HoodieDataFile dataFile) {
    this.dataFile = dataFile;
  }

  public void addLogFile(HoodieLogFile logFile) {
    this.logFiles.add(logFile);
  }

  public Stream<HoodieLogFile> getLogFiles() {
    return logFiles.stream();
  }

  public String getBaseCommitTime() {
    return baseCommitTime;
  }

  public String getFileId() {
    return fileId;
  }

  public Optional<HoodieDataFile> getDataFile() {
    return Optional.ofNullable(dataFile);
  }

  public String getOutstandingCompactionInstant() {
    return outstandingCompactionInstant;
  }

  public void setOutstandingCompactionInstant(String outstandingCompactionInstant) {
    this.outstandingCompactionInstant = outstandingCompactionInstant;
  }

  public String getBaseInstantForLogAppend() {
    if (outstandingCompactionInstant != null) {
      return outstandingCompactionInstant;
    }
    return baseCommitTime;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("FileSlice {");
    sb.append("baseCommitTime=").append(baseCommitTime);
    sb.append(", outstandingCompactionInstant=").append(outstandingCompactionInstant);
    sb.append(", dataFile='").append(dataFile).append('\'');
    sb.append(", logFiles='").append(logFiles).append('\'');
    sb.append('}');
    return sb.toString();
  }
}