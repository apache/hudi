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
 * log files, containing changes to the data file from that commit time
 */
public class FileSlice implements Serializable {

  /**
   * File Group Id of the Slice
   */
  private HoodieFileGroupId fileGroupId;

  /**
   * Point in the timeline, at which the slice was created
   */
  private String baseInstantTime;

  /**
   * data file, with the compacted data, for this slice
   */
  private HoodieDataFile dataFile;

  /**
   * List of appendable log files with real time data - Sorted with greater log version first -
   * Always empty for copy_on_write storage.
   */
  private final TreeSet<HoodieLogFile> logFiles;

  public FileSlice(String partitionPath, String baseInstantTime, String fileId) {
    this(new HoodieFileGroupId(partitionPath, fileId), baseInstantTime);
  }

  public FileSlice(HoodieFileGroupId fileGroupId, String baseInstantTime) {
    this.fileGroupId = fileGroupId;
    this.baseInstantTime = baseInstantTime;
    this.dataFile = null;
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

  public Optional<HoodieDataFile> getDataFile() {
    return Optional.ofNullable(dataFile);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("FileSlice {");
    sb.append("fileGroupId=").append(fileGroupId);
    sb.append("baseCommitTime=").append(baseInstantTime);
    sb.append(", dataFile='").append(dataFile).append('\'');
    sb.append(", logFiles='").append(logFiles).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
