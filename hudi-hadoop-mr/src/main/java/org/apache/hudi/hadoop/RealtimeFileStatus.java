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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieLogFile;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * With the base input format implementations in Hadoop/Hive,
 * we need to encode additional information in Path to track base files and logs files for realtime read.
 * Hence, this class tracks a log/base file status
 * in Path.
 */
public class RealtimeFileStatus extends FileStatus {
  // a flag to mark this split is produced by incremental query or not.
  private boolean belongToIncrementalFileStatus = false;
  // the log files belong this fileStatus.
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  // max commit time of current fileStatus.
  private String maxCommitTime = "";
  // the basePath of current hoodie table.
  private String basePath = "";
  // the base file belong to this status;
  private String baseFilePath = "";
  // the bootstrap file belong to this status.
  // only if current query table is bootstrap table, this field is used.
  private FileStatus bootStrapFileStatus;

  public RealtimeFileStatus(FileStatus fileStatus) throws IOException {
    super(fileStatus);
  }

  @Override
  public Path getPath() {
    Path path = super.getPath();
    PathWithLogFilePath pathWithLogFilePath = new PathWithLogFilePath(path.getParent(), path.getName());
    pathWithLogFilePath.setBelongToIncrementalPath(belongToIncrementalFileStatus);
    pathWithLogFilePath.setDeltaLogFiles(deltaLogFiles);
    pathWithLogFilePath.setMaxCommitTime(maxCommitTime);
    pathWithLogFilePath.setBasePath(basePath);
    pathWithLogFilePath.setBaseFilePath(baseFilePath);
    if (bootStrapFileStatus != null) {
      pathWithLogFilePath.setPathWithBootstrapFileStatus((PathWithBootstrapFileStatus)bootStrapFileStatus.getPath());
    }
    return pathWithLogFilePath;
  }

  public void setBelongToIncrementalFileStatus(boolean belongToIncrementalFileStatus) {
    this.belongToIncrementalFileStatus = belongToIncrementalFileStatus;
  }

  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  public void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles) {
    this.deltaLogFiles = deltaLogFiles;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public void setMaxCommitTime(String maxCommitTime) {
    this.maxCommitTime = maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public void setBaseFilePath(String baseFilePath) {
    this.baseFilePath = baseFilePath;
  }

  public void setBootStrapFileStatus(FileStatus bootStrapFileStatus) {
    this.bootStrapFileStatus = bootStrapFileStatus;
  }
}
