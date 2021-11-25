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

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Encode additional information in Path to track matching log file and base files.
 * Hence, this class tracks a log/base file status.
 */
public class PathWithLogFilePath extends Path {
  // a flag to mark this split is produced by incremental query or not.
  private boolean belongToIncrementalPath = false;
  // the log files belong this path.
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  // max commit time of current path.
  private String maxCommitTime = "";
  // the basePath of current hoodie table.
  private String basePath = "";
  // the base file belong to this path;
  private String baseFilePath = "";
  // the bootstrap file belong to this path.
  // only if current query table is bootstrap table, this field is used.
  private PathWithBootstrapFileStatus pathWithBootstrapFileStatus;

  public PathWithLogFilePath(Path parent, String child) {
    super(parent, child);
  }

  public void setBelongToIncrementalPath(boolean belongToIncrementalPath) {
    this.belongToIncrementalPath = belongToIncrementalPath;
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

  public boolean splitable() {
    return !baseFilePath.isEmpty();
  }

  public PathWithBootstrapFileStatus getPathWithBootstrapFileStatus() {
    return pathWithBootstrapFileStatus;
  }

  public void setPathWithBootstrapFileStatus(PathWithBootstrapFileStatus pathWithBootstrapFileStatus) {
    this.pathWithBootstrapFileStatus = pathWithBootstrapFileStatus;
  }

  public boolean includeBootstrapFilePath() {
    return pathWithBootstrapFileStatus != null;
  }

  public BaseFileWithLogsSplit buildSplit(Path file, long start, long length, String[] hosts) {
    BaseFileWithLogsSplit bs = new BaseFileWithLogsSplit(file, start, length, hosts);
    bs.setBelongToIncrementalSplit(belongToIncrementalPath);
    bs.setDeltaLogFiles(deltaLogFiles);
    bs.setMaxCommitTime(maxCommitTime);
    bs.setBasePath(basePath);
    bs.setBaseFilePath(baseFilePath);
    return bs;
  }
}
