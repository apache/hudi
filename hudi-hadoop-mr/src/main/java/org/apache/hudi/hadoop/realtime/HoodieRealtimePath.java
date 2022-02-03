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

package org.apache.hudi.hadoop.realtime;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link Path} implementation encoding additional information necessary to appropriately read
 * base files of the MOR tables, such as list of delta log files (holding updated records) associated
 * w/ the base file, etc.
 */
public class HoodieRealtimePath extends Path {
  /**
   * Marks whether this path produced as part of Incremental Query
   */
  private boolean belongsToIncrementalQuery = false;
  /**
   * List of delta log-files holding updated records for this base-file
   */
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  private String maxCommitTime = "";
  /**
   * Base path of the table this path belongs to
   */
  private String basePath = "";
  /**
   * File status for the Bootstrap file (only relevant if this table is a bootstrapped table
   */
  private PathWithBootstrapFileStatus pathWithBootstrapFileStatus;

  public HoodieRealtimePath(Path parent, String child) {
    super(parent, child);
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

  public boolean getBelongsToIncrementalQuery() {
    return belongsToIncrementalQuery;
  }

  public void setBelongsToIncrementalQuery(boolean belongsToIncrementalQuery) {
    this.belongsToIncrementalQuery = belongsToIncrementalQuery;
  }

  public boolean isSplitable() {
    return !toString().isEmpty();
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

  public HoodieRealtimeFileSplit buildSplit(Path file, long start, long length, String[] hosts) {
    HoodieRealtimeFileSplit bs = new HoodieRealtimeFileSplit(file, start, length, hosts);
    bs.setBelongsToIncrementalQuery(belongsToIncrementalQuery);
    bs.setDeltaLogFiles(deltaLogFiles);
    bs.setMaxCommitTime(maxCommitTime);
    bs.setBasePath(basePath);
    return bs;
  }
}
