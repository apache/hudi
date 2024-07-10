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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import org.apache.hadoop.fs.Path;

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
  private final boolean belongsToIncrementalQuery;
  /**
   * List of delta log-files holding updated records for this base-file
   */
  private final List<HoodieLogFile> deltaLogFiles;
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  private final String maxCommitTime;
  /**
   * Base path of the table this path belongs to
   */
  private final String basePath;
  /**
   * Virtual key configuration of the table this split belongs to
   */
  private final Option<HoodieVirtualKeyInfo> virtualKeyInfo;
  /**
   * File status for the Bootstrap file (only relevant if this table is a bootstrapped table
   */
  private PathWithBootstrapFileStatus pathWithBootstrapFileStatus;

  public HoodieRealtimePath(Path parent,
                            String child,
                            String basePath,
                            List<HoodieLogFile> deltaLogFiles,
                            String maxCommitTime,
                            boolean belongsToIncrementalQuery,
                            Option<HoodieVirtualKeyInfo> virtualKeyInfo) {
    super(parent, child);
    this.basePath = basePath;
    this.deltaLogFiles = deltaLogFiles;
    this.maxCommitTime = maxCommitTime;
    this.belongsToIncrementalQuery = belongsToIncrementalQuery;
    this.virtualKeyInfo = virtualKeyInfo;
  }

  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  public boolean getBelongsToIncrementalQuery() {
    return belongsToIncrementalQuery;
  }

  public boolean isSplitable() {
    return !toString().contains(".log") && deltaLogFiles.isEmpty() && !includeBootstrapFilePath();
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

  public Option<HoodieVirtualKeyInfo> getVirtualKeyInfo() {
    return virtualKeyInfo;
  }
}
