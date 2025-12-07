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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimePath;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * With the base input format implementations in Hadoop/Hive,
 * we need to encode additional information in Path to track base files and logs files for realtime read.
 * Hence, this class tracks a log/base file status
 * in Path.
 */
public class RealtimeFileStatus extends FileStatus {
  /**
   * Base path of the table this path belongs to
   */
  private final String basePath;
  /**
   * List of delta log-files holding updated records for this base-file
   */
  @Getter
  private final List<HoodieLogFile> deltaLogFiles;
  /**
   * Marks whether this path produced as part of Incremental Query
   */
  private final boolean belongsToIncrementalQuery;
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  @Getter
  @Setter
  private String maxCommitTime = "";
  /**
   * File status for the Bootstrap file (only relevant if this table is a bootstrapped table
   */
  @Setter
  private FileStatus bootStrapFileStatus;
  /**
   * Virtual key configuration of the table this split belongs to
   */
  private final Option<HoodieVirtualKeyInfo> virtualKeyInfo;

  public RealtimeFileStatus(FileStatus fileStatus,
                            String basePath,
                            List<HoodieLogFile> deltaLogFiles,
                            boolean belongsToIncrementalQuery,
                            Option<HoodieVirtualKeyInfo> virtualKeyInfo) throws IOException {
    super(fileStatus);
    this.basePath = basePath;
    this.deltaLogFiles = deltaLogFiles;
    this.belongsToIncrementalQuery = belongsToIncrementalQuery;
    this.virtualKeyInfo = virtualKeyInfo;
  }

  @Override
  public Path getPath() {
    Path path = super.getPath();

    HoodieRealtimePath realtimePath = new HoodieRealtimePath(path.getParent(), path.getName(), basePath,
        deltaLogFiles, maxCommitTime, belongsToIncrementalQuery, virtualKeyInfo);

    if (bootStrapFileStatus != null) {
      realtimePath.setPathWithBootstrapFileStatus((PathWithBootstrapFileStatus)bootStrapFileStatus.getPath());
    }

    return realtimePath;
  }
}
