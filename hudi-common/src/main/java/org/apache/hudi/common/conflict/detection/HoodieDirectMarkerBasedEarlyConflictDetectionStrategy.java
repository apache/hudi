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

package org.apache.hudi.common.conflict.detection;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class HoodieDirectMarkerBasedEarlyConflictDetectionStrategy implements HoodieEarlyConflictDetectionStrategy {

  private static final Logger LOG = LogManager.getLogger(HoodieDirectMarkerBasedEarlyConflictDetectionStrategy.class);
  protected final String basePath;
  protected final FileSystem fs;
  protected final String partitionPath;
  protected final String fileId;
  protected final String instantTime;
  protected final HoodieActiveTimeline activeTimeline;
  protected final HoodieConfig config;
  protected Set<HoodieInstant> completedCommitInstants;
  protected final Boolean checkCommitConflict;
  protected final Long maxAllowableHeartbeatIntervalInMs;

  public HoodieDirectMarkerBasedEarlyConflictDetectionStrategy(String basePath, HoodieWrapperFileSystem fs, String partitionPath, String fileId, String instantTime,
                                                               HoodieActiveTimeline activeTimeline, HoodieConfig config, Boolean checkCommitConflict, Long maxAllowableHeartbeatIntervalInMs,
                                                               HashSet<HoodieInstant> completedCommitInstants) {
    this.basePath = basePath;
    this.fs = fs;
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.instantTime = instantTime;
    this.completedCommitInstants = completedCommitInstants;
    this.activeTimeline = activeTimeline;
    this.config = config;
    this.checkCommitConflict = checkCommitConflict;
    this.maxAllowableHeartbeatIntervalInMs = maxAllowableHeartbeatIntervalInMs;
  }

  /**
   * We need to do list operation here.
   * In order to reduce the list pressure as much as possible, first we build path prefix in advance:  '$base_path/.temp/instant_time/partition_path',
   * and only list these specific partition_paths we need instead of list all the '$base_path/.temp/'
   * @param basePath
   * @param partitionPath
   * @param fileId 162b13d7-9530-48cf-88a4-02241817ae0c-0_1-74-100_003.parquet
   * @return true if current fileID is already existed under .temp/instant_time/partition_path/..
   * @throws IOException
   */
  public boolean checkMarkerConflict(HoodieActiveTimeline activeTimeline, String basePath, String partitionPath, String fileId,
                                      FileSystem fs, String instantTime) throws IOException {
    String tempFolderPath = basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;

    List<String> candidateInstants = MarkerUtils.getCandidateInstants(activeTimeline, Arrays.stream(fs.listStatus(new Path(tempFolderPath))).map(FileStatus::getPath).collect(Collectors.toList()),
        instantTime, maxAllowableHeartbeatIntervalInMs, fs, basePath);

    long res = candidateInstants.stream().flatMap(currentMarkerDirPath -> {
      try {
        Path markerPartitionPath;
        if (StringUtils.isNullOrEmpty(partitionPath)) {
          markerPartitionPath = new Path(currentMarkerDirPath);
        } else {
          markerPartitionPath = new Path(currentMarkerDirPath, partitionPath);
        }

        if (!StringUtils.isNullOrEmpty(partitionPath) && !fs.exists(markerPartitionPath)) {
          return Stream.empty();
        } else {
          return Arrays.stream(fs.listStatus(markerPartitionPath)).parallel()
              .filter((path) -> path.toString().contains(fileId));
        }
      } catch (IOException e) {
        throw new HoodieIOException("IOException occurs during checking marker file conflict");
      }
    }).count();

    if (res != 0L) {
      LOG.warn("Detected conflict marker files: " + partitionPath + "/" + fileId + " for " + instantTime);
      return true;
    }
    return false;
  }
}
