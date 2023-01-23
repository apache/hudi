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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This abstract strategy is used for direct marker writers, trying to do early conflict detection.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class DirectMarkerBasedDetectionStrategy implements EarlyConflictDetectionStrategy {

  private static final Logger LOG = LogManager.getLogger(DirectMarkerBasedDetectionStrategy.class);

  protected final FileSystem fs;
  protected final String partitionPath;
  protected final String fileId;
  protected final String instantTime;
  protected final HoodieActiveTimeline activeTimeline;
  protected final HoodieConfig config;

  public DirectMarkerBasedDetectionStrategy(HoodieWrapperFileSystem fs, String partitionPath, String fileId, String instantTime,
                                            HoodieActiveTimeline activeTimeline, HoodieConfig config) {
    this.fs = fs;
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.instantTime = instantTime;
    this.activeTimeline = activeTimeline;
    this.config = config;
  }

  /**
   * We need to do list operation here.
   * In order to reduce the list pressure as much as possible, first we build path prefix in advance:
   * '$base_path/.temp/instant_time/partition_path', and only list these specific partition_paths
   * we need instead of list all the '$base_path/.temp/'
   *
   * @param basePath                          Base path of the table.
   * @param maxAllowableHeartbeatIntervalInMs Heartbeat timeout.
   * @return true if current fileID is already existed under .temp/instant_time/partition_path/..
   * @throws IOException upon errors.
   */
  public boolean checkMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) throws IOException {
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
