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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class HoodieDirectMarkerBasedEarlyConflictDetectionStrategy implements HoodieEarlyConflictDetectionStrategy {
  private static final Logger LOG = LogManager.getLogger(HoodieDirectMarkerBasedEarlyConflictDetectionStrategy.class);

  public abstract boolean hasMarkerConflict(String basePath, FileSystem fs, String partitionPath, String dataFileName, String instantTime,
                                            Set<HoodieInstant> completedCommitInstants, HoodieTableMetaClient metaClient);

  public abstract void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName);

  /**
   * We need to do list operation here.
   * In order to reduce the list pressure as much as possible, first we build path prefix in advance:  '$base_path/.temp/instant_time/partition_path',
   * and only list these specific partition_paths we need instead of list all the '$base_path/.temp/'
   * @param basePath
   * @param partitionPath
   * @param fileId
   * @return true if current fileID is already existed under .temp/instant_time/partition_path/..
   * @throws IOException
   */
  public boolean checkMarkerConflict(String basePath, String partitionPath, String fileId,
                                      FileSystem fs, String instantTime) throws IOException {
    String tempFolderPath = getTempFolderPath(basePath);
    long res = Arrays.stream(fs.listStatus(new Path(tempFolderPath)))
        .parallel()
        .map(FileStatus::getPath)
        .filter(markerPath -> {
          return !markerPath.getName().equalsIgnoreCase(instantTime);
        })
        .flatMap(currentMarkerDirPath -> {
          try {
            Path markerPartitionPath;
            if (StringUtils.isNullOrEmpty(partitionPath)) {
              markerPartitionPath = currentMarkerDirPath;
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

  private String getTempFolderPath(String basePath) {
    return basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;
  }

  public boolean checkCommitConflict(HoodieTableMetaClient metaClient, Set<HoodieInstant> completedCommitInstants, String fileId) {
    Set<HoodieInstant> currentInstants = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants().collect(Collectors.toSet());
    currentInstants.removeAll(completedCommitInstants);
    Set<String> missingFileIDs = currentInstants.stream().flatMap(instant -> {
      try {
        return HoodieCommitMetadata.fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class)
            .getFileIdAndRelativePaths().keySet().stream();
      } catch (Exception e) {
        return Stream.empty();
      }
    }).collect(Collectors.toSet());
    return missingFileIDs.contains(fileId);
  }
}
