/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * A class which represents a directory and the files and directories inside it.
 * <p>
 * A {@code PartitionFileInfo} object saves the name of the partition and various properties requires of each file
 * required for initializing the metadata table. Saving limited properties reduces the total memory footprint when
 * a very large number of files are present in the dataset being initialized.
 */
@Getter
public class DirectoryInfo implements Serializable {

  // Relative path of the directory (relative to the base directory)
  private final String relativePath;
  // Map of filenames within this partition to their respective sizes
  private final Map<String, Long> filenameToSizeMap;
  // List of directories within this partition
  private final List<StoragePath> subDirectories = new ArrayList<>();
  // Is this a hoodie partition
  private boolean isHoodiePartition = false;

  public DirectoryInfo(String relativePath, List<StoragePathInfo> pathInfos, String maxInstantTime, Set<String> pendingDataInstants) {
    this(relativePath, pathInfos, maxInstantTime, pendingDataInstants, true);
  }

  /**
   * When files are directly fetched from Metadata table we do not need to validate HoodiePartitions.
   */
  public DirectoryInfo(String relativePath, List<StoragePathInfo> pathInfos, String maxInstantTime, Set<String> pendingDataInstants,
                       boolean validateHoodiePartitions) {
    this.relativePath = relativePath;

    // Pre-allocate with the maximum length possible
    filenameToSizeMap = new HashMap<>(pathInfos.size());

    // Presence of partition meta file implies this is a HUDI partition
    // if input files are directly fetched from MDT, it may not contain the HoodiePartitionMetadata file. So, we can ignore the validation for isHoodiePartition.
    isHoodiePartition = !validateHoodiePartitions || pathInfos.stream().anyMatch(status -> status.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
    for (StoragePathInfo pathInfo : pathInfos) {
      // Do not attempt to search for more subdirectories inside directories that are partitions
      if (!isHoodiePartition && pathInfo.isDirectory()) {
        // Ignore .hoodie directory as there cannot be any partitions inside it
        if (!pathInfo.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
          this.subDirectories.add(pathInfo.getPath());
        }
      } else if (isHoodiePartition && FSUtils.isDataFile(pathInfo.getPath())) {
        // Regular HUDI data file (base file or log file)
        String dataFileCommitTime = FSUtils.getCommitTime(pathInfo.getPath().getName());
        // Limit the file listings to files which were created by successful commits before the maxInstant time.
        if (!pendingDataInstants.contains(dataFileCommitTime) && compareTimestamps(dataFileCommitTime, LESSER_THAN_OR_EQUALS, maxInstantTime)) {
          filenameToSizeMap.put(pathInfo.getPath().getName(), pathInfo.getLength());
        }
      }
    }
  }

  public static Map<String, List<FileInfo>> getPartitionToFileInfo(List<DirectoryInfo> partitionInfoList) {
    return partitionInfoList.stream()
        .map(p -> {
          String partitionName = HoodieTableMetadataUtil.getPartitionIdentifierForFilesPartition(p.getRelativePath());
          List<FileInfo> files = p.getFilenameToSizeMap().entrySet().stream()
              .map(entry -> FileInfo.of(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList());
          return Pair.of(partitionName, files);
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
