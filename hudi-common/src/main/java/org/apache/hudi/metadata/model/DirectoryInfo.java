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
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Represents information about a directory in the filesystem.
 */
public class DirectoryInfo implements Serializable {
  // Relative path of the directory (relative to the base directory)
  private final String relativePath;
  // Map of filenames within this partition to their respective sizes
  private final HashMap<String, Long> filenameToSizeMap;
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

  public String getRelativePath() {
    return relativePath;
  }

  public int getTotalFiles() {
    return filenameToSizeMap.size();
  }

  public HashMap<String, Long> getFilenameToSizeMap() {
    return filenameToSizeMap;
  }

  public List<StoragePath> getSubDirectories() {
    return subDirectories;
  }

  public boolean isHoodiePartition() {
    return isHoodiePartition;
  }

  private static final String LESSER_THAN_OR_EQUALS = "<=";

  private boolean compareTimestamps(String commit1, String operator, String commit2) {
    return commit1.compareTo(commit2) <= 0;
  }
} 