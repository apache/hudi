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

package org.apache.hudi.client.validator;

import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link StashPartitionRenameHelper} that copies files
 * individually from source to target and then deletes them from source.
 * <p>
 * This implementation is correct but may be slow for large partitions on cloud
 * storage. For production use on HDFS or cloud storage with efficient rename APIs,
 * a custom implementation can be plugged in via the
 * {@code hoodie.stash.rename.helper.class} configuration.
 */
public class DefaultStashPartitionRenameHelper implements StashPartitionRenameHelper {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStashPartitionRenameHelper.class);

  @Override
  public void stashPartitionFiles(HoodieStorage storage, StoragePath partitionPath, StoragePath stashPath) throws IOException {
    moveFiles(storage, partitionPath, stashPath);
  }

  @Override
  public void restorePartitionFiles(HoodieStorage storage, StoragePath stashPath, StoragePath partitionPath) throws IOException {
    moveFiles(storage, stashPath, partitionPath);
  }

  private void moveFiles(HoodieStorage storage, StoragePath sourcePath, StoragePath targetPath) throws IOException {
    // Ensure target directory exists
    if (!storage.exists(targetPath)) {
      storage.createDirectory(targetPath);
    }

    List<StoragePathInfo> sourceFiles = storage.listDirectEntries(sourcePath);
    if (sourceFiles.isEmpty()) {
      LOG.info("Source path {} has no files, nothing to move.", sourcePath);
      return;
    }

    // Determine which files already exist in target (from a prior partial attempt)
    Set<String> existingTargetFileNames = getExistingFileNames(storage, targetPath);

    // Copy files not already in target
    for (StoragePathInfo sourceFileInfo : sourceFiles) {
      String fileName = sourceFileInfo.getPath().getName();
      if (existingTargetFileNames.contains(fileName)) {
        LOG.info("File {} already exists in target {}, skipping copy.", fileName, targetPath);
      } else {
        StoragePath destFilePath = new StoragePath(targetPath, fileName);
        LOG.info("Copying file {} to {}", sourceFileInfo.getPath(), destFilePath);
        FileIOUtils.copy(storage, sourceFileInfo.getPath(), destFilePath);
      }
    }

    // Delete all source files after successful copy
    for (StoragePathInfo sourceFileInfo : sourceFiles) {
      LOG.info("Deleting source file {}", sourceFileInfo.getPath());
      storage.deleteFile(sourceFileInfo.getPath());
    }
  }

  private Set<String> getExistingFileNames(HoodieStorage storage, StoragePath targetPath) throws IOException {
    if (!storage.exists(targetPath)) {
      return Collections.emptySet();
    }
    return storage.listDirectEntries(targetPath).stream()
        .map(info -> info.getPath().getName())
        .collect(Collectors.toSet());
  }
}
