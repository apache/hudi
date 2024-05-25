/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utils for table path.
 */
public class TablePathUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TablePathUtils.class);

  private static boolean hasTableMetadataFolder(HoodieStorage storage, StoragePath path) {
    if (path == null) {
      return false;
    }

    try {
      return storage.exists(new StoragePath(path, HoodieTableMetaClient.METAFOLDER_NAME));
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie metadata folder for " + path, ioe);
    }
  }

  public static boolean isHoodieTablePath(HoodieStorage storage, StoragePath path) {
    return hasTableMetadataFolder(storage, path);
  }

  public static Option<StoragePath> getTablePath(HoodieStorage storage, StoragePath path) throws HoodieException, IOException {
    LOG.info("Getting table path from path : " + path);

    StoragePathInfo pathInfo = storage.getPathInfo(path);
    StoragePath directory =
        pathInfo.isFile() ? pathInfo.getPath().getParent() : pathInfo.getPath();

    if (hasTableMetadataFolder(storage, directory)) {
      // Handle table folder itself
      return Option.of(directory);
    }

    // Handle metadata folder or metadata sub folder path
    Option<StoragePath> tablePath = getTablePathFromMetaFolderPath(directory);
    if (tablePath.isPresent()) {
      return tablePath;
    }

    // Handle partition folder
    return getTablePathFromPartitionPath(storage, directory);
  }

  private static boolean isInsideTableMetaFolder(String path) {
    return path != null && path.contains("/" + HoodieTableMetaClient.METAFOLDER_NAME);
  }

  private static boolean isInsideMetadataTableInMetaFolder(String path) {
    return path != null && path.contains("/" + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
  }

  private static Option<StoragePath> getTablePathFromMetaFolderPath(StoragePath path) {
    String pathStr = path.toString();

    // NOTE: Since Metadata Table itself resides w/in the Meta-folder, we need to make sure
    //       that we don't misinterpret attempt to read MT table itself
    if (isInsideTableMetaFolder(pathStr) && !isInsideMetadataTableInMetaFolder(pathStr)) {
      int index = pathStr.indexOf("/" + HoodieTableMetaClient.METAFOLDER_NAME);
      return Option.of(new StoragePath(pathStr.substring(0, index)));
    }

    return Option.empty();
  }

  private static Option<StoragePath> getTablePathFromPartitionPath(HoodieStorage storage, StoragePath partitionPath) {
    try {
      if (HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath)) {
        HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(storage, partitionPath);
        metadata.readFromFS();
        return Option.of(getNthParent(partitionPath, metadata.getPartitionDepth()));
      } else {
        // Simply traverse directory structure until found .hoodie folder
        StoragePath current = partitionPath;
        while (current != null) {
          if (hasTableMetadataFolder(storage, current)) {
            return Option.of(current);
          }
          current = current.getParent();
        }

        return Option.empty();
      }
    } catch (IOException ioe) {
      throw new HoodieException("Error reading partition metadata for " + partitionPath, ioe);
    }
  }

  private static StoragePath getNthParent(StoragePath path, int n) {
    StoragePath parent = path;
    for (int i = 0; i < n; i++) {
      parent = parent.getParent();
    }
    return parent;
  }
}
