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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TablePathUtils {

  private static final Logger LOG = LogManager.getLogger(TablePathUtils.class);

  private static boolean hasTableMetadataFolder(FileSystem fs, Path path) {
    if (path == null) {
      return false;
    }

    try {
      return fs.exists(new Path(path, HoodieTableMetaClient.METAFOLDER_NAME));
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie metadata folder for " + path, ioe);
    }
  }

  public static Option<Path> getTablePath(FileSystem fs, Path path) throws HoodieException, IOException {
    LOG.info("Getting table path from path : " + path);

    FileStatus fileStatus = fs.getFileStatus(path);
    Path directory = fileStatus.isFile() ? fileStatus.getPath().getParent() : fileStatus.getPath();

    if (TablePathUtils.hasTableMetadataFolder(fs, directory)) {
      // Handle table folder itself
      return Option.of(directory);
    }

    // Handle metadata folder or metadata sub folder path
    Option<Path> tablePath = getTablePathFromTableMetadataPath(fs, directory);
    if (tablePath.isPresent()) {
      return tablePath;
    }

    // Handle partition folder
    return getTablePathFromPartitionPath(fs, directory);
  }

  private static boolean isTableMetadataFolder(String path) {
    return path != null && path.endsWith("/" + HoodieTableMetaClient.METAFOLDER_NAME);
  }

  private static boolean isInsideTableMetadataFolder(String path) {
    return path != null && path.contains("/" + HoodieTableMetaClient.METAFOLDER_NAME + "/");
  }

  private static Option<Path> getTablePathFromTableMetadataPath(FileSystem fs, Path path) {
    String pathStr = path.toString();

    if (isTableMetadataFolder(pathStr)) {
      return Option.of(path.getParent());
    } else if (isInsideTableMetadataFolder(pathStr)) {
      int index = pathStr.indexOf("/" + HoodieTableMetaClient.METAFOLDER_NAME);
      return Option.of(new Path(pathStr.substring(0, index)));
    }

    return Option.empty();
  }

  private static Option<Path> getTablePathFromPartitionPath(FileSystem fs, Path partitionPath) {
    try {
      if (HoodiePartitionMetadata.hasPartitionMetadata(fs, partitionPath)) {
        HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, partitionPath);
        metadata.readFromFS();
        return Option.of(getNthParent(partitionPath, metadata.getPartitionDepth()));
      }
    } catch (IOException ioe) {
      throw new HoodieException("Error reading partition metadata for " + partitionPath, ioe);
    }

    return Option.empty();
  }

  private static Path getNthParent(Path path, int n) {
    Path parent = path;
    for (int i = 0; i < n; i++) {
      parent = parent.getParent();
    }
    return parent;
  }
}
