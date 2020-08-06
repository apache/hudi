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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public class BootstrapUtils {

  /**
   * Returns leaf folders with files under a path.
   * @param fs  File System
   * @param basePathStr Base Path to look for leaf folders
   * @param filePathFilter  Filters to skip directories/paths
   * @return list of partition paths with files under them.
   * @throws IOException
   */
  public static List<Pair<String, List<HoodieFileStatus>>> getAllLeafFoldersWithFiles(FileSystem fs, String basePathStr,
                                                                                      PathFilter filePathFilter) throws IOException {
    final Path basePath = new Path(basePathStr);
    final Map<Integer, List<String>> levelToPartitions = new HashMap<>();
    final Map<String, List<HoodieFileStatus>> partitionToFiles = new HashMap<>();
    FSUtils.processFiles(fs, basePathStr, (status) -> {
      if (status.isFile() && filePathFilter.accept(status.getPath())) {
        String relativePath = FSUtils.getRelativePartitionPath(basePath, status.getPath().getParent());
        List<HoodieFileStatus> statusList = partitionToFiles.get(relativePath);
        if (null == statusList) {
          Integer level = (int) relativePath.chars().filter(ch -> ch == '/').count();
          List<String> dirs = levelToPartitions.get(level);
          if (null == dirs) {
            dirs = new ArrayList<>();
            levelToPartitions.put(level, dirs);
          }
          dirs.add(relativePath);
          statusList = new ArrayList<>();
          partitionToFiles.put(relativePath, statusList);
        }
        statusList.add(FileStatusUtils.fromFileStatus(status));
      }
      return true;
    }, true);
    OptionalInt maxLevelOpt = levelToPartitions.keySet().stream().mapToInt(x -> x).max();
    int maxLevel = maxLevelOpt.orElse(-1);
    return maxLevel >= 0 ? levelToPartitions.get(maxLevel).stream()
        .map(d -> Pair.of(d, partitionToFiles.get(d))).collect(Collectors.toList()) : new ArrayList<>();
  }
}
