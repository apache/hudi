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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;

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
   * @param metaClient Hoodie table metadata client
   * @param fs  File System
   * @param context JHoodieEngineContext
   * @return list of partition paths with files under them.
   * @throws IOException
   */
  public static List<Pair<String, List<HoodieFileStatus>>> getAllLeafFoldersWithFiles(HoodieTableMetaClient metaClient,
      FileSystem fs, String basePathStr, HoodieEngineContext context) throws IOException {
    final Path basePath = new Path(basePathStr);
    final String baseFileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    final Map<Integer, List<String>> levelToPartitions = new HashMap<>();
    final Map<String, List<HoodieFileStatus>> partitionToFiles = new HashMap<>();
    PathFilter filePathFilter = getFilePathFilter(baseFileExtension);
    PathFilter metaPathFilter = getExcludeMetaPathFilter();

    FileStatus[] topLevelStatuses = fs.listStatus(basePath);
    List<String> subDirectories = new ArrayList<>();

    List<Pair<HoodieFileStatus, Pair<Integer, String>>> result = new ArrayList<>();

    for (FileStatus topLevelStatus: topLevelStatuses) {
      if (topLevelStatus.isFile() && filePathFilter.accept(topLevelStatus.getPath())) {
        String relativePath = FSUtils.getRelativePartitionPath(basePath, topLevelStatus.getPath().getParent());
        Integer level = (int) relativePath.chars().filter(ch -> ch == '/').count();
        HoodieFileStatus hoodieFileStatus = FileStatusUtils.fromFileStatus(topLevelStatus);
        result.add(Pair.of(hoodieFileStatus, Pair.of(level, relativePath)));
      } else if (topLevelStatus.isDirectory() && metaPathFilter.accept(topLevelStatus.getPath())) {
        subDirectories.add(topLevelStatus.getPath().toString());
      }
    }

    if (subDirectories.size() > 0) {
      result.addAll(context.flatMap(subDirectories, directory -> {
        PathFilter pathFilter = getFilePathFilter(baseFileExtension);
        Path path = new Path(directory);
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(path, true);
        List<Pair<HoodieFileStatus, Pair<Integer, String>>> res = new ArrayList<>();
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          if (pathFilter.accept(status.getPath())) {
            String relativePath = FSUtils.getRelativePartitionPath(new Path(basePathStr), status.getPath().getParent());
            Integer level = (int) relativePath.chars().filter(ch -> ch == '/').count();
            HoodieFileStatus hoodieFileStatus = FileStatusUtils.fromFileStatus(status);
            res.add(Pair.of(hoodieFileStatus, Pair.of(level, relativePath)));
          }
        }
        return res.stream();
      }, subDirectories.size()));
    }

    result.forEach(val -> {
      String relativePath = val.getRight().getRight();
      List<HoodieFileStatus> statusList = partitionToFiles.get(relativePath);
      if (null == statusList) {
        Integer level = val.getRight().getLeft();
        List<String> dirs = levelToPartitions.get(level);
        if (null == dirs) {
          dirs = new ArrayList<>();
          levelToPartitions.put(level, dirs);
        }
        dirs.add(relativePath);
        statusList = new ArrayList<>();
        partitionToFiles.put(relativePath, statusList);
      }
      statusList.add(val.getLeft());
    });

    OptionalInt maxLevelOpt = levelToPartitions.keySet().stream().mapToInt(x -> x).max();
    int maxLevel = maxLevelOpt.orElse(-1);
    return maxLevel >= 0 ? levelToPartitions.get(maxLevel).stream()
            .map(d -> Pair.of(d, partitionToFiles.get(d))).collect(Collectors.toList()) : new ArrayList<>();
  }

  private static PathFilter getFilePathFilter(String baseFileExtension) {
    return (path) -> {
      return path.getName().endsWith(baseFileExtension);
    };
  }

  private static PathFilter getExcludeMetaPathFilter() {
    // Avoid listing and including any folders under the metafolder
    return (path) -> !path.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME);
  }
}
