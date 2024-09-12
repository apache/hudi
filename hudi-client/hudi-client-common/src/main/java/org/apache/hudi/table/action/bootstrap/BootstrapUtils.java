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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

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
   * @param baseFileFormat Hoodie base file format
   * @param storage  Hoodie Storage
   * @param context JHoodieEngineContext
   * @return list of partition paths with files under them.
   * @throws IOException
   */
  public static List<Pair<String, List<HoodieFileStatus>>> getAllLeafFoldersWithFiles(
      HoodieFileFormat baseFileFormat,
      HoodieStorage storage,
      String basePathStr,
      HoodieEngineContext context) throws IOException {
    final StoragePath basePath = new StoragePath(basePathStr);
    final String baseFileExtension = baseFileFormat.getFileExtension();
    final Map<Integer, List<String>> levelToPartitions = new HashMap<>();
    final Map<String, List<HoodieFileStatus>> partitionToFiles = new HashMap<>();
    StoragePathFilter filePathFilter = getFilePathFilter(baseFileExtension);
    StoragePathFilter metaPathFilter = getExcludeMetaPathFilter();

    List<StoragePathInfo> topLevelPathInfos = storage.listDirectEntries(basePath);
    List<String> subDirectories = new ArrayList<>();

    List<Pair<HoodieFileStatus, Pair<Integer, String>>> result = new ArrayList<>();

    for (StoragePathInfo topLevelPathInfo: topLevelPathInfos) {
      if (topLevelPathInfo.isFile() && filePathFilter.accept(topLevelPathInfo.getPath())) {
        String relativePath = FSUtils.getRelativePartitionPath(basePath, topLevelPathInfo.getPath().getParent());
        Integer level = (int) relativePath.chars().filter(ch -> ch == '/').count();
        HoodieFileStatus hoodieFileStatus = FSUtils.fromPathInfo(topLevelPathInfo);
        result.add(Pair.of(hoodieFileStatus, Pair.of(level, relativePath)));
      } else if (topLevelPathInfo.isDirectory() && metaPathFilter.accept(topLevelPathInfo.getPath())) {
        subDirectories.add(topLevelPathInfo.getPath().toString());
      }
    }

    if (!subDirectories.isEmpty()) {
      result.addAll(context.flatMap(subDirectories, directory -> {
        StoragePathFilter pathFilter = getFilePathFilter(baseFileExtension);
        StoragePath path = new StoragePath(directory);
        HoodieStorage tmpStorage = HoodieStorageUtils.getStorage(path, HadoopFSUtils.getStorageConf());
        return tmpStorage.listFiles(path).stream()
            .filter(pathInfo -> pathFilter.accept(pathInfo.getPath()))
            .map(pathInfo -> {
              String relativePath = FSUtils.getRelativePartitionPath(basePath, pathInfo.getPath().getParent());
              Integer level = (int) relativePath.chars().filter(ch -> ch == '/').count();
              HoodieFileStatus hoodieFileStatus = FSUtils.fromPathInfo(pathInfo);
              return Pair.of(hoodieFileStatus, Pair.of(level, relativePath));
            });
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

  private static StoragePathFilter getFilePathFilter(String baseFileExtension) {
    return path -> path.getName().endsWith(baseFileExtension);
  }

  private static StoragePathFilter getExcludeMetaPathFilter() {
    // Avoid listing and including any folders under the meta folder
    return path -> !path.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME);
  }
}
