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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Marker operations of directly accessing the file system to create and delete
 * marker files for table version 6 and below.
 * Each data file has a corresponding marker file.
 * The difference compared to table version 8 is the way of creating markers
 * for log files, i.e., using APPEND type.
 */
public class DirectWriteMarkersV1 extends DirectWriteMarkers implements AppendMarkerHandler {
  private static final Predicate<String> APPEND_MARKER_PREDICATE =
      pathStr -> pathStr.contains(HoodieTableMetaClient.MARKER_EXTN)
          && pathStr.endsWith(IOType.APPEND.name());

  public DirectWriteMarkersV1(HoodieTable table, String instantTime) {
    super(table, instantTime);
  }

  @Override
  public Option<StoragePath> createLogMarkerIfNotExists(String partitionPath,
                                                        String fileName,
                                                        HoodieWriteConfig writeConfig,
                                                        String fileId,
                                                        HoodieActiveTimeline activeTimeline) {
    return createIfNotExists(partitionPath, fileName, IOType.APPEND, writeConfig, fileId, activeTimeline);
  }

  @Override
  public Set<String> getAppendedLogPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Pair<List<String>, Set<String>> subDirectoriesAndDataFiles = getSubDirectoriesByMarkerCondition(storage.listDirectEntries(markerDirPath), APPEND_MARKER_PREDICATE);
    List<String> subDirectories = subDirectoriesAndDataFiles.getLeft();
    Set<String> logFiles = subDirectoriesAndDataFiles.getRight();
    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      StorageConfiguration<?> storageConf = storage.getConf();
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining marker files for all created, merged paths");
      logFiles.addAll(context.flatMap(subDirectories, directory -> {
        Queue<StoragePath> candidatesDirs = new LinkedList<>();
        candidatesDirs.add(new StoragePath(directory));
        List<String> result = new ArrayList<>();
        while (!candidatesDirs.isEmpty()) {
          StoragePath path = candidatesDirs.remove();
          HoodieStorage storage = HoodieStorageUtils.getStorage(path, storageConf);
          List<StoragePathInfo> storagePathInfos = storage.listDirectEntries(path);
          for (StoragePathInfo pathInfo : storagePathInfos) {
            if (pathInfo.isDirectory()) {
              candidatesDirs.add(pathInfo.getPath());
            } else {
              String pathStr = pathInfo.getPath().toString();
              if (APPEND_MARKER_PREDICATE.test(pathStr)) {
                result.add(translateMarkerToDataPath(pathStr));
              }
            }
          }
        }
        return result.stream();
      }, parallelism));
    }

    return logFiles;
  }
}
