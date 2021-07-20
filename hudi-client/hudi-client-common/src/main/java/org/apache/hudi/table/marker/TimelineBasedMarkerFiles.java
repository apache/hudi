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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Marker file operations of using timeline service as a proxy to create and delete marker files.
 * Each data file has a corresponding marker entry, which is stored in a limited number of
 * marker files maintained by the timeline service (each marker file contains multiple marker
 * entries).
 */
public class TimelineBasedMarkerFiles extends MarkerFiles {
  private static final Logger LOG = LogManager.getLogger(TimelineBasedMarkerFiles.class);
  private final RemoteHoodieTableFileSystemView remoteFSView;

  public TimelineBasedMarkerFiles(String basePath, String markerFolderPath, String instantTime, RemoteHoodieTableFileSystemView remoteFSView) {
    super(basePath, markerFolderPath, instantTime);
    this.remoteFSView = remoteFSView;
  }

  public TimelineBasedMarkerFiles(HoodieTable table, String instantTime) {
    this(table.getMetaClient().getBasePath(),
        table.getMetaClient().getMarkerFolderPath(instantTime),
        instantTime,
        FileSystemViewManager.createRemoteFileSystemView(
            table.getContext().getHadoopConf(),
            table.getConfig().getViewStorageConfig(), table.getMetaClient())
    );
  }

  @Override
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    return remoteFSView.deleteMarkerDir(markerDirPath.toString());
  }

  @Override
  public boolean doesMarkerDirExist() {
    return remoteFSView.doesMarkerDirExist(markerDirPath.toString());
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Set<String> markerPaths = remoteFSView.getCreateAndMergeMarkerFilePaths(markerDirPath.toString());
    return markerPaths.stream().map(MarkerFiles::stripMarkerSuffix).collect(Collectors.toSet());
  }

  @Override
  public Set<String> allMarkerFilePaths() {
    return remoteFSView.getAllMarkerFilePaths(markerDirPath.toString());
  }

  @Override
  protected Option<Path> create(String partitionPath, String dataFileName, IOType type, boolean checkIfExists) {
    LOG.info("[timeline-based] Create marker file : " + partitionPath + " " + dataFileName);
    long startTimeMs = System.currentTimeMillis();
    String markerFileName = getMarkerFileName(dataFileName, type);
    boolean success = remoteFSView.createMarker(markerDirPath.toString(), String.format("%s/%s", partitionPath, markerFileName));
    LOG.info("[timeline-based] Created marker file in " + (System.currentTimeMillis() - startTimeMs) + " ms");
    if (success) {
      return Option.of(new Path(new Path(markerDirPath, partitionPath), markerFileName));
    } else {
      return Option.empty();
    }
  }
}
