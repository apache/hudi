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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.timeline.TimelineServiceClient;
import org.apache.hudi.timeline.TimelineServiceClientBase.RequestMethod;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.marker.MarkerOperation.ALL_MARKERS_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.CREATE_AND_MERGE_MARKERS_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.CREATE_MARKER_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.DELETE_MARKER_DIR_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKERS_DIR_EXISTS_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_BASEPATH_PARAM;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_NAME_PARAM;

/**
 * Marker operations of using timeline server as a proxy to create and delete markers.
 * Each data file has a corresponding marker entry, which is stored in a limited number of
 * underlying files maintained by the timeline server (each file contains multiple marker
 * entries).
 */
@Slf4j
public class TimelineServerBasedWriteMarkers extends WriteMarkers {
  private static final TypeReference<Boolean> BOOLEAN_TYPE_REFERENCE = new TypeReference<Boolean>() {};
  private static final TypeReference<Set<String>> SET_TYPE_REFERENCE = new TypeReference<Set<String>>() {};
  private final TimelineServiceClient timelineServiceClient;

  public TimelineServerBasedWriteMarkers(HoodieTable table, String instantTime) {
    this(table.getMetaClient().getBasePath().toString(),
        table.getMetaClient().getMarkerFolderPath(instantTime), instantTime,
        table.getConfig().getViewStorageConfig());
  }

  TimelineServerBasedWriteMarkers(String basePath,
                                  String markerFolderPath,
                                  String instantTime,
                                  FileSystemViewStorageConfig  fileSystemViewStorageConfig) {
    super(basePath, markerFolderPath, instantTime);
    this.timelineServiceClient = new TimelineServiceClient(fileSystemViewStorageConfig);
  }

  @Override
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          DELETE_MARKER_DIR_URL, paramsMap, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to delete marker directory " + markerDirPath, e);
    }
  }

  @Override
  public boolean doesMarkerDirExist() {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          MARKERS_DIR_EXISTS_URL, paramsMap, BOOLEAN_TYPE_REFERENCE, RequestMethod.GET);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to check marker directory " + markerDirPath, e);
    }
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      Set<String> markerPaths = executeRequestToTimelineServer(
          CREATE_AND_MERGE_MARKERS_URL, paramsMap, SET_TYPE_REFERENCE, RequestMethod.GET);
      return markerPaths.stream().map(WriteMarkers::stripMarkerSuffix).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to get CREATE and MERGE data file paths in "
          + markerDirPath, e);
    }
  }

  @Override
  public Set<String> allMarkerFilePaths() {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          ALL_MARKERS_URL, paramsMap, SET_TYPE_REFERENCE, RequestMethod.GET);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to get all markers in " + markerDirPath, e);
    }
  }

  @Override
  protected Option<StoragePath> create(String partitionPath, String fileName, IOType type, boolean checkIfExists) {
    HoodieTimer timer = HoodieTimer.start();
    String markerFileName = getMarkerFileName(fileName, type);

    Map<String, String> paramsMap = getConfigMap(partitionPath, markerFileName, false);
    boolean success = executeCreateMarkerRequest(paramsMap, partitionPath, markerFileName);
    log.info("[timeline-server-based] Created marker file " + partitionPath + "/" + markerFileName
        + " in " + timer.endTimer() + " ms");
    if (success) {
      return Option.of(new StoragePath(FSUtils.constructAbsolutePath(markerDirPath, partitionPath), markerFileName));
    } else {
      return Option.empty();
    }
  }

  @Override
  public Option<StoragePath> createWithEarlyConflictDetection(String partitionPath, String fileName, IOType type, boolean checkIfExists,
                                                              HoodieWriteConfig config, String fileId, HoodieActiveTimeline activeTimeline) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    String markerFileName = getMarkerFileName(fileName, type);
    Map<String, String> paramsMap = getConfigMap(partitionPath, markerFileName, true);

    boolean success = executeCreateMarkerRequest(paramsMap, partitionPath, markerFileName);

    log.info("[timeline-server-based] Created marker file with early conflict detection " + partitionPath + "/" + markerFileName
        + " in " + timer.endTimer() + " ms");

    if (success) {
      return Option.of(new StoragePath(FSUtils.constructAbsolutePath(markerDirPath, partitionPath), markerFileName));
    } else {
      // this failed may due to early conflict detection, so we need to throw out.
      throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
    }
  }

  /**
   * Executes marker creation request with specific parameters.
   *
   * @param paramsMap      Parameters to be included in the marker request.
   * @param partitionPath  Relative partition path.
   * @param markerFileName Marker file name.
   * @return {@code true} if successful; {@code false} otherwise.
   */
  private boolean executeCreateMarkerRequest(Map<String, String> paramsMap, String partitionPath, String markerFileName) {
    boolean success;
    try {
      success = executeRequestToTimelineServer(
          CREATE_MARKER_URL, paramsMap, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to create marker file " + partitionPath + "/" + markerFileName, e);
    }
    return success;
  }

  /**
   * Gets parameter map for marker creation request.
   *
   * @param partitionPath  Relative partition path.
   * @param markerFileName Marker file name.
   * @return parameter map.
   */
  private Map<String, String> getConfigMap(
      String partitionPath, String markerFileName, boolean initEarlyConflictDetectionConfigs) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    if (StringUtils.isNullOrEmpty(partitionPath)) {
      paramsMap.put(MARKER_NAME_PARAM, markerFileName);
    } else {
      paramsMap.put(MARKER_NAME_PARAM, partitionPath + "/" + markerFileName);
    }

    if (initEarlyConflictDetectionConfigs) {
      paramsMap.put(MARKER_BASEPATH_PARAM, basePath);
    }

    return paramsMap;
  }

  protected <T> T executeRequestToTimelineServer(String requestPath,
                                                 Map<String, String> queryParameters,
                                                 TypeReference reference,
                                                 RequestMethod method) throws IOException {
    return timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(method, requestPath).addQueryParams(queryParameters).build())
        .getDecodedContent(reference);
  }
}
