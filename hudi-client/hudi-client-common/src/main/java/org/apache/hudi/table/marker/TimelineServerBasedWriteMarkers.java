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

import org.apache.hudi.common.conflict.detection.HoodieEarlyConflictDetectionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.table.HoodieTable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_CONFLICT_CHECKER_BATCH_INTERVAL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_CONFLICT_CHECKER_ENABLE;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_CONFLICT_CHECKER_HEART_BEAT_INTERVAL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_CONFLICT_CHECKER_PERIOD;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_CONFLICT_CHECKER_STRATEGY;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_NAME_PARAM;

/**
 * Marker operations of using timeline server as a proxy to create and delete markers.
 * Each data file has a corresponding marker entry, which is stored in a limited number of
 * underlying files maintained by the timeline server (each file contains multiple marker
 * entries).
 */
public class TimelineServerBasedWriteMarkers extends WriteMarkers {
  private static final Logger LOG = LogManager.getLogger(TimelineServerBasedWriteMarkers.class);
  private final ObjectMapper mapper;
  private final String timelineServerHost;
  private final int timelineServerPort;
  private final int timeoutSecs;

  public TimelineServerBasedWriteMarkers(HoodieTable table, String instantTime) {
    this(table.getMetaClient().getBasePath(),
        table.getMetaClient().getMarkerFolderPath(instantTime), instantTime,
        table.getConfig().getViewStorageConfig().getRemoteViewServerHost(),
        table.getConfig().getViewStorageConfig().getRemoteViewServerPort(),
        table.getConfig().getViewStorageConfig().getRemoteTimelineClientTimeoutSecs());
  }

  TimelineServerBasedWriteMarkers(String basePath, String markerFolderPath, String instantTime,
                                  String timelineServerHost, int timelineServerPort, int timeoutSecs) {
    super(basePath, markerFolderPath, instantTime);
    this.mapper = new ObjectMapper();
    this.timelineServerHost = timelineServerHost;
    this.timelineServerPort = timelineServerPort;
    this.timeoutSecs = timeoutSecs;
  }

  @Override
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          DELETE_MARKER_DIR_URL, paramsMap, new TypeReference<Boolean>() {}, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to delete marker directory " + markerDirPath.toString(), e);
    }
  }

  @Override
  public boolean doesMarkerDirExist() {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          MARKERS_DIR_EXISTS_URL, paramsMap, new TypeReference<Boolean>() {}, RequestMethod.GET);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to check marker directory " + markerDirPath.toString(), e);
    }
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      Set<String> markerPaths = executeRequestToTimelineServer(
          CREATE_AND_MERGE_MARKERS_URL, paramsMap, new TypeReference<Set<String>>() {}, RequestMethod.GET);
      return markerPaths.stream().map(WriteMarkers::stripMarkerSuffix).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to get CREATE and MERGE data file paths in "
          + markerDirPath.toString(), e);
    }
  }

  @Override
  public Set<String> allMarkerFilePaths() {
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      return executeRequestToTimelineServer(
          ALL_MARKERS_URL, paramsMap, new TypeReference<Set<String>>() {}, RequestMethod.GET);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to get all markers in " + markerDirPath.toString(), e);
    }
  }

  @Override
  protected Option<Path> create(String partitionPath, String dataFileName, IOType type, boolean checkIfExists) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    String markerFileName = getMarkerFileName(dataFileName, type);

    Map<String, String> paramsMap = initConfigMap(partitionPath, markerFileName);
    boolean success = executeCreateMarkerRequest(paramsMap, partitionPath, markerFileName);
    LOG.info("[timeline-server-based] Created marker file " + partitionPath + "/" + markerFileName
        + " in " + timer.endTimer() + " ms");
    if (success) {
      return Option.of(new Path(FSUtils.getPartitionPath(markerDirPath, partitionPath), markerFileName));
    } else {
      return Option.empty();
    }
  }

  @Override
  public Option<Path> createWithEarlyConflictDetection(String partitionPath, String dataFileName, IOType type, boolean checkIfExists,
                                                       HoodieEarlyConflictDetectionStrategy resolutionStrategy,
                                                       Set<HoodieInstant> completedCommitInstants, HoodieWriteConfig config) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    String markerFileName = getMarkerFileName(dataFileName, type);
    Map<String, String> paramsMap = initConfigMap(partitionPath, markerFileName);

    paramsMap.put(MARKER_CONFLICT_CHECKER_BATCH_INTERVAL, config.getMarkerConflictCheckerBatchInterval());
    paramsMap.put(MARKER_CONFLICT_CHECKER_PERIOD, config.getMarkerConflictCheckerPeriod());
    paramsMap.put(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    paramsMap.put(MARKER_BASEPATH_PARAM, basePath);
    paramsMap.put(MARKER_CONFLICT_CHECKER_HEART_BEAT_INTERVAL, String.valueOf(config.getHoodieClientHeartbeatIntervalInMs()));
    paramsMap.put(MARKER_CONFLICT_CHECKER_ENABLE, String.valueOf(config.isEarlyConflictDetectionEnable()));
    paramsMap.put(MARKER_CONFLICT_CHECKER_STRATEGY, config.getEarlyConflictDetectionStrategyClassName());

    boolean success = executeCreateMarkerRequest(paramsMap, partitionPath, markerFileName);

    LOG.info("[timeline-server-based] Created marker file with early conflict detection " + partitionPath + "/" + markerFileName
        + " in " + timer.endTimer() + " ms");

    if (success) {
      return Option.of(new Path(FSUtils.getPartitionPath(markerDirPath, partitionPath), markerFileName));
    } else {
      // this failed may due to early conflict detection, so we need to throw out.
      throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
    }
  }

  /**
   * execute create marker request with specific parasMap
   * @param paramsMap
   * @param partitionPath
   * @param markerFileName
   * @return
   */
  private boolean executeCreateMarkerRequest(Map<String, String> paramsMap, String partitionPath, String markerFileName) {
    boolean success;
    try {
      success = executeRequestToTimelineServer(
          CREATE_MARKER_URL, paramsMap, new TypeReference<Boolean>() {
          }, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to create marker file " + partitionPath + "/" + markerFileName, e);
    }
    return success;
  }

  /**
   * init create marker related config maps.
   * @param partitionPath
   * @param markerFileName
   * @return
   */
  private Map<String, String> initConfigMap(String partitionPath, String markerFileName) {

    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    if (StringUtils.isNullOrEmpty(partitionPath)) {
      paramsMap.put(MARKER_NAME_PARAM, markerFileName);
    } else {
      paramsMap.put(MARKER_NAME_PARAM, partitionPath + "/" + markerFileName);
    }
    return paramsMap;
  }

  private <T> T executeRequestToTimelineServer(String requestPath, Map<String, String> queryParameters,
                                               TypeReference reference, RequestMethod method) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(timelineServerHost).setPort(timelineServerPort).setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    LOG.info("Sending request : (" + url + ")");
    Response response;
    int timeout = this.timeoutSecs * 1000; // msec
    switch (method) {
      case GET:
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
      case POST:
      default:
        response = Request.Post(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
    }
    String content = response.returnContent().asString();
    return (T) mapper.readValue(content, reference);
  }

  private enum RequestMethod {
    GET, POST
  }
}
