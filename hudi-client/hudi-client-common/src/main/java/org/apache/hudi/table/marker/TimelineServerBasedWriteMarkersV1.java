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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.timeline.TimelineServiceClientBase;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.marker.MarkerOperation.APPEND_MARKERS_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;

/**
 * Marker operations of using timeline server as a proxy to create and delete markers
 * for table version 6.
 */
public class TimelineServerBasedWriteMarkersV1 extends TimelineServerBasedWriteMarkers implements AppendMarkerHandler {
  public TimelineServerBasedWriteMarkersV1(HoodieTable table, String instantTime) {
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
    Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markerDirPath.toString());
    try {
      Set<String> markerPaths = executeRequestToTimelineServer(
          APPEND_MARKERS_URL, paramsMap, new TypeReference<Set<String>>() {
          }, TimelineServiceClientBase.RequestMethod.GET);
      return markerPaths.stream().map(WriteMarkers::stripMarkerSuffix).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to get APPEND log file paths in "
          + markerDirPath.toString(), e);
    }
  }
}
