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
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.marker.MarkerType.DIRECT;
import static org.apache.hudi.common.table.marker.MarkerType.TIMELINE_SERVER_BASED;
import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.apache.hudi.common.util.MarkerUtils.readMarkerType;
import static org.apache.hudi.common.util.MarkerUtils.readTimelineServerBasedMarkersFromFileSystem;

/**
 * A utility class for marker-based rollback.
 */
public class MarkerBasedRollbackUtils {

  private static final Logger LOG = LogManager.getLogger(MarkerBasedRollbackUtils.class);

  /**
   * Gets all marker paths.
   *
   * @param table       instance of {@code HoodieTable} to use
   * @param context     instance of {@code HoodieEngineContext} to use
   * @param instant     instant of interest to rollback
   * @param parallelism parallelism to use
   * @return a list of all markers
   * @throws IOException
   */
  public static List<String> getAllMarkerPaths(HoodieTable table, HoodieEngineContext context,
                                               String instant, int parallelism) throws IOException {
    String markerDir = table.getMetaClient().getMarkerFolderPath(instant);
    FileSystem fileSystem = table.getMetaClient().getFs();
    Option<MarkerType> markerTypeOption = readMarkerType(fileSystem, markerDir);

    // If there is no marker type file "MARKERS.type", first assume "DIRECT" markers are used.
    // If not, then fallback to "TIMELINE_SERVER_BASED" markers.
    if (!markerTypeOption.isPresent()) {
      WriteMarkers writeMarkers = WriteMarkersFactory.get(DIRECT, table, instant);
      try {
        return new ArrayList<>(writeMarkers.allMarkerFilePaths());
      } catch (IOException | IllegalArgumentException e) {
        LOG.warn(String.format("%s not present and %s marker failed with error: %s. So, falling back to %s marker",
            MARKER_TYPE_FILENAME, DIRECT, e.getMessage(), TIMELINE_SERVER_BASED));
        return getTimelineServerBasedMarkers(context, parallelism, markerDir, fileSystem);
      }
    }

    switch (markerTypeOption.get()) {
      case TIMELINE_SERVER_BASED:
        // Reads all markers written by the timeline server
        return getTimelineServerBasedMarkers(context, parallelism, markerDir, fileSystem);
      default:
        throw new HoodieException(
            "The marker type \"" + markerTypeOption.get().name() + "\" is not supported.");
    }
  }

  private static List<String> getTimelineServerBasedMarkers(HoodieEngineContext context, int parallelism, String markerDir, FileSystem fileSystem) {
    Map<String, Set<String>> markersMap = readTimelineServerBasedMarkersFromFileSystem(markerDir, fileSystem, context, parallelism);
    return markersMap.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
