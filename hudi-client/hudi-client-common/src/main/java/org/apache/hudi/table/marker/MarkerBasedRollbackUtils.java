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
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class MarkerBasedRollbackUtils {

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
    HoodieStorage storage = table.getStorage();
    Option<MarkerType> markerTypeOption = readMarkerType(storage, markerDir);

    // If there is no marker type file "MARKERS.type", first assume "DIRECT" markers are used.
    // If not, then fallback to "TIMELINE_SERVER_BASED" markers.
    if (!markerTypeOption.isPresent()) {
      WriteMarkers writeMarkers = WriteMarkersFactory.get(DIRECT, table, instant);
      try {
        return new ArrayList<>(writeMarkers.allMarkerFilePaths());
      } catch (IOException e) {
        // Do NOT fall back to TIMELINE_SERVER_BASED on transient IO failures (e.g., HDFS throttling).
        // The timeline server looks in a different location and would return 0 markers, causing the
        // rollback to skip deleting data files and leaving orphan files on the table.
        log.warn("{} not present and {} marker listing failed with IO error. "
                + "Propagating exception, rollback will retry rather than fall back to {}.",
            MARKER_TYPE_FILENAME, DIRECT, TIMELINE_SERVER_BASED, e);
        throw e;
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException indicates a marker path format mismatch, fall back to timeline server.
        log.warn("{} not present and {} marker failed. Falling back to {} marker",
            MARKER_TYPE_FILENAME, DIRECT, TIMELINE_SERVER_BASED, e);
        return getTimelineServerBasedMarkers(context, parallelism, markerDir, storage);
      }
    }

    switch (markerTypeOption.get()) {
      case TIMELINE_SERVER_BASED:
        // Reads all markers written by the timeline server
        return getTimelineServerBasedMarkers(context, parallelism, markerDir, storage);
      default:
        throw new HoodieException(
            "The marker type \"" + markerTypeOption.get().name() + "\" is not supported.");
    }
  }

  private static List<String> getTimelineServerBasedMarkers(HoodieEngineContext context,
                                                            int parallelism,
                                                            String markerDir,
                                                            HoodieStorage storage) {
    Map<String, Set<String>> markersMap =
        readTimelineServerBasedMarkersFromFileSystem(markerDir, storage, context, parallelism);
    return markersMap.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
