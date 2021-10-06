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
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A utility class for marker-based rollback.
 */
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
    FileSystem fileSystem = table.getMetaClient().getFs();
    Option<MarkerType> markerTypeOption = MarkerUtils.readMarkerType(fileSystem, markerDir);

    // If there is no marker type file "MARKERS.type", we assume "DIRECT" markers are used
    if (!markerTypeOption.isPresent()) {
      WriteMarkers writeMarkers = WriteMarkersFactory.get(MarkerType.DIRECT, table, instant);
      return new ArrayList<>(writeMarkers.allMarkerFilePaths());
    }

    switch (markerTypeOption.get()) {
      case TIMELINE_SERVER_BASED:
        // Reads all markers written by the timeline server
        Map<String, Set<String>> markersMap =
            MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
                markerDir, fileSystem, context, parallelism);
        return markersMap.values().stream().flatMap(Collection::stream)
            .collect(Collectors.toCollection(ArrayList::new));
      default:
        throw new HoodieException(
            "The marker type \"" + markerTypeOption.get().name() + "\" is not supported.");
    }
  }
}
