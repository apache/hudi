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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.DirectWriteMarkers;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
/**
 * Downgrade handler to assist in downgrading hoodie table from version 2 to 1.
 */
public class TwoToOneDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(
      HoodieWriteConfig config, HoodieEngineContext context, String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();

    // re-create marker files if any partial timeline server based markers are found
    HoodieTimeline inflightTimeline = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    List<HoodieInstant> commits = inflightTimeline.getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant inflightInstant : commits) {
      // Converts the markers in new format to old format of direct markers
      try {
        convertToDirectMarkers(
            inflightInstant.getTimestamp(), table, context, config.getMarkersDeleteParallelism());
      } catch (IOException e) {
        throw new HoodieException("Converting marker files to DIRECT style failed during downgrade", e);
      }
    }
    return Collections.EMPTY_MAP;
  }

  /**
   * Converts the markers in new format(timeline server based) to old format of direct markers,
   * i.e., one marker file per data file, without MARKERS.type file.
   * This needs to be idempotent.
   * 1. read all markers from timeline server based marker files
   * 2. create direct style markers
   * 3. delete marker type file
   * 4. delete timeline server based marker files
   *
   * @param commitInstantTime instant of interest for marker conversion.
   * @param table             instance of {@link HoodieTable} to use
   * @param context           instance of {@link HoodieEngineContext} to use
   * @param parallelism       parallelism to use
   */
  private void convertToDirectMarkers(final String commitInstantTime,
                                      HoodieTable table,
                                      HoodieEngineContext context,
                                      int parallelism) throws IOException {
    String markerDir = table.getMetaClient().getMarkerFolderPath(commitInstantTime);
    FileSystem fileSystem = FSUtils.getFs(markerDir, context.getHadoopConf().newCopy());
    Option<MarkerType> markerTypeOption = MarkerUtils.readMarkerType(fileSystem, markerDir);
    if (markerTypeOption.isPresent()) {
      switch (markerTypeOption.get()) {
        case TIMELINE_SERVER_BASED:
          // Reads all markers written by the timeline server
          Map<String, Set<String>> markersMap =
              MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
                  markerDir, fileSystem, context, parallelism);
          DirectWriteMarkers directWriteMarkers = new DirectWriteMarkers(table, commitInstantTime);
          // Recreates the markers in the direct format
          markersMap.values().stream().flatMap(Collection::stream)
              .forEach(directWriteMarkers::create);
          // Deletes marker type file
          MarkerUtils.deleteMarkerTypeFile(fileSystem, markerDir);
          // Deletes timeline server based markers
          deleteTimelineBasedMarkerFiles(context, markerDir, fileSystem, parallelism);
          break;
        default:
          throw new HoodieException("The marker type \"" + markerTypeOption.get().name()
              + "\" is not supported for rollback.");
      }
    } else {
      if (fileSystem.exists(new Path(markerDir))) {
        // In case of partial failures during downgrade, there is a chance that marker type file was deleted,
        // but timeline server based marker files are left.  So deletes them if any
        deleteTimelineBasedMarkerFiles(context, markerDir, fileSystem, parallelism);
      }
    }
  }

  private void deleteTimelineBasedMarkerFiles(HoodieEngineContext context, String markerDir,
                                              FileSystem fileSystem, int parallelism) throws IOException {
    // Deletes timeline based marker files if any.
    Predicate<FileStatus> prefixFilter = fileStatus ->
        fileStatus.getPath().getName().startsWith(MARKERS_FILENAME_PREFIX);
    FSUtils.parallelizeSubPathProcess(context, fileSystem, new Path(markerDir), parallelism,
            prefixFilter, pairOfSubPathAndConf ->
                    FSUtils.deleteSubPath(pairOfSubPathAndConf.getKey(), pairOfSubPathAndConf.getValue(), false));
  }
}
