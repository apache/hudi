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

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to generate {@code WriteMarkers} instance based on the {@code MarkerType}.
 */
public class WriteMarkersFactory {
  private static final Logger LOG = LoggerFactory.getLogger(WriteMarkersFactory.class);

  /**
   * @param markerType the type of markers to use
   * @param table {@code HoodieTable} instance
   * @param instantTime current instant time
   * @return  {@code WriteMarkers} instance based on the {@code MarkerType}
   */
  public static WriteMarkers get(MarkerType markerType, HoodieTable table, String instantTime) {
    LOG.debug("Instantiated MarkerFiles with marker type: {}", markerType);
    switch (markerType) {
      case DIRECT:
        return getDirectWriteMarkers(table, instantTime);
      case TIMELINE_SERVER_BASED:
        if (!table.getConfig().isEmbeddedTimelineServerEnabled() && !table.getConfig().isRemoteViewStorageType()) {
          LOG.warn("Timeline-server-based markers are configured as the marker type "
              + "but embedded timeline server is not enabled.  Falling back to direct markers.");
          return getDirectWriteMarkers(table, instantTime);
        }
        String basePath = table.getMetaClient().getBasePath().toString();
        if (StorageSchemes.HDFS.getScheme().equals(
            HadoopFSUtils.getFs(basePath, table.getContext().getStorageConf(), true).getScheme())) {
          LOG.warn("Timeline-server-based markers are not supported for HDFS: "
              + "base path {}.  Falling back to direct markers.", basePath);
          return getDirectWriteMarkers(table, instantTime);
        }
        return table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
            ? new TimelineServerBasedWriteMarkers(table, instantTime)
            : new TimelineServerBasedWriteMarkersV1(table, instantTime);
      default:
        throw new HoodieException("The marker type \"" + markerType.name() + "\" is not supported.");
    }
  }

  /**
   * @param markerType  the type of markers to use
   * @param table       {@code HoodieTable} instance
   * @param instantTime current instant time
   * @return {@code AppendMarkerHandler} instance based on the {@code MarkerType}
   */
  public static AppendMarkerHandler getAppendMarkerHandler(MarkerType markerType, HoodieTable table, String instantTime) {
    ValidationUtils.checkArgument(
        table.getMetaClient().getTableConfig().getTableVersion()
            .lesserThan(HoodieTableVersion.EIGHT),
        "Expects table version 6 and below for getting the AppendMarkerHandler");
    return (AppendMarkerHandler) WriteMarkersFactory.get(markerType, table, instantTime);
  }

  private static DirectWriteMarkers getDirectWriteMarkers(HoodieTable table, String instantTime) {
    return table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
        ? new DirectWriteMarkers(table, instantTime) : new DirectWriteMarkersV1(table, instantTime);
  }
}
