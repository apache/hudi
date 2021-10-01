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
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A factory to generate {@code WriteMarkers} instance based on the {@code MarkerType}.
 */
public class WriteMarkersFactory {
  private static final Logger LOG = LogManager.getLogger(WriteMarkersFactory.class);

  /**
   * @param markerType  the type of markers to use
   * @param metaClient  {@link HoodieTableMetaClient} instance to use
   * @param config      Write config
   * @param context     {@link HoodieEngineContext} instance to use
   * @param instantTime current instant time
   * @return {@code WriteMarkers} instance based on the {@code MarkerType}
   */
  public static WriteMarkers get(
      MarkerType markerType, HoodieTableMetaClient metaClient, HoodieWriteConfig config,
      HoodieEngineContext context, String instantTime) {
    LOG.debug("Instantiated MarkerFiles with marker type: " + markerType.toString());
    switch (markerType) {
      case DIRECT:
        return new DirectWriteMarkers(metaClient, instantTime);
      case TIMELINE_SERVER_BASED:
        String basePath = metaClient.getBasePath();
        if (StorageSchemes.HDFS.getScheme().equals(
            FSUtils.getFs(basePath, context.getHadoopConf().newCopy()).getScheme())) {
          throw new HoodieException("Timeline-server-based markers are not supported for HDFS: "
              + "base path " + basePath);
        }
        return new TimelineServerBasedWriteMarkers(metaClient, config, instantTime);
      default:
        throw new HoodieException("The marker type \"" + markerType.name() + "\" is not supported.");
    }
  }
}
