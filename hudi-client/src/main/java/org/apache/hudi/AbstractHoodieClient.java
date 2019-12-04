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

package org.apache.hudi;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public abstract class AbstractHoodieClient implements Serializable, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieClient.class);

  protected final transient FileSystem fs;
  protected final transient JavaSparkContext jsc;
  protected final HoodieWriteConfig config;
  protected final String basePath;

  /**
   * Timeline Server has the same lifetime as that of Client. Any operations done on the same timeline service will be
   * able to take advantage of the cached file-system view. New completed actions will be synced automatically in an
   * incremental fashion.
   */
  private transient Option<EmbeddedTimelineService> timelineServer;
  private final boolean shouldStopTimelineServer;

  protected AbstractHoodieClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) {
    this(jsc, clientConfig, Option.empty());
  }

  protected AbstractHoodieClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      Option<EmbeddedTimelineService> timelineServer) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.timelineServer = timelineServer;
    shouldStopTimelineServer = !timelineServer.isPresent();
    startEmbeddedServerView();
  }

  /**
   * Releases any resources used by the client.
   */
  public void close() {
    stopEmbeddedServerView(true);
  }

  private synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
    if (timelineServer.isPresent() && shouldStopTimelineServer) {
      // Stop only if owner
      LOG.info("Stopping Timeline service !!");
      timelineServer.get().stop();
    }

    timelineServer = Option.empty();
    // Reset Storage Config to Client specified config
    if (resetViewStorageConfig) {
      config.resetViewStorageConfig();
    }
  }

  private synchronized void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        LOG.info("Starting Timeline service !!");
        timelineServer = Option.of(new EmbeddedTimelineService(jsc.hadoopConfiguration(), jsc.getConf(),
            config.getClientSpecifiedViewStorageConfig()));
        try {
          timelineServer.get().startServer();
          // Allow executor to find this newly instantiated timeline service
          config.setViewStorageConfig(timelineServer.get().getRemoteFileSystemViewConfig());
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public Option<EmbeddedTimelineService> getTimelineServer() {
    return timelineServer;
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(jsc, config, loadActiveTimelineOnLoad);
  }
}
