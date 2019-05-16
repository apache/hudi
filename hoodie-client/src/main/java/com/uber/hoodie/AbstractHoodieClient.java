/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.uber.hoodie;

import com.uber.hoodie.client.embedded.EmbeddedTimelineService;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs)
 * Also, manages embedded timeline-server if enabled.
 */
public abstract class AbstractHoodieClient implements Serializable {

  private static final Logger logger = LogManager.getLogger(AbstractHoodieClient.class);

  protected final transient FileSystem fs;
  protected final transient JavaSparkContext jsc;
  protected final HoodieWriteConfig config;
  protected final String basePath;

  /**
   * Timeline Server has the same lifetime as that of Client.
   * Any operations done on the same timeline service will be able to take advantage
   * of the cached file-system view. New completed actions will be synced automatically
   * in an incremental fashion.
   */
  private transient EmbeddedTimelineService timelineServer;

  protected AbstractHoodieClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    startEmbeddedServerView();
  }

  /**
   * Releases any resources used by the client.
   */
  public void close() {
    stopEmbeddedServerView(true);
  }

  private synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
    if (timelineServer != null) {
      logger.info("Stopping Timeline service !!");
      timelineServer.stop();
      timelineServer = null;
      // Reset Storage Config to Client specified config
      if (resetViewStorageConfig) {
        config.resetViewStorageConfig();
      }
    }
  }

  private synchronized void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (timelineServer == null) {
        // Run Embedded Timeline Server
        logger.info("Starting Timeline service !!");
        timelineServer = new EmbeddedTimelineService(jsc.hadoopConfiguration(), jsc.getConf(),
            config.getClientSpecifiedViewStorageConfig());
        try {
          timelineServer.startServer();
          // Allow executor to find this newly instantiated timeline service
          config.setViewStorageConfig(timelineServer.getRemoteFileSystemViewConfig());
        } catch (IOException e) {
          logger.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        logger.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      logger.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }
}
