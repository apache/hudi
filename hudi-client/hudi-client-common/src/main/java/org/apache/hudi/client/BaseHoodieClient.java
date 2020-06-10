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

package org.apache.hudi.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.embedded.AbstractEmbeddedTimelineService;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public abstract class BaseHoodieClient implements Serializable, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(BaseHoodieClient.class);

  protected final transient FileSystem fs;
  protected final transient HoodieEngineContext context;
  protected final transient Configuration hadoopConf;
  protected final HoodieWriteConfig config;
  protected final String basePath;
  protected final Lock timeServerLock = new ReentrantLock();

  /**
   * Timeline Server has the same lifetime as that of Client. Any operations done on the same timeline service will be
   * able to take advantage of the cached file-system view. New completed actions will be synced automatically in an
   * incremental fashion.
   */
  public transient Option<AbstractEmbeddedTimelineService> timelineServer;
  private final boolean shouldStopTimelineServer;

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig,
                             Option<AbstractEmbeddedTimelineService> timelineServer) {
    this.hadoopConf = context.getHadoopConf().get();
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), hadoopConf);
    this.context = context;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.timelineServer = timelineServer;
    shouldStopTimelineServer = !timelineServer.isPresent();
    startEmbeddedServerView();
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    stopEmbeddedServerView(true);
  }

  public synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
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

  /**
   * synchronize to be done.
   */
  public abstract void startEmbeddedServerView();

  public HoodieWriteConfig getConfig() {
    return config;
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(hadoopConf, config, loadActiveTimelineOnLoad);
  }

}
