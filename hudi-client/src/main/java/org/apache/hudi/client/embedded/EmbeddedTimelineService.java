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

package org.apache.hudi.client.embedded;

import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.NetworkUtils;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.IOException;

/**
 * Timeline Service that runs as part of write client.
 */
public class EmbeddedTimelineService {

  private static Logger logger = LogManager.getLogger(EmbeddedTimelineService.class);

  private int serverPort;
  private String hostAddr;
  private final SerializableConfiguration hadoopConf;
  private final FileSystemViewStorageConfig config;
  private transient FileSystemViewManager viewManager;
  private transient TimelineService server;

  public EmbeddedTimelineService(Configuration hadoopConf, SparkConf sparkConf, FileSystemViewStorageConfig config) {
    setHostAddrFromSparkConf(sparkConf);
    if (hostAddr == null) {
      this.hostAddr = NetworkUtils.getHostname();
    }
    this.config = config;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.viewManager = createViewManager();
  }

  private FileSystemViewManager createViewManager() {
    // Using passed-in configs to build view storage configs
    FileSystemViewStorageConfig.Builder builder =
        FileSystemViewStorageConfig.newBuilder().fromProperties(config.getProps());
    FileSystemViewStorageType storageType = builder.build().getStorageType();
    if (storageType.equals(FileSystemViewStorageType.REMOTE_ONLY)
        || storageType.equals(FileSystemViewStorageType.REMOTE_FIRST)) {
      // Reset to default if set to Remote
      builder.withStorageType(FileSystemViewStorageType.MEMORY);
    }
    return FileSystemViewManager.createViewManager(hadoopConf, builder.build());
  }

  public void startServer() throws IOException {
    server = new TimelineService(0, viewManager, hadoopConf.newCopy());
    serverPort = server.startService();
    logger.info("Started embedded timeline server at " + hostAddr + ":" + serverPort);
  }

  private void setHostAddrFromSparkConf(SparkConf sparkConf) {
    String hostAddr = sparkConf.get("spark.driver.host", null);
    if (hostAddr != null) {
      logger.info("Overriding hostIp to (" + hostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = hostAddr;
    } else {
      logger.warn("Unable to find driver bind address from spark config");
    }
  }

  /**
   * Retrieves proper view storage configs for remote clients to access this service.
   */
  public FileSystemViewStorageConfig getRemoteFileSystemViewConfig() {
    return FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.REMOTE_FIRST)
        .withRemoteServerHost(hostAddr).withRemoteServerPort(serverPort).build();
  }

  public FileSystemViewManager getViewManager() {
    return viewManager;
  }

  public void stop() {
    if (null != server) {
      logger.info("Closing Timeline server");
      this.server.close();
      this.server = null;
      this.viewManager = null;
      logger.info("Closed Timeline server");
    }
  }
}
