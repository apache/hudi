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

import org.apache.hudi.client.common.EngineProperty;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Helper class to instantiate embedded timeline service.
 */
public class EmbeddedTimelineServerHelper {

  private static final Logger LOG = LogManager.getLogger(EmbeddedTimelineService.class);

  /**
   * Instantiate Embedded Timeline Server.
   * @param context Hoodie Engine Context
   * @param config     Hoodie Write Config
   * @return TimelineServer if configured to run
   * @throws IOException
   */
  public static Option<EmbeddedTimelineService> createEmbeddedTimelineService(
      HoodieEngineContext context, HoodieWriteConfig config) throws IOException {
    Option<EmbeddedTimelineService> timelineServer = Option.empty();
    if (config.isEmbeddedTimelineServerEnabled()) {
      // Run Embedded Timeline Server
      LOG.info("Starting Timeline service !!");
      Option<String> hostAddr = context.getProperty(EngineProperty.EMBEDDED_SERVER_HOST);
      timelineServer = Option.of(new EmbeddedTimelineService(context, hostAddr.orElse(null),
          config.getEmbeddedTimelineServerPort(), config.getClientSpecifiedViewStorageConfig()));
      timelineServer.get().startServer();
      updateWriteConfigWithTimelineServer(timelineServer.get(), config);
    }
    return timelineServer;
  }

  /**
   * Adjusts hoodie write config with timeline server settings.
   * @param timelineServer Embedded Timeline Server
   * @param config  Hoodie Write Config
   */
  public static void updateWriteConfigWithTimelineServer(EmbeddedTimelineService timelineServer,
      HoodieWriteConfig config) {
    // Allow executor to find this newly instantiated timeline service
    if (config.isEmbeddedTimelineServerEnabled()) {
      config.setViewStorageConfig(timelineServer.getRemoteFileSystemViewConfig());
    }
  }
}
