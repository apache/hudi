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

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class to instantiate embedded timeline service.
 */
public class EmbeddedTimelineServerHelper {

  private static final Logger LOG = LogManager.getLogger(EmbeddedTimelineService.class);
  private static final Map<String, EmbeddedTimelineService> PATH_TO_SERVER = new HashMap<>();
  private static final Map<EmbeddedTimelineService, Integer> SERVER_TO_COUNTER = new IdentityHashMap<>();

  /**
   * Instantiate Embedded Timeline Server.
   * @param context Hoodie Engine Context
   * @param config     Hoodie Write Config
   * @return TimelineServer if configured to run
   * @throws IOException
   */
  public static Option<EmbeddedTimelineService> createEmbeddedTimelineService(
      HoodieEngineContext context, HoodieWriteConfig config) throws IOException {
    if (config.isEmbeddedTimelineServerReuseEnabled()) {
      synchronized (PATH_TO_SERVER) {
        String path = config.getBasePath();
        EmbeddedTimelineService timelineServer = PATH_TO_SERVER.get(path);
        if (timelineServer == null || !timelineServer.canReuse()) {
          timelineServer = EmbeddedTimelineServerHelper.startTimelineService(context, config);
          PATH_TO_SERVER.put(path, timelineServer);
        }
        int serverCounter = 1 + SERVER_TO_COUNTER.getOrDefault(timelineServer, 0);
        SERVER_TO_COUNTER.put(timelineServer, serverCounter);
        if (serverCounter > 1) {
          updateWriteConfigWithTimelineServer(timelineServer, config);
        }
        return Option.of(timelineServer);
      }
    }
    if (config.isEmbeddedTimelineServerEnabled()) {
      return Option.of(startTimelineService(context, config));
    } else {
      return Option.empty();
    }
  }

  private static EmbeddedTimelineService startTimelineService(
      HoodieEngineContext context, HoodieWriteConfig config) throws IOException {
    // Run Embedded Timeline Server
    LOG.info("Starting Timeline service !!");
    Option<String> hostAddr = context.getProperty(EngineProperty.EMBEDDED_SERVER_HOST);
    EmbeddedTimelineService timelineService = new EmbeddedTimelineService(
        context, hostAddr.orElse(null), config);
    timelineService.startServer();
    updateWriteConfigWithTimelineServer(timelineService, config);
    return timelineService;
  }

  /**
   * Stops Embedded Timeline Server.
   * @param timelineServer Embedded Timeline Server
   * @throws NullPointerException if timelineServer is null
   */
  public static void stopEmbeddedTimelineService(EmbeddedTimelineService timelineServer) {
    Objects.requireNonNull(timelineServer);
    synchronized (PATH_TO_SERVER) {
      Integer serverCounter = SERVER_TO_COUNTER.get(timelineServer);
      if (serverCounter == null) {
        timelineServer.stop();
      } else if (serverCounter == 1) {
        PATH_TO_SERVER.values().removeIf(s -> s == timelineServer);
        SERVER_TO_COUNTER.remove(timelineServer);
        timelineServer.stop();
      } else {
        SERVER_TO_COUNTER.put(timelineServer, serverCounter - 1);
      }
    }
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
