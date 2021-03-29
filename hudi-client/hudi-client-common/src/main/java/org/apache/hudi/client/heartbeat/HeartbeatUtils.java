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

package org.apache.hudi.client.heartbeat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to delete heartbeat for completed or failed instants with expired heartbeats.
 */
public class HeartbeatUtils {

  private static final Logger LOG = LogManager.getLogger(HeartbeatUtils.class);

  /**
   * Deletes the heartbeat file for the specified instant.
   * @param fs
   * @param basePath
   * @param instantTime
   * @return
   */
  public static boolean deleteHeartbeatFile(FileSystem fs, String basePath, String instantTime) {
    boolean deleted = false;
    try {
      String heartbeatFolderPath = HoodieTableMetaClient.getHeartbeatFolderPath(basePath);
      deleted = fs.delete(new Path(heartbeatFolderPath + File.separator + instantTime), false);
      if (!deleted) {
        LOG.error("Failed to delete heartbeat for instant " + instantTime);
      }
    } catch (IOException io) {
      LOG.error("Unable to delete heartbeat for instant " + instantTime, io);
    }
    return deleted;
  }

  /**
   * Deletes the heartbeat files for instants with expired heartbeats without any active instant.
   * @param allExistingHeartbeatInstants
   * @param metaClient
   * @param basePath
   */
  public static void cleanExpiredHeartbeats(List<String> allExistingHeartbeatInstants,
                                            HoodieTableMetaClient metaClient, String basePath) {
    Set<String> nonExpiredHeartbeatInstants = metaClient.getActiveTimeline()
        .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    allExistingHeartbeatInstants.stream().forEach(instant -> {
      if (!nonExpiredHeartbeatInstants.contains(instant)) {
        deleteHeartbeatFile(metaClient.getFs(), basePath, instant);
      }
    });
  }

  /**
   * Check if the heartbeat corresponding to instantTime has expired. If yes, abort by throwing an exception.
   * @param instantTime
   * @param table
   * @param heartbeatClient
   * @param config
   */
  public static void abortIfHeartbeatExpired(String instantTime, HoodieTable table,
                                             HoodieHeartbeatClient heartbeatClient, HoodieWriteConfig config) {
    ValidationUtils.checkArgument(heartbeatClient != null);
    try {
      if (config.getFailedWritesCleanPolicy().isLazy() && heartbeatClient.isHeartbeatExpired(instantTime)) {
        throw new HoodieException("Heartbeat for instant " + instantTime + " has expired, last heartbeat "
            + heartbeatClient.getLastHeartbeatTime(table.getMetaClient().getFs(), config.getBasePath(), instantTime));
      }
    } catch (IOException io) {
      throw new HoodieException("Unable to read heartbeat", io);
    }
  }
}
