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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

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
      deleted = fs.delete(new Path(heartbeatFolderPath + Path.SEPARATOR + instantTime), false);
      if (!deleted) {
        LOG.error("Failed to delete heartbeat for instant " + instantTime);
      } else {
        LOG.info("Deleted the heartbeat for instant " + instantTime);
      }
    } catch (IOException io) {
      LOG.error("Unable to delete heartbeat for instant " + instantTime, io);
    }
    return deleted;
  }

  /**
   * Deletes the heartbeat file for the specified instant.
   * @param fs Hadoop FileSystem instance
   * @param basePath Hoodie table base path
   * @param instantTime Commit instant time
   * @param config HoodieWriteConfig instance
   * @return Boolean indicating whether heartbeat file was deleted or not
   */
  public static boolean deleteHeartbeatFile(FileSystem fs, String basePath, String instantTime, HoodieWriteConfig config) {
    if (config.getFailedWritesCleanPolicy().isLazy()) {
      return deleteHeartbeatFile(fs, basePath, instantTime);
    }

    return false;
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
            + HoodieHeartbeatClient.getLastHeartbeatTime(table.getMetaClient().getFs(), config.getBasePath(), instantTime));
      }
    } catch (IOException io) {
      throw new HoodieException("Unable to read heartbeat", io);
    }
  }
}
