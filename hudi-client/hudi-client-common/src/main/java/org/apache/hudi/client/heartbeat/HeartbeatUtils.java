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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils.getLastHeartbeatTime;

/**
 * Helper class to delete heartbeat for completed or failed instants with expired heartbeats.
 */
@Slf4j
public class HeartbeatUtils {

  /**
   * Deletes the heartbeat file for the specified instant.
   *
   * @param storage     {@link HoodieStorage} instance.
   * @param basePath    Hudi table base path.
   * @param instantTime commit instant time.
   * @return whether the file is successfully deleted.
   */
  public static boolean deleteHeartbeatFile(HoodieStorage storage,
                                            String basePath,
                                            String instantTime) {
    boolean deleted = false;
    try {
      String heartbeatFolderPath = HoodieTableMetaClient.getHeartbeatFolderPath(basePath);
      deleted = storage.deleteFile(new StoragePath(heartbeatFolderPath, instantTime));
      if (!deleted) {
        log.error("Failed to delete heartbeat for instant {}", instantTime);
      } else {
        log.info("Deleted the heartbeat for instant {}", instantTime);
      }
    } catch (IOException io) {
      log.error("Unable to delete heartbeat for instant {}", instantTime, io);
    }
    return deleted;
  }

  /**
   * Deletes the heartbeat file for the specified instant.
   *
   * @param storage     {@link HoodieStorage} instance.
   * @param basePath    Hoodie table base path
   * @param instantTime Commit instant time
   * @param config      HoodieWriteConfig instance
   * @return Boolean indicating whether heartbeat file was deleted or not
   */
  public static boolean deleteHeartbeatFile(HoodieStorage storage,
                                            String basePath,
                                            String instantTime,
                                            HoodieWriteConfig config) {
    if (config.getFailedWritesCleanPolicy().isLazy()) {
      return deleteHeartbeatFile(storage, basePath, instantTime);
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
        throw new HoodieException(
            "Heartbeat for instant " + instantTime + " has expired, last heartbeat "
                + getLastHeartbeatTime(table.getStorage(), config.getBasePath(), instantTime));
      }
    } catch (IOException io) {
      throw new HoodieException("Unable to read heartbeat", io);
    }
  }
}
