/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.heartbeat;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Common utils for Hudi heartbeat
 */
public class HoodieHeartbeatUtils {
  private static final Logger LOG = LogManager.getLogger(HoodieHeartbeatUtils.class);

  /**
   * Use modification time as last heart beat time
   *
   * @param fs
   * @param basePath
   * @param instantTime
   * @return
   * @throws IOException
   */
  public static Long getLastHeartbeatTime(FileSystem fs, String basePath, String instantTime) throws IOException {
    Path heartbeatFilePath = new Path(HoodieTableMetaClient.getHeartbeatFolderPath(basePath) + Path.SEPARATOR + instantTime);
    if (fs.exists(heartbeatFilePath)) {
      return fs.getFileStatus(heartbeatFilePath).getModificationTime();
    } else {
      // NOTE : This can happen when a writer is upgraded to use lazy cleaning and the last write had failed
      return 0L;
    }
  }

  public static boolean isHeartbeatExpired(String instantTime, long maxAllowableHeartbeatIntervalInMs, FileSystem fs, String basePath) throws IOException {
    Long currentTime = System.currentTimeMillis();
    Long lastHeartbeatTime = getLastHeartbeatTime(fs, basePath, instantTime);
    if (currentTime - lastHeartbeatTime > maxAllowableHeartbeatIntervalInMs) {
      LOG.warn("Heartbeat expired, for instant: " + instantTime);
      return true;
    }
    return false;
  }
}
