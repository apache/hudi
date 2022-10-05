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

package org.apache.hudi.integ.testsuite.helpers;

import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.testutils.HiveTestService;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.server.HiveServer2;

import java.io.IOException;

/**
 * Hive Service provider.
 */
public class HiveServiceProvider {

  private HiveTestService hiveService;
  private HiveServer2 hiveServer;
  private Config config;

  public HiveServiceProvider(Config config) {
    this.config = config;
  }

  public void startLocalHiveServiceIfNeeded(Configuration configuration) throws IOException {
    if (config.isHiveLocal()) {
      hiveService = new HiveTestService(configuration);
      hiveServer = hiveService.start();
    }
  }

  public void syncToLocalHiveIfNeeded(HoodieTestSuiteWriter writer) {
    HiveSyncTool hiveSyncTool;
    if (this.config.isHiveLocal()) {
      hiveSyncTool = new HiveSyncTool(writer.getWriteConfig().getProps(),
          getLocalHiveServer().getHiveConf());
    } else {
      hiveSyncTool = new HiveSyncTool(writer.getWriteConfig().getProps(),
          writer.getConfiguration());
    }
    hiveSyncTool.syncHoodieTable();
  }

  public void stopLocalHiveServiceIfNeeded() throws IOException {
    if (config.isHiveLocal()) {
      if (hiveService != null) {
        hiveService.stop();
      }
    }
  }

  public HiveServer2 getLocalHiveServer() {
    return hiveServer;
  }
}
