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

import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;

import org.apache.hadoop.conf.Configuration;

public class ZookeeperServiceProvider {

  private final Configuration configuration;
  private final Config config;
  private ZookeeperTestService zookeeperTestService;

  public ZookeeperServiceProvider(Config config, Configuration configuration) {
    this.configuration = configuration;
    this.config = config;
  }

  public void startLocalZookeeperIfNeeded() throws Exception {
    if (config.isHiveLocal() && zookeeperTestService == null) {
      zookeeperTestService = new ZookeeperTestService(configuration);
      try {
        zookeeperTestService.start();
      } catch (Exception e) {
        try {
          zookeeperTestService.stop();
        } catch (Exception stopException) {
          e.addSuppressed(stopException);
        } finally {
          zookeeperTestService = null;
        }
        throw e;
      }
    }
  }

  public void stopLocalZookeeperIfNeeded() throws Exception {
    if (config.isHiveLocal() && zookeeperTestService != null) {
      try {
        zookeeperTestService.stop();
      } finally {
        zookeeperTestService = null;
      }
    }
  }

  public String getConnectString() {
    return getZookeeperTestService().connectString();
  }

  public int getClientPort() {
    return getZookeeperTestService().clientPort();
  }

  private ZookeeperTestService getZookeeperTestService() {
    if (zookeeperTestService == null) {
      throw new IllegalStateException("Local Zookeeper service is not running");
    }
    return zookeeperTestService;
  }
}
