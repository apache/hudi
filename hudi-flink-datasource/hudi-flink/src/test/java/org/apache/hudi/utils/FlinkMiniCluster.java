/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Class for tests that run multiple tests and want to reuse the same Flink cluster.
 * Unlike {@link AbstractTestBase}, this class is designed to run with JUnit 5.
 */
@Slf4j
public class FlinkMiniCluster implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

  public static final int DEFAULT_PARALLELISM = 4;

  private static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setConfiguration(getDefaultConfig())
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .build());

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    MINI_CLUSTER_RESOURCE.before();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    MINI_CLUSTER_RESOURCE.after();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    cleanupRunningJobs();
  }

  private static Configuration getDefaultConfig() {
    Configuration config = new Configuration();
    // flink job uses child-first classloader by default, async services fired by flink job are not
    // guaranteed to be killed right away, which then may trigger classloader leak checking exception.
    config.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
    return config;
  }

  private void cleanupRunningJobs() throws Exception {
    if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
      // do nothing if the MiniCluster is not running
      log.warn("Mini cluster is not running after the test!");
      return;
    }

    for (JobStatusMessage path : MINI_CLUSTER_RESOURCE.getClusterClient().listJobs().get()) {
      if (!path.getJobState().isTerminalState()) {
        try {
          MINI_CLUSTER_RESOURCE.getClusterClient().cancel(path.getJobId()).get();
        } catch (Exception ignored) {
          // ignore exceptions when cancelling dangling jobs
        }
      }
    }
  }
}
