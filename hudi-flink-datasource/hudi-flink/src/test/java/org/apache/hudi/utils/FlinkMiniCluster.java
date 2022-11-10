package org.apache.hudi.utils;

import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for tests that run multiple tests and want to reuse the same Flink cluster.
 * Unlike {@link AbstractTestBase}, this class is designed to run with JUnit 5.
 */
public class FlinkMiniCluster implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniCluster.class);

  public static final int DEFAULT_PARALLELISM = 4;

  private static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
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

  private void cleanupRunningJobs() throws Exception {
    if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
      // do nothing if the MiniCluster is not running
      LOG.warn("Mini cluster is not running after the test!");
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
