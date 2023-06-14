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

package org.apache.hudi.util.testrunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.NetworkTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.launcher.listeners.TestExecutionSummary.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

public class TestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);

  public static void main(String[] args) {
    final LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
        .selectors(selectClass(TestRunner.class))
        .build();

    final Launcher launcher = LauncherFactory.create();
    final SummaryGeneratingListener listener = new SummaryGeneratingListener();

    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);

    TestExecutionSummary summary = listener.getSummary();
    long testFoundCount = summary.getTestsFoundCount();
    List<Failure> failures = summary.getFailures();
    LOG.info("getTestsSucceededCount() - " + summary.getTestsSucceededCount());
    LOG.info("yxchang: Test runner!!!!!!!");
    failures.forEach(failure -> LOG.error("failure - " + failure.getException()));

  }

  @Test
  public void testHoodieLogFormat() throws IOException {
    int namenodeRpcPort = NetworkTestUtils.nextFreePort();
    int datanodePort = NetworkTestUtils.nextFreePort();
    int datanodeIpcPort = NetworkTestUtils.nextFreePort();
    int datanodeHttpPort = NetworkTestUtils.nextFreePort();

    // Configure and start the HDFS cluster
    // boolean format = shouldFormatDFSCluster(localDFSLocation, clean);
    String bindIP = "127.0.0.1";
    FileSystem fs = FileSystem.get(configureDFSCluster(new Configuration(), Files.createTempDirectory("hdfs-test-service" + System.currentTimeMillis()).toString(),
        bindIP, namenodeRpcPort,
        datanodePort, datanodeIpcPort, datanodeHttpPort));

    Path workDir = fs.getWorkingDirectory();
    String basePath = new Path(workDir.toString(), "testHoodieLogFormat_" + System.currentTimeMillis()).toString();
    Path partitionPath = new Path(basePath, "partition_path");
    String spillableBasePathStr = new Path(workDir.toString(), ".spillable_path").toString();
    HoodieTestUtils.init(fs.getConf(), basePath, HoodieTableType.MERGE_ON_READ);

    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(
                HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    assertEquals(0, writer.getCurrentSize(), "Just created this log, size should be 0");
    assertTrue(writer.getLogFile().getFileName().startsWith("."), "Check all log files should start with a .");
    assertEquals(1, writer.getLogFile().getLogVersion(), "Version should be 1 for new log created");
    writer.close();
  }

  private Configuration configureDFSCluster(Configuration config, String dfsBaseDir, String bindIP,
      int namenodeRpcPort, int datanodePort, int datanodeIpcPort, int datanodeHttpPort) {

    LOG.info("HDFS force binding to ip: " + bindIP);
    config.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + bindIP + ":" + namenodeRpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, bindIP + ":" + datanodePort);
    config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, bindIP + ":" + datanodeIpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, bindIP + ":" + datanodeHttpPort);
    // When a datanode registers with the namenode, the Namenode do a hostname
    // check of the datanode which will fail on OpenShift due to reverse DNS
    // issues with the internal IP addresses. This config disables that check,
    // and will allow a datanode to connect regardless.
    config.setBoolean("dfs.namenode.datanode.registration.ip-hostname-check", false);
    config.set("hdfs.minidfs.basedir", dfsBaseDir);
    // allow current user to impersonate others
    String user = System.getProperty("user.name");
    config.set("hadoop.proxyuser." + user + ".groups", "*");
    config.set("hadoop.proxyuser." + user + ".hosts", "*");
    config.setBoolean("dfs.permissions", false);
    config.set("dfs.blocksize","16777216");
    return config;
  }
}
