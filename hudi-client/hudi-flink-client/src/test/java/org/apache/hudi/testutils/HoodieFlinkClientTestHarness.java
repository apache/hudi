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

package org.apache.hudi.testutils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HoodieFlinkClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  protected static final Logger LOG = LogManager.getLogger(HoodieFlinkClientTestHarness.class);
  private String testMethodName;
  protected transient Configuration hadoopConf = null;
  protected transient FileSystem fs;
  protected transient HoodieFlinkWriteClient writeClient;
  protected transient HoodieTableFileSystemView tableView;
  protected transient MiniClusterWithClientResource flinkCluster = null;

  // dfs
  protected String dfsBasePath;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  @BeforeEach
  public void setTestMethodName(TestInfo testInfo) {
    if (testInfo.getTestMethod().isPresent()) {
      testMethodName = testInfo.getTestMethod().get().getName();
    } else {
      testMethodName = "Unknown";
    }
  }

  protected void initFlinkMiniCluster() {
    flinkCluster = new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());
  }

  protected void initFileSystem() {
    hadoopConf = new Configuration();
    initFileSystemWithConfiguration(hadoopConf);
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }
    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    initMetaClient(getTableType());
  }

  protected void initMetaClient(HoodieTableType tableType) throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);
  }


  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
      fs = null;
    }
  }

  protected void cleanupFlinkMiniCluster() {
    if (flinkCluster != null) {
      flinkCluster.after();
      flinkCluster = null;
    }
  }

  public static class SimpleSink implements SinkFunction<HoodieRecord> {

    // must be static
    public static List<HoodieRecord> valuesList = new ArrayList<>();

    @Override
    public synchronized void invoke(HoodieRecord value) throws Exception {
      valuesList.add(value);
    }
  }
}
