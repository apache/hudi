/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.testutils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.util.FlinkClientUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * The test harness for resource initialization and cleanup.
 */
@SuppressWarnings("checkstyle:Indentation")
public abstract class HoodieClientTestHarness extends HoodieCommonTestHarness
    implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieClientTestHarness.class);

  protected transient HoodieFlinkEngineContext context = HoodieFlinkEngineContext.DEFAULT;
  protected transient Configuration hadoopConf = FlinkClientUtil.getHadoopConf();
  protected transient FileSystem fs;
  protected transient ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  protected transient HoodieFlinkWriteClient<?> writeClient;
  protected transient HoodieTableFileSystemView tableView;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  @AfterAll
  public static void tearDownAll() throws IOException {
    FileSystem.closeAll();
  }

  /**
   * Initializes resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void initResources() throws IOException {
    initPath();
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
  }

  /**
   * Cleanups resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void cleanupResources() throws IOException {
    cleanupClients();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupDFS();
    cleanupExecutorService();
    System.gc();
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    initFileSystemWithConfiguration(hadoopConf);
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
      fs = null;
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified
   * by {@code getTableType()}.
   *
   * @throws IOException IOException
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
   * Cleanups hoodie clients.
   */
  protected void cleanupClients() {
    if (metaClient != null) {
      metaClient = null;
    }
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
    }
    if (tableView != null) {
      tableView.close();
      tableView = null;
    }
  }

  /**
   * Cleanups the distributed file system.
   *
   * @throws IOException IOException
   */
  protected void cleanupDFS() throws IOException {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
      hdfsTestService = null;
      dfsCluster = null;
      dfs = null;
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
  }

  /**
   * Cleanups the executor service.
   */
  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which
      // is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places)
      // calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  public HoodieFlinkWriteClient<?> getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new HoodieFlinkWriteClient<>(context, cfg);
    return writeClient;
  }
}
