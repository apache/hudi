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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

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
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieJavaClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieJavaClientTestHarness.class);

  private String testMethodName;
  protected transient Configuration hadoopConf = null;
  protected transient HoodieJavaEngineContext context = null;
  protected transient TestJavaTaskContextSupplier taskContextSupplier = null;
  protected transient FileSystem fs;
  protected transient ExecutorService executorService;
  protected transient HoodieTableFileSystemView tableView;
  protected transient HoodieJavaWriteClient writeClient;

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

  /**
   * Initializes resource group for the subclasses of {@link HoodieJavaClientTestHarness}.
   */
  public void initResources() throws IOException {
    initPath();
    hadoopConf = new Configuration();
    taskContextSupplier = new TestJavaTaskContextSupplier();
    context = new HoodieJavaEngineContext(hadoopConf, taskContextSupplier);
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
  }

  public class TestJavaTaskContextSupplier extends TaskContextSupplier {
    int partitionId = 0;
    int stageId = 0;
    long attemptId = 0;

    public void reset() {
      stageId += 1;
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> partitionId;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> stageId;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> attemptId;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }
  }

  /**
   * Cleanups resource group for the subclasses of {@link HoodieJavaClientTestHarness}.
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
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
      fs = null;
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
   * @throws IOException
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
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  public HoodieJavaWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new HoodieJavaWriteClient(context, cfg);
    return writeClient;
  }
}
