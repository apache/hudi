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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieClientTestHarness.class);
  
  private String testMethodName;
  protected transient JavaSparkContext jsc = null;
  protected transient Configuration hadoopConf = null;
  protected transient SQLContext sqlContext;
  protected transient FileSystem fs;
  protected transient HoodieTestDataGenerator dataGen = null;
  protected transient ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  private static AtomicInteger instantGen = new AtomicInteger(1);
  protected transient HoodieWriteClient writeClient;
  protected transient HoodieReadClient readClient;
  protected transient HoodieTableFileSystemView tableView;

  protected final SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();

  public String getNextInstant() {
    return String.format("%09d", instantGen.getAndIncrement());
  }

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
   * Initializes resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void initResources() throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
  }

  /**
   * Cleanups resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void cleanupResources() throws IOException {
    cleanupClients();
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupDFS();
    cleanupExecutorService();
    System.gc();
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext}) with the given application name.
   *
   * @param appName The specified application name.
   */
  protected void initSparkContexts(String appName) {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(appName + "#" + testMethodName));
    jsc.setLogLevel("ERROR");
    hadoopConf = jsc.hadoopConfiguration();

    // SQLContext stuff
    sqlContext = new SQLContext(jsc);
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext}) 
   * with a default name matching the name of the class.
   */
  protected void initSparkContexts() {
    initSparkContexts(this.getClass().getSimpleName());
  }

  /**
   * Cleanups Spark contexts ({@link JavaSparkContext} and {@link SQLContext}).
   */
  protected void cleanupSparkContexts() {
    if (sqlContext != null) {
      LOG.info("Clearing sql context cache of spark-session used in previous test-case");
      sqlContext.clearCache();
      sqlContext = null;
    }

    if (jsc != null) {
      LOG.info("Closing spark context used in previous test-case");
      jsc.close();
      jsc.stop();
      jsc = null;
    }
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    initFileSystemWithConfiguration(hadoopConf);
  }

  /**
   * Initializes file system with a default empty configuration.
   */
  protected void initFileSystemWithDefaultConfiguration() {
    initFileSystemWithConfiguration(new Configuration());
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
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, getTableType());
  }

  /**
   * Cleanups hoodie clients.
   */
  protected void cleanupClients() throws IOException {
    if (metaClient != null) {
      metaClient = null;
    }
    if (readClient != null) {
      readClient = null;
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
   * Initializes a test data generator which used to generate test datas.
   *
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  /**
   * Cleanups test data generator.
   *
   */
  protected void cleanupTestDataGenerator() {
    if (dataGen != null) {
      dataGen = null;
    }
  }

  /**
   * Initializes a distributed file system and base directory.
   *
   * @throws IOException
   */
  protected void initDFS() throws IOException {
    FileSystem.closeAll();
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
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
   * Initializes executor service with a fixed thread pool.
   *
   * @param threadNum specify the capacity of the fixed thread pool
   */
  protected void initExecutorServiceWithFixedThreadPool(int threadNum) {
    executorService = Executors.newFixedThreadPool(threadNum);
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

  public HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return getHoodieWriteClient(cfg, false);
  }

  public HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean rollbackInflightCommit) {
    return getHoodieWriteClient(cfg, rollbackInflightCommit, HoodieIndex.createIndex(cfg));
  }

  public HoodieReadClient getHoodieReadClient(String basePath) {
    readClient = new HoodieReadClient(jsc, basePath, SQLContext.getOrCreate(jsc.sc()));
    return readClient;
  }

  public HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean rollbackInflightCommit,
      HoodieIndex index) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new HoodieWriteClient(jsc, cfg, rollbackInflightCommit, index);
    return writeClient;
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration conf, String basePath) {
    metaClient = new HoodieTableMetaClient(conf, basePath);
    return metaClient;
  }

  public HoodieTableFileSystemView getHoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses) {
    if (tableView == null) {
      tableView =  new HoodieTableFileSystemView(metaClient, visibleActiveTimeline, fileStatuses);
    } else {
      tableView.init(metaClient, visibleActiveTimeline, fileStatuses);
    }
    return tableView;
  }
}
