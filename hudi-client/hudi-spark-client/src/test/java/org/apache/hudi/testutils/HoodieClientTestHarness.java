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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.WorkloadStat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieClientTestHarness.class);
  
  private String testMethodName;
  protected transient JavaSparkContext jsc = null;
  protected transient HoodieSparkEngineContext context = null;
  protected transient Configuration hadoopConf = null;
  protected transient SQLContext sqlContext;
  protected transient FileSystem fs;
  protected transient ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  protected transient SparkRDDWriteClient writeClient;
  protected transient HoodieReadClient readClient;
  protected transient HoodieTableFileSystemView tableView;

  protected final SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();

  // dfs
  protected String dfsBasePath;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  @AfterAll
  public static void tearDownAll() throws IOException {
    FileSystem.closeAll();
  }

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
    context = new HoodieSparkEngineContext(jsc);
    hadoopConf = context.getHadoopConf().get();
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

    if (context != null) {
      LOG.info("Closing spark engine context used in previous test-case");
      context = null;
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
    initMetaClient(getTableType());
  }

  protected void initMetaClient(HoodieTableType tableType) throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);
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
   * Initializes a distributed file system and base directory.
   *
   * @throws IOException
   */
  protected void initDFS() throws IOException {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    this.basePath = dfsBasePath;
    this.hadoopConf = dfs.getConf();
    dfs.mkdirs(new Path(dfsBasePath));
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initDFSMetaClient() throws IOException {
    if (dfsBasePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }
    metaClient = HoodieTestUtils.init(dfs.getConf(), dfsBasePath, getTableType());
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

  public HoodieReadClient getHoodieReadClient(String basePath) {
    readClient = new HoodieReadClient(context, basePath, SQLContext.getOrCreate(jsc.sc()));
    return readClient;
  }

  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new SparkRDDWriteClient(context, cfg);
    return writeClient;
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration conf, String basePath) {
    metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
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

  protected Pair<HashMap<String, WorkloadStat>, WorkloadStat> buildProfile(JavaRDD<HoodieRecord> inputRecordsRDD) {
    HashMap<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
    WorkloadStat globalStat = new WorkloadStat();

    // group the records by partitionPath + currentLocation combination, count the number of
    // records in each partition
    Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = inputRecordsRDD
        .mapToPair(record -> new Tuple2<>(
            new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .countByKey();

    // count the number of both inserts and updates in each partition, update the counts to workLoadStats
    for (Map.Entry<Tuple2<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey()._2();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
    return Pair.of(partitionPathStatMap, globalStat);
  }
}
