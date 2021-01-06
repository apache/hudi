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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieFlinkClientTestHarness extends HoodieCommonTestHarness implements java.io.Serializable {

  private static final org.apache.log4j.Logger LOG = org.apache.log4j.LogManager.getLogger(org.apache.hudi.testutils.HoodieFlinkClientTestHarness.class);

  private String testMethodName;
  protected transient org.apache.hudi.client.common.HoodieFlinkEngineContext context = null;
  protected transient org.apache.hadoop.conf.Configuration hadoopConf = null;
  protected transient org.apache.hadoop.fs.FileSystem fs;
  protected transient org.apache.hudi.common.testutils.HoodieTestDataGenerator dataGen = null;
  protected transient java.util.concurrent.ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  protected transient HoodieFlinkWriteClient writeClient;
  protected transient org.apache.hudi.common.table.view.HoodieTableFileSystemView tableView;

  protected final FlinkTaskContextSupplier supplier = new FlinkTaskContextSupplier(null);

  // dfs
  protected String dfsBasePath;
  protected transient org.apache.hudi.common.testutils.minicluster.HdfsTestService hdfsTestService;
  protected transient org.apache.hadoop.hdfs.MiniDFSCluster dfsCluster;
  protected transient org.apache.hadoop.hdfs.DistributedFileSystem dfs;

  @org.junit.jupiter.api.BeforeEach
  public void setTestMethodName(org.junit.jupiter.api.TestInfo testInfo) {
    if (testInfo.getTestMethod().isPresent()) {
      testMethodName = testInfo.getTestMethod().get().getName();
    } else {
      testMethodName = "Unknown";
    }
  }

  /**
   * Cleanups resource group for the subclasses of .
   */
  public void cleanupResources() throws java.io.IOException {
    cleanupClients();
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupDFS();
    cleanupExecutorService();
    System.gc();
  }

  /**
   * Initializes the Flink contexts with the given application name.
   *
   */
  protected void initFlinkContexts() {
    context = new org.apache.hudi.client.common.HoodieFlinkEngineContext(supplier);
    hadoopConf = context.getHadoopConf().get();
  }

  /**
   * Cleanups Flink contexts.
   */
  protected void cleanupSparkContexts() {
    if (context != null) {
      LOG.info("Closing spark engine context used in previous test-case");
      context = null;
    }
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    /*if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }*/

    initFileSystemWithConfiguration(hadoopConf);
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws java.io.IOException {
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
  protected void initMetaClient() throws java.io.IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    metaClient = org.apache.hudi.common.testutils.HoodieTestUtils.init(context.getHadoopConf().get(), basePath, getTableType());
  }

  /**
   * Cleanups hoodie clients.
   */
  protected void cleanupClients() throws java.io.IOException {
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
   * Cleanups test data generator.
   *
   */
  protected void cleanupTestDataGenerator() {
    if (dataGen != null) {
      dataGen = null;
    }
  }

  /**
   * Cleanups the distributed file system.
   *
   * @throws IOException
   */
  protected void cleanupDFS() throws java.io.IOException {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
      hdfsTestService = null;
      dfsCluster = null;
      dfs = null;
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    org.apache.hadoop.fs.FileSystem.closeAll();
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

  private void initFileSystemWithConfiguration(org.apache.hadoop.conf.Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = org.apache.hudi.common.fs.FSUtils.getFs(basePath, configuration);
    if (fs instanceof org.apache.hadoop.fs.LocalFileSystem) {
      org.apache.hadoop.fs.LocalFileSystem lfs = (org.apache.hadoop.fs.LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected org.apache.hudi.common.util.collection.Pair<java.util.HashMap<String, org.apache.hudi.table.WorkloadStat>, org.apache.hudi.table.WorkloadStat> buildProfile(List<org.apache.hudi.common.model.HoodieRecord> inputRecords) {
    java.util.HashMap<String, org.apache.hudi.table.WorkloadStat> partitionPathStatMap = new java.util.HashMap<>();
    org.apache.hudi.table.WorkloadStat globalStat = new org.apache.hudi.table.WorkloadStat();

    // group the records by partitionPath + currentLocation combination, count the number of
    // records in each partition
    Map<scala.Tuple2<String, org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieRecordLocation>>, Long> partitionLocationCounts = new java.util.HashMap<>();
    inputRecords.forEach( record -> {
      scala.Tuple2<String, org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieRecordLocation>> tuple2 =
              new scala.Tuple2<>(record.getPartitionPath(), org.apache.hudi.common.util.Option.ofNullable(record.getCurrentLocation()));
      long count = partitionLocationCounts.getOrDefault(tuple2, 0L) + 1L;
      partitionLocationCounts.put(tuple2, count);
    });

    // count the number of both inserts and updates in each partition, update the counts to workLoadStats
    for (java.util.Map.Entry<scala.Tuple2<String, org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieRecordLocation> locOption = e.getKey()._2();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new org.apache.hudi.table.WorkloadStat());
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
    return org.apache.hudi.common.util.collection.Pair.of(partitionPathStatMap, globalStat);
  }
}
