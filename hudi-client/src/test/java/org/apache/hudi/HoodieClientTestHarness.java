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

package org.apache.hudi;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieClientTestHarness implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(HoodieClientTestHarness.class);

  protected transient JavaSparkContext jsc = null;
  protected transient SQLContext sqlContext;
  protected transient FileSystem fs;
  protected String basePath = null;
  protected TemporaryFolder folder = null;
  protected transient HoodieTestDataGenerator dataGen = null;
  protected transient ExecutorService executorService;

  //dfs
  protected String dfsBasePath;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  protected void initSparkContexts(String appName) {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(appName));
    jsc.setLogLevel("ERROR");

    //SQLContext stuff
    sqlContext = new SQLContext(jsc);
  }

  protected void initSparkContexts() {
    initSparkContexts("TestHoodieClient");
  }

  protected void cleanupSparkContexts() {
    if (sqlContext != null) {
      logger.info("Clearing sql context cache of spark-session used in previous test-case");
      sqlContext.clearCache();
      sqlContext = null;
    }

    if (jsc != null) {
      logger.info("Closing spark context used in previous test-case");
      jsc.close();
      jsc.stop();
      jsc = null;
    }
  }

  protected void initTempFolderAndPath() throws IOException {
    folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
  }

  protected void cleanupTempFolderAndPath() throws IOException {
    if (basePath != null) {
      new File(basePath).delete();
    }

    if (folder != null) {
      logger.info("Explicitly removing workspace used in previously run test-case");
      folder.delete();
    }
  }

  protected void initFileSystem() {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected void initFileSystemWithDefaultConfiguration() {
    fs = FSUtils.getFs(basePath, new Configuration());
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      logger.warn("Closing file-system instance used in previous test-run");
      fs.close();
    }
  }

  protected void initTableType() throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath, getTableType());
  }

  protected void cleanupTableType() {

  }

  protected void initTestDataGenerator() throws IOException {
    dataGen = new HoodieTestDataGenerator();
  }

  protected void cleanupTestDataGenerator() throws IOException {
    dataGen = null;
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }

  protected void initDFS() throws IOException {
    FileSystem.closeAll();
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
  }

  protected void cleanupDFS() throws IOException {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
  }

  protected void initExecutorServiceWithFixedThreadPool(int threadNum) {
    executorService = Executors.newFixedThreadPool(threadNum);
  }

  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

}
