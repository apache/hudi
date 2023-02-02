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
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieJavaClientTestHarness extends HoodieCommonTestHarness {

  private static final Logger LOG = LogManager.getLogger(HoodieJavaClientTestHarness.class);

  protected Configuration hadoopConf;
  protected HoodieJavaEngineContext context;
  protected TestJavaTaskContextSupplier taskContextSupplier;
  protected FileSystem fs;
  protected ExecutorService executorService;
  protected HoodieTableFileSystemView tableView;
  protected HoodieJavaWriteClient writeClient;

  @BeforeEach
  protected void initResources() throws IOException {
    basePath = tempDir.resolve("java_client_tests" + System.currentTimeMillis()).toUri().getPath();
    hadoopConf = new Configuration();
    taskContextSupplier = new TestJavaTaskContextSupplier();
    context = new HoodieJavaEngineContext(hadoopConf, taskContextSupplier);
    initFileSystem(basePath, hadoopConf);
    initTestDataGenerator();
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

  @AfterEach
  protected void cleanupResources() throws IOException {
    cleanupClients();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupExecutorService();
  }

  protected void initFileSystem(String basePath, Configuration hadoopConf) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, hadoopConf);
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
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
      fs = null;
    }
  }

  protected void initMetaClient() throws IOException {
    initMetaClient(getTableType());
  }

  protected void initMetaClient(HoodieTableType tableType) throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);
  }

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

  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  protected HoodieJavaWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new HoodieJavaWriteClient(context, cfg);
    return writeClient;
  }
}
