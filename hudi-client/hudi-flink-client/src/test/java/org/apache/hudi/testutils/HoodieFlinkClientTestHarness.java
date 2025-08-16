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
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.TestFlinkHoodieBloomIndex;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * The test harness for resource initialization and cleanup.
 */
public class HoodieFlinkClientTestHarness extends HoodieCommonTestHarness {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkClientTestHarness.class);
  protected StorageConfiguration<Configuration> storageConf;
  protected FileSystem fs;
  protected HoodieFlinkEngineContext context;
  protected ExecutorService executorService;
  protected HoodieFlinkWriteClient writeClient;
  protected HoodieTableFileSystemView tableView;

  protected void initFileSystem() {
    storageConf = HoodieTestUtils.getDefaultStorageConf();
    initFileSystemWithConfiguration(storageConf);
    context = HoodieFlinkEngineContext.DEFAULT;
  }

  private void initFileSystemWithConfiguration(StorageConfiguration<Configuration> configuration) {
    checkState(basePath != null);
    fs = HadoopFSUtils.getFs(basePath, configuration);
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
    checkState(basePath != null);
    metaClient = HoodieTestUtils.init(storageConf, basePath, tableType);
  }

  protected List<HoodieRecord> tagLocation(
      HoodieIndex index, List<HoodieRecord> records, HoodieTable table) {
    return ((HoodieData<HoodieRecord>) index.tagLocation(HoodieListData.eager(records), context, table)).collectAsList();
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
   * Cleanups resource group for the subclasses of  {@link TestFlinkHoodieBloomIndex}.
   */
  public void cleanupResources() throws java.io.IOException {
    cleanupClients();
    cleanupFlinkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupExecutorService();
    System.gc();
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
   * Cleanups the executor service.
   */
  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  /**
   * Cleanups Flink contexts.
   */
  protected void cleanupFlinkContexts() {
    if (context != null) {
      LOG.info("Closing flink engine context used in previous test-case");
      context = null;
    }
  }
}
