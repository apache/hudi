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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;

/**
 * The common hoodie test harness to provide the basic infrastructure.
 */
public class HoodieCommonTestHarness {

  protected static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  protected String tableName;
  protected String basePath;
  protected URI baseUri;
  protected HoodieTestDataGenerator dataGen;
  protected HoodieTableMetaClient metaClient;
  @TempDir
  public java.nio.file.Path tempDir;

  protected void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Initializes basePath.
   */
  protected void initPath() {
    initPath("dataset");
  }

  /**
   * Initializes basePath with folder name.
   *
   * @param folderName Folder name.
   */
  protected void initPath(String folderName) {
    try {
      java.nio.file.Path basePath = tempDir.resolve(folderName);
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toAbsolutePath().toString();
      this.baseUri = basePath.toUri();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Initializes a test data generator which used to generate test data.
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  protected void initTestDataGenerator(String[] partitionPaths) {
    dataGen = new HoodieTestDataGenerator(partitionPaths);
  }

  /**
   * Cleanups test data generator.
   */
  protected void cleanupTestDataGenerator() {
    if (dataGen != null) {
      dataGen = null;
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
      initPath();
    }
    metaClient = HoodieTestUtils.init(basePath, getTableType());
  }

  protected void cleanMetaClient() {
    if (metaClient != null) {
      metaClient = null;
    }
  }

  protected void refreshFsView() throws IOException {
    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    return getFileSystemView(timeline, false);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline, boolean enableIncrementalTimelineSync) {
    return new HoodieTableFileSystemView(metaClient, timeline, enableIncrementalTimelineSync);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) throws IOException {
    return getFileSystemView(metaClient, metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants());
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline)
      throws IOException {
    return getFileSystemView(timeline, true);
  }

  protected SyncableFileSystemView getFileSystemViewWithUnCommittedSlices(HoodieTableMetaClient metaClient) {
    try {
      return new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline(),
          HoodieTestTable.of(metaClient).listAllBaseAndLogFiles()
      );
    } catch (IOException ioe) {
      throw new HoodieIOException("Error getting file system view", ioe);
    }
  }

  /**
   * Gets a default {@link HoodieTableType#COPY_ON_WRITE} table type. Sub-classes can override this method to specify a
   * new table type.
   *
   * @return an instance of Hoodie table type.
   */
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }
}
