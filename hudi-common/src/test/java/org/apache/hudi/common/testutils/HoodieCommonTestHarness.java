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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewExpectedState;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The common hoodie test harness to provide the basic infrastructure.
 */
public class HoodieCommonTestHarness {

  protected String tableName = null;
  protected String basePath = null;
  protected transient HoodieTestDataGenerator dataGen = null;
  protected transient HoodieTableMetaClient metaClient;
  @TempDir
  public java.nio.file.Path tempDir;

  protected void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Initializes basePath.
   */
  protected void initPath() {
    try {
      java.nio.file.Path basePath = tempDir.resolve("dataset");
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Initializes a test data generator which used to generate test datas.
   *
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  protected void initTestDataGenerator(String[] partitionPaths) {
    dataGen = new HoodieTestDataGenerator(partitionPaths);
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
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    basePath = metaClient.getBasePath();
  }

  protected void refreshFsView() throws IOException {
    metaClient = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
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
   * Used to verify fils system view on various file systems.
   */
  protected void verifyFileSystemView(String partitionPath, FileSystemViewExpectedState expectedState,
                                    SyncableFileSystemView tableFileSystemView) {
    tableFileSystemView.sync();
    // Verify base files
    assertEquals(expectedState.baseFilesCurrentlyPresent,tableFileSystemView.getLatestBaseFiles(partitionPath)
        .map(HoodieBaseFile::getFileName)
        .collect(Collectors.toSet()));

    // Verify log files
    assertEquals(expectedState.logFilesCurrentlyPresent, tableFileSystemView.getAllFileSlices(partitionPath)
        .flatMap(FileSlice::getLogFiles)
        .map(logFile -> logFile.getPath().getName())
        .collect(Collectors.toSet()));
    // Verify file groups part of pending compaction operations
    assertEquals(expectedState.pendingCompactionFgIdsCurrentlyPresent, tableFileSystemView.getPendingCompactionOperations()
        .map(pair -> pair.getValue().getFileGroupId().getFileId())
        .collect(Collectors.toSet()));

    // Verify file groups part of pending log compaction operations
    assertEquals(expectedState.pendingLogCompactionFgIdsCurrentlyPresent, tableFileSystemView.getPendingLogCompactionOperations()
        .map(pair -> pair.getValue().getFileGroupId().getFileId())
        .collect(Collectors.toSet()));
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
