/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HoodieStashPartitionsTool#validateFileGroupsForStash}.
 * Uses {@link HoodieTestTable} to create controlled file layouts without running
 * actual Spark writes.
 */
public class TestStashPartitionValidation {

  @TempDir
  private Path tempDir;

  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;
  private String basePath;

  private static final String PARTITION = "2023/01/01";
  private static final String FILE_ID_1 = "file-group-1";
  private static final String FILE_ID_2 = "file-group-2";

  @BeforeEach
  public void init() throws Exception {
    basePath = tempDir.toAbsolutePath().toUri().toString();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    storage = metaClient.getStorage();
  }

  /**
   * Given: A partition with one file group, one file slice, base file only.
   * When: validateFileGroupsForStash is called.
   * Then: Validation passes.
   */
  @Test
  public void testValidationPassesForCleanPartition() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001")
        .withBaseFilesInPartition(PARTITION, FILE_ID_1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, PARTITION)) {
      assertDoesNotThrow(() ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Collections.singletonList(PARTITION)));
    }
  }

  /**
   * Given: A partition with multiple file groups, each having one file slice with base file only.
   * When: validateFileGroupsForStash is called.
   * Then: Validation passes.
   */
  @Test
  public void testValidationPassesForMultipleCleanFileGroups() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001")
        .withBaseFilesInPartition(PARTITION, FILE_ID_1, FILE_ID_2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, PARTITION)) {
      assertDoesNotThrow(() ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Collections.singletonList(PARTITION)));
    }
  }

  /**
   * Given: A partition with a file group that has two file slices (two commits).
   * When: validateFileGroupsForStash is called.
   * Then: Validation fails — more than one file slice.
   */
  @Test
  public void testValidationFailsForMultipleFileSlices() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001")
        .withBaseFilesInPartition(PARTITION, FILE_ID_1);
    testTable.addCommit("002")
        .withBaseFilesInPartition(PARTITION, FILE_ID_1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, PARTITION)) {
      HoodieValidationException ex = assertThrows(HoodieValidationException.class, () ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Collections.singletonList(PARTITION)));
      assertTrue(ex.getMessage().contains("file slice(s), expected exactly 1"));
    }
  }

  /**
   * Given: A partition with a file group that has a base file and log files.
   * When: validateFileGroupsForStash is called.
   * Then: Validation fails — log files present.
   */
  @Test
  public void testValidationFailsForLogFiles() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001")
        .withBaseFilesInPartition(PARTITION, FILE_ID_1);
    testTable.addDeltaCommit("002")
        .withLogFile(PARTITION, FILE_ID_1, 0);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, PARTITION)) {
      HoodieValidationException ex = assertThrows(HoodieValidationException.class, () ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Collections.singletonList(PARTITION)));
      assertTrue(ex.getMessage().contains("log files") || ex.getMessage().contains("file slice(s), expected exactly 1"));
    }
  }

  /**
   * Given: Two partitions — one clean, one with multiple file slices.
   * When: validateFileGroupsForStash is called for both.
   * Then: Validation fails for the dirty partition.
   */
  @Test
  public void testValidationFailsIfAnyPartitionIsDirty() throws Exception {
    String cleanPartition = "2023/01/01";
    String dirtyPartition = "2023/01/02";

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001")
        .withBaseFilesInPartition(cleanPartition, FILE_ID_1)
        .getLeft()
        .withBaseFilesInPartition(dirtyPartition, FILE_ID_2);
    testTable.addCommit("002")
        .withBaseFilesInPartition(dirtyPartition, FILE_ID_2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, cleanPartition, dirtyPartition)) {
      HoodieValidationException ex = assertThrows(HoodieValidationException.class, () ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Arrays.asList(cleanPartition, dirtyPartition)));
      assertTrue(ex.getMessage().contains(dirtyPartition));
    }
  }

  /**
   * Given: An empty partition (no file groups).
   * When: validateFileGroupsForStash is called.
   * Then: Validation passes — nothing to check.
   */
  @Test
  public void testValidationPassesForEmptyPartition() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = buildFsView(metaClient, PARTITION)) {
      assertDoesNotThrow(() ->
          HoodieStashPartitionsTool.validateFileGroupsForStash(fsView, Collections.singletonList(PARTITION)));
    }
  }

  // ---- Helper methods ----

  /**
   * Builds a {@link HoodieTableFileSystemView} by listing files from the given partitions.
   */
  private HoodieTableFileSystemView buildFsView(HoodieTableMetaClient metaClient, String... partitions) throws IOException {
    List<StoragePathInfo> allFiles = new ArrayList<>();
    StoragePath base = metaClient.getBasePath();
    HoodieStorage stor = metaClient.getStorage();
    for (String partition : partitions) {
      StoragePath partitionPath = new StoragePath(base, partition);
      if (stor.exists(partitionPath)) {
        allFiles.addAll(stor.listDirectEntries(partitionPath));
      }
    }
    return new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), allFiles);
  }
}
