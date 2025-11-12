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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.MarkerBasedCommitMetadataResolver;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.HoodieTestCommitGenerator.getBaseFilename;
import static org.apache.hudi.table.action.rollback.RollbackHelperV1.getPathInfoUnderPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCommitMetadataResolver extends HoodieCommonTestHarness {

  private final HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
  private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
  private final HoodieEngineContext context = mock(HoodieEngineContext.class);
  private final HoodieTable table = mock(HoodieTable.class);

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testReconcileMetadataForMissingFiles() throws IOException {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setTableVersion(HoodieTableVersion.SIX);

    // Mock table type as MERGE_ON_READ and action as DELTA_COMMIT
    when(table.getMetaClient()).thenReturn(metaClient);
    Mockito.when(table.getConfig()).thenReturn(writeConfig);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(new StoragePath(basePath));
    when(metaClient.getMarkerFolderPath(any())).thenReturn(basePath + ".hoodie/.temp");
    when(table.getContext()).thenReturn(context);
    when(writeConfig.getViewStorageConfig()).thenReturn(FileSystemViewStorageConfig.newBuilder().build());
    when(writeConfig.getMarkersType()).thenReturn(MarkerType.DIRECT);
    when(writeConfig.getBasePath()).thenReturn(basePath);
    String instantTime = WriteClientTestUtils.createNewInstantTime();

    // Setup dummy commit metadata
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";
    String file1P0C0 = UUID.randomUUID().toString();
    String file1P1C0 = UUID.randomUUID().toString();
    Map<String, List<String>> part1ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
      {
        put(p0, CollectionUtils.createImmutableList(file1P0C0));
        put(p1, CollectionUtils.createImmutableList(file1P1C0));
      }
    });
    Pair<HoodieCommitMetadata, List<String>> commitMetadataWithLogFiles =
        generateCommitMetadata(instantTime, part1ToFileId, metaClient, 1, 2);

    // Assume these are paths to log files that were supposed to be in commitMetadata but are missing
    Set<String> missingLogFiles = new HashSet<>(Arrays.asList("path/to/log1", "path/to/log2"));
    when(table.getFileSystemView()).thenReturn(mock(org.apache.hudi.common.table.view.HoodieTableFileSystemView.class));
    missingLogFiles.addAll(commitMetadataWithLogFiles.getRight());
    when(table.getFileSystemView()).thenReturn(mock(org.apache.hudi.common.table.view.HoodieTableFileSystemView.class));

    // Mock filesystem and file status
    FileSystem fs = mock(FileSystem.class);
    when(table.getStorageConf()).thenReturn(storageConf);
    when(fs.exists(any())).thenReturn(true);

    // Call the method under test
    HoodieCommitMetadata reconciledMetadata = new MarkerBasedCommitMetadataResolver()
        .reconcileMetadataForMissingFiles(
            writeConfig, context, table, instantTime, commitMetadataWithLogFiles.getLeft());

    // Assertions to verify if the missing files are added
    assertFalse(reconciledMetadata.getPartitionToWriteStats().isEmpty(), "CommitMetadata should not be empty after reconciliation");
    assertEquals(2, reconciledMetadata.getPartitionToWriteStats().size());
    assertTrue(reconciledMetadata.getPartitionToWriteStats().containsKey(p0), "Partition " + p0 + " should be present in the commit metadata");
    assertTrue(reconciledMetadata.getPartitionToWriteStats().containsKey(p1), "Partition " + p1 + " should be present in the commit metadata");
    assertEquals(1, reconciledMetadata.getPartitionToWriteStats().get(p0).size(), "There should be 1 write stats for partition " + p0);
    assertEquals(1, reconciledMetadata.getPartitionToWriteStats().get(p1).size(), "There should be 1 write stats for partition " + p1);
    assertEquals(file1P0C0, reconciledMetadata.getPartitionToWriteStats().get(p0).get(0).getFileId(), "FileId for partition " + p0 + " should be " + file1P0C0);
    assertEquals(file1P1C0, reconciledMetadata.getPartitionToWriteStats().get(p1).get(0).getFileId(), "FileId for partition " + p1 + " should be " + file1P1C0);
  }

  private static Pair<HoodieCommitMetadata, List<String>> generateCommitMetadata(String instantTime, Map<String, List<String>> partitionToFilePaths,
                                                                                 HoodieTableMetaClient metaClient, int... versions) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
    List<String> allLogFiles = new ArrayList<>();
    partitionToFilePaths.forEach((partitionPath, fileList) -> fileList.forEach(f -> {
      HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
      List<String> logFiles = new ArrayList<>();
      for (int version : versions) {
        try {
          logFiles.add(FileCreateUtils.createLogFile(metaClient, partitionPath, instantTime, f, version));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      allLogFiles.addAll(logFiles);
      writeStat.setPartitionPath(partitionPath);
      writeStat.setPath(partitionPath + "/" + getBaseFilename(instantTime, f));
      writeStat.setFileId(f);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      writeStat.setTotalLogBlocks(logFiles.size());
      writeStat.setLogFiles(logFiles);
      metadata.addWriteStat(partitionPath, writeStat);
    }));
    return Pair.of(metadata, allLogFiles);
  }

  @Test
  public void testGetPathInfoUnderPartition(@TempDir Path tempDir) throws IOException {
    StoragePath hoodieTempDir = getHoodieTempDir();
    HoodieStorage storage = super.metaClient.getStorage();
    prepareTestDirectory(storage, hoodieTempDir);
    List<Option<StoragePathInfo>> fileStatusList = getPathInfoUnderPartition(
        storage,
        new StoragePath(baseUri.toString(), ".hoodie/.temp"),
        new HashSet<>(Collections.singletonList("file3.txt")),
        false);
    assertEquals(1, fileStatusList.size());

    // With ignoreMissingFiles set to true, error is not thrown in case some files are missing
    fileStatusList = getPathInfoUnderPartition(
        storage,
        new StoragePath(baseUri.toString(), ".hoodie/.temp"),
        new HashSet<>(Arrays.asList("file3.txt", "file4.txt")),
        true);
    assertEquals(2, fileStatusList.size());
    assertTrue(fileStatusList.get(0).isPresent());
    // For missing files, option of storage path info is empty
    assertFalse(fileStatusList.get(1).isPresent());

    assertThrows(HoodieIOException.class, () -> getPathInfoUnderPartition(
        storage,
        new StoragePath(baseUri.toString(), ".hoodie/.temp"),
        new HashSet<>(Collections.singletonList("file4.txt")),
        false));
  }

  private void prepareTestDirectory(HoodieStorage storage, StoragePath rootDir) throws IOException {
    // Directory structure
    // .hoodie/.temp/
    //  - subdir1
    //    - file1.txt
    //  - subdir2
    //    - file2.txt
    //  - file3
    String subDir1 = rootDir + "/subdir1";
    String file1 = subDir1 + "/file1.txt";
    String subDir2 = rootDir + "/subdir2";
    String file2 = subDir2 + "/file2.txt";
    String file3 = rootDir + "/file3.txt";
    String[] dirs = new String[] {rootDir.toString(), subDir1, subDir2};
    String[] files = new String[] {file1, file2, file3};
    // clean up first
    cleanUpTestDirectory(storage, rootDir);
    for (String dir : dirs) {
      storage.createDirectory(new StoragePath(dir));
    }
    for (String filename : files) {
      storage.create(new StoragePath(filename));
    }
  }

  private StoragePath getHoodieTempDir() {
    return new StoragePath(baseUri.toString(), ".hoodie/.temp");
  }

  private void cleanUpTestDirectory(HoodieStorage storage, StoragePath rootDir) throws IOException {
    storage.deleteDirectory(rootDir);
  }
}
