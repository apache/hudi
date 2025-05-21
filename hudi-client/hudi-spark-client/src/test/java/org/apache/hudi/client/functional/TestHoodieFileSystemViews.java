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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests diff file system views.
 */
public class TestHoodieFileSystemViews extends HoodieClientTestBase {

  private HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;

  protected HoodieTableType getTableType() {
    return tableType;
  }

  public static List<Arguments> tableTypeMetadataFSVTypeArgs() {
    List<Arguments> testCases = new ArrayList<>();
    for (HoodieTableType tableType : HoodieTableType.values()) {
      for (boolean enableMdt : Arrays.asList(true, false)) {
        for (FileSystemViewStorageType viewStorageType : Arrays.asList(FileSystemViewStorageType.MEMORY, FileSystemViewStorageType.SPILLABLE_DISK)) {
          for (int writerVersion : Arrays.asList(6, 8)) {
            testCases.add(Arguments.of(tableType, enableMdt, viewStorageType, writerVersion));
          }
        }
      }
    }
    return testCases;
  }

  @ParameterizedTest
  @MethodSource("tableTypeMetadataFSVTypeArgs")
  public void testFileSystemViewConsistency(HoodieTableType tableType, boolean enableMdt, FileSystemViewStorageType storageType, int writeVersion) throws IOException {
    this.tableType = tableType;
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      configBuilder.withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(true)
          .withMaxNumDeltaCommitsBeforeCompaction(3).build());
    }
    configBuilder
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(storageType).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMdt).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(true).withInlineClusteringNumCommits(5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(4).build())
        // set aggressive values so that within 20 batches few iterations of cleaner and archival will kick in
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(6, 8).build())
        .withWriteTableVersion(writeVersion);
    HoodieWriteConfig config = configBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      insertRecords(client, "001", 100, WriteOperationType.BULK_INSERT);
      insertRecords(client, "002", 100, WriteOperationType.INSERT);
      metaClient = HoodieTableMetaClient.reload(metaClient);

      // base line file system view is in-memory for any combination.
      HoodieTableFileSystemView expectedFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(context, metaClient,
          HoodieMetadataConfig.newBuilder().enable(false).build());

      // to be compared against.
      // if no mdt enabled, compare w/ spillable.
      // if mdt is enabled, depending on storage type, either it will be mdt fsv or spillable fsv w/ mdt enabled.
      FileSystemViewStorageConfig viewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(config.getProps())
          .withStorageType(storageType).build();
      HoodieTableFileSystemView actualFileSystemView = (HoodieTableFileSystemView) FileSystemViewManager
          .createViewManager(context, config.getMetadataConfig(), viewStorageConfig, config.getCommonConfig(),
              (SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata>) v1 ->
                  HoodieTableMetadata.create(context, metaClient.getStorage(), config.getMetadataConfig(), config.getBasePath()))
          .getFileSystemView(basePath);

      assertFileSystemViews(config, enableMdt, storageType);
      for (int i = 3; i < 10; i++) {
        String commitTime = String.format("%10d", i);
        upsertRecords(client, commitTime, 50);
      }
      expectedFileSystemView.sync();
      actualFileSystemView.sync();
      assertForFSVEquality(expectedFileSystemView, actualFileSystemView, enableMdt);
      for (int i = 10; i < 20; i++) {
        String commitTime = String.format("%10d", i);
        upsertRecords(client, commitTime, 50);
      }

      // mimic failed write for last completed operation and retry few more operations.
      HoodieInstant lastInstant = metaClient.reloadActiveTimeline().lastInstant().get();
      StoragePath instantPath = HoodieTestUtils
          .getCompleteInstantPath(metaClient.getStorage(),
              metaClient.getTimelinePath(),
              lastInstant.requestedTime(), lastInstant.getAction());
      metaClient.getStorage().deleteFile(instantPath);

      expectedFileSystemView.sync();
      actualFileSystemView.sync();
      assertForFSVEquality(expectedFileSystemView, actualFileSystemView, enableMdt);

      // add few more updates
      for (int i = 21; i < 23; i++) {
        String commitTime = String.format("%10d", i);
        upsertRecords(client, commitTime, 50);
      }
      actualFileSystemView.close();
      expectedFileSystemView.close();
    }
    assertFileSystemViews(config, enableMdt, storageType);
  }

  private void assertFileSystemViews(HoodieWriteConfig writeConfig, boolean enableMdt, FileSystemViewStorageType baseStorageType) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // base line file system view is in-memory for any combination.
    HoodieTableFileSystemView expectedFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(context, metaClient,
        HoodieMetadataConfig.newBuilder().enable(false).build());

    // to be compared against.
    // if no mdt enabled, compare w/ spillable.
    // if mdt is enabled, depending on storage type, either it will be mdt fsv or spillable fsv w/ mdt enabled.
    FileSystemViewStorageConfig viewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(writeConfig.getProps())
        .withStorageType(baseStorageType).build();
    HoodieTableFileSystemView actualFileSystemView = (HoodieTableFileSystemView) FileSystemViewManager
        .createViewManager(context, writeConfig.getMetadataConfig(), viewStorageConfig, writeConfig.getCommonConfig(),
            (SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata>) v1 ->
                HoodieTableMetadata.create(context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath()))
        .getFileSystemView(basePath);
    try {
      assertForFSVEquality(expectedFileSystemView, actualFileSystemView, enableMdt);
    } finally {
      expectedFileSystemView.close();
      actualFileSystemView.close();
    }
  }

  private void assertForFSVEquality(HoodieTableFileSystemView fsv1, HoodieTableFileSystemView fsv2, boolean enableMdt) {
    List<String> allPartitionNames = Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH);
    fsv1.loadPartitions(allPartitionNames);
    if (enableMdt) {
      // assumes 2nd one is metadata enabled. loadAllPartitions() cannot be invoked for non-mdt fsv.
      fsv2.loadAllPartitions();
    } else {
      fsv2.loadPartitions(allPartitionNames);
    }
    List<StoragePath> allPartitions1 = fsv1.getPartitionPaths();
    List<StoragePath> allPartitions2 = fsv2.getPartitionPaths();
    Collections.sort(allPartitions1);
    Collections.sort(allPartitions2);
    assertEquals(allPartitions1, allPartitions2);

    allPartitionNames.forEach(path -> {
      List<HoodieBaseFile> latestBaseFiles1 = fsv1.getLatestBaseFiles(path).collect(Collectors.toList());
      List<HoodieBaseFile> latestBaseFiles2 = fsv2.getLatestBaseFiles(path).collect(Collectors.toList());
      assertBaseFileListEquality(latestBaseFiles1, latestBaseFiles2);

      List<FileSlice> fileSlices1 = fsv1.getLatestFileSlices(path).collect(Collectors.toList());
      List<FileSlice> fileSlices2 = fsv2.getLatestFileSlices(path).collect(Collectors.toList());
      assertFileSliceListEquality(fileSlices1, fileSlices2);
    });
  }

  private void assertBaseFileListEquality(List<HoodieBaseFile> baseFileList1, List<HoodieBaseFile> baseFileList2) {
    assertEquals(baseFileList1.size(), baseFileList2.size());
    Map<String, HoodieBaseFile> fileNameToBaseFileMap1 = new HashMap<>();
    baseFileList1.forEach(entry -> {
      fileNameToBaseFileMap1.put(entry.getFileName(), entry);
    });
    Map<String, HoodieBaseFile> fileNameToBaseFileMap2 = new HashMap<>();
    baseFileList2.forEach(entry -> {
      fileNameToBaseFileMap2.put(entry.getFileName(), entry);
    });
    fileNameToBaseFileMap1.entrySet().forEach((kv) -> {
      assertTrue(fileNameToBaseFileMap2.containsKey(kv.getKey()));
      assertBaseFileEquality(kv.getValue(), fileNameToBaseFileMap2.get(kv.getKey()));
    });
  }

  public void assertBaseFileEquality(HoodieBaseFile baseFile1, HoodieBaseFile baseFile2) {
    assertEquals(baseFile1.getFileName(), baseFile2.getFileName());
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertEquals(baseFile1.getFileLen(), baseFile2.getFileLen());
    assertEquals(baseFile1.getFileSize(), baseFile2.getFileSize());
  }

  private void assertFileSliceListEquality(List<FileSlice> fileSlices1, List<FileSlice> fileSlices2) {
    assertEquals(fileSlices1.size(), fileSlices1.size());
    Map<Pair<String, String>, FileSlice> fileNameToFileSliceMap1 = new HashMap<>();
    fileSlices1.forEach(entry -> {
      fileNameToFileSliceMap1.put(Pair.of(entry.getFileId(), entry.getBaseInstantTime()), entry);
    });
    Map<Pair<String, String>, FileSlice> fileNameToFileSliceMap2 = new HashMap<>();
    fileSlices2.forEach(entry -> {
      fileNameToFileSliceMap2.put(Pair.of(entry.getFileId(), entry.getBaseInstantTime()), entry);
    });
    fileNameToFileSliceMap1.entrySet().forEach((kv) -> {
      assertTrue(fileNameToFileSliceMap2.containsKey(kv.getKey()));
      assertFileSliceEquality(kv.getValue(), fileNameToFileSliceMap2.get(kv.getKey()));
    });
  }

  private void assertFileSliceEquality(FileSlice fileSlice1, FileSlice fileSlice2) {
    assertEquals(fileSlice1.getBaseFile().isPresent(), fileSlice2.getBaseFile().isPresent());
    if (fileSlice1.getBaseFile().isPresent()) {
      assertBaseFileEquality(fileSlice1.getBaseFile().get(), fileSlice2.getBaseFile().get());
    }
    List<HoodieLogFile> logFiles1 = fileSlice1.getLogFiles().collect(Collectors.toList());
    List<HoodieLogFile> logFiles2 = fileSlice2.getLogFiles().collect(Collectors.toList());
    assertEquals(logFiles1.size(), logFiles2.size());
    int counter = 0;
    for (HoodieLogFile logFile1 : logFiles1) {
      HoodieLogFile logFile2 = logFiles2.get(counter++);
      assertLogFileEquality(logFile1, logFile2);
    }
  }

  private void assertLogFileEquality(HoodieLogFile logFile1, HoodieLogFile logFile2) {
    assertEquals(logFile1.getFileName(), logFile2.getFileName());
    assertEquals(logFile1.getFileId(), logFile2.getFileId());
    assertEquals(logFile1.getLogVersion(), logFile2.getLogVersion());
    assertEquals(logFile1.getFileSize(), logFile2.getFileSize());
    assertEquals(logFile1.getDeltaCommitTime(), logFile2.getDeltaCommitTime());
    assertEquals(logFile1.getFileExtension(), logFile2.getFileExtension());
    assertEquals(logFile1.getLogWriteToken(), logFile2.getLogWriteToken());
  }

  private void insertRecords(SparkRDDWriteClient client, String commitTime, int numRecords, WriteOperationType operationType) {
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime, numRecords);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 2);
    List<WriteStatus> statuses = operationType == WriteOperationType.BULK_INSERT ? client.bulkInsert(insertRecordsRDD1, commitTime, Option.empty()).collect() :
        client.insert(insertRecordsRDD1, commitTime).collect();
    assertNoWriteErrors(statuses);
  }

  private void upsertRecords(SparkRDDWriteClient client, String commitTime, int numRecords) {
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    List<HoodieRecord> updates = dataGen.generateUniqueUpdates(commitTime, numRecords);
    JavaRDD<HoodieRecord> updatesRdd = jsc.parallelize(updates, 2);
    List<WriteStatus> statuses = client.upsert(updatesRdd, commitTime).collect();
    assertNoWriteErrors(statuses);
  }
}
