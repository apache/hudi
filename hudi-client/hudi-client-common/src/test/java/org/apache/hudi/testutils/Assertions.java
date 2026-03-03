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

package org.apache.hudi.testutils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.CheckedFunction;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.keygen.KeyGenUtils.getComplexKeygenErrorMessage;
import static org.apache.hudi.utils.HoodieWriterClientTestHarness.getFileUtilsInstance;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Commonly used assertion functions.
 */
public class Assertions {

  /**
   * Assert no failures in writing hoodie files.
   */
  public static void assertNoWriteErrors(List<WriteStatus> statuses) {
    assertAll(statuses.stream().map(status -> () ->
        assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId())));
  }

  public static void assertRecordCounts(int recordCount, HoodieStorage storage, String basePath, List<WriteStatus> statuses, FileFormatUtils fileUtils) {
    assertEquals(recordCount,
        statuses.stream().mapToInt(status -> fileUtils.readRowKeys(storage, new StoragePath(basePath, status.getStat().getPath())).size()).sum(),
        "Should contain " + recordCount + " records");
  }

  public static void assertRecordCommits(HoodieStorage storage, List<String> expectedCommitTimes, FileFormatUtils fileUtils, String basePath,
                                         String filePath, Set<String> commitKeys) {
    StoragePath newFile = new StoragePath(basePath, filePath);
    for (GenericRecord record : fileUtils.readAvroRecords(storage, newFile)) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
      assertTrue(expectedCommitTimes.contains(recCommitTime),
          "Record commit time not in expected commits: " + expectedCommitTimes);
      assertTrue(commitKeys.contains(recordKey),
          "Record key expected to be part of the commit keys");
    }
  }

  public static void assertFileAndRecordCountsAtCommitTime(int expectedFileCount, int expectedRecordCount, HoodieTable table, String commitTime, String partitionPath,
                                                                FileFormatUtils fileUtils, HoodieStorage storage) {
    List<HoodieBaseFile> files = table.getBaseFileOnlyView()
        .getLatestBaseFilesBeforeOrOn(partitionPath, commitTime).collect(Collectors.toList());
    assertEquals(expectedFileCount, files.size(), "Expected " + expectedFileCount + " valid data files.");
    int totalInserts = 0;
    for (HoodieBaseFile file : files) {
      assertEquals(commitTime, file.getCommitTime(), "All files must be at commit " + commitTime);
      totalInserts += fileUtils.readAvroRecords(storage, new StoragePath(file.getPath())).size();
    }
    assertEquals(expectedRecordCount, totalInserts, "Expected total recorcount of " + expectedRecordCount);
  }

  /**
   * Asserts that an existing file is expanded/updated with expected record counts.
   */
  public static void assertFileExpansion(String prevCommitTime, String fileId, List<WriteStatus> newCommitStatusList) {

    assertEquals(1, newCommitStatusList.size(), "Just 1 file needs to be updated.");
    assertEquals(fileId, newCommitStatusList.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(prevCommitTime, newCommitStatusList.get(0).getStat().getPrevCommit(), "Existing file should be expanded");
  }

  /**
   * Assert each file size equal to its source of truth.
   *
   * @param fileSizeGetter to retrieve the source of truth of file size.
   */
  public static void assertFileSizesEqual(List<WriteStatus> statuses, CheckedFunction<WriteStatus, Long> fileSizeGetter) {
    assertAll(statuses.stream().map(status -> () ->
        assertEquals(fileSizeGetter.apply(status), status.getStat().getFileSizeInBytes())));
  }

  public static void assertPartitionMetadataForRecords(String basePath, List<HoodieRecord> inputRecords,
                                                       HoodieStorage storage) throws IOException {
    String[] partitionPathSet = inputRecords.stream()
        .map(HoodieRecord::getPartitionPath).distinct().toArray(String[]::new);
    assertPartitionMetadata(basePath, partitionPathSet, storage);
  }

  public static void assertPartitionMetadataForKeys(String basePath, List<HoodieKey> inputKeys,
                                                    HoodieStorage storage) throws IOException {
    String[] partitionPathSet = inputKeys.stream()
        .map(HoodieKey::getPartitionPath).distinct().toArray(String[]::new);
    assertPartitionMetadata(basePath, partitionPathSet, storage);
  }

  /**
   * Ensure presence of partition meta-data at known depth.
   *
   * @param partitionPaths Partition paths to check
   * @param storage        {@link HoodieStorage} instance.
   * @throws IOException in case of error
   */
  public static void assertPartitionMetadata(String basePath, String[] partitionPaths,
                                             HoodieStorage storage) throws IOException {
    for (String partitionPath : partitionPaths) {
      assertTrue(
          HoodiePartitionMetadata.hasPartitionMetadata(
              storage, new StoragePath(basePath, partitionPath)));
      HoodiePartitionMetadata pmeta =
          new HoodiePartitionMetadata(storage, new StoragePath(basePath, partitionPath));
      pmeta.readFromFS();
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_DEPTH, pmeta.getPartitionDepth());
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  public static void assertNoDupesWithinPartition(List<HoodieRecord<IndexedRecord>> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(key), "key " + key + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(key);
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param recordDelegates List of Hoodie record delegates
   */
  public static void assertNoDuplicatesInPartition(List<HoodieRecordDelegate> recordDelegates) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecordDelegate r : recordDelegates) {
      String recordKey = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(recordKey), "key " + recordKey + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(recordKey);
    }
  }

  public static void assertActualAndExpectedPartitionPathRecordKeyMatches(List<Pair<String, String>> expectedPartitionPathRecKeyPairs,
                                                                          List<Pair<String, String>> actualPartitionPathRecKeyPairs) {
    // verify all partitionpath, record key matches
    assertEquals(expectedPartitionPathRecKeyPairs.size(), actualPartitionPathRecKeyPairs.size());
    for (Pair<String, String> entry : actualPartitionPathRecKeyPairs) {
      assertTrue(expectedPartitionPathRecKeyPairs.contains(entry));
    }

    for (Pair<String, String> entry : expectedPartitionPathRecKeyPairs) {
      assertTrue(actualPartitionPathRecKeyPairs.contains(entry));
    }
  }

  public static void assertComplexKeyGeneratorValidationThrows(Executable writeOperation, String operation) {
    HoodieException exception = assertThrows(HoodieException.class, writeOperation);
    assertEquals(getComplexKeygenErrorMessage(operation), exception.getMessage());
  }

  public static void assertRecordCountInFile(int expectedRecords, HoodieStorage storage, HoodieTableMetaClient metaClient, String basePath, String filePath) {
    StoragePath newFile = new StoragePath(basePath, filePath);
    assertEquals(expectedRecords, getFileUtilsInstance(metaClient).readRowKeys(storage, newFile).size(),
        "file should contain " + expectedRecords + " records");
  }
}
