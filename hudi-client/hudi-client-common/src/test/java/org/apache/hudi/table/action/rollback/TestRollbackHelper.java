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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
class TestRollbackHelper extends HoodieRollbackTestBase {
  private static final int ROLLBACK_LOG_VERSION = 20;

  @Override
  @BeforeEach
  void setup() throws IOException {
    super.setup();
  }

  @AfterEach
  void tearDown() throws IOException {
    storage.deleteDirectory(basePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"SIX", "EIGHT"})
  void testMaybeDeleteAndCollectStatsWithMultipleRequestsPerFileGroup(HoodieTableVersion tableVersion) throws IOException {
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    RollbackHelper rollbackHelper = new RollbackHelper(table, config);

    List<SerializableHoodieRollbackRequest> rollbackRequests = new ArrayList<>();
    String baseInstantTimeOfLogFiles = "001";
    String partition1 = "partition1";
    String partition2 = "partition2";
    String baseFileId1 = UUID.randomUUID().toString();
    String baseFileId2 = UUID.randomUUID().toString();
    String baseFileId3 = UUID.randomUUID().toString();
    String logFileId1 = UUID.randomUUID().toString();
    String logFileId2 = UUID.randomUUID().toString();
    // Base files to roll back
    StoragePath baseFilePath1 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId1, instantToRollback);
    StoragePath baseFilePath2 = addRollbackRequestForBaseFile(rollbackRequests, partition2, baseFileId2, instantToRollback);
    StoragePath baseFilePath3 = addRollbackRequestForBaseFile(rollbackRequests, partition2, baseFileId3, instantToRollback);
    // Log files to roll back
    Map<String, Long> logFilesToRollback1 = addRollbackRequestForLogFiles(
        rollbackRequests, tableVersion, partition2, logFileId1, baseInstantTimeOfLogFiles, IntStream.of(1));
    // Multiple rollback requests of log files belonging to the same file group
    Map<String, Long> logFilesToRollback2 = IntStream.range(1, ROLLBACK_LOG_VERSION).boxed()
        .flatMap(version -> addRollbackRequestForLogFiles(
            rollbackRequests, tableVersion, partition2, logFileId2, baseInstantTimeOfLogFiles, IntStream.of(version))
            .entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // Empty rollback request
    rollbackRequests.add(new SerializableHoodieRollbackRequest(
        HoodieRollbackRequest.newBuilder()
            .setPartitionPath(partition2)
            .setFileId(baseFileId3)
            .setLatestBaseInstant(instantToRollback)
            .setFilesToBeDeleted(Collections.emptyList())
            .setLogBlocksToBeDeleted(Collections.emptyMap()).build()));

    setupMocksAndValidateInitialState(rollbackInstantTime, rollbackRequests);
    List<Pair<String, HoodieRollbackStat>> rollbackStats = rollbackHelper.maybeDeleteAndCollectStats(
        new HoodieLocalEngineContext(storage.getConf()),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        rollbackRequests, true, 5);
    validateStateAfterRollback(rollbackRequests);
    StoragePath rollbackLogPath1 = new StoragePath(new StoragePath(basePath, partition2),
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
            ? FSUtils.makeLogFileName(logFileId1, HoodieFileFormat.HOODIE_LOG.getFileExtension(),
            instantToRollback, 1, HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            : FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId1, 2));
    StoragePath rollbackLogPath2 = new StoragePath(new StoragePath(basePath, partition2),
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
            ? FSUtils.makeLogFileName(logFileId2, HoodieFileFormat.HOODIE_LOG.getFileExtension(),
            instantToRollback, 1, HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            : FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId2, ROLLBACK_LOG_VERSION));
    List<Pair<String, HoodieRollbackStat>> expected = new ArrayList<>();
    expected.add(Pair.of(partition1,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition1)
            .withDeletedFileResult(baseFilePath1.toString(), true)
            .build()));
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .withDeletedFileResult(baseFilePath2.toString(), true)
            .build()));
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .withDeletedFileResult(baseFilePath3.toString(), true)
            .build()));
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      // For log file rollbacks in table version 8, the log files are deleted in parallel
      // so there is no grouping per file group
      getFullLogPathList(logFilesToRollback1.keySet(), partition2).forEach(logFilePath -> {
        expected.add(Pair.of(partition2,
            HoodieRollbackStat.newBuilder()
                .withPartitionPath(partition2)
                .withDeletedFileResult(logFilePath, true)
                .build()));
      });
      getFullLogPathList(logFilesToRollback2.keySet(), partition2).forEach(logFilePath -> {
        expected.add(Pair.of(partition2,
            HoodieRollbackStat.newBuilder()
                .withPartitionPath(partition2)
                .withDeletedFileResult(logFilePath, true)
                .build()));
      });
    } else {
      // For log file rollbacks in table version 6, the log files are kept and
      // the rollback command log block is added
      expected.add(Pair.of(partition2,
          HoodieRollbackStat.newBuilder()
              .withPartitionPath(partition2)
              .withRollbackBlockAppendResults(Collections.singletonMap(
                  storage.getPathInfo(rollbackLogPath1), 1L))
              .build()));
      expected.add(Pair.of(partition2,
          HoodieRollbackStat.newBuilder()
              .withPartitionPath(partition2)
              .withRollbackBlockAppendResults(Collections.singletonMap(
                  storage.getPathInfo(rollbackLogPath2), 1L))
              .build()));
    }
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .build()));
    assertRollbackStatsEquals(expected, rollbackStats);
  }

  @ParameterizedTest
  @ValueSource(strings = {"SIX", "EIGHT"})
  void testMaybeDeleteAndCollectStatsWithSingleRequestPerFileGroup(HoodieTableVersion tableVersion) throws IOException {
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    RollbackHelper rollbackHelper = new RollbackHelper(table, config);

    List<SerializableHoodieRollbackRequest> rollbackRequests = new ArrayList<>();
    String baseInstantTimeOfLogFiles = "001";
    String partition = "partition1";
    String baseFileId = UUID.randomUUID().toString();
    String logFileId = UUID.randomUUID().toString();
    // Base files to roll back
    StoragePath baseFilePath = addRollbackRequestForBaseFile(
        rollbackRequests, partition, baseFileId, instantToRollback);
    // A single rollback request of log files belonging to the same file group
    Map<String, Long> logFilesToRollback = addRollbackRequestForLogFiles(
        rollbackRequests, tableVersion, partition, logFileId, baseInstantTimeOfLogFiles, IntStream.range(1, ROLLBACK_LOG_VERSION));

    setupMocksAndValidateInitialState(rollbackInstantTime, rollbackRequests);
    List<Pair<String, HoodieRollbackStat>> rollbackStats = rollbackHelper.maybeDeleteAndCollectStats(
        new HoodieLocalEngineContext(storage.getConf()),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        rollbackRequests, true, 5);
    validateStateAfterRollback(rollbackRequests);
    StoragePath rollbackLogPath = new StoragePath(new StoragePath(basePath, partition),
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
            ? FSUtils.makeLogFileName(logFileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(),
            instantToRollback, 1, HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            : FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId, ROLLBACK_LOG_VERSION));
    List<Pair<String, HoodieRollbackStat>> expected = new ArrayList<>();
    expected.add(Pair.of(partition,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition)
            .withDeletedFileResult(baseFilePath.toString(), true)
            .build()
    ));
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      // For log file rollbacks in table version 8, the log files are deleted in parallel
      // so there is no grouping per file group
      getFullLogPathList(logFilesToRollback.keySet(), partition).forEach(logFilePath -> {
        expected.add(Pair.of(partition,
            HoodieRollbackStat.newBuilder()
                .withPartitionPath(partition)
                .withDeletedFileResult(logFilePath, true)
                .build()));
      });
    } else {
      // For log file rollbacks in table version 6, the log files are kept and
      // the rollback command log block is added
      expected.add(Pair.of(partition,
          HoodieRollbackStat.newBuilder()
              .withPartitionPath(partition)
              .withRollbackBlockAppendResults(Collections.singletonMap(
                  storage.getPathInfo(rollbackLogPath), 1L))
              .build()));
    }
    assertRollbackStatsEquals(expected, rollbackStats);
  }

  @Test
  public void testFailedDeletionsWithRollbackExecution() throws IOException {
    HoodieTableMetaClient mockedMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(mockedMetaClient);
    when(mockedMetaClient.getBasePath()).thenReturn(basePath);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(mockedMetaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieStorage mockedStorage = mock(HoodieStorage.class);
    when(mockedMetaClient.getStorage()).thenReturn(mockedStorage);
    StorageConfiguration storageConfiguration = storage.getConf();
    when(mockedStorage.getConf()).thenReturn(storageConfiguration);
    HoodieEngineContext context = new HoodieLocalEngineContext(storageConfiguration);

    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    RollbackHelper rollbackHelper = new RollbackHelper(mockHoodieTable, config);

    List<SerializableHoodieRollbackRequest> rollbackRequests = new ArrayList<>();
    String partition1 = "partition1";
    String baseFileId1 = UUID.randomUUID().toString();
    String baseFileId2 = UUID.randomUUID().toString();
    String baseFileId3 = UUID.randomUUID().toString();

    // Base files to roll back
    StoragePath baseFilePath1 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId1, instantToRollback);
    StoragePath baseFilePath2 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId2, instantToRollback);
    StoragePath baseFilePath3 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId3, instantToRollback);

    // test all diff combinations where rollback execution will succeed
    // deletion succeeds
    when(mockedStorage.deleteFile(baseFilePath1)).thenReturn(true);
    // fs.delete -> false, fs.exists -> false
    when(mockedStorage.deleteFile(baseFilePath2)).thenReturn(false);
    when(mockedStorage.exists(baseFilePath2)).thenReturn(false);
    // fs.delete throws FileNotFoundException
    when(mockedStorage.deleteFile(baseFilePath3)).thenThrow(new FileNotFoundException("File does not exist"));

    when(timeline.lastInstant()).thenReturn(
        Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));

    List<Pair<String, HoodieRollbackStat>> rollbackStats = rollbackHelper.maybeDeleteAndCollectStats(
        context,
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        rollbackRequests, true, 5);

    List<Pair<String, HoodieRollbackStat>> expected = new ArrayList<>();
    expected.add(Pair.of(partition1,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition1)
            .withDeletedFileResult(baseFilePath1.toString(), true)
            .build()));
    expected.add(Pair.of(partition1,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition1)
            .withDeletedFileResult(baseFilePath2.toString(), true)
            .build()));
    expected.add(Pair.of(partition1,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition1)
            .withDeletedFileResult(baseFilePath3.toString(), true)
            .build()));
    assertRollbackStatsEquals(expected, rollbackStats);

    // error case, where fs.delete returned false and fs.exists returned true
    rollbackRequests.clear();
    String baseFileId4 = UUID.randomUUID().toString();
    StoragePath baseFilePath4 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId4, instantToRollback);

    // fs.delete -> false, fs.exists -> true
    when(mockedStorage.deleteFile(baseFilePath4)).thenReturn(false);
    when(mockedStorage.exists(baseFilePath4)).thenReturn(true);
    // execute and assert failure
    assertFailedDeletion(rollbackHelper, context, rollbackInstantTime, instantToRollback, rollbackRequests, baseFilePath4.toString());
  }

  private void assertFailedDeletion(RollbackHelper rollbackHelper, HoodieEngineContext context, String rollbackInstantTime, String instantToRollback,
                                    List<SerializableHoodieRollbackRequest> rollbackRequests, String expectedFileToFailOnDeletion) {
    try {
      rollbackHelper.maybeDeleteAndCollectStats(
          context,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
          rollbackRequests, true, 5);
      fail("Should not have reached here");
    } catch (HoodieException e) {
      if (!(e.getCause() instanceof HoodieIOException)) {
        log.error("Expected HoodieIOException to be thrown, but found " + e.getCause() + ", w/ error msg " + e.getCause().getMessage());
      }
      assertTrue(e.getCause() instanceof HoodieIOException);
      assertTrue(e.getCause().getMessage().contains("Failing to delete file during rollback execution failed : " + expectedFileToFailOnDeletion));
    }
  }

  private void assertRollbackStatsEquals(List<Pair<String, HoodieRollbackStat>> expected,
                                         List<Pair<String, HoodieRollbackStat>> actual) {
    assertEquals(expected.size(), actual.size());
    List<Pair<String, HoodieRollbackStat>> sortedExpected = getSortedRollbackStats(expected);
    List<Pair<String, HoodieRollbackStat>> sortedActual = getSortedRollbackStats(actual);

    for (int i = 0; i < sortedExpected.size(); i++) {
      Pair<String, HoodieRollbackStat> expectedStat = sortedExpected.get(i);
      Pair<String, HoodieRollbackStat> actualStat = sortedActual.get(i);
      assertEquals(expectedStat.getKey(), actualStat.getKey());
      assertEquals(expectedStat.getValue().getPartitionPath(),
          actualStat.getValue().getPartitionPath());
      assertEquals(expectedStat.getValue().getSuccessDeleteFiles()
              .stream().sorted().collect(Collectors.toList()),
          actualStat.getValue().getSuccessDeleteFiles()
              .stream().sorted().collect(Collectors.toList()));
      assertEquals(expectedStat.getValue().getFailedDeleteFiles()
              .stream().sorted().collect(Collectors.toList()),
          actualStat.getValue().getFailedDeleteFiles()
              .stream().sorted().collect(Collectors.toList()));
      assertEquals(expectedStat.getValue().getCommandBlocksCount().size(),
          actualStat.getValue().getCommandBlocksCount().size());
      if (!expectedStat.getValue().getCommandBlocksCount().isEmpty()) {
        assertEquals(expectedStat.getValue().getCommandBlocksCount()
                .keySet().stream().findFirst().get().getPath(),
            actualStat.getValue().getCommandBlocksCount()
                .keySet().stream().findFirst().get().getPath());
      }
      Map<String, Long> expectedLogFileMap = expectedStat.getValue().getLogFilesFromFailedCommit();
      Map<String, Long> actualLogFileMap = actualStat.getValue().getLogFilesFromFailedCommit();
      assertEquals(expectedLogFileMap.size(), actualLogFileMap.size());
      for (Map.Entry<String, Long> entry : expectedLogFileMap.entrySet()) {
        assertTrue(actualLogFileMap.containsKey(entry.getKey()));
        assertEquals(entry.getValue(), actualLogFileMap.get(entry.getKey()));
      }
    }
  }

  private static List<Pair<String, HoodieRollbackStat>> getSortedRollbackStats(
      List<Pair<String, HoodieRollbackStat>> rollbackStats) {
    return rollbackStats.stream()
        .sorted(Comparator.comparing(
            e -> Triple.of(
                e.getRight().getSuccessDeleteFiles().size(),
                e.getRight().getCommandBlocksCount().size(),
                !e.getRight().getSuccessDeleteFiles().isEmpty()
                    ? e.getRight().getSuccessDeleteFiles().get(0)
                    : !e.getRight().getCommandBlocksCount().isEmpty()
                    ? e.getRight().getCommandBlocksCount().keySet().stream().findFirst().get()
                    : ""),
            Comparator.naturalOrder()))
        .collect(Collectors.toList());
  }

  private StoragePath addRollbackRequestForBaseFile(List<SerializableHoodieRollbackRequest> rollbackRequests,
                                                    String partition,
                                                    String fileId,
                                                    String instantTime) throws IOException {
    StoragePath baseFilePath = createBaseFileToRollback(partition, fileId, instantTime);
    rollbackRequests.add(new SerializableHoodieRollbackRequest(
        HoodieRollbackRequest.newBuilder()
            .setPartitionPath(partition)
            .setFileId(fileId)
            .setLatestBaseInstant(instantTime)
            .setFilesToBeDeleted(Collections.singletonList(baseFilePath.toString()))
            .setLogBlocksToBeDeleted(Collections.emptyMap()).build()));
    return baseFilePath;
  }

  private Map<String, Long> addRollbackRequestForLogFiles(List<SerializableHoodieRollbackRequest> rollbackRequests,
                                                          HoodieTableVersion tableVersion,
                                                          String partition,
                                                          String fileId,
                                                          String instantTime,
                                                          IntStream logVersions) {
    Map<String, Long> logBlocksToBeDeleted = createLogFilesToRollback(
        partition, fileId, instantTime, logVersions, 10L);
    HoodieRollbackRequest.Builder builder = HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId(fileId)
        .setLatestBaseInstant(instantTime);
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      builder.setFilesToBeDeleted(getFullLogPathList(logBlocksToBeDeleted.keySet(), partition))
          .setLogBlocksToBeDeleted(Collections.emptyMap());
    } else {
      builder.setFilesToBeDeleted(Collections.emptyList())
          .setLogBlocksToBeDeleted(logBlocksToBeDeleted);
    }
    rollbackRequests.add(new SerializableHoodieRollbackRequest(builder.build()));
    return logBlocksToBeDeleted;
  }

  private List<String> getFullLogPathList(Collection<String> logFileNames,
                                          String partition) {
    return logFileNames.stream()
        .map(logFileName ->
            new StoragePath(new StoragePath(basePath, partition), logFileName).toString())
        .collect(Collectors.toList());
  }

  private void setupMocksAndValidateInitialState(String rollbackInstantTime,
                                                 List<SerializableHoodieRollbackRequest> rollbackRequests) {
    when(timeline.lastInstant()).thenReturn(Option.of(
        INSTANT_GENERATOR.createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));
    rollbackRequests.forEach(request -> {
      if (!request.getFilesToBeDeleted().isEmpty()) {
        assertTrue(request.getFilesToBeDeleted().stream().map(path -> {
          try {
            return storage.exists(new StoragePath(path));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalAnd).get());
      } else if (!request.getLogBlocksToBeDeleted().isEmpty()) {
        StoragePath partitionPath = new StoragePath(basePath, request.getPartitionPath());
        assertTrue(request.getLogBlocksToBeDeleted().keySet().stream().map(logFileName -> {
          try {
            return storage.exists(new StoragePath(partitionPath, logFileName));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalAnd).get());
      }
    });
  }

  private void validateStateAfterRollback(List<SerializableHoodieRollbackRequest> rollbackRequests) {
    rollbackRequests.forEach(request -> {
      if (!request.getFilesToBeDeleted().isEmpty()) {
        assertFalse(request.getFilesToBeDeleted().stream().map(path -> {
          try {
            return storage.exists(new StoragePath(path));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalOr).get());
      } else if (!request.getLogBlocksToBeDeleted().isEmpty()) {
        StoragePath partitionPath = new StoragePath(basePath, request.getPartitionPath());
        assertTrue(request.getLogBlocksToBeDeleted().keySet().stream().map(logFileName -> {
          try {
            return storage.exists(new StoragePath(partitionPath, logFileName));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalAnd).get());
      }
    });
  }
}

