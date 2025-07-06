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
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBaseRollbackHelper extends HoodieRollbackTestBase {
  private static final int ROLLBACK_LOG_VERSION = 20;
  private static final Logger LOG = LoggerFactory.getLogger(TestBaseRollbackHelper.class);

  @Override
  @BeforeEach
  void setup() throws IOException {
    super.setup();
  }

  @AfterEach
  void tearDown() throws IOException {
    fs.delete(basePath, true);
  }

  @Test
  void testMaybeDeleteAndCollectStatsWithMultipleRequestsPerFileGroup() throws IOException {
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    BaseRollbackHelper rollbackHelper = new BaseRollbackHelper(table, config);

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
    Path baseFilePath1 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId1, instantToRollback);
    Path baseFilePath2 = addRollbackRequestForBaseFile(rollbackRequests, partition2, baseFileId2, instantToRollback);
    Path baseFilePath3 = addRollbackRequestForBaseFile(rollbackRequests, partition2, baseFileId3, instantToRollback);
    // Log files to roll back
    Map<String, Long> logFilesToRollback1 = addRollbackRequestForLogFiles(
        rollbackRequests, partition2, logFileId1, baseInstantTimeOfLogFiles, IntStream.of(1));
    // Multiple rollback requests of log files belonging to the same file group
    Map<String, Long> logFilesToRollback2 = IntStream.range(1, ROLLBACK_LOG_VERSION).boxed()
        .flatMap(version -> addRollbackRequestForLogFiles(
            rollbackRequests, partition2, logFileId2, baseInstantTimeOfLogFiles, IntStream.of(version))
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
        new HoodieLocalEngineContext(HoodieTestUtils.getDefaultHadoopConf()),
        rollbackInstantTime,
        new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        rollbackRequests, true, 5);
    validateStateAfterRollback(rollbackRequests);
    Path rollbackLogPath1 = new Path(new Path(basePath, partition2),
        FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId1, 2));
    Path rollbackLogPath2 = new Path(new Path(basePath, partition2),
        FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId2, ROLLBACK_LOG_VERSION));
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
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .withRollbackBlockAppendResults(Collections.singletonMap(
                fs.getFileStatus(rollbackLogPath1), 1L))
            .withLogFilesFromFailedCommit(logFilesToRollback1)
            .build()));
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .withRollbackBlockAppendResults(Collections.singletonMap(
                fs.getFileStatus(rollbackLogPath2), 1L))
            .withLogFilesFromFailedCommit(logFilesToRollback2)
            .build()));
    expected.add(Pair.of(partition2,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition2)
            .build()));
    assertRollbackStatsEquals(expected, rollbackStats);
  }

  @Test
  void testMaybeDeleteAndCollectStatsWithSingleRequestPerFileGroup() throws IOException {
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    BaseRollbackHelper rollbackHelper = new BaseRollbackHelper(table, config);

    List<SerializableHoodieRollbackRequest> rollbackRequests = new ArrayList<>();
    String baseInstantTimeOfLogFiles = "001";
    String partition = "partition1";
    String baseFileId = UUID.randomUUID().toString();
    String logFileId = UUID.randomUUID().toString();
    // Base files to roll back
    Path baseFilePath = addRollbackRequestForBaseFile(
        rollbackRequests, partition, baseFileId, instantToRollback);
    // A single rollback request of log files belonging to the same file group
    Map<String, Long> logFilesToRollback = addRollbackRequestForLogFiles(
        rollbackRequests, partition, logFileId, baseInstantTimeOfLogFiles, IntStream.range(1, ROLLBACK_LOG_VERSION));

    setupMocksAndValidateInitialState(rollbackInstantTime, rollbackRequests);
    List<Pair<String, HoodieRollbackStat>> rollbackStats = rollbackHelper.maybeDeleteAndCollectStats(
        new HoodieLocalEngineContext(HoodieTestUtils.getDefaultHadoopConf()),
        rollbackInstantTime,
        new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        rollbackRequests, true, 5);
    validateStateAfterRollback(rollbackRequests);
    Path rollbackLogPath = new Path(new Path(basePath, partition),
        FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId, ROLLBACK_LOG_VERSION));
    List<Pair<String, HoodieRollbackStat>> expected = new ArrayList<>();
    expected.add(Pair.of(partition,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition)
            .withDeletedFileResult(baseFilePath.toString(), true)
            .build()
    ));
    expected.add(Pair.of(partition,
        HoodieRollbackStat.newBuilder()
            .withPartitionPath(partition)
            .withRollbackBlockAppendResults(Collections.singletonMap(
                fs.getFileStatus(rollbackLogPath), 1L))
            .withLogFilesFromFailedCommit(logFilesToRollback)
            .build()
    ));
    assertRollbackStatsEquals(expected, rollbackStats);
  }

  @Test
  void testFailedDeletionsWithRollbackExecution() throws IOException {
    HoodieTableMetaClient mockedMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(mockedMetaClient);
    when(mockedMetaClient.getBasePathV2()).thenReturn(basePath);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(mockedMetaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieWrapperFileSystem fs = mock(HoodieWrapperFileSystem.class);
    when(mockedMetaClient.getFs()).thenReturn(fs);
    when(fs.getConf()).thenReturn(HoodieTestUtils.getDefaultHadoopConf());
    HoodieEngineContext context = new HoodieLocalEngineContext(HoodieTestUtils.getDefaultHadoopConf());

    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    BaseRollbackHelper rollbackHelper = new BaseRollbackHelper(mockHoodieTable, config);

    List<SerializableHoodieRollbackRequest> rollbackRequests = new ArrayList<>();
    String partition1 = "partition1";
    String baseFileId1 = UUID.randomUUID().toString();
    String baseFileId2 = UUID.randomUUID().toString();
    String baseFileId3 = UUID.randomUUID().toString();

    // Base files to roll back
    Path baseFilePath1 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId1, instantToRollback);
    Path baseFilePath2 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId2, instantToRollback);
    Path baseFilePath3 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId3, instantToRollback);

    // test all diff combinations where rollback execution will succeed
    // deletion succeeds
    when(fs.delete(baseFilePath1)).thenReturn(true);
    // fs.delete -> false, fs.exists -> false
    when(fs.delete(baseFilePath2)).thenReturn(false);
    when(fs.exists(baseFilePath2)).thenReturn(false);
    // fs.delete throws FileNotFoundException
    when(fs.delete(baseFilePath3)).thenThrow(new FileNotFoundException("File does not exist"));

    when(timeline.lastInstant()).thenReturn(Option.of(new HoodieInstant(
        true, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));

    List<Pair<String, HoodieRollbackStat>> rollbackStats = rollbackHelper.maybeDeleteAndCollectStats(
        context,
        rollbackInstantTime,
        new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
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
    Path baseFilePath4 = addRollbackRequestForBaseFile(rollbackRequests, partition1, baseFileId4, instantToRollback);

    // fs.delete -> false, fs.exists -> true
    when(fs.delete(baseFilePath4)).thenReturn(false);
    when(fs.exists(baseFilePath4)).thenReturn(true);
    // execute and assert failure
    assertFailedDeletion(rollbackHelper, context, rollbackInstantTime, instantToRollback, rollbackRequests, baseFilePath4.toString());
  }

  private void assertFailedDeletion(BaseRollbackHelper rollbackHelper, HoodieEngineContext context, String rollbackInstantTime, String instantToRollback,
                                    List<SerializableHoodieRollbackRequest> rollbackRequests, String expectedFileToFailOnDeletion) {
    try {
      rollbackHelper.maybeDeleteAndCollectStats(
          context,
          rollbackInstantTime,
          new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
          rollbackRequests, true, 5);
      fail("Should not have reached here");
    } catch (Throwable t) {
      if (!(t.getCause() instanceof HoodieIOException)) {
        LOG.error("Expected HoodieIOException to be thrown, but found {}, w/ error msg {}", t.getCause(), t.getCause().getMessage());
      }
      assertTrue(t.getCause() instanceof HoodieIOException);
      assertTrue(t.getCause().getMessage().contains("Failing to delete file during rollback execution failed : " + expectedFileToFailOnDeletion));
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
                e.getRight().getLogFilesFromFailedCommit().size(),
                !e.getRight().getSuccessDeleteFiles().isEmpty()
                    ? e.getRight().getSuccessDeleteFiles().get(0)
                    : !e.getRight().getLogFilesFromFailedCommit().isEmpty()
                    ? e.getRight().getLogFilesFromFailedCommit().keySet().stream().findFirst().get()
                    : ""),
            Comparator.naturalOrder()))
        .collect(Collectors.toList());
  }

  private Path addRollbackRequestForBaseFile(List<SerializableHoodieRollbackRequest> rollbackRequests,
                                             String partition,
                                             String fileId,
                                             String instantTime) throws IOException {
    Path baseFilePath = createBaseFileToRollback(partition, fileId, instantTime);
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
                                                          String partition,
                                                          String fileId,
                                                          String instantTime,
                                                          IntStream logVersions) {
    Map<String, Long> logBlocksToBeDeleted = createLogFilesToRollback(
        partition, fileId, instantTime, logVersions, 10L);
    rollbackRequests.add(new SerializableHoodieRollbackRequest(
        HoodieRollbackRequest.newBuilder()
            .setPartitionPath(partition)
            .setFileId(fileId)
            .setLatestBaseInstant(instantTime)
            .setFilesToBeDeleted(Collections.emptyList())
            .setLogBlocksToBeDeleted(logBlocksToBeDeleted).build()));
    return logBlocksToBeDeleted;
  }

  private void setupMocksAndValidateInitialState(String rollbackInstantTime,
                                                 List<SerializableHoodieRollbackRequest> rollbackRequests) {
    when(timeline.lastInstant()).thenReturn(Option.of(new HoodieInstant(
        true, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));
    rollbackRequests.forEach(request -> {
      if (!request.getFilesToBeDeleted().isEmpty()) {
        assertTrue(request.getFilesToBeDeleted().stream().map(path -> {
          try {
            return fs.exists(new Path(path));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalAnd).get());
      } else if (!request.getLogBlocksToBeDeleted().isEmpty()) {
        Path partitionPath = new Path(basePath, request.getPartitionPath());
        assertTrue(request.getLogBlocksToBeDeleted().keySet().stream().map(logFileName -> {
          try {
            return fs.exists(new Path(partitionPath, logFileName));
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
            return fs.exists(new Path(path));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalOr).get());
      } else if (!request.getLogBlocksToBeDeleted().isEmpty()) {
        Path partitionPath = new Path(basePath, request.getPartitionPath());
        assertTrue(request.getLogBlocksToBeDeleted().keySet().stream().map(logFileName -> {
          try {
            return fs.exists(new Path(partitionPath, logFileName));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).reduce(Boolean::logicalAnd).get());
      }
    });
  }
}
