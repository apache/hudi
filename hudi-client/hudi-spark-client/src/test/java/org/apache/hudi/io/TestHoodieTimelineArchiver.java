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

package org.apache.hudi.io;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieLSMTimelineManifest;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.HoodieTestCommitGenerator.getBaseFilename;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.convertCommitMetadataToAvro;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadata;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createCompactionCommitInMetadataTable;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.config.HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metrics.HoodieMetrics.ARCHIVE_ACTION;
import static org.apache.hudi.metrics.HoodieMetrics.DELETE_INSTANTS_NUM_STR;
import static org.apache.hudi.metrics.HoodieMetrics.DURATION_STR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TestHoodieTimelineArchiver extends HoodieSparkClientTestHarness {

  private HoodieTableMetadataWriter metadataWriter;
  private HoodieTestTable testTable;

  public void init() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
  }

  public void init(HoodieTableType tableType) throws Exception {
    initPath();
    initSparkContexts();
    initTimelineService();
    initMetaClient();
    storage = metaClient.getStorage();
    metaClient.getStorage().createDirectory(new StoragePath(basePath));
    metaClient = HoodieTestUtils.init(storageConf, basePath, tableType);
  }

  private void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) {
    if (enableMetadataTable) {
      metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, writeConfig, context);
      // reload because table configs could have been updated
      metaClient = HoodieTableMetaClient.reload(metaClient);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
  }

  @AfterEach
  public void clean() throws Exception {
    cleanupResources();
    if (metadataWriter != null) {
      metadataWriter.close();
    }
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata, int minArchivalCommits, int maxArchivalCommits, int maxDeltaCommitsMetadataTable) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits, maxDeltaCommitsMetadataTable, HoodieTableType.COPY_ON_WRITE);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits,
        maxDeltaCommits, maxDeltaCommitsMetadataTable, tableType, 10,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits,
        5, maxDeltaCommitsMetadataTable, tableType, 10,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           int archiveFilesBatch) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits, 5,
        maxDeltaCommitsMetadataTable, HoodieTableType.COPY_ON_WRITE, archiveFilesBatch,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType,
                                                           int archiveFilesBatch,
                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                           WriteConcurrencyMode writeConcurrencyMode) throws Exception {
    return initTestTableAndGetWriteConfig(
        enableMetadata,
        minArchivalCommits,
        maxArchivalCommits,
        maxDeltaCommits,
        maxDeltaCommitsMetadataTable,
        tableType,
        archiveFilesBatch,
        failedWritesCleaningPolicy,
        writeConcurrencyMode,
        ARCHIVE_BEYOND_SAVEPOINT.defaultValue());
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType,
                                                           int archiveFilesBatch,
                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                           WriteConcurrencyMode writeConcurrencyMode,
                                                           boolean archiveProceedBeyondSavepoints) throws Exception {
    init(tableType);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).withFailedWritesCleaningPolicy(failedWritesCleaningPolicy).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withTimelineCompactionBatchSize(archiveFilesBatch)
            .archiveCommitsWith(minArchivalCommits, maxArchivalCommits)
            .withArchiveBeyondSavepoint(archiveProceedBeyondSavepoints).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsMetadataTable)
            .withMetadataIndexColumnStats(false).build())
        // test uses test table infra. So, col stats is not available/populated.
        .withWriteConcurrencyMode(writeConcurrencyMode)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build())
        .forTable("test-trip-table").build();
    initWriteConfigAndMetatableWriter(writeConfig, enableMetadata);
    return writeConfig;
  }

  @Test
  public void testArchiveEmptyTable() throws Exception {
    init();
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table").build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);
    int result = archiver.archiveIfRequired(context);
    assertEquals(0, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveTableWithArchival(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2);

    // min archival commits is 4 and max archival commits is 5. and so, after 6th commit, 2 commits will be archived.
    // 1,2,3,4,5,6 : after archival -> 3,4,5,6
    // after 2 more commits, earliest 2 will be archived
    // 3,4,5,6,7,8 : after archival -> 5,6,7,8
    // after 9 no-op wrt archival.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (i < 6) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i == 6) {
        // archival should have kicked in.
        verifyArchival(
            getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
            getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000005", "00000006")),
            commitsAfterArchival, false);
        if (enableMetadata) {
          // disable metadata table in the write config but files metadata partition is still available
          disableMetadataTable(writeConfig);
        }
      } else if (i < 8) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i == 8) {
        // archival should have kicked in.
        verifyArchival(
            getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004")),
            getActiveCommitInstants(Arrays.asList("00000005", "00000006", "00000007", "00000008")),
            commitsAfterArchival, false);
      } else {
        assertEquals(originalCommits, commitsAfterArchival);
      }
    }
  }

  @Test
  public void testArchiveTableWithReplaceCommits() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 2);
    for (int i = 1; i < 9; i++) {
      if (i < 3) {
        testTable.doWriteOperation(WriteClientTestUtils.createNewInstantTime(), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(),
            Arrays.asList("p1", "p2"), 2);
      } else {
        testTable.doWriteOperation(WriteClientTestUtils.createNewInstantTime(), WriteOperationType.INSERT_OVERWRITE, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      }
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      if (i == 8) {
        // after all rounds, only 4 should be left in active timeline. 5,6,7,8
        assertEquals(6, originalCommits.size());
        assertEquals(4, commitsAfterArchival.size());
      }
    }
  }

  // @Disabled("HUDI-6385")
  @ParameterizedTest
  @ValueSource(strings = {"KEEP_LATEST_BY_HOURS", "KEEP_LATEST_COMMITS"})
  public void testArchivalWithAutoAdjustmentBasedOnCleanConfigs(String cleaningPolicy) throws Exception {
    // This test verifies that when the archival configs are more aggressive than the cleaning
    // configs, the archiver adjust the min and max commits to keep automatically based on the
    // cleaning configs.
    init();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(5).withMetadataIndexColumnStats(false).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .withCleanerPolicy(HoodieCleaningPolicy.valueOf(cleaningPolicy))
            .cleanerNumHoursRetained(1)
            .retainCommits(5)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build())
        .build();

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";
    Instant instant = Instant.now();
    ZonedDateTime commitDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    List<HoodieInstant> expectedAllCommits = new ArrayList<>();

    // The following commits should be archived
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 90, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 80, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 70, true, testTable, config));
    // The following commits should not be archived
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 50, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 45, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 40, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 30, false, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 20, false, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 10, true, testTable, config));
    expectedAllCommits.add(triggerCommit(p0, p1, commitDateTime, 5, true, testTable, config));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList =
        archiveAndGetCommitsList(config, true);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
    assertInstantListEquals(expectedAllCommits, originalCommits);
    assertInstantListEquals(
        expectedAllCommits.subList(2, expectedAllCommits.size()), commitsAfterArchival);
  }

  private HoodieInstant triggerCommit(
      String p0, String p1, ZonedDateTime curDateTime, int minutesForCommit,
      boolean isComplete, HoodieTestTable testTable, HoodieWriteConfig config) throws Exception {

    String file1P0C0 = UUID.randomUUID().toString();
    String file1P1C0 = UUID.randomUUID().toString();
    String commitTs = TimelineUtils.formatDate(Date.from(curDateTime.minusMinutes(minutesForCommit).toInstant()));
    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context)) {
      Map<String, List<String>> part1ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
        {
          put(p0, CollectionUtils.createImmutableList(file1P0C0));
          put(p1, CollectionUtils.createImmutableList(file1P1C0));
        }
      });
      return commitWithMdt(commitTs, part1ToFileId, testTable, metadataWriter, true, true, isComplete);
    }
  }

  private HoodieInstant commitWithMdt(String instantTime, Map<String, List<String>> partToFileId,
                                      HoodieTestTable testTable, HoodieTableMetadataWriter metadataWriter,
                                      boolean addBaseFiles, boolean addLogFiles, boolean isComplete) throws Exception {
    testTable.addInflightCommit(instantTime);
    HoodieCommitMetadata commitMeta;
    if (isComplete) {
      Map<String, List<String>> partToFileIds = new HashMap<>();
      partToFileId.forEach((key, value) -> {
        try {
          List<String> files = new ArrayList<>();
          FileCreateUtilsLegacy.createPartitionMetaFile(basePath, key);
          if (addBaseFiles) {
            files.addAll(testTable.withBaseFilesInPartition(key, value.toArray(new String[0])).getValue());
          }
          if (addLogFiles) {
            value.forEach(logFilePrefix -> {
              try {
                files.addAll(testTable.withLogFile(key, logFilePrefix, 1, 2).getValue());
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
          }
          partToFileIds.put(key, files);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      commitMeta = generateCommitMetadata(instantTime, partToFileIds);
      metadataWriter.performTableServices(Option.of(instantTime), true);
      metadataWriter.update(commitMeta, instantTime);
      metaClient.getActiveTimeline().saveAsComplete(
          INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime),
          Option.of(commitMeta));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return INSTANT_GENERATOR.createNewInstant(
        isComplete ? State.COMPLETED : State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime);
  }

  protected static HoodieCommitMetadata generateCommitMetadata(
      String instantTime, Map<String, List<String>> partitionToFilePaths) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
    partitionToFilePaths.forEach((partitionPath, fileList) -> fileList.forEach(f -> {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partitionPath);
      writeStat.setPath(partitionPath + "/" + getBaseFilename(instantTime, f));
      writeStat.setFileId(f);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      metadata.addWriteStat(partitionPath, writeStat);
    }));
    return metadata;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSavepointWithArchival(boolean archiveBeyondSavepoint) throws Exception {
    boolean enableMetadata = false;
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 5, 2, HoodieTableType.COPY_ON_WRITE,
        10, HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER, archiveBeyondSavepoint);

    // min archival commits is 2 and max archival commits is 4. and so, after 5th commit, 3 commits will be archived.
    for (int i = 1; i < 5; i++) {
      testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    }

    // savepoint 3rd commit
    String commitToSavepoint = String.format("%08d", 3);
    HoodieSavepointMetadata savepointMetadata = testTable.doSavepoint(commitToSavepoint);
    testTable.addSavepoint(commitToSavepoint, savepointMetadata);

    for (int i = 5; i < 7; i++) {
      testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    }
    // trigger archival
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

    if (archiveBeyondSavepoint) {
      // retains only 2 commits. C3 and C8. and savepointed commit for C3.
      verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000004", "00000005")),
          Stream.concat(getActiveCommitInstants(Arrays.asList("00000003", "00000006")).stream(), getActiveSavepointedCommitInstants(Arrays.asList("00000003")).stream())
              .collect(Collectors.toList()), commitsAfterArchival, true);
    } else {
      // archives only C1 and C2. stops at first savepointed commit C3.
      verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
          Stream.concat(getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000005", "00000006")).stream(),
                  getActiveSavepointedCommitInstants(Arrays.asList("00000003")).stream())
              .collect(Collectors.toList()), commitsAfterArchival, false);
    }

    for (int i = 7; i < 10; i++) {
      testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    }

    // once savepoint is removed. C3 will be archived.
    testTable.deleteSavepoint(commitToSavepoint);
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();

    metaClient.reloadActiveTimeline();
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005", "00000006", "00000007")),
        getActiveCommitInstants(Arrays.asList("00000008", "00000009")), commitsAfterArchival, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testClusteringCommitAndSavepointWithArchival(boolean archiveBeyondSavepoint) throws Exception {
    boolean enableMetadata = false;
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 5, 2, HoodieTableType.COPY_ON_WRITE,
        10, HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER, archiveBeyondSavepoint);

    // min archival commits is 2 and max archival commits is 4. and so, after 5th commit, 3 commits will be archived.
    for (int i = 1; i < 8; i++) {
      if (i < 3 || i == 5) {
        testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      } else {
        testTable.doCluster(String.format("%08d", i), Collections.singletonMap("p1", Collections.singletonList("f1")), Collections.singletonList("p2"), 2);
      }
    }

    // savepoint 3rd commit
    String commitToSavepoint = String.format("%08d", 3);
    HoodieSavepointMetadata savepointMetadata = testTable.doSavepoint(commitToSavepoint);
    testTable.addSavepoint(commitToSavepoint, savepointMetadata);

    // trigger archival
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

    // Only commits 1, 2 should be archived since savepoint exists at c3. In the case when
    // archiveBeyondSavepoint is false, archival is blocked since replace commits have not yet been cleaned
    List<HoodieInstant> expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000005"));
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000006", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION));
    expectedActiveInstants.addAll(getActiveSavepointedCommitInstants(Arrays.asList("00000003")));
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
        expectedActiveInstants, commitsAfterArchival, false);

    // add a clean commit with earliest instant to retain as c8 and earliest savepoint as c3.
    // After this commit clean along with snapshot blocks archival even though earliest instant to retain is c8
    Map<String, Integer> cleanStats = new HashMap<>();
    cleanStats.put("p1", 1);
    cleanStats.put("p2", 2);
    testTable.doClean(String.format("%08d", 8), cleanStats,
        Collections.singletonMap(CleanerUtils.SAVEPOINTED_TIMESTAMPS, "00000003"));

    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    commitsAfterArchival = commitsList.getValue();
    // archives only C1 and C2. stops at first replace commit C3 after savepoint based on cleaner
    expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000005"));
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000006", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION));
    expectedActiveInstants.addAll(getActiveSavepointedCommitInstants(Arrays.asList("00000003")));
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000008"), CLEAN_ACTION));
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
        expectedActiveInstants, commitsAfterArchival, false);

    // add a clean commit with earliest savepoint set as c7
    testTable.doClean(String.format("%08d", 9), cleanStats, Collections.singletonMap(CleanerUtils.SAVEPOINTED_TIMESTAMPS, "00000007"));

    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    commitsAfterArchival = commitsList.getValue();

    if (archiveBeyondSavepoint) {
      // retains the 2 commits - C3 and C7. Since minInstantsToKeep is 2, c3 is retained. Archival is now blocked at
      // c7 since that is the replace commit after earliest savepoint c7 in cleaner
      expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000003", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION);
      expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000008", "00000009"), CLEAN_ACTION));
      expectedActiveInstants.addAll(getActiveSavepointedCommitInstants(Arrays.asList("00000003")));
      List<HoodieInstant> archivedCommitInstants = getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000005"));
      archivedCommitInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000004", "00000006"), HoodieTimeline.REPLACE_COMMIT_ACTION));
      verifyArchival(archivedCommitInstants, expectedActiveInstants, commitsAfterArchival, true);
    } else {
      // archives only C1 and C2. stops at c3 since clean earliest savepoint is c3.
      expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000005"));
      expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000006", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION));
      expectedActiveInstants.addAll(getActiveSavepointedCommitInstants(Arrays.asList("00000003")));
      expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000008", "00000009"), CLEAN_ACTION));
      verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
          expectedActiveInstants, commitsAfterArchival, false);
    }

    // savepoint is removed
    testTable.deleteSavepoint(commitToSavepoint);

    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    commitsAfterArchival = commitsList.getValue();

    if (archiveBeyondSavepoint) {
      // change from last state - Removal of savepoint instant from the active timeline since it is deleted
      expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000003", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION);
      expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000008", "00000009"), CLEAN_ACTION));
      List<HoodieInstant> archivedCommitInstants = getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000005"));
      archivedCommitInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000004", "00000006"), HoodieTimeline.REPLACE_COMMIT_ACTION));
      verifyArchival(archivedCommitInstants, expectedActiveInstants, commitsAfterArchival, true);
    } else {
      // change from last state - Removal of savepoint instant from the active timeline since it is deleted
      // since savepoint is now deleted, it does not block archival.
      // archival is triggered since clean also does not block it
      // c6 and c7 are retained since min instants to keep is 2
      expectedActiveInstants = getActiveCommitInstants(Arrays.asList("00000006", "00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION);
      expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000008", "00000009"), CLEAN_ACTION));
      List<HoodieInstant> archivedCommitInstants = getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000005"));
      archivedCommitInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000003", "00000004"), HoodieTimeline.REPLACE_COMMIT_ACTION));
      verifyArchival(archivedCommitInstants, expectedActiveInstants, commitsAfterArchival, false);
    }
  }

  @Test
  public void testCompactionWithCorruptVersionFile() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 2, 3);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // create a version pointer file with invalid version number.
    metaClient.getStorage().deleteDirectory(LSMTimeline.getVersionFilePath(metaClient.getArchivePath()));
    FileIOUtils.createFileInPath(metaClient.getStorage(),
        LSMTimeline.getVersionFilePath(metaClient.getArchivePath()),
        Option.of(HoodieInstantWriter.convertByteArrayToWriter(getUTF8Bytes("invalid_version"))));

    // check that invalid manifest file will not block archived timeline loading.
    HoodieActiveTimeline rawActiveTimeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(5 * 3 + 4, rawActiveTimeline.countInstants() + archivedTimeLine.countInstants());

    // trigger several archive with the invalid manifest file.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("1000000" + i, WriteOperationType.UPSERT,
          i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"),
          2);
      archiveAndGetCommitsList(writeConfig);
    }

    // loading archived timeline and active timeline success
    HoodieActiveTimeline rawActiveTimeline1 = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine1 = metaClient.getArchivedTimeline().reload();

    // check instant number
    assertEquals(4 * 3 + 14,
        archivedTimeLine1.countInstants() + rawActiveTimeline1.countInstants());

    // if there are damaged archive files and damaged plan, hoodie can still load correctly.
    StoragePath damagedFile =
        new StoragePath(metaClient.getArchivePath(), "300_301_1.parquet");
    FileIOUtils.createFileInPath(
        metaClient.getStorage(), damagedFile,
        Option.of(HoodieInstantWriter.convertByteArrayToWriter(getUTF8Bytes("dummy"))));

    assertDoesNotThrow(() -> metaClient.getArchivedTimeline().reload(),
        "Archived timeline can skip the invalid data and manifest files smartly");
  }

  @Test
  public void testCompactionRecoverWithoutManifestFile() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 2, 3);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation(WriteClientTestUtils.createNewInstantTime(), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // do a single merge small archive files
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    LSMTimelineWriter timelineWriter = LSMTimelineWriter.getInstance(writeConfig, table);
    List<String> candidateFiles = LSMTimeline.latestSnapshotManifest(metaClient, metaClient.getArchivePath()).getFiles().stream()
        .sorted().map(HoodieLSMTimelineManifest.LSMFileEntry::getFileName).collect(Collectors.toList());

    String compactedFileName = LSMTimelineWriter.compactedFileName(candidateFiles);
    timelineWriter.compactFiles(candidateFiles, compactedFileName);

    // check loading archived and active timeline success
    HoodieActiveTimeline rawActiveTimeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(5 * 3 + 4, rawActiveTimeline.countInstants() + archivedTimeLine.reload().countInstants());
  }

  @Test
  public void testCompactionCleaning() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 2, 3);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 19; i++) {
      testTable.doWriteOperation(
          WriteClientTestUtils.createNewInstantTime(), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }
    // now we have version 6, 7, 8, 9 version of snapshots

    // loading archived timeline and active timeline success
    HoodieActiveTimeline rawActiveTimeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline();
    assertEquals(4 * 3 + 14, rawActiveTimeline.countInstants() + archivedTimeLine.countInstants());

    assertEquals(9, LSMTimeline.latestSnapshotVersion(metaClient, metaClient.getArchivePath()));
    assertEquals(Arrays.asList(7, 8, 9), LSMTimeline.allSnapshotVersions(metaClient, metaClient.getArchivePath()).stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void testReadArchivedCompactionPlan() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 5, HoodieTableType.MERGE_ON_READ);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 11; i += 2) {
      testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      testTable.doCompaction(String.format("%08d", (i + 1)), Arrays.asList("p1", "p2"));
      archiveAndGetCommitsList(writeConfig);
    }

    // loading archived timeline instants
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline();
    // load instant details
    archivedTimeLine.loadCompactionDetailsInMemory("00000001", "00000011");
    List<HoodieInstant> compactionInstants = archivedTimeLine.getCommitAndReplaceTimeline().getInstants();
    assertEquals(2, compactionInstants.size(), "Two compactions instants should be archived.");
    List<Option<byte[]>> planDetailsList = compactionInstants.stream().map(archivedTimeLine::getInstantDetails).collect(Collectors.toList());
    assertTrue(planDetailsList.stream().allMatch(Option::isPresent), "All the compaction instants should have plan details.");
    // parse the compaction plan for each instant
    for (Option<byte[]> planDetails : planDetailsList) {
      assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(planDetails.get()), HoodieCompactionPlan.class));
    }
  }

  @Test
  public void testDowngradeArchivedTimeline() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(false, 1, 2, 5, HoodieTableType.MERGE_ON_READ);

    // do ingestion and trigger archive actions here.
    Map<String, Integer> cleanStats = new HashMap<>();
    cleanStats.put("p1", 1);
    cleanStats.put("p2", 2);
    for (int i = 1; i < 17; i += 2) {
      if (i == 3) {
        testTable.doRollback(String.format("%08d", 1), String.format("%08d", 3));
      } else if (i == 5) {
        testTable.doCluster(String.format("%08d", i), Collections.emptyMap(), Arrays.asList("p1", "p2"), 20);
      } else if (i == 7 || i == 13) {
        testTable.doCompaction(String.format("%08d", i), Arrays.asList("p1", "p2"));
      } else {
        testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
        testTable.doClean(String.format("%08d", i + 1), cleanStats, Collections.emptyMap());
      }
    }
    testTable.doCompaction(String.format("%08d", 17), Arrays.asList("p1", "p2"));

    // 1 - dc, 2- clean, 3 - rollback, 5 -> clustering, 7 -> compaction, 9 -> dc, 10 -> clean. 11 -> dc,
    // 12 -> clean. 13 -> compaction, 15 -> dc, 16 -> clean, 17 -> compaction
    Pair<List<HoodieInstant>, List<HoodieInstant>> result = archiveAndGetCommitsList(writeConfig);
    // after archival, only instants 16 and 17 are in active timeline.
    List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
    //List<String> expectedArchivedInstants = Arrays.asList(new String[]{String.format("%08d",1), String.format("%08d",2), String.format("%08d",12)})
    expectedActiveInstants.add(getHoodieInstant(CLEAN_ACTION, String.format("%08d",16)));
    expectedActiveInstants.add(getHoodieInstant(COMMIT_ACTION, String.format("%08d",17)));

    // validate active instants
    List<HoodieInstant> actualActiveInstants = new ArrayList<>(result.getRight());
    Collections.sort(actualActiveInstants);
    Collections.sort(expectedActiveInstants);
    assertEquals(expectedActiveInstants, actualActiveInstants);

    List<HoodieInstant> actualArchivedCommits = new ArrayList<>(result.getKey());
    actualArchivedCommits.removeAll(result.getValue());

    List<HoodieInstant> expectedArchivedInstants = new ArrayList<>();
    expectedArchivedInstants.add(getHoodieInstant(DELTA_COMMIT_ACTION, String.format("%08d",1)));
    expectedArchivedInstants.add(getHoodieInstant(CLEAN_ACTION, String.format("%08d",2)));
    expectedArchivedInstants.add(getHoodieInstant(ROLLBACK_ACTION, String.format("%08d",3)));
    expectedArchivedInstants.add(getHoodieInstant(REPLACE_COMMIT_ACTION, String.format("%08d",5)));
    expectedArchivedInstants.add(getHoodieInstant(COMMIT_ACTION, String.format("%08d",7)));
    expectedArchivedInstants.add(getHoodieInstant(DELTA_COMMIT_ACTION, String.format("%08d",9)));
    expectedArchivedInstants.add(getHoodieInstant(CLEAN_ACTION, String.format("%08d",10)));
    expectedArchivedInstants.add(getHoodieInstant(DELTA_COMMIT_ACTION, String.format("%08d",11)));
    expectedArchivedInstants.add(getHoodieInstant(CLEAN_ACTION, String.format("%08d",12)));
    expectedArchivedInstants.add(getHoodieInstant(COMMIT_ACTION, String.format("%08d",13)));
    expectedArchivedInstants.add(getHoodieInstant(DELTA_COMMIT_ACTION, String.format("%08d",15)));

    // validate archived instants
    Collections.sort(actualArchivedCommits);
    Collections.sort(expectedArchivedInstants);
    assertEquals(expectedArchivedInstants, actualArchivedCommits);

    // loading archived timeline instants
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline();
    archivedTimeLine.loadCompletedInstantDetailsInMemory();

    // Downgrade to table version 6
    new UpgradeDowngrade(metaClient, writeConfig, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.SIX, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    metaClient.getArchivedTimeline().loadCompletedInstantDetailsInMemory();
    HoodieTimeline downgradedArchivedTimeline = metaClient.getArchivedTimeline();
    // verify expected archived instants
    expectedArchivedInstants.forEach(instant -> assertTrue(downgradedArchivedTimeline.containsInstant(instant)));
    // verify the contents of older archived timeline and downgraded archived timeline
    for (HoodieInstant instant : archivedTimeLine.getInstants()) {
      assertTrue(Arrays.equals(archivedTimeLine.getInstantReader().getInstantDetails(instant).get(),
          downgradedArchivedTimeline.getInstantReader().getInstantDetails(instant).get()));
    }
  }

  private HoodieInstant getHoodieInstant(String action, String instantTime) {
    return new HoodieInstant(State.COMPLETED, action, instantTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testArchivalWithMultiWriters(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 5, 2,
        HoodieTableType.COPY_ON_WRITE, 10,
        HoodieFailedWritesCleaningPolicy.LAZY, WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);

    final ExecutorService executors = Executors.newFixedThreadPool(2);
    List<CompletableFuture<Boolean>> completableFutureList = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicReference<String> lastInstant = new AtomicReference<>();
    IntStream.range(0, 2).forEach(index -> {
      completableFutureList.add(CompletableFuture.supplyAsync(() -> {
        HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
        try {
          // wait until 4 commits are available so that archival thread will have something to archive.
          countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new HoodieException("Should not have thrown InterruptedException ", e);
        }
        metaClient.reloadActiveTimeline();
        while (!metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime().equals(lastInstant.get())
            || metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() > 5) {
          try {
            TimelineArchiverV2 archiver = new TimelineArchiverV2(writeConfig, table);
            archiver.archiveIfRequired(context, true);
            // if not for below sleep, both archiving threads acquires lock in quick succession and does not give space for main thread
            // to complete the write operation when metadata table is enabled.
            if (enableMetadata) {
              Thread.sleep(2);
            }
          } catch (IOException e) {
            throw new HoodieException("IOException thrown while archiving ", e);
          } catch (InterruptedException e) {
            throw new HoodieException("Should not have thrown InterruptedException ", e);
          }
          table.getMetaClient().reloadActiveTimeline();
        }
        return true;
      }, executors));
    });

    // do ingestion and trigger archive actions here.
    final int numWrites = 30;
    for (int i = 1; i < numWrites; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      if (i == 29) {
        lastInstant.set(instant);
      }
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      if (i == 6) {
        // start up archival threads only after 4 commits.
        countDownLatch.countDown();
      }
    }

    try {
      CompletableFuture completableFuture = allOfTerminateOnFailure(completableFutureList);
      completableFuture.get();
    } finally {
      executors.shutdownNow();
    }
  }

  private static CompletableFuture allOfTerminateOnFailure(List<CompletableFuture<Boolean>> futures) {
    CompletableFuture<?> failure = new CompletableFuture();
    AtomicBoolean jobFailed = new AtomicBoolean(false);
    int counter = 0;
    while (counter < futures.size()) {
      CompletableFuture<Boolean> curFuture = futures.get(counter);
      int finalCounter = counter;
      curFuture.exceptionally(ex -> {
        if (!jobFailed.getAndSet(true)) {
          log.warn("One of the job failed. Cancelling all other futures. " + ex.getCause() + ", " + ex.getMessage());
          int secondCounter = 0;
          while (secondCounter < futures.size()) {
            if (secondCounter != finalCounter) {
              futures.get(secondCounter).cancel(true);
            }
            secondCounter++;
          }
        }
        return null;
      });
      counter++;
    }
    return CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNoArchivalUntilMaxArchiveConfigWithExtraInflightCommits(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2);

    // when max archival commits is set to 5, until 6th commit there should not be any archival.
    for (int i = 1; i < 6; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2);
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    // add couple of inflight. no archival should kick in.
    testTable.doWriteOperation("00000006", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2, false, true);
    testTable.doWriteOperation("00000007", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2, false, true);

    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits, commitsAfterArchival);
  }

  private static Stream<Arguments> archiveCommitSavepointNoHoleParams() {
    return Arrays.stream(new Boolean[][]{
        {true, true},
        {false, true},
        {true, false},
        {false, false}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("archiveCommitSavepointNoHoleParams")
  public void testArchiveCommitSavepointNoHole(boolean enableMetadataTable, boolean archiveBeyondSavepoint) throws Exception {
    init();
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table")
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 5)
            .withArchiveBeyondSavepoint(archiveBeyondSavepoint).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .build();

    HoodieTestDataGenerator.createCommitFile(basePath, "100", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "101", storageConf);
    HoodieTestDataGenerator.createSavepointFile(basePath, "101", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "102", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "103", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "104", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "105", storageConf);
    HoodieTable table = HoodieSparkTable.create(cfg, context);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(storageConf, basePath, "105");
    }

    HoodieTimeline timeline =
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    archiver.archiveIfRequired(context);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    if (archiveBeyondSavepoint) {
      // commits in active timeline = 101 and 105.
      assertEquals(2, timeline.countInstants(),
          "Since archiveBeyondSavepoint config is enabled, we will archive commits 102, 103 ");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "101")),
          "Savepointed commits should always be safe");
      assertFalse(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "102")),
          "102 expected to be archived");
      assertFalse(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "103")),
          "103 expected to be archived");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "105")),
          "104 expected to be archived");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "105")),
          "105 expected to be in active timeline");
    } else {
      assertEquals(5, timeline.countInstants(),
          "Since we have a savepoint at 101, we should never archive any commit after 101 (we only archive 100)");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "101")),
          "Archived commits should always be safe");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "102")),
          "Archived commits should always be safe");
      assertTrue(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "103")),
          "Archived commits should always be safe");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPendingClusteringWillBlockArchival(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2);
    HoodieTestDataGenerator.createPendingClusterFile(basePath, "00000000", storageConf);
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2);
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    HoodieTimeline timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(7, timeline.countInstants(),
        "Since we have a pending clustering instant at 00000000, we should never archive any commit after 00000000");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveRollbacksTestTable(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2);

    List<Pair<String, String>> instants = new ArrayList<>();
    boolean hasArchivedInstants = false;
    for (int i = 1; i < 8; i += 3) {
      String commitInstant1 = WriteClientTestUtils.createNewInstantTime();
      instants.add(Pair.of(commitInstant1, HoodieTimeline.COMMIT_ACTION));
      testTable.doWriteOperation(
          commitInstant1,
          WriteOperationType.UPSERT,
          i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"),
          2);
      try {
        String rollbackInstant = WriteClientTestUtils.createNewInstantTime();
        testTable.doRollback(commitInstant1, rollbackInstant);
        instants.add(Pair.of(rollbackInstant, HoodieTimeline.ROLLBACK_ACTION));
      } catch (HoodieMetadataException e) {
        // The instant that triggers compaction can not be rolled back
        // because it's instant time is less than the latest compaction instant
        // on the MDT timeline.
        // ignore
      }

      // we need enough delta commits to trigger archival on MDT.
      String commitInstant2 = WriteClientTestUtils.createNewInstantTime();
      instants.add(Pair.of(commitInstant2, HoodieTimeline.COMMIT_ACTION));
      testTable.doWriteOperation(commitInstant2, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);

      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      int numArchivedInstants = originalCommits.size() - commitsAfterArchival.size();
      if ((originalCommits.size() - commitsAfterArchival.size()) > 0) {
        hasArchivedInstants = true;
        List<HoodieInstant> expectedArchivedInstants = instants.subList(0, numArchivedInstants).stream()
            .map(p -> INSTANT_GENERATOR.createNewInstant(State.COMPLETED, p.getValue(), p.getKey())).collect(Collectors.toList());
        List<HoodieInstant> expectedActiveInstants = instants.subList(numArchivedInstants, instants.size()).stream()
            .map(p -> INSTANT_GENERATOR.createNewInstant(State.COMPLETED, p.getValue(), p.getKey())).collect(Collectors.toList());
        verifyArchival(expectedArchivedInstants, expectedActiveInstants, commitsAfterArchival, false);
      }
    }
    assertTrue(hasArchivedInstants, "Some instants should be archived");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNoArchivalWithInflightCompactionInMiddle(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2, 2,
        HoodieTableType.MERGE_ON_READ);

    // when max archival commits is set to 5, even after 7 commits, if there is an inflight compaction in the middle, archival should not kick in.
    HoodieCommitMetadata inflightCompactionMetadata = null;
    for (int i = 1; i < 8; i++) {
      if (i == 2) {
        inflightCompactionMetadata = testTable.doCompaction("0000000" + i, Arrays.asList("p1", "p2"), true);
      } else {
        testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      }

      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (enableMetadata) {
        if (i != 7) {
          assertEquals(originalCommits, commitsAfterArchival);
        } else {
          // on 7th commit, archival will kick in, but cannot archive any commit,
          // since 1st deltacommit is the greatest completed commit before an oldest inflight commit.
          assertEquals(originalCommits.size() - commitsAfterArchival.size(), 0);
        }
      } else {
        if (i != 7) {
          assertEquals(originalCommits, commitsAfterArchival);
        } else {
          // on 7th commit, archival will kick in, but cannot archive any commit,
          // since 1st deltacommit is the greatest completed commit before an oldest inflight commit.
          assertEquals(originalCommits.size() - commitsAfterArchival.size(), 0);
          for (int j = 1; j <= 7; j++) {
            if (j == 1) {
              // first commit should not be archived
              assertTrue(commitsAfterArchival.contains(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j)));
            } else if (j == 2) {
              // 2nd compaction should not be archived
              assertFalse(commitsAfterArchival.contains(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "0000000" + j)));
            } else {
              // every other commit should not be archived
              assertTrue(commitsAfterArchival.contains(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j)));
            }
          }
        }
      }
    }

    // move inflight compaction to complete and add one regular write commit. archival should archive more commits.
    // an extra one commit is required, bcoz compaction in data table will not trigger table services in metadata table.
    // before this move, timeline : 2_inflight_compaction, 3,4,5,6,7.
    // after this move: 5,6,7,8 (2,3,4 will be archived)
    testTable.moveInflightCompactionToComplete("00000002", inflightCompactionMetadata);
    testTable.doWriteOperation("00000008", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), 2);

    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

    List<HoodieInstant> archivedInstants = getAllArchivedCommitInstants(Arrays.asList("00000001", "00000003", "00000004"), HoodieTimeline.DELTA_COMMIT_ACTION);
    archivedInstants.add(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000002"));
    verifyArchival(archivedInstants,
        getActiveCommitInstants(Arrays.asList("00000005", "00000006", "00000007", "00000008"), HoodieTimeline.DELTA_COMMIT_ACTION),
        commitsAfterArchival, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveCommitTimeline(boolean enableMetadataTable) throws Exception {
    init();
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table")
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                .withRemoteServerPort(timelineServicePort).build())
            .withMetadataConfig(
                HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTestDataGenerator.createCommitFile(basePath, "1", storageConf);
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieTestDataGenerator.createCommitFile(basePath, "2", storageConf);
    StoragePath markerPath = new StoragePath(metaClient.getMarkerFolderPath("2"));
    storage.createDirectory(markerPath);
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieTestDataGenerator.createCommitFile(basePath, "3", storageConf);
    HoodieInstant instant3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "3");

    //add 2 more instants to pass filter criteria set in compaction config above
    HoodieTestDataGenerator.createCommitFile(basePath, "4", storageConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "5", storageConf);

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(storageConf, basePath, "5");
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);
    archiver.archiveIfRequired(context);
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<HoodieInstant> archivedInstants = Arrays.asList(instant1, instant2, instant3);
    assertEquals(new HashSet<>(archivedInstants),
        archivedTimeline.filterCompletedInstants().getInstantsAsStream()
            .collect(Collectors.toSet()));
    assertFalse(storage.exists(markerPath));
  }

  @Test
  public void testArchiveMetrics() throws Exception {
    init();
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetricsConfig(HoodieMetricsConfig
                .newBuilder()
                .on(true)
                .withReporterType("INMEMORY")
                .build())
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table").build();
    HoodieMetrics metrics = new HoodieMetrics(cfg, storage);
    BaseHoodieWriteClient client = getHoodieWriteClient(cfg);
    client.archive();
    assertTrue(metrics.getMetrics().getRegistry().getNames().contains(metrics.getMetricsName(ARCHIVE_ACTION, DURATION_STR)));
    assertTrue(metrics.getMetrics().getRegistry().getNames().contains(metrics.getMetricsName(ARCHIVE_ACTION, DELETE_INSTANTS_NUM_STR)));
  }

  private void verifyInflightInstants(HoodieTableMetaClient metaClient, int expectedTotalInstants) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload()
        .getTimelineOfActions(Collections.singleton(CLEAN_ACTION)).filterInflights();
    assertEquals(expectedTotalInstants, timeline.countInstants(),
        "Loaded inflight clean actions and the count should match");
  }

  @Test
  public void testConvertCommitMetadata() throws Exception {
    init();
    HoodieCommitMetadata hoodieCommitMetadata = new HoodieCommitMetadata();
    hoodieCommitMetadata.setOperationType(WriteOperationType.INSERT);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = convertCommitMetadataToAvro(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveTableWithCleanCommits(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 8);
    // Min archival commits is 2 and max archival commits is 4.
    // When metadata table is not enabled, after 5th write instant, archive will be triggered.
    // When metadata table is enabled, after 8th instant (5 write instants + 3 clean instants) >= maxDeltaCommitsMetadataTable,
    // archival kicks in when compaction in metadata table triggered.
    Map<String, Integer> cleanStats = new HashMap<>();
    cleanStats.put("p1", 1);
    cleanStats.put("p2", 2);
    for (int i = 1; i <= 8; i++) {
      if (i == 1) {
        testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 20);
      } else if (i <= 3 || i == 5) {
        testTable.doClean(String.format("%08d", i), cleanStats);
      } else {
        testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      }
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (i <= 7) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else {
        // when i == 8 compaction in metadata table will be triggered, and then allow archive:
        // clean: 2,3,5: all will be archived
        // write: 1,4,6,7,8: after archival -> 7,8
        List<HoodieInstant> expectedActiveInstants = new ArrayList<>(getActiveCommitInstants(Arrays.asList("00000007", "00000008")));
        List<HoodieInstant> expectedArchiveInstants = new ArrayList<>();
        expectedArchiveInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000004", "00000006")));
        expectedArchiveInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000002", "00000003", "00000005"), CLEAN_ACTION));

        verifyArchival(expectedArchiveInstants, expectedActiveInstants, commitsAfterArchival, false);
      }
    }
  }

  @Test
  public void testArchiveTableWithCleanerEarliestSavepoint() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(false, 4, 5, 2);

    // min archival commits is 4 and max archival commits is 5
    // (commits have to be greater than 5)
    // and so, after 6th instant, 2 instants will be archived.
    // Instant 1 -> 10 are commits except 6 which is a clean instant.
    // Clean instants have SAVEPOINTED_TIMESTAMPS set as 3 till clean instant 14.
    // At 16th clean instant, SAVEPOINTED_TIMESTAMPS is set as 10.
    Map<String, Integer> cleanStats = new HashMap<>();
    cleanStats.put("p1", 1);
    cleanStats.put("p2", 2);
    for (int i = 1; i <= 16; i++) {
      if (i == 7) {
        testTable.doCluster(String.format("%08d", i), Collections.emptyMap(), Arrays.asList("p1", "p2"), 20);
      } else if (i != 6 && i <= 12) {
        testTable.doWriteOperation(String.format("%08d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 20);
      } else {
        String savepoint = "00000003";
        if (i == 16) {
          // only 16th clean commit has set savepoint as 10. Before 16th clean commit only commits
          // 1 and 2 should be archived.
          savepoint = "00000010";
        }
        testTable.doClean(String.format("%08d", i), cleanStats, Collections.singletonMap(CleanerUtils.SAVEPOINTED_TIMESTAMPS, savepoint));
      }
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (i <= 6) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i <= 10) {
        // clean instants have not been archived yet, SAVEPOINTED_TIMESTAMPS is set as 3 but since there are no replace commits
        // between earliest savepoint 3 and earliest instant to retain which is 6, archival is not blocked at 3 but at commit 6
        // Archival gets triggered at commit 7 and 9, these would remove commits (1,2) and (3,4) respectively
        // Other commits do not trigger archival since maxInstantsToKeep is 5 and after archival instants are 4
        assertEquals(5 + (i % 2 == 0 ? 1 : 0), commitsAfterArchival.size(), commitsAfterArchival.toString());
      } else if (i < 16) {
        // Archival gets triggered at 11 and it archives commits before 6 since archival is blocked at commit 6
        // due to cleaner earliest instant to retain set at 6
        // clean instants have not been archived yet, SAVEPOINTED_TIMESTAMPS is set as 3 but since there are no replace commits
        // between earliest savepoint 3 and earliest instant to retain which is 6, archival is not blocked at 3 but at commit 6
        assertEquals(i - 5, commitsAfterArchival.size(), commitsAfterArchival.toString());
      } else {
        // At 16th clean instant, SAVEPOINTED_TIMESTAMPS is set as 10
        // There are no replace commits between earliest savepoint (10) and cleaner earliest instant to retain (16)
        // Therefore archival will not be blocked by savepoint
        // active commits were 3,4,5,7,8,9,10,11,12 => After archival only 4 instants would remain 9,10,11,12 based on archival config
        // clean instants were 6,13,14,15,16 => clean instant 6 would be archived since commits instants are archived till commit 8
        List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
        expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000009", "00000010", "00000011", "00000012")));
        expectedActiveInstants.addAll(
            getActiveCommitInstants(Arrays.asList("00000013", "00000014", "00000015", "00000016"), CLEAN_ACTION));
        List<HoodieInstant> expectedArchivedInstants = new ArrayList<>();
        expectedArchivedInstants.addAll(getAllArchivedCommitInstants(
            Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005", "00000008"), HoodieTimeline.COMMIT_ACTION));
        expectedArchivedInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000006"), CLEAN_ACTION));
        expectedArchivedInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000007"), HoodieTimeline.REPLACE_COMMIT_ACTION));
        verifyArchival(expectedArchivedInstants, expectedActiveInstants, commitsAfterArchival, false);
      }
    }
  }

  @Test
  public void testArchiveRollbacksAndCleanTestTable() throws Exception {
    int minArchiveCommits = 2;
    int maxArchiveCommits = 4;
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, minArchiveCommits, maxArchiveCommits, 2);

    List<Pair<String, String>> instants = new ArrayList<>();
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    instants.add(Pair.of(instant1, HoodieTimeline.COMMIT_ACTION));
    // trigger 1 commit to add a lot of files so that future cleans can clean them up
    testTable.doWriteOperation(instant1, WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 20);

    Map<String, Integer> partitionToFileDeleteCount = new HashMap<>();
    partitionToFileDeleteCount.put("p1", 1);
    partitionToFileDeleteCount.put("p2", 1);

    for (int i = 2; i < 5; i++) {
      String cleanInstant = WriteClientTestUtils.createNewInstantTime();
      instants.add(Pair.of(cleanInstant, CLEAN_ACTION));
      testTable.doClean(cleanInstant, partitionToFileDeleteCount);
    }

    // the step size should be the number of new commits yielded in one loop.
    for (int i = 5; i <= 11; i += 2) {
      String commitInstant1 = WriteClientTestUtils.createNewInstantTime();
      instants.add(Pair.of(commitInstant1, HoodieTimeline.COMMIT_ACTION));
      testTable.doWriteOperation(commitInstant1, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      try {
        String rollbackInstant = WriteClientTestUtils.createNewInstantTime();
        testTable.doRollback(commitInstant1, rollbackInstant);
        instants.add(Pair.of(rollbackInstant, HoodieTimeline.ROLLBACK_ACTION));
      } catch (HoodieMetadataException e) {
        // The instant that triggers compaction can not be rolled back
        // because it's instant time is less than the latest compaction instant
        // on the MDT timeline.
        // ignore
      }
    }

    // trigger archival:
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> allCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
    final int archived = 1;
    assertThat("The archived commits number is not as expected", allCommits.size() - commitsAfterArchival.size(), is(archived));

    List<HoodieInstant> expectedArchiveInstants = instants.subList(0, archived).stream()
        .map(p -> INSTANT_GENERATOR.createNewInstant(State.COMPLETED, p.getValue(), p.getKey())).collect(Collectors.toList());
    List<HoodieInstant> expectedActiveInstants = instants.subList(archived, instants.size()).stream()
        .map(p -> INSTANT_GENERATOR.createNewInstant(State.COMPLETED, p.getValue(), p.getKey())).collect(Collectors.toList());

    verifyArchival(expectedArchiveInstants, expectedActiveInstants, commitsAfterArchival, false);
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void testArchiveCompletedRollbackAndClean(boolean isEmpty, boolean enableMetadataTable) throws Exception {
    init();
    int minInstantsToKeep = 2;
    int maxInstantsToKeep = 10;
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table")
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(minInstantsToKeep, maxInstantsToKeep).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    int startInstant = 1;
    List<HoodieInstant> expectedArchivedInstants = new ArrayList<>();
    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant++) {
      createCleanMetadata(String.format("%02d", startInstant), false, false, isEmpty || i % 2 == 0);
      expectedArchivedInstants.add(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, CLEAN_ACTION, String.format("%02d", startInstant)));
    }

    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant += 2) {
      createCommitAndRollbackFile(startInstant + 1 + "", startInstant + "", false, isEmpty || i % 2 == 0);
      expectedArchivedInstants.add(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, String.format("%02d", startInstant)));
    }

    // Clean and rollback instants are archived only till the last clean instant in the timeline
    createCleanMetadata(String.format("%02d", startInstant), false, false, isEmpty);

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(storageConf, basePath, Integer.toString(99));
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);

    archiver.archiveIfRequired(context);

    Stream<HoodieInstant> currentInstants = metaClient.getActiveTimeline().reload().getInstantsAsStream();
    Map<Object, List<HoodieInstant>> actionInstantMap = currentInstants.collect(Collectors.groupingBy(HoodieInstant::getAction));

    // The commit order is: clean, clean, clean, ..., commit, rollback, commit, rollback ...
    // So after archive, actionInstantMap will contain commit and rollback,
    // the number will be equal to minInstantsToKeep
    assertTrue(actionInstantMap.containsKey("commit"), "Commit Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("commit").size(), "Should have min instant");

    assertTrue(actionInstantMap.containsKey("rollback"), "Rollback Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("rollback").size(), "Should have min instant");

    // verify all expected instants are part of archived timeline
    metaClient.getArchivedTimeline().loadCompletedInstantDetailsInMemory();
    HoodieInstant firstInstant = metaClient.reloadActiveTimeline().firstInstant().get();
    expectedArchivedInstants = expectedArchivedInstants.stream()
        .filter(entry -> compareTimestamps(entry.requestedTime(), LESSER_THAN, firstInstant.requestedTime()
        )).collect(Collectors.toList());
    expectedArchivedInstants.forEach(entry -> assertTrue(metaClient.getArchivedTimeline().containsInstant(entry)));
  }

  @Test
  public void testArchiveTableWithMetadataTableCompaction() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 4, 5, 7);

    // min archival commits is 4 and max archival commits is 5. and so, after 6th commit, ideally archival should kick in. but max delta commits in metadata table is set to 7. and so
    // archival will kick in only by 7th commit in datatable(1 commit for bootstrap + 6 commits from data table).
    // and then 2nd compaction will take place
    List<String> instants = new ArrayList<>();
    for (int i = 1; i < 7; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    // one more commit will trigger compaction in metadata table and will let archival move forward.
    String instant7 = WriteClientTestUtils.createNewInstantTime();
    instants.add(instant7);
    testTable.doWriteOperation(instant7, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
    // before archival 1,2,3,4,5,6,7
    // after archival 4,5,6,7
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 3);
    verifyArchival(getAllArchivedCommitInstants(instants.subList(0, 3)),
        getActiveCommitInstants(instants.subList(3, 7)), commitsAfterArchival, false);

    // 3 more commits, 4 to 6 will be archived. but will not move after 6 since compaction has to kick in metadata table.
    for (int i = 0; i < 3; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    }
    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 3);
    verifyArchival(getAllArchivedCommitInstants(instants.subList(0, 6)), getActiveCommitInstants(instants.subList(6, 10)), commitsAfterArchival, false);

    // No archival should kick in since compaction has not kicked in metadata table
    for (int i = 0; i < 2; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    }
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits, commitsAfterArchival);
    verifyArchival(getAllArchivedCommitInstants(instants.subList(0, 6)), getActiveCommitInstants(instants.subList(6, 12)), commitsAfterArchival, false);

    String instant13 = WriteClientTestUtils.createNewInstantTime();
    instants.add(instant13);
    testTable.doWriteOperation(instant13, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits, commitsAfterArchival);

    // one more commit will trigger compaction in metadata table and will let archival move forward.
    String instant14 = WriteClientTestUtils.createNewInstantTime();
    instants.add(instant14);
    testTable.doWriteOperation(instant14, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    // before archival 7,8,9,10,11,12,13,14
    // after archival 11,12,13,14
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 4);
    verifyArchival(getAllArchivedCommitInstants(instants.subList(0, 10)), getActiveCommitInstants(instants.subList(10, 14)), commitsAfterArchival, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchivalWithMaxDeltaCommitsGuaranteeForCompaction(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(
        enableMetadata, 4, 5, 8, 1, HoodieTableType.MERGE_ON_READ);

    // When max archival commits is set to 5, even after 8 delta commits, since the number of delta
    // commits is still smaller than 8, the archival should not kick in.
    // The archival should only kick in after the 9th delta commit
    // instant 1 to 9
    List<String> instants = new ArrayList<>();
    for (int i = 1; i < 10; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, i == 1
          ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      if (i <= 8) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else {
        assertEquals(1, originalCommits.size() - commitsAfterArchival.size());
        assertFalse(commitsAfterArchival.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(0))));
        IntStream.range(2, 10).forEach(j ->
            assertTrue(commitsAfterArchival.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      }
    }

    String compactionInstant = WriteClientTestUtils.createNewInstantTime();
    instants.add(compactionInstant);
    testTable.doCompaction(compactionInstant, Arrays.asList("p1", "p2"));

    // instant 11 to 19
    for (int i = 1; i < 10; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      testTable.doWriteOperation(instant, WriteOperationType.UPSERT, i == 1
          ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      if (i <= 2) {
        // first 7 delta commits before the completed compaction should be archived in data table
        IntStream.range(1, 8).forEach(j ->
            assertFalse(commitsAfterArchival.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
        assertEquals(i == 1 ? 6 : 0, originalCommits.size() - commitsAfterArchival.size());
        // instant from 11 should be in the active timeline
        assertTrue(commitsAfterArchival.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(7))));
        assertTrue(commitsAfterArchival.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(8))));
        assertTrue(commitsAfterArchival.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, compactionInstant)));
        for (int j = 1; j <= i; j++) {
          assertTrue(commitsAfterArchival.contains(
              INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(9 + j))));
        }
      } else {
        // first 9 delta commits before the completed compaction should be archived in data table
        IntStream.range(1, 10).forEach(j ->
            assertFalse(commitsAfterArchival.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
        if (i == 3) {
          assertEquals(2, originalCommits.size() - commitsAfterArchival.size());
        } else if (i < 8) {
          assertEquals(originalCommits, commitsAfterArchival);
        } else {
          assertEquals(1, originalCommits.size() - commitsAfterArchival.size());
          assertFalse(commitsAfterArchival.contains(
              INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, compactionInstant)));
          // i == 8 -> [11, 18] should be in the active timeline
          // i == 9 -> [12, 19] should be in the active timeline
          if (i == 9) {
            assertFalse(commitsAfterArchival.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(10))));
          }
          IntStream.range(i - 7, i + 1).forEach(j ->
              assertTrue(commitsAfterArchival.contains(
                  INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(9 + j)))));
        }
      }
    }
  }

  /**
   * Test archival functionality when there are inflights files.
   * Archive should hold on to the greatest completed commit that is less than the oldes inflight commit.
   *
   * @throws Exception
   */
  @Test
  public void testGetCommitInstantsToArchiveDuringInflightCommits() throws Exception {
    HoodieWriteConfig cfg = initTestTableAndGetWriteConfig(false, 3, 4, 2);

    Set<String> expectedInstants = new HashSet<>();
    // Create 3 completed commits.
    for (int i = 0; i < 3; i++) {
      String instantTime = "100" + i;
      HoodieTestDataGenerator.createCommitFile(basePath, instantTime, storageConf);
      expectedInstants.add(instantTime);
    }
    // Create an inflight file.
    String replaceInstant = "1003";
    HoodieTestDataGenerator.createReplaceCommitRequestedFile(basePath, replaceInstant, storageConf);
    expectedInstants.add(replaceInstant);
    // Create 3 more instants
    for (int i = 4; i < 7; i++) {
      String instantTime = "100" + i;
      HoodieTestDataGenerator.createCommitFile(basePath, instantTime, storageConf);
      expectedInstants.add(instantTime);
    }
    // Create another inflight commit
    HoodieTestDataGenerator.createRequestedCommitFile(basePath, "1007", storageConf);
    HoodieTestDataGenerator.createPendingCommitFile(basePath, "1007", storageConf);
    expectedInstants.add("1007");
    // Create 6 more instants
    for (int i = 0; i < 6; i++) {
      String instantTime = "101" + i;
      HoodieTestDataGenerator.createCommitFile(basePath, instantTime, storageConf);
      expectedInstants.add(instantTime);
    }
    HoodieTimeline timeline = metaClient.reloadActiveTimeline().getWriteTimeline();

    // Check the count of instants.
    assertEquals(expectedInstants.size(), timeline.countInstants(), "Loaded 14 commits and the count should match");

    // Run archival
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);
    archiver.archiveIfRequired(context);
    expectedInstants.remove("1000");
    expectedInstants.remove("1001");
    timeline = metaClient.reloadActiveTimeline().getWriteTimeline();

    // Check the count of instants after archive it should have 2 less instants
    // because 103 replacecommit's inflight will block archival.
    assertEquals(12, timeline.countInstants(), "After archival only first 2 commits should be archived");
    assertEquals(expectedInstants.size(), timeline.countInstants(), "After archival only first 2 commits should be archived");

    HoodieTimeline finalTimeline = timeline;
    assertEquals(12, expectedInstants.stream().filter(instant -> finalTimeline.containsInstant(instant)).count());
    assertEquals("1002", timeline.getInstantsAsStream().findFirst().get().requestedTime());

    // Delete replacecommit requested instant.
    StoragePath replaceCommitRequestedPath = new StoragePath(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
            + INSTANT_FILE_NAME_GENERATOR.makeRequestedReplaceFileName(replaceInstant));
    metaClient.getStorage().deleteDirectory(replaceCommitRequestedPath);
    metaClient.reloadActiveTimeline();

    // Run archival
    archiver.archiveIfRequired(context);
    timeline = metaClient.reloadActiveTimeline().getWriteTimeline();
    expectedInstants.removeAll(Arrays.asList("1002", "1003", "1004", "1005"));

    // Check the count of instants after archive it should have 3 more less instants
    // This time 1007 inflight commit will block archival.
    assertEquals(8, timeline.countInstants(), "After archival only first 2 commits should be archived");
    assertEquals(expectedInstants.size(), timeline.countInstants(), "After archival only first 2 commits should be archived");
    HoodieTimeline refreshedTimeline = timeline;
    assertEquals(8, expectedInstants.stream().filter(instant -> refreshedTimeline.containsInstant(instant)).count());
    assertEquals("1006", timeline.getInstantsAsStream().findFirst().get().requestedTime());
  }

  /**
   * If replacecommit inflight is the oldest commit in the timeline or for that matter any inflight commit is present
   * then the archival is blocked from there. This method test this scenario.
   */
  @Test
  public void testWithOldestReplaceCommit() throws Exception {
    HoodieWriteConfig cfg = initTestTableAndGetWriteConfig(false, 2, 3, 2);

    HoodieTestDataGenerator.createReplaceCommitRequestedFile(basePath, "1001", storageConf);
    HoodieTestDataGenerator.createReplaceCommitInflightFile(basePath, "1001", storageConf);
    // Create 8 completed commits.
    for (int i = 2; i < 10; i++) {
      String instantTime = "100" + i;
      HoodieTestDataGenerator.createCommitFile(basePath, instantTime, storageConf);
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(cfg, table);

    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    assertEquals(9, timeline.countInstants(), "Loaded 9 commits and the count should match");
    archiver.archiveIfRequired(context);
    timeline = metaClient.reloadActiveTimeline();
    assertEquals(9, timeline.countInstants(),
        "Since we have a pending replacecommit at 1001, we should never archive any commit after 1001");
    assertEquals("1001", timeline.getInstantsAsStream().findFirst().get().requestedTime());
  }

  @Test
  public void testArchivalAndCompactionInMetadataTable() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Test configs where metadata table has more aggressive archival configs than the compaction config
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 6).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(8)
            .withMetadataIndexColumnStats(false) // test uses test table infra. So, col stats is not available/populated.
            .build())
        .forTable("test-trip-table").build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.createMetaClient(
        metaClient.getStorageConf(), HoodieTableMetadata.getMetadataTableBasePath(basePath));

    List<String> instants = new ArrayList<>();
    for (int i = 1; i <= 19; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      if (i != 2) {
        testTable.doWriteOperation(instant, WriteOperationType.UPSERT,
            i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      } else {
        // For i == 2, roll back the first commit 1, so the active timeline of the
        // data table has one rollback instant
        // The completed rollback should not block the archival in the metadata table
        testTable.doRollback(instants.get(0), instant);
      }
      // archival
      archiveAndGetCommitsList(writeConfig);

      metadataTableMetaClient = HoodieTableMetaClient.reload(metadataTableMetaClient);
      List<HoodieInstant> metadataTableInstants = metadataTableMetaClient.getActiveTimeline()
          .getCommitsTimeline().filterCompletedInstants().getInstants();

      final String mdtInitCommit = HoodieInstantTimeGenerator.instantTimePlusMillis(SOLO_COMMIT_TIMESTAMP, 0L);
      if (i == 1) {
        // In the metadata table timeline, the first delta commit is "00000000000000000"
        assertEquals(i + 1, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, mdtInitCommit)));
        assertTrue(metadataTableInstants.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(0))));
      } else if (i == 2) {
        assertEquals(i - 1, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, mdtInitCommit)));
        assertFalse(metadataTableInstants.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(1))));
      } else if (i <= 9) {
        // In the metadata table timeline, the first delta commit is "00000000000000000"
        // from metadata table init, delta commits 1 till 8 are added
        // later on without archival or compaction
        // rollback in DT will also trigger rollback in MDT
        assertEquals(i - 1, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, mdtInitCommit)));
        // rolled back commits may not be present in MDT timeline [1]
        IntStream.range(3, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      } else if (i == 10) {
        // i == 10
        // The instant "00000000000000000" was archived since it's less than
        // the earliest commit on the dataset active timeline,
        // the dataset active timeline has instants:
        //   [7.commit, 8.commit, 9.commit, 10.commit]
        assertEquals(9, metadataTableInstants.size());
        // mdt timeline 3,..., 10, a completed compaction commit
        IntStream.range(3, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      } else if (i <= 13) {
        // In the metadata table timeline, the first delta commit is 7
        // because it equals with the earliest commit on the dataset timeline, after archival,
        // delta commits 11 till 12 are added later on without archival or compaction
        // mdt timeline [7, 8, 9, 10, a completed compaction commit] for i = 10
        assertEquals(i - 5, metadataTableInstants.size());
        assertEquals(1, metadataTableMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants());
        IntStream.range(7, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      } else if (i <= 17) {
        // In the metadata table timeline, the second commit is a compaction commit
        // from metadata table compaction, after archival, delta commits 14
        // till 17 are added later on without archival or compaction
        // mdt timeline: [10, a completed compaction commit, 11, ... 14, 15, ... 17]
        assertEquals(i - 8, metadataTableInstants.size());
        assertEquals(1, metadataTableMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants());
        IntStream.range(10, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      } else if (i == 18) {
        // i == 18
        // commits in MDT [10, a completed compaction commit, 11, ... 17, 18, a completed compaction commit]
        // another compaction is triggered by this commit so everything upto 18 is compacted.
        assertEquals(11, metadataTableInstants.size());
        assertEquals(2, metadataTableMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants());
        IntStream.range(10, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      } else {
        // i == 19
        // compaction happened in last commit, and archival is triggered with latest compaction retained plus maxInstantToKeep = 6
        // commits in MDT [14, .... 17, a completed compaction commit, 18]
        assertEquals(6, metadataTableInstants.size());
        assertTrue(metadata(writeConfig, context).getLatestCompactionTime().isPresent());
        IntStream.range(15, i).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(j - 1)))));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPendingClusteringAfterArchiveCommit(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 4, 5, 2);
    // timeline:0000000(completed)->00000001(completed)->00000002(replace&inflight)->00000003(completed)->...->00000007(completed)
    HoodieTestDataGenerator.createPendingClusterFile(basePath, "00000002", storageConf);
    for (int i = 1; i < 8; i++) {
      if (i != 2) {
        testTable.doWriteOperation("0000000" + i, WriteOperationType.CLUSTER, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2);
      }
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    HoodieTimeline timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(),
        "Since we have a pending clustering instant at 00000002, we should never archive any commit after 00000000");
  }

  @Test
  public void testRetryArchivalAfterPreviousFailedDeletion() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 4, 2);
    for (int i = 0; i <= 5; i++) {
      testTable.doWriteOperation("10" + i, WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), 1);
    }
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(writeConfig, table);

    HoodieTimeline timeline = metaClient.getActiveTimeline().getWriteTimeline();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    assertTrue(archiver.archiveIfRequired(context) > 0);
    // Simulate archival failing to delete by re-adding the .commit instant files
    // (101.commit, 102.commit, and 103.commit instant files)
    HoodieTestDataGenerator.createOnlyCompletedCommitFile(basePath, "101_1001", storage.getConf());
    HoodieTestDataGenerator.createOnlyCompletedCommitFile(basePath, "102_1021", storage.getConf());
    HoodieTestDataGenerator.createOnlyCompletedCommitFile(basePath, "103_1031", storage.getConf());
    timeline = metaClient.getActiveTimeline().reload().getWriteTimeline();
    assertEquals(5, timeline.countInstants(), "Due to simulating partial archival deletion, there should"
        + "be 5 instants (as instant times 101-103 .commit files should remain in timeline)");
    // Re-running archival again should archive and delete the 101.commit, 102.commit, and 103.commit instant files
    table.getMetaClient().reloadActiveTimeline();
    table = HoodieSparkTable.create(writeConfig, context, metaClient);
    archiver = new TimelineArchiverV2(writeConfig, table);
    assertTrue(archiver.archiveIfRequired(context) > 0);
    timeline = metaClient.getActiveTimeline().reload().getWriteTimeline();
    assertEquals(2, timeline.countInstants(), "The instants from prior archival should "
        + "be deleted now");
  }

  /**
   * IMPORTANT: this method is only suitable for one time trigger of archival validation.
   */
  private Pair<List<HoodieInstant>, List<HoodieInstant>> archiveAndGetCommitsList(HoodieWriteConfig writeConfig) throws IOException {
    return archiveAndGetCommitsList(writeConfig, false);
  }

  private void disableMetadataTable(HoodieWriteConfig writeConfig) {
    writeConfig.setValue(HoodieMetadataConfig.ENABLE.key(), "false");
    writeConfig.getMetadataConfig().setValue(HoodieMetadataConfig.ENABLE.key(), "false");
  }

  private Pair<List<HoodieInstant>, List<HoodieInstant>> archiveAndGetCommitsList(
      HoodieWriteConfig writeConfig, boolean includeIncompleteInstants) throws IOException {
    metaClient.reloadActiveTimeline();
    HoodieTimeline timeline = includeIncompleteInstants
        ? metaClient.getActiveTimeline().reload().getAllCommitsTimeline()
        : metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants();
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    TimelineArchiverV2 archiver = new TimelineArchiverV2(writeConfig, table);
    archiver.archiveIfRequired(context);
    timeline = includeIncompleteInstants
        ? metaClient.getActiveTimeline().reload().getAllCommitsTimeline()
        : metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> commitsAfterArchival = timeline.getInstants();
    return Pair.of(originalCommits, commitsAfterArchival);
  }

  private void verifyArchival(List<HoodieInstant> expectedArchivedInstants, List<HoodieInstant> expectedActiveInstants, List<HoodieInstant> commitsAfterArchival, boolean isArchivalBeyondSavepoint) {
    expectedActiveInstants.sort(Comparator.comparing(HoodieInstant::requestedTime));
    commitsAfterArchival.sort(Comparator.comparing(HoodieInstant::requestedTime));
    assertEquals(expectedActiveInstants, commitsAfterArchival);
    expectedArchivedInstants.forEach(entry -> assertFalse(commitsAfterArchival.contains(entry)));
    HoodieArchivedTimeline archivedTimeline = TIMELINE_FACTORY.createArchivedTimeline(metaClient);
    List<HoodieInstant> actualArchivedInstants = archivedTimeline.getInstants();
    actualArchivedInstants.sort(Comparator.comparing(HoodieInstant::requestedTime));
    expectedArchivedInstants.sort(Comparator.comparing(HoodieInstant::requestedTime));
    assertEquals(actualArchivedInstants, expectedArchivedInstants);

    HoodieTimeline timeline = metaClient.getActiveTimeline();
    expectedArchivedInstants.forEach(entry -> {
          // check safety
          // when isArchivalBeyondSavepoint is set to true, there is a case possible where archival instant
          // is after the first completed instant in the timeline. Lets say savepoint was taken at commit c1
          // and commit c1 is a deltacommit which is not archived but c2 got archived. If savepoint is deleted
          // then containsOrBeforeTimelineStarts check would fail for c2 archived commit as the function compares
          // against first non savepoint commit in the timeline which is now deleted. With c1 it would succeed but
          // once c1 is removed, it would start failing.
          if (!entry.getAction().equals(HoodieTimeline.ROLLBACK_ACTION) && !isArchivalBeyondSavepoint) {
            assertTrue(timeline.containsOrBeforeTimelineStarts(entry.requestedTime()), "Archived commits should always be safe");
          }
        }
    );
  }

  private List<HoodieInstant> getAllArchivedCommitInstants(List<String> commitTimes) {
    return getAllArchivedCommitInstants(commitTimes, HoodieTimeline.COMMIT_ACTION);
  }

  private List<HoodieInstant> getAllArchivedCommitInstants(List<String> commitTimes, String action) {
    List<HoodieInstant> allInstants = new ArrayList<>();
    commitTimes.forEach(commitTime -> allInstants.add(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, action, commitTime)));
    return allInstants;
  }

  private List<HoodieInstant> getActiveCommitInstants(List<String> commitTimes) {
    return getActiveCommitInstants(commitTimes, HoodieTimeline.COMMIT_ACTION);
  }

  private List<HoodieInstant> getActiveSavepointedCommitInstants(List<String> commitTimes) {
    return getActiveCommitInstants(commitTimes, HoodieTimeline.SAVEPOINT_ACTION);
  }

  private List<HoodieInstant> getActiveCommitInstants(List<String> commitTimes, String action) {
    List<HoodieInstant> allInstants = new ArrayList<>();
    commitTimes.forEach(entry -> allInstants.add(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, action, entry)));
    return allInstants;
  }

  private void createCommitAndRollbackFile(String commitToRollback, String rollbackTIme, boolean isRollbackInflight) throws IOException {
    createCommitAndRollbackFile(commitToRollback, rollbackTIme, isRollbackInflight, false);
  }

  private void createCommitAndRollbackFile(String commitToRollback, String rollbackTIme, boolean isRollbackInflight, boolean isEmpty) throws IOException {
    HoodieTestDataGenerator.createCommitFile(basePath, commitToRollback, storageConf);
    createRollbackMetadata(rollbackTIme, commitToRollback, isRollbackInflight, isEmpty);
  }

  private HoodieInstant createRollbackMetadata(String rollbackTime, String commitToRollback, boolean inflight, boolean isEmpty) throws IOException {
    if (inflight) {
      HoodieTestTable.of(metaClient).addInflightRollback(rollbackTime);
    } else {
      HoodieRollbackMetadata hoodieRollbackMetadata = HoodieRollbackMetadata.newBuilder()
          .setVersion(1)
          .setStartRollbackTime(rollbackTime)
          .setTotalFilesDeleted(1)
          .setTimeTakenInMillis(1000)
          .setCommitsRollback(Collections.singletonList(commitToRollback))
          .setPartitionMetadata(Collections.emptyMap())
          .setInstantsRollback(Collections.emptyList())
          .build();
      HoodieTestTable.of(metaClient).addRollback(rollbackTime, hoodieRollbackMetadata, isEmpty, null);
      HoodieTestTable.of(metaClient).addRollbackCompleted(rollbackTime, hoodieRollbackMetadata, isEmpty);
    }
    return INSTANT_GENERATOR.createNewInstant(inflight ? State.INFLIGHT : State.COMPLETED, "rollback", rollbackTime);
  }

  private void assertInstantListEquals(List<HoodieInstant> expected, List<HoodieInstant> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      HoodieInstant expectedInstant = expected.get(i);
      HoodieInstant actualInstant = actual.get(i);
      assertEquals(expectedInstant.requestedTime(), actualInstant.requestedTime());
      assertEquals(expectedInstant.getAction(), actualInstant.getAction());
      assertEquals(expectedInstant.getState(), actualInstant.getState());
    }
  }
}
