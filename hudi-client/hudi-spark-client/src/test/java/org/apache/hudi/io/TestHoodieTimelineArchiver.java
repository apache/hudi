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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createCompactionCommitInMetadataTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTimelineArchiver extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieTimelineArchiver.class);

  private Configuration hadoopConf;
  private HoodieWrapperFileSystem wrapperFs;
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
    hadoopConf = context.getHadoopConf().get();
    metaClient.getFs().mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);
    wrapperFs = metaClient.getFs();
    hadoopConf.addResource(wrapperFs.getConf());
  }

  private void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) {
    if (enableMetadataTable) {
      metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, writeConfig, context);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
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
        maxDeltaCommits, maxDeltaCommitsMetadataTable, tableType, false, 10, 209715200,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits,
        5, maxDeltaCommitsMetadataTable, tableType, false, 10, 209715200,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           boolean enableArchiveMerge,
                                                           int archiveFilesBatch,
                                                           long size) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits, 5,
        maxDeltaCommitsMetadataTable, HoodieTableType.COPY_ON_WRITE, enableArchiveMerge, archiveFilesBatch, size,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER);
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType,
                                                           boolean enableArchiveMerge,
                                                           int archiveFilesBatch,
                                                           long size,
                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                           WriteConcurrencyMode writeConcurrencyMode) throws Exception {
    return initTestTableAndGetWriteConfig(enableMetadata, minArchivalCommits, maxArchivalCommits, maxDeltaCommits, maxDeltaCommitsMetadataTable,
        tableType, enableArchiveMerge, archiveFilesBatch, size, failedWritesCleaningPolicy, writeConcurrencyMode, HoodieCompactionConfig.ARCHIVE_PROCEED_BEYOND_SAVEPOINTS.defaultValue());
  }

  private HoodieWriteConfig initTestTableAndGetWriteConfig(boolean enableMetadata,
                                                           int minArchivalCommits,
                                                           int maxArchivalCommits,
                                                           int maxDeltaCommits,
                                                           int maxDeltaCommitsMetadataTable,
                                                           HoodieTableType tableType,
                                                           boolean enableArchiveMerge,
                                                           int archiveFilesBatch,
                                                           long size,
                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                           WriteConcurrencyMode writeConcurrencyMode,
                                                           boolean shouldArhivalProceedBeyondSavepoints) throws Exception {
    init(tableType);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minArchivalCommits, maxArchivalCommits)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits)
            .withArchiveMergeEnable(enableArchiveMerge)
            .withArchiveMergeFilesBatchSize(archiveFilesBatch)
            .withArchiveMergeSmallFileLimit(size)
            .withFailedWritesCleaningPolicy(failedWritesCleaningPolicy)
            .withArhiveProceedBeyondArchival(shouldArhivalProceedBeyondSavepoints)
            .build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsMetadataTable).build())
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
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    boolean result = archiver.archiveIfRequired(context);
    assertTrue(result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveTableWithArchival(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 2);

    // min archival commits is 2 and max archival commits is 4. and so, after 5th commit, 3 commits will be archived.
    // 1,2,3,4,5 : after archival -> 4,5
    // after 3 more commits, earliest 3 will be archived
    // 4,5,6,7,8 : after archival -> 7, 8
    // after 9 no-op wrt archival.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (i < 5) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i == 5) {
        // archival should have kicked in.
        verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003")), getActiveCommitInstants(Arrays.asList("00000004", "00000005")), commitsAfterArchival);
      } else if (i < 8) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i == 8) {
        // archival should have kicked in.
        verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005", "00000006")),
            getActiveCommitInstants(Arrays.asList("00000007", "00000008")), commitsAfterArchival);
      } else {
        assertEquals(originalCommits, commitsAfterArchival);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSavepointsWithArchival(boolean shouldArchiveBeyondSavepoints) throws Exception {
    boolean enableMetadata = false;
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 5, 2, HoodieTableType.COPY_ON_WRITE,
        false, 10, 209715200,
        HoodieFailedWritesCleaningPolicy.EAGER, WriteConcurrencyMode.SINGLE_WRITER, shouldArchiveBeyondSavepoints);

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

    if (shouldArchiveBeyondSavepoints) {
      // retains only 2 commits. C3 and C8. and savepointed commit for C3.
      verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000004", "00000005")),
          Stream.concat(getActiveCommitInstants(Arrays.asList("00000003", "00000006")).stream(), getActiveSavepointedCommitInstants(Arrays.asList("00000003")).stream())
              .collect(Collectors.toList()), commitsAfterArchival);
    } else {
      // archives only C1 and C2. stops at first savepointed commit C3.
      verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002")),
          Stream.concat(getActiveCommitInstants(Arrays.asList("00000003", "00000004", "00000005", "00000006")).stream(),
                  getActiveSavepointedCommitInstants(Arrays.asList("00000003")).stream())
              .collect(Collectors.toList()), commitsAfterArchival);
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
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002","00000003", "00000004", "00000005", "00000006", "00000007")),
        getActiveCommitInstants(Arrays.asList("00000008", "00000009")), commitsAfterArchival);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeSmallArchiveFilesRecoverFromBuildPlanFailed(boolean enableArchiveMerge) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 3, 2, enableArchiveMerge, 3, 209715200);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // build a merge small archive plan with dummy content
    // this plan can not be deserialized.
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    FileStatus[] fsStatuses = metaClient.getFs().globStatus(
        new Path(metaClient.getArchivePath() + "/.commits_.archive*"));
    List<String> candidateFiles = Arrays.stream(fsStatuses).map(fs -> fs.getPath().toString()).collect(Collectors.toList());

    archiver.reOpenWriter();
    Path plan = new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
    archiver.buildArchiveMergePlan(candidateFiles, plan, ".commits_.archive.3_1-0-1");
    String s = "Dummy Content";
    // stain the current merge plan file.
    FileIOUtils.createFileInPath(metaClient.getFs(), plan, Option.of(s.getBytes()));

    // check that damaged plan file will not block archived timeline loading.
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(7 * 3, rawActiveTimeline.countInstants() + archivedTimeLine.countInstants());

    // trigger several archive after left damaged merge small archive file plan.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("1000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // loading archived timeline and active timeline success
    HoodieActiveTimeline rawActiveTimeline1 = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine1 = metaClient.getArchivedTimeline().reload();

    // check instant number
    assertEquals(16 * 3, archivedTimeLine1.countInstants() + rawActiveTimeline1.countInstants());

    // if there are damaged archive files and damaged plan, hoodie need throw ioe while loading archived timeline.
    Path damagedFile = new Path(metaClient.getArchivePath(), ".commits_.archive.300_1-0-1");
    FileIOUtils.createFileInPath(metaClient.getFs(), damagedFile, Option.of(s.getBytes()));

    assertThrows(HoodieException.class, () -> metaClient.getArchivedTimeline().reload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeSmallArchiveFilesRecoverFromMergeFailed(boolean enableArchiveMerge) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 3, 2, enableArchiveMerge, 3, 209715200);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // do a single merge small archive files
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    FileStatus[] fsStatuses = metaClient.getFs().globStatus(
        new Path(metaClient.getArchivePath() + "/.commits_.archive*"));
    List<String> candidateFiles = Arrays.stream(fsStatuses).map(fs -> fs.getPath().toString()).collect(Collectors.toList());
    archiver.reOpenWriter();

    archiver.buildArchiveMergePlan(candidateFiles, new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME), ".commits_.archive.3_1-0-1");
    archiver.mergeArchiveFiles(Arrays.stream(fsStatuses).collect(Collectors.toList()));
    HoodieLogFormat.Writer writer = archiver.reOpenWriter();

    // check loading archived and active timeline success
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(7 * 3, rawActiveTimeline.countInstants() + archivedTimeLine.reload().countInstants());

    String s = "Dummy Content";
    // stain the current merged archive file.
    FileIOUtils.createFileInPath(metaClient.getFs(), writer.getLogFile().getPath(), Option.of(s.getBytes()));

    // do another archive actions with merge small archive files.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("1000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // check result.
    // we need to load archived timeline successfully and ignore the parsing damage merged archive files exception.
    HoodieActiveTimeline rawActiveTimeline1 = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine1 = metaClient.getArchivedTimeline().reload();

    assertEquals(16 * 3, archivedTimeLine1.countInstants() + rawActiveTimeline1.countInstants());

    // if there are a damaged merged archive files and other common damaged archive file.
    // hoodie need throw ioe while loading archived timeline because of parsing the damaged archive file.
    Path damagedFile = new Path(metaClient.getArchivePath(), ".commits_.archive.300_1-0-1");
    FileIOUtils.createFileInPath(metaClient.getFs(), damagedFile, Option.of(s.getBytes()));

    assertThrows(HoodieException.class, () -> metaClient.getArchivedTimeline().reload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeSmallArchiveFilesRecoverFromDeleteFailed(boolean enableArchiveMerge) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 3, 2, enableArchiveMerge, 3, 209715200);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // do a single merge small archive files
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    FileStatus[] fsStatuses = metaClient.getFs().globStatus(
        new Path(metaClient.getArchivePath() + "/.commits_.archive*"));
    List<String> candidateFiles = Arrays.stream(fsStatuses).map(fs -> fs.getPath().toString()).collect(Collectors.toList());

    archiver.reOpenWriter();

    archiver.buildArchiveMergePlan(candidateFiles, new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME), ".commits_.archive.3_1-0-1");
    archiver.mergeArchiveFiles(Arrays.stream(fsStatuses).collect(Collectors.toList()));
    archiver.reOpenWriter();

    // delete only one of the small archive file to simulate delete action failed.
    metaClient.getFs().delete(fsStatuses[0].getPath());

    // loading archived timeline and active timeline success
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(7 * 3, rawActiveTimeline.countInstants() + archivedTimeLine.countInstants());

    // do another archive actions with merge small archive files.
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("1000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    // check result.
    HoodieActiveTimeline rawActiveTimeline1 = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine1 = metaClient.getArchivedTimeline().reload();

    assertEquals(16 * 3, archivedTimeLine1.countInstants() + rawActiveTimeline1.countInstants());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLoadArchiveTimelineWithDamagedPlanFile(boolean enableArchiveMerge) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 3, 2, enableArchiveMerge, 3, 209715200);

    // do ingestion and trigger archive actions here.
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    Path plan = new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
    String s = "Dummy Content";
    // stain the current merge plan file.
    FileIOUtils.createFileInPath(metaClient.getFs(), plan, Option.of(s.getBytes()));

    // check that damaged plan file will not block archived timeline loading.
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine = metaClient.getArchivedTimeline().reload();
    assertEquals(7 * 3, rawActiveTimeline.countInstants() + archivedTimeLine.countInstants());

    // if there are damaged archive files and damaged plan, hoodie need throw ioe while loading archived timeline.
    Path damagedFile = new Path(metaClient.getArchivePath(), ".commits_.archive.300_1-0-1");
    FileIOUtils.createFileInPath(metaClient.getFs(), damagedFile, Option.of(s.getBytes()));

    assertThrows(HoodieException.class, () -> metaClient.getArchivedTimeline().reload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchivalWithMultiWriters(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 5, 2,
        HoodieTableType.COPY_ON_WRITE, false, 10, 209715200,
        HoodieFailedWritesCleaningPolicy.LAZY, WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);

    final ExecutorService executors = Executors.newFixedThreadPool(2);
    List<CompletableFuture<Boolean>> completableFutureList = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(1);
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
        while (!metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get().getTimestamp().endsWith("29")
            || metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() > 4) {
          try {
            HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
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
    for (int i = 1; i < 30; i++) {
      testTable.doWriteOperation("0000000" + String.format("%02d", i), WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      if (i == 5) {
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

  public static CompletableFuture allOfTerminateOnFailure(List<CompletableFuture<Boolean>> futures) {
    CompletableFuture<?> failure = new CompletableFuture();
    AtomicBoolean jobFailed = new AtomicBoolean(false);
    for (CompletableFuture<?> f : futures) {
      f.exceptionally(ex -> {
        if (!jobFailed.getAndSet(true)) {
          LOG.warn("One of the job failed. Cancelling all other futures. " + ex.getCause() + ", " + ex.getMessage());
          futures.forEach(future -> future.cancel(true));
        }
        return null;
      });
    }
    return CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLoadArchiveTimelineWithUncompletedMergeArchiveFile(boolean enableArchiveMerge) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 3, 2, enableArchiveMerge, 3, 209715200);
    for (int i = 1; i < 8; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      archiveAndGetCommitsList(writeConfig);
    }

    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    FileStatus[] fsStatuses = metaClient.getFs().globStatus(
        new Path(metaClient.getArchivePath() + "/.commits_.archive*"));
    List<String> candidateFiles = Arrays.stream(fsStatuses).map(fs -> fs.getPath().toString()).collect(Collectors.toList());

    archiver.reOpenWriter();

    archiver.buildArchiveMergePlan(candidateFiles, new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME), ".commits_.archive.3_1-0-1");
    archiver.mergeArchiveFiles(Arrays.stream(fsStatuses).collect(Collectors.toList()));
    HoodieLogFormat.Writer writer = archiver.reOpenWriter();

    String s = "Dummy Content";
    // stain the current merged archive file.
    FileIOUtils.createFileInPath(metaClient.getFs(), writer.getLogFile().getPath(), Option.of(s.getBytes()));

    // if there's only a damaged merged archive file, we need to ignore the exception while reading this damaged file.
    HoodieActiveTimeline rawActiveTimeline1 = new HoodieActiveTimeline(metaClient, false);
    HoodieArchivedTimeline archivedTimeLine1 = metaClient.getArchivedTimeline();

    assertEquals(7 * 3, archivedTimeLine1.countInstants() + rawActiveTimeline1.countInstants());

    // if there are a damaged merged archive files and other common damaged archive file.
    // hoodie need throw ioe while loading archived timeline because of parsing the damaged archive file.
    Path damagedFile = new Path(metaClient.getArchivePath(), ".commits_.archive.300_1-0-1");
    FileIOUtils.createFileInPath(metaClient.getFs(), damagedFile, Option.of(s.getBytes()));

    assertThrows(HoodieException.class, () -> metaClient.getArchivedTimeline().reload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNoArchivalUntilMaxArchiveConfigWithExtraInflightCommits(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 5, 2);

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
    return Arrays.stream(new Boolean[][] {
        {true, true},
        {false, true},
        {true, false},
        {false, false}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("archiveCommitSavepointNoHoleParams")
  public void testArchiveCommitSavepointNoHole(boolean enableMetadataTable, boolean archiveBeyondSavepoints) throws Exception {
    init();
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).withArhiveProceedBeyondArchival(archiveBeyondSavepoints).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .build();

    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createSavepointFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", wrapperFs.getConf());
    HoodieTable table = HoodieSparkTable.create(cfg, context);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf, wrapperFs, basePath, "105");
    }

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    assertTrue(archiver.archiveIfRequired(context));
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    if (archiveBeyondSavepoints) {
      // commits in active timeline = 101 and 105.
      assertEquals(2, timeline.countInstants(),
          "Since archiveBeyondSavepoint config is enabled, we will archive commits 102, 103 ");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101")),
          "Savepointed commits should always be safe");
      assertFalse(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")),
          "102 expected to be archived");
      assertFalse(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")),
          "103 expected to be archived");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "105")),
          "104 expected to be archived");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "105")),
          "105 expected to be in active timeline");
    } else {
      assertEquals(5, timeline.countInstants(),
          "Since we have a savepoint at 101, we should never archive any commit after 101 (we only archive 100)");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101")),
          "Archived commits should always be safe");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")),
          "Archived commits should always be safe");
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")),
          "Archived commits should always be safe");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPendingClusteringWillBlockArchival(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 5, 2);
    HoodieTestDataGenerator.createPendingReplaceFile(basePath, "00000000", wrapperFs.getConf());
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
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 3, 2);

    for (int i = 1; i < 9; i += 2) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      testTable.doRollback("0000000" + i, "0000000" + (i + 1));

      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      if (i != 7) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else {
        // only time when archival will kick in
        List<HoodieInstant> expectedArchivedInstants = new ArrayList<>();
        expectedArchivedInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000003")));
        expectedArchivedInstants.addAll(getAllArchivedCommitInstants(Arrays.asList("00000002", "00000004"), HoodieTimeline.ROLLBACK_ACTION));
        List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
        expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000005", "00000007")));
        expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000006", "00000008"), HoodieTimeline.ROLLBACK_ACTION));
        verifyArchival(expectedArchivedInstants, expectedActiveInstants, commitsAfterArchival);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNoArchivalWithInflightCompactionInMiddle(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 2, 2,
        HoodieTableType.MERGE_ON_READ);

    // when max archival commits is set to 4, even after 7 commits, if there is an inflight compaction in the middle, archival should not kick in.
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
        assertEquals(originalCommits, commitsAfterArchival);
      } else {
        if (i != 6) {
          assertEquals(originalCommits, commitsAfterArchival);
        } else {
          // on 7th commit, archival will kick in. but will archive only one commit since 2nd compaction commit is inflight.
          assertEquals(originalCommits.size() - commitsAfterArchival.size(), 1);
          for (int j = 1; j <= 6; j++) {
            if (j == 1) {
              // first commit should be archived
              assertFalse(commitsAfterArchival.contains(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j)));
            } else if (j == 2) {
              // 2nd compaction should not be archived
              assertFalse(commitsAfterArchival.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "0000000" + j)));
            } else {
              // every other commit should not be archived
              assertTrue(commitsAfterArchival.contains(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j)));
            }
          }
        }
      }
    }

    // move inflight compaction to complete and add one regular write commit. archival should archive more commits.
    // an extra one commit is required, bcoz compaction in data table will not trigger table services in metadata table.
    // before this move, timeline : 2_inflight_compaction, 3,4,5,6,7.
    // after this move: 6,7,8 (2,3,4,5 will be archived)
    testTable.moveInflightCompactionToComplete("00000002", inflightCompactionMetadata);
    testTable.doWriteOperation("00000008", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), 2);

    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

    List<HoodieInstant> archivedInstants = getAllArchivedCommitInstants(Arrays.asList("00000001", "00000003", "00000004", "00000005", "00000006"), HoodieTimeline.DELTA_COMMIT_ACTION);
    archivedInstants.add(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "00000002"));
    archivedInstants.add(new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000002"));
    verifyArchival(archivedInstants, getActiveCommitInstants(Arrays.asList("00000007", "00000008"), HoodieTimeline.DELTA_COMMIT_ACTION), commitsAfterArchival);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveCommitTimeline(boolean enableMetadataTable) throws Exception {
    init();
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table")
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                .withRemoteServerPort(timelineServicePort).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTestDataGenerator.createCommitFile(basePath, "1", wrapperFs.getConf());
    HoodieInstant instant1 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieTestDataGenerator.createCommitFile(basePath, "2", wrapperFs.getConf());
    Path markerPath = new Path(metaClient.getMarkerFolderPath("2"));
    wrapperFs.mkdirs(markerPath);
    HoodieInstant instant2 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieTestDataGenerator.createCommitFile(basePath, "3", wrapperFs.getConf());
    HoodieInstant instant3 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");

    //add 2 more instants to pass filter criteria set in compaction config above
    HoodieTestDataGenerator.createCommitFile(basePath, "4", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "5", wrapperFs.getConf());

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf, wrapperFs, basePath, "5");
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    boolean result = archiver.archiveIfRequired(context);
    assertTrue(result);
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<HoodieInstant> archivedInstants = Arrays.asList(instant1, instant2, instant3);
    assertEquals(new HashSet<>(archivedInstants),
        archivedTimeline.filterCompletedInstants().getInstants().collect(Collectors.toSet()));
    assertFalse(wrapperFs.exists(markerPath));
  }

  private void verifyInflightInstants(HoodieTableMetaClient metaClient, int expectedTotalInstants) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload()
        .getTimelineOfActions(Collections.singleton(HoodieTimeline.CLEAN_ACTION)).filterInflights();
    assertEquals(expectedTotalInstants, timeline.countInstants(),
        "Loaded inflight clean actions and the count should match");
  }

  @Test
  public void testConvertCommitMetadata() throws Exception {
    init();
    HoodieCommitMetadata hoodieCommitMetadata = new HoodieCommitMetadata();
    hoodieCommitMetadata.setOperationType(WriteOperationType.INSERT);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = MetadataConversionUtils
        .convertCommitMetadata(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveTableWithCleanCommits(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(enableMetadata, 2, 4, 2);

    // min archival commits is 2 and max archival commits is 4
    // (either clean commits has to be > 4 or commits has to be greater than 4)
    // and so, after 5th commit, 3 commits will be archived.
    // 1,2,3,4,5,6 : after archival -> 1,5,6 (because, 2,3,4,5 and 6 are clean commits and are eligible for archival)
    // after 7th and 8th commit no-op wrt archival.
    Map<String, Integer> cleanStats = new HashMap<>();
    cleanStats.put("p1", 1);
    cleanStats.put("p2", 2);
    for (int i = 1; i < 9; i++) {
      if (i == 1) {
        testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 10);
      } else if (i < 7) {
        testTable.doClean("0000000" + i, cleanStats);
      } else {
        testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      }
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      if (i < 6) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else if (i == 6) {
        if (!enableMetadata) {
          // 1,2,3,4,5,6 : after archival -> 1,5,6 (bcoz, 2,3,4,5 and 6 are clean commits and are eligible for archival)
          List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
          expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000001")));
          expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000005", "00000006"), HoodieTimeline.CLEAN_ACTION));
          verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000002", "00000003", "00000004"), HoodieTimeline.CLEAN_ACTION), expectedActiveInstants, commitsAfterArchival);
        } else {
          // with metadata enabled, archival in data table is fenced based on compaction in metadata table. Clean commits in data table will not trigger compaction in
          // metadata table.
          List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
          expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000001")));
          expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000002", "00000003", "00000004", "00000005", "00000006"), HoodieTimeline.CLEAN_ACTION));
          verifyArchival(getAllArchivedCommitInstants(Collections.emptyList(), HoodieTimeline.CLEAN_ACTION), expectedActiveInstants, commitsAfterArchival);
        }
      } else {
        if (!enableMetadata) {
          assertEquals(originalCommits, commitsAfterArchival);
        } else {
          if (i == 7) {
            // when i == 7 compaction in metadata table will be triggered and hence archival in datatable will kick in.
            // 1,2,3,4,5,6 : after archival -> 1,5,6 (bcoz, 2,3,4,5 and 6 are clean commits and are eligible for archival)
            List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
            expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000001", "00000007")));
            expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000005", "00000006"), HoodieTimeline.CLEAN_ACTION));
            verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000002", "00000003", "00000004"), HoodieTimeline.CLEAN_ACTION), expectedActiveInstants, commitsAfterArchival);
          } else {
            assertEquals(originalCommits, commitsAfterArchival);
          }
        }
      }
    }
  }

  @Test
  public void testArchiveRollbacksAndCleanTestTable() throws Exception {
    int minArchiveCommits = 2;
    int maxArchiveCommits = 9;
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, minArchiveCommits, maxArchiveCommits, 2);

    // trigger 1 commit to add lot of files so that future cleans can clean them up
    testTable.doWriteOperation("00000001", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 20);

    Map<String, Integer> partitionToFileDeleteCount = new HashMap<>();
    partitionToFileDeleteCount.put("p1", 1);
    partitionToFileDeleteCount.put("p2", 1);
    // we are triggering 10 clean commits. (1 is commit, 2 -> 11 is clean)
    for (int i = 2; i <= (maxArchiveCommits + 2); i++) {
      testTable.doClean((i > 9 ? ("000000") : ("0000000")) + i, partitionToFileDeleteCount);
    }

    // we are triggering 7 commits and 7 rollbacks for the same
    for (int i = 12; i <= (2 * maxArchiveCommits); i += 2) {
      testTable.doWriteOperation("000000" + i, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      testTable.doRollback("000000" + i, "000000" + (i + 1));
    }

    // trigger archival
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

    // out of 10 clean commits, 8 will be archived. 2 to 9. 10 and 11 will be active.
    // wrt regular commits, there aren't 9 commits yet and so all of them will be active.
    List<HoodieInstant> expectedActiveInstants = new ArrayList<>();
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000010", "00000011"), HoodieTimeline.CLEAN_ACTION));
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000001", "00000012", "00000014", "00000016", "00000018")));
    expectedActiveInstants.addAll(getActiveCommitInstants(Arrays.asList("00000013", "00000015", "00000017", "00000019"), HoodieTimeline.ROLLBACK_ACTION));
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000002", "00000003", "00000004", "00000005", "00000006", "00000007", "00000008", "00000009"),
        HoodieTimeline.CLEAN_ACTION), expectedActiveInstants, commitsAfterArchival);
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
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minInstantsToKeep, maxInstantsToKeep).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                .withRemoteServerPort(timelineServicePort).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    int startInstant = 1;
    List<HoodieInstant> expectedArchivedInstants = new ArrayList<>();
    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant++) {
      createCleanMetadata(startInstant + "", false, false, isEmpty || i % 2 == 0);
      expectedArchivedInstants.add(new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startInstant + ""));
      expectedArchivedInstants.add(new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, startInstant + ""));
      expectedArchivedInstants.add(new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, startInstant + ""));
    }

    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant += 2) {
      createCommitAndRollbackFile(startInstant + 1 + "", startInstant + "", false, isEmpty || i % 2 == 0);
      expectedArchivedInstants.add(new HoodieInstant(State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, startInstant + ""));
      expectedArchivedInstants.add(new HoodieInstant(State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, startInstant + ""));
      expectedArchivedInstants.add(new HoodieInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, startInstant + ""));
    }

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf, wrapperFs, basePath, Integer.toString(99));
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);

    archiver.archiveIfRequired(context);

    Stream<HoodieInstant> currentInstants = metaClient.getActiveTimeline().reload().getInstants();
    Map<Object, List<HoodieInstant>> actionInstantMap = currentInstants.collect(Collectors.groupingBy(HoodieInstant::getAction));

    assertTrue(actionInstantMap.containsKey("clean"), "Clean Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("clean").size(), "Should have min instant");

    assertTrue(actionInstantMap.containsKey("rollback"), "Rollback Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("rollback").size(), "Should have min instant");

    // verify all expected instants are part of archived timeline
    metaClient.getArchivedTimeline().loadCompletedInstantDetailsInMemory();
    HoodieInstant firstInstant = metaClient.reloadActiveTimeline().firstInstant().get();
    expectedArchivedInstants = expectedArchivedInstants.stream()
        .filter(entry -> HoodieTimeline.compareTimestamps(entry.getTimestamp(), HoodieTimeline.LESSER_THAN, firstInstant.getTimestamp()
        )).collect(Collectors.toList());
    expectedArchivedInstants.forEach(entry -> assertTrue(metaClient.getArchivedTimeline().containsInstant(entry)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchiveInflightClean(boolean enableMetadataTable) throws Exception {
    init();
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table")
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                .withRemoteServerPort(timelineServicePort).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    createCleanMetadata("10", false);
    createCleanMetadata("11", false);
    HoodieInstant notArchivedInstant1 = createCleanMetadata("12", false);
    HoodieInstant notArchivedInstant2 = createCleanMetadata("13", false);
    HoodieInstant notArchivedInstant3 = createCleanMetadata("14", true);

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf, wrapperFs, basePath, "14");
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);

    archiver.archiveIfRequired(context);

    List<HoodieInstant> notArchivedInstants = metaClient.getActiveTimeline().reload().getInstants().collect(Collectors.toList());
    assertEquals(3, notArchivedInstants.size(), "Not archived instants should be 3");
    assertEquals(notArchivedInstants, Arrays.asList(notArchivedInstant1, notArchivedInstant2, notArchivedInstant3), "");
  }

  @Test
  public void testArchiveTableWithMetadataTableCompaction() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 4, 7);

    // min archival commits is 2 and max archival commits is 4. and so, after 5th commit, ideally archival should kick in. but max delta commits in metadata table is set to 6. and so
    // archival will kick in only by 7th commit in datatable(1 commit for bootstrap + 6 commits from data table).
    // and then 2nd compaction will take place
    for (int i = 1; i < 6; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // trigger archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    // two more commits will trigger compaction in metadata table and will let archival move forward.
    testTable.doWriteOperation("00000006", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    testTable.doWriteOperation("00000007", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
    List<HoodieInstant> originalCommits = commitsList.getKey();
    List<HoodieInstant> commitsAfterArchival = commitsList.getValue();
    // before archival 1,2,3,4,5,6,7
    // after archival 6,7
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 5);
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005")),
        getActiveCommitInstants(Arrays.asList("00000006", "00000007")), commitsAfterArchival);

    // 3 more commits, 6 and 7 will be archived. but will not move after 6 since compaction has to kick in metadata table.
    testTable.doWriteOperation("00000008", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    testTable.doWriteOperation("00000009", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits, commitsAfterArchival);

    // ideally, this will archive commits 6, 7, 8 but since compaction in metadata is until 6, only 6 will get archived,
    testTable.doWriteOperation("00000010", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 1);
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005", "00000006")),
        getActiveCommitInstants(Arrays.asList("00000007", "00000008", "00000009", "00000010")), commitsAfterArchival);

    // and then 2nd compaction will take place at 12th commit
    for (int i = 11; i < 14; i++) {
      testTable.doWriteOperation("000000" + i, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // trigger archival
      commitsList = archiveAndGetCommitsList(writeConfig);
      originalCommits = commitsList.getKey();
      commitsAfterArchival = commitsList.getValue();
      assertEquals(originalCommits, commitsAfterArchival);
    }

    // one more commit will trigger compaction in metadata table and will let archival move forward.
    testTable.doWriteOperation("00000014", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    // trigger archival
    commitsList = archiveAndGetCommitsList(writeConfig);
    originalCommits = commitsList.getKey();
    commitsAfterArchival = commitsList.getValue();
    // before archival 7,8,9,10,11,12,13,14
    // after archival 13,14
    assertEquals(originalCommits.size() - commitsAfterArchival.size(), 6);
    verifyArchival(getAllArchivedCommitInstants(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005", "00000006", "00000007", "00000008",
        "00000009", "00000010", "00000011", "00000012")), getActiveCommitInstants(Arrays.asList("00000013", "00000014")), commitsAfterArchival);
  }

  @Test
  public void testArchiveCommitsWithCompactionCommitInMetadataTableTimeline() throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(true, 2, 4, 20);
    int startInstantTime = 100;
    int numCommits = 15;
    int numExpectedArchived = 6; // "100" till "105" should be archived in this case

    for (int i = startInstantTime; i < startInstantTime + numCommits; i++) {
      HoodieTestDataGenerator.createCommitFile(basePath, Integer.toString(i), wrapperFs.getConf());
    }
    // Simulate a compaction commit in metadata table timeline
    // so the archival in data table can happen
    createCompactionCommitInMetadataTable(hadoopConf, wrapperFs, basePath, "105");

    HoodieTable table = HoodieSparkTable.create(writeConfig, context);
    HoodieTimelineArchiver archiveLog = new HoodieTimelineArchiver(writeConfig, table);

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(numCommits, timeline.countInstants(), String.format("Loaded %d commits and the count should match", numCommits));
    assertTrue(archiveLog.archiveIfRequired(context));
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(numCommits - numExpectedArchived, timeline.countInstants(),
        "Since we have a compaction commit of 105 in metadata table timeline, we should never archive any commit after that");
    for (int i = startInstantTime + numExpectedArchived; i < startInstantTime + numCommits; i++) {
      assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, Integer.toString(i))),
          String.format("Commit %d should not be archived", i));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testArchivalWithMaxDeltaCommitsGuaranteeForCompaction(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = initTestTableAndGetWriteConfig(
        enableMetadata, 2, 4, 8, 1, HoodieTableType.MERGE_ON_READ);

    // When max archival commits is set to 4, even after 8 delta commits, since the number of delta
    // commits is still smaller than 8, the archival should not kick in.
    // The archival should only kick in after the 9th delta commit
    // instant "00000001" to "00000009"
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("0000000" + i, WriteOperationType.UPSERT, i == 1
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
            new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "00000001")));
        IntStream.range(2, 10).forEach(j ->
            assertTrue(commitsAfterArchival.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j))));
      }
    }

    testTable.doCompaction("00000010", Arrays.asList("p1", "p2"));

    // instant "00000011" to "00000019"
    for (int i = 1; i < 10; i++) {
      testTable.doWriteOperation("0000001" + i, WriteOperationType.UPSERT, i == 1
          ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // archival
      Pair<List<HoodieInstant>, List<HoodieInstant>> commitsList = archiveAndGetCommitsList(writeConfig);
      List<HoodieInstant> originalCommits = commitsList.getKey();
      List<HoodieInstant> commitsAfterArchival = commitsList.getValue();

      // first 9 delta commits before the completed compaction should be archived
      IntStream.range(1, 10).forEach(j ->
          assertFalse(commitsAfterArchival.contains(
              new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j))));

      if (i == 1) {
        assertEquals(8, originalCommits.size() - commitsAfterArchival.size());
        // instant from "00000011" should be in the active timeline
        assertTrue(commitsAfterArchival.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000010")));
        assertTrue(commitsAfterArchival.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "00000011")));
      } else if (i < 8) {
        assertEquals(originalCommits, commitsAfterArchival);
      } else {
        assertEquals(1, originalCommits.size() - commitsAfterArchival.size());
        assertFalse(commitsAfterArchival.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000010")));
        // i == 8 -> ["00000011", "00000018"] should be in the active timeline
        // i == 9 -> ["00000012", "00000019"] should be in the active timeline
        if (i == 9) {
          assertFalse(commitsAfterArchival.contains(
              new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "00000011")));
        }
        IntStream.range(i - 7, i + 1).forEach(j ->
            assertTrue(commitsAfterArchival.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000001" + j))));
      }
    }
  }

  @Test
  public void testArchivalAndCompactionInMetadataTable() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Test configs where metadata table has more aggressive archival configs than the compaction config
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .retainCommits(1).archiveCommitsWith(2, 4).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(8)
            .retainCommits(1).archiveCommitsWith(2, 4).build())
        .forTable("test-trip-table").build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    HoodieTableMetaClient metadataTableMetaClient = HoodieTableMetaClient.builder()
        .setConf(metaClient.getHadoopConf())
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))
        .setLoadActiveTimelineOnLoad(true).build();

    for (int i = 1; i <= 17; i++) {
      testTable.doWriteOperation("000000" + String.format("%02d", i), WriteOperationType.UPSERT,
          i == 1 ? Arrays.asList("p1", "p2") : Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
      // archival
      archiveAndGetCommitsList(writeConfig);

      metadataTableMetaClient = HoodieTableMetaClient.reload(metadataTableMetaClient);
      List<HoodieInstant> metadataTableInstants = metadataTableMetaClient.getActiveTimeline()
          .getCommitsTimeline().filterCompletedInstants().getInstants()
          .collect(Collectors.toList());

      if (i <= 7) {
        // In the metadata table timeline, the first delta commit is "00000000000000"
        // from metadata table init, delta commits "00000001" till "00000007" are added
        // later on without archival or compaction
        assertEquals(i + 1, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "00000000000000")));
        IntStream.range(1, i + 1).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j))));
      } else if (i == 8) {
        // i == 8
        // The instant "00000000000000" was archived since it's less than
        // the earliest instant on the dataset active timeline,
        // the dataset active timeline has instants of range [00000001 ~ 00000008]
        // because when it does the archiving, no compaction instant on the
        // metadata active timeline exists yet.
        assertEquals(9, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000007001")));
        IntStream.range(1, i + 1).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000000" + j))));
      } else if (i <= 11) {
        // In the metadata table timeline, the first delta commit is "00000007"
        // because it equals with the earliest commit on the dataset timeline, after archival,
        // delta commits "00000008" till "00000011" are added later on without archival or compaction
        assertEquals(i - 5, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000007001")));
        IntStream.range(7, i + 1).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
                    "000000" + String.format("%02d", j)))));
      } else if (i <= 14) {
        // In the metadata table timeline, the first delta commit is "00000007001"
        // from metadata table compaction, after archival, delta commits "00000008"
        // till "00000014" are added later on without archival or compaction
        assertEquals(i - 6, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000007001")));
        IntStream.range(8, i + 1).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
                    "000000" + String.format("%02d", j)))));
      } else if (i == 15) {
        // Only delta commits "00000008" till "00000015" are in the active timeline
        assertEquals(8, metadataTableInstants.size());
        assertFalse(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000007001")));
        IntStream.range(8, 16).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
                    "000000" + String.format("%02d", j)))));
      } else if (i == 16) {
        // i == 16
        // dataset timeline has commits "00000015" and "00000016",
        // the metadata timeline has commits [00000008, 00000016] and "00000015001"
        assertEquals(10, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000015001")));
        IntStream.range(8, 17).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
                    "000000" + String.format("%02d", j)))));
      } else {
        // i == 17
        // Only commits [00000015, 00000017] and "00000015001" are on the metadata timeline
        assertEquals(4, metadataTableInstants.size());
        assertTrue(metadataTableInstants.contains(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "00000015001")));
        IntStream.range(15, 18).forEach(j ->
            assertTrue(metadataTableInstants.contains(
                new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
                    "000000" + String.format("%02d", j)))));
      }
    }
  }

  private Pair<List<HoodieInstant>, List<HoodieInstant>> archiveAndGetCommitsList(HoodieWriteConfig writeConfig) throws IOException {
    metaClient.reloadActiveTimeline();
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    archiver.archiveIfRequired(context);
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> commitsAfterArchival = timeline.getInstants().collect(Collectors.toList());
    return Pair.of(originalCommits, commitsAfterArchival);
  }

  private void verifyArchival(List<HoodieInstant> expectedArchivedInstants, List<HoodieInstant> expectedActiveInstants, List<HoodieInstant> commitsAfterArchival) {
    Collections.sort(expectedActiveInstants, Comparator.comparing(HoodieInstant::getTimestamp));
    Collections.sort(commitsAfterArchival, Comparator.comparing(HoodieInstant::getTimestamp));
    assertEquals(expectedActiveInstants, commitsAfterArchival);
    expectedArchivedInstants.forEach(entry -> assertFalse(commitsAfterArchival.contains(entry)));
    HoodieArchivedTimeline archivedTimeline = new HoodieArchivedTimeline(metaClient);
    List<HoodieInstant> actualArchivedInstants = archivedTimeline.getInstants().collect(Collectors.toList());
    Collections.sort(actualArchivedInstants, Comparator.comparing(HoodieInstant::getTimestamp));
    Collections.sort(expectedArchivedInstants, Comparator.comparing(HoodieInstant::getTimestamp));
    assertEquals(actualArchivedInstants, expectedArchivedInstants);

    HoodieTimeline timeline = metaClient.getActiveTimeline();
    expectedArchivedInstants.forEach(entry -> {
          // check safety
          if (!entry.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
            assertTrue(timeline.containsOrBeforeTimelineStarts(entry.getTimestamp()), "Archived commits should always be safe");
          }
        }
    );
  }

  private List<HoodieInstant> getArchivedInstants(HoodieInstant instant) {
    List<HoodieInstant> instants = new ArrayList<>();
    if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION) || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)
        || instant.getAction().equals(HoodieTimeline.CLEAN_ACTION) || instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
      instants.add(new HoodieInstant(State.REQUESTED, instant.getAction(), instant.getTimestamp()));
    }
    instants.add(new HoodieInstant(State.INFLIGHT, instant.getAction(), instant.getTimestamp()));
    instants.add(new HoodieInstant(State.COMPLETED, instant.getAction(), instant.getTimestamp()));
    return instants;
  }

  private List<HoodieInstant> getAllArchivedCommitInstants(List<String> commitTimes) {
    return getAllArchivedCommitInstants(commitTimes, HoodieTimeline.COMMIT_ACTION);
  }

  private List<HoodieInstant> getAllArchivedCommitInstants(List<String> commitTimes, String action) {
    List<HoodieInstant> allInstants = new ArrayList<>();
    commitTimes.forEach(entry -> allInstants.addAll(getArchivedInstants(new HoodieInstant(State.COMPLETED, action, entry))));
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
    commitTimes.forEach(entry -> allInstants.add(new HoodieInstant(State.COMPLETED, action, entry)));
    return allInstants;
  }

  private void createCommitAndRollbackFile(String commitToRollback, String rollbackTIme, boolean isRollbackInflight) throws IOException {
    createCommitAndRollbackFile(commitToRollback, rollbackTIme, isRollbackInflight, false);
  }

  private void createCommitAndRollbackFile(String commitToRollback, String rollbackTIme, boolean isRollbackInflight, boolean isEmpty) throws IOException {
    HoodieTestDataGenerator.createCommitFile(basePath, commitToRollback, wrapperFs.getConf());
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
      HoodieTestTable.of(metaClient).addRollback(rollbackTime, hoodieRollbackMetadata, isEmpty);
    }
    return new HoodieInstant(inflight, "rollback", rollbackTime);
  }
}
