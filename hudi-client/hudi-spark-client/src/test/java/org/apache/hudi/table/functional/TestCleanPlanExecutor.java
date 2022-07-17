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

package org.apache.hudi.table.functional;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.TestCleaner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests covering different clean plan policies/strategies.
 */
public class TestCleanPlanExecutor extends TestCleaner {

  @Test
  public void testInvalidCleaningTriggerStrategy() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).enable(false).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withIncrementalCleaningMode(true)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .withCleanBootstrapBaseFileEnabled(true)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2)
            .withCleaningTriggerStrategy("invalid_strategy").build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).enable(false).build()).build();
    Exception e = assertThrows(IllegalArgumentException.class, () -> runCleaner(config, true), "should fail when invalid trigger strategy is provided!");
    assertTrue(e.getMessage().contains("No enum constant org.apache.hudi.table.action.clean.CleaningTriggerStrategy.invalid_strategy"));
  }

  private static Stream<Arguments> argumentsForTestKeepLatestCommits() {
    return Stream.of(
        Arguments.of(false, false, false, false),
        Arguments.of(true, false, false, false),
        Arguments.of(true, true, false, false),
        Arguments.of(false, false, true, false),
        Arguments.of(false, false, false, true)
    );
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for COW table.
   */
  @ParameterizedTest
  @MethodSource("argumentsForTestKeepLatestCommits")
  public void testKeepLatestCommits(
      boolean simulateFailureRetry, boolean simulateMetadataFailure,
      boolean enableIncrementalClean, boolean enableBootstrapSourceClean) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withIncrementalCleaningMode(enableIncrementalClean)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .withCleanBootstrapBaseFileEnabled(enableBootstrapSourceClean)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(2)
            .withMaxCommitsBeforeCleaning(2)
          .build()).build();

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = enableBootstrapSourceClean ? generateBootstrapIndexAndSourceData(p0, p1) : null;

    // make 1 commit, with 1 file per partition
    String file1P0C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p0).get(0).getFileId()
                : UUID.randomUUID().toString();
    String file1P1C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p1).get(0).getFileId()
                : UUID.randomUUID().toString();
    testTable.addInflightCommit("00000000000001").withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);

    HoodieCommitMetadata commitMetadata = generateCommitMetadata("00000000000001",
        Collections.unmodifiableMap(new HashMap<String, List<String>>() {
          {
            put(p0, CollectionUtils.createImmutableList(file1P0C0));
            put(p1, CollectionUtils.createImmutableList(file1P1C0));
          }
        })
    );
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000001"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> hoodieCleanStatsOne =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 2, true);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    Map<String, String> partitionAndFileId002 = testTable.addInflightCommit("00000000000003").getFileIdsWithBaseFilesInPartitions(p0, p1);
    String file2P0C1 = partitionAndFileId002.get(p0);
    String file2P1C1 = partitionAndFileId002.get(p1);
    testTable.forCommit("00000000000003").withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);
    commitMetadata = generateCommitMetadata("00000000000003", new HashMap<String, List<String>>() {
      {
        put(p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1));
        put(p1, CollectionUtils.createImmutableList(file1P1C0, file2P1C1));
      }
    });
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000003"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsTwo =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 4, true);
    assertEquals(0, hoodieCleanStatsTwo.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000003", file2P1C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    String file3P0C2 = testTable.addInflightCommit("00000000000005")
        .withBaseFilesInPartition(p0, file1P0C0)
        .withBaseFilesInPartition(p0, file2P0C1)
        .getFileIdsWithBaseFilesInPartitions(p0).get(p0);
    commitMetadata = generateCommitMetadata("00000000000003",
        CollectionUtils.createImmutableMap(
            p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1, file3P0C2)));
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000005"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    List<HoodieCleanStat> hoodieCleanStatsThree =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 6, true);
    assertEquals(0, hoodieCleanStatsThree.size(),
        "Must not clean any file. We have to keep 1 version before the latest commit time to keep");
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    String file4P0C3 = testTable.addInflightCommit("00000000000007")
        .withBaseFilesInPartition(p0, file1P0C0)
        .withBaseFilesInPartition(p0, file2P0C1)
        .getFileIdsWithBaseFilesInPartitions(p0).get(p0);
    commitMetadata = generateCommitMetadata("00000000000004",
        CollectionUtils.createImmutableMap(
            p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1, file4P0C3)));
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000007"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    List<HoodieCleanStat> hoodieCleanStatsFour =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 8, true);
    // enableBootstrapSourceClean would delete the bootstrap base file as the same time
    HoodieCleanStat partitionCleanStat = getCleanStat(hoodieCleanStatsFour, p0);

    assertEquals(enableBootstrapSourceClean ? 2 : 1, partitionCleanStat.getSuccessDeleteFiles().size()
        + (partitionCleanStat.getSuccessDeleteBootstrapBaseFiles() == null ? 0
        : partitionCleanStat.getSuccessDeleteBootstrapBaseFiles().size()), "Must clean at least one old file");
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file1P0C0));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file1P0C0));
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file3P0C2));
    assertTrue(testTable.baseFileExists(p0, "00000000000007", file4P0C3));
    if (enableBootstrapSourceClean) {
      assertFalse(Files.exists(Paths.get(bootstrapMapping.get(
                    p0).get(0).getBootstrapFileStatus().getPath().getUri())));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);

    String file5P0C4 = testTable.addInflightCommit("00000000000009")
        .withBaseFilesInPartition(p0, file1P0C0)
        .withBaseFilesInPartition(p0, file2P0C1)
        .getFileIdsWithBaseFilesInPartitions(p0).get(p0);
    commitMetadata = generateCommitMetadata("00000000000009", CollectionUtils.createImmutableMap(
        p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1, file5P0C4)));
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000009"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    List<HoodieCleanStat> hoodieCleanStatsFive =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 10, true);

    assertEquals(0, hoodieCleanStatsFive.size(), "Must not clean any files since at least 2 commits are needed from last clean operation before "
        + "clean can be scheduled again");
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file1P0C0));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file1P0C0));
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file3P0C2));
    assertTrue(testTable.baseFileExists(p0, "00000000000007", file4P0C3));

    // No cleaning on partially written file, with no commit.
    testTable.forCommit("00000000000011").withBaseFilesInPartition(p0, file3P0C2);
    commitMetadata = generateCommitMetadata("00000000000011", CollectionUtils.createImmutableMap(p0,
        CollectionUtils.createImmutableList(file3P0C2)));
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "00000000000011"));
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "00000000000011"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsFive2 =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure, 12, true);
    HoodieCleanStat cleanStat = getCleanStat(hoodieCleanStatsFive2, p0);
    assertNull(cleanStat, "Must not clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000005", file3P0C2));
    assertTrue(testTable.baseFileExists(p0, "00000000000007", file4P0C3));
  }

  /**
   * Test Hudi COW Table Cleaner - Keep the latest file versions policy.
   */
  @Test
  public void testKeepLatestFileVersions() throws Exception {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);

    final String p0 = "2020/01/01";
    final String p1 = "2020/01/02";

    // make 1 commit, with 1 file per partition
    final String file1P0C0 = UUID.randomUUID().toString();
    final String file1P1C0 = UUID.randomUUID().toString();

    Map<String, List<Pair<String, Integer>>> c1PartitionToFilesNameLengthMap = new HashMap<>();
    c1PartitionToFilesNameLengthMap.put(p0, Collections.singletonList(Pair.of(file1P0C0, 100)));
    c1PartitionToFilesNameLengthMap.put(p1, Collections.singletonList(Pair.of(file1P1C0, 200)));
    testTable.doWriteOperation("00000000000001", WriteOperationType.INSERT, Arrays.asList(p0, p1),
        c1PartitionToFilesNameLengthMap, false, false);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleanerWithInstantFormat(config, true);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    final String file2P0C1 = UUID.randomUUID().toString();
    final String file2P1C1 = UUID.randomUUID().toString();
    Map<String, List<Pair<String, Integer>>> c2PartitionToFilesNameLengthMap = new HashMap<>();
    c2PartitionToFilesNameLengthMap.put(p0, Arrays.asList(Pair.of(file1P0C0, 101), Pair.of(file2P0C1, 100)));
    c2PartitionToFilesNameLengthMap.put(p1, Arrays.asList(Pair.of(file1P1C0, 201), Pair.of(file2P1C1, 200)));
    testTable.doWriteOperation("00000000000002", WriteOperationType.UPSERT, Collections.emptyList(),
        c2PartitionToFilesNameLengthMap, false, false);

    // enableBootstrapSourceClean would delete the bootstrap base file at the same time
    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleaner(config, 1, true);
    HoodieCleanStat cleanStat = getCleanStat(hoodieCleanStatsTwo, p0);
    assertEquals(1, cleanStat.getSuccessDeleteFiles().size()
        + (cleanStat.getSuccessDeleteBootstrapBaseFiles() == null ? 0
        : cleanStat.getSuccessDeleteBootstrapBaseFiles().size()), "Must clean at least 1 file");

    cleanStat = getCleanStat(hoodieCleanStatsTwo, p1);
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000002", file2P1C1));
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertFalse(testTable.baseFileExists(p1, "00000000000001", file1P1C0));
    assertEquals(1, cleanStat.getSuccessDeleteFiles().size()
        + (cleanStat.getSuccessDeleteBootstrapBaseFiles() == null ? 0
        : cleanStat.getSuccessDeleteBootstrapBaseFiles().size()), "Must clean at least 1 file");

    // make next commit, with 2 updates to existing files, and 1 insert
    final String file3P0C2 = UUID.randomUUID().toString();
    Map<String, List<Pair<String, Integer>>> c3PartitionToFilesNameLengthMap = new HashMap<>();
    c3PartitionToFilesNameLengthMap.put(p0, Arrays.asList(Pair.of(file1P0C0, 102), Pair.of(file2P0C1, 101),
        Pair.of(file3P0C2, 100)));
    testTable.doWriteOperation("00000000000003", WriteOperationType.UPSERT, Collections.emptyList(),
        c3PartitionToFilesNameLengthMap, false, false);

    List<HoodieCleanStat> hoodieCleanStatsThree = runCleaner(config, 3, true);
    assertEquals(2,
        getCleanStat(hoodieCleanStatsThree, p0)
            .getSuccessDeleteFiles().size(), "Must clean two files");
    assertFalse(testTable.baseFileExists(p0, "00000000000002", file1P0C0));
    assertFalse(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file3P0C2));

    // No cleaning on partially written file, with no commit.
    testTable.forCommit("00000000000004").withBaseFilesInPartition(p0, file3P0C2);

    List<HoodieCleanStat> hoodieCleanStatsFour = runCleaner(config);
    assertEquals(0, hoodieCleanStatsFour.size(), "Must not clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file3P0C2));
  }

  @Test
  public void testKeepLatestFileVersionsWithBootstrapFileClean() throws Exception {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanBootstrapBaseFileEnabled(true)
                .withCleanerParallelism(1)
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);

    final String p0 = "2020/01/01";
    final String p1 = "2020/01/02";
    final Map<String, List<BootstrapFileMapping>> bootstrapMapping = generateBootstrapIndexAndSourceData(p0, p1);

    // make 1 commit, with 1 file per partition
    final String file1P0C0 = bootstrapMapping.get(p0).get(0).getFileId();
    final String file1P1C0 = bootstrapMapping.get(p1).get(0).getFileId();

    Map<String, List<Pair<String, Integer>>> c1PartitionToFilesNameLengthMap = new HashMap<>();
    c1PartitionToFilesNameLengthMap.put(p0, Collections.singletonList(Pair.of(file1P0C0, 100)));
    c1PartitionToFilesNameLengthMap.put(p1, Collections.singletonList(Pair.of(file1P1C0, 200)));
    testTable.doWriteOperation("00000000000001", WriteOperationType.INSERT, Arrays.asList(p0, p1),
        c1PartitionToFilesNameLengthMap, false, false);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleanerWithInstantFormat(config, true);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    final String file2P0C1 = UUID.randomUUID().toString();
    final String file2P1C1 = UUID.randomUUID().toString();
    Map<String, List<Pair<String, Integer>>> c2PartitionToFilesNameLengthMap = new HashMap<>();
    c2PartitionToFilesNameLengthMap.put(p0, Arrays.asList(Pair.of(file1P0C0, 101), Pair.of(file2P0C1, 100)));
    c2PartitionToFilesNameLengthMap.put(p1, Arrays.asList(Pair.of(file1P1C0, 201), Pair.of(file2P1C1, 200)));
    testTable.doWriteOperation("00000000000002", WriteOperationType.UPSERT, Collections.emptyList(),
        c2PartitionToFilesNameLengthMap, false, false);

    // should delete the bootstrap base file at the same time
    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleaner(config, 1, true);
    HoodieCleanStat cleanStat = getCleanStat(hoodieCleanStatsTwo, p0);
    assertEquals(2, cleanStat.getSuccessDeleteFiles().size()
        + (cleanStat.getSuccessDeleteBootstrapBaseFiles() == null ? 0
        : cleanStat.getSuccessDeleteBootstrapBaseFiles().size()), "Must clean at least 1 file");

    HoodieFileStatus fstatus =
        bootstrapMapping.get(p0).get(0).getBootstrapFileStatus();
    // This ensures full path is recorded in metadata.
    assertTrue(cleanStat.getSuccessDeleteBootstrapBaseFiles().contains(fstatus.getPath().getUri()),
        "Successful delete files were " + cleanStat.getSuccessDeleteBootstrapBaseFiles()
            + " but did not contain " + fstatus.getPath().getUri());
    assertFalse(Files.exists(Paths.get(bootstrapMapping.get(
        p0).get(0).getBootstrapFileStatus().getPath().getUri())));

    cleanStat = getCleanStat(hoodieCleanStatsTwo, p1);
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000002", file2P1C1));
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertFalse(testTable.baseFileExists(p1, "00000000000001", file1P1C0));
    assertEquals(2, cleanStat.getSuccessDeleteFiles().size()
        + (cleanStat.getSuccessDeleteBootstrapBaseFiles() == null ? 0
        : cleanStat.getSuccessDeleteBootstrapBaseFiles().size()), "Must clean at least 1 file");

    fstatus = bootstrapMapping.get(p1).get(0).getBootstrapFileStatus();
    // This ensures full path is recorded in metadata.
    assertTrue(cleanStat.getSuccessDeleteBootstrapBaseFiles().contains(fstatus.getPath().getUri()),
        "Successful delete files were " + cleanStat.getSuccessDeleteBootstrapBaseFiles()
            + " but did not contain " + fstatus.getPath().getUri());
    assertFalse(Files.exists(Paths.get(bootstrapMapping.get(
        p1).get(0).getBootstrapFileStatus().getPath().getUri())));

    // make next commit, with 2 updates to existing files, and 1 insert
    final String file3P0C2 = UUID.randomUUID().toString();
    Map<String, List<Pair<String, Integer>>> c3PartitionToFilesNameLengthMap = new HashMap<>();
    c3PartitionToFilesNameLengthMap.put(p0, Arrays.asList(Pair.of(file1P0C0, 102), Pair.of(file2P0C1, 101),
        Pair.of(file3P0C2, 100)));
    testTable.doWriteOperation("00000000000003", WriteOperationType.UPSERT, Collections.emptyList(),
        c3PartitionToFilesNameLengthMap, false, false);

    List<HoodieCleanStat> hoodieCleanStatsThree = runCleaner(config, 3, true);
    assertEquals(2,
        getCleanStat(hoodieCleanStatsThree, p0)
            .getSuccessDeleteFiles().size(), "Must clean two files");
    assertFalse(testTable.baseFileExists(p0, "00000000000002", file1P0C0));
    assertFalse(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file3P0C2));

    // No cleaning on partially written file, with no commit.
    testTable.forCommit("00000000000004").withBaseFilesInPartition(p0, file3P0C2);

    List<HoodieCleanStat> hoodieCleanStatsFour = runCleaner(config);
    assertEquals(0, hoodieCleanStatsFour.size(), "Must not clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000003", file3P0C2));
  }

  /**
   * Test HoodieTable.clean() Cleaning by versions logic for MOR table with Log files.
   */
  @Test
  public void testKeepLatestFileVersionsMOR() throws Exception {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true)
            // Column Stats Index is disabled, since these tests construct tables which are
            // not valid (empty commit metadata, invalid parquet files)
            .withMetadataIndexColumnStats(false)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1)
            .build()).build();

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";

    // Make 3 files, one base file and 2 log files associated with base file
    String file1P0 = testTable.addDeltaCommit("000").getFileIdsWithBaseFilesInPartitions(p0).get(p0);
    testTable.forDeltaCommit("000")
                .withLogFile(p0, file1P0, 1)
                .withLogFile(p0, file1P0, 2);

    // Make 2 files, one base file and 1 log files associated with base file
    testTable.addDeltaCommit("001")
                .withBaseFilesInPartition(p0, file1P0)
                .withLogFile(p0, file1P0, 3);

    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config);
    assertEquals(3,
                getCleanStat(hoodieCleanStats, p0).getSuccessDeleteFiles()
                        .size(), "Must clean three files, one base and 2 log files");
    assertFalse(testTable.baseFileExists(p0, "000", file1P0));
    assertFalse(testTable.logFilesExist(p0, "000", file1P0, 1, 2));
    assertTrue(testTable.baseFileExists(p0, "001", file1P0));
    assertTrue(testTable.logFileExists(p0, "001", file1P0, 3));
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files.
   */
  @Test
  public void testKeepLatestCommitsMOR() throws Exception {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true)
                            // Column Stats Index is disabled, since these tests construct tables which are
                            // not valid (empty commit metadata, invalid parquet files)
                            .withMetadataIndexColumnStats(false).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1).build())
        .build();

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";

    // Make 3 files, one base file and 2 log files associated with base file
    String file1P0 = testTable.addDeltaCommit("000").getFileIdsWithBaseFilesInPartitions(p0).get(p0);
    testTable.forDeltaCommit("000")
                .withLogFile(p0, file1P0, 1)
                .withLogFile(p0, file1P0, 2);

    // Make 2 files, one base file and 1 log files associated with base file
    testTable.addDeltaCommit("001")
                .withBaseFilesInPartition(p0, file1P0)
                .withLogFile(p0, file1P0, 3);

    // Make 2 files, one base file and 1 log files associated with base file
    testTable.addDeltaCommit("002")
                .withBaseFilesInPartition(p0, file1P0)
                .withLogFile(p0, file1P0, 4);

    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config);
    assertEquals(3,
                getCleanStat(hoodieCleanStats, p0).getSuccessDeleteFiles()
                        .size(), "Must clean three files, one base and 2 log files");
    assertFalse(testTable.baseFileExists(p0, "000", file1P0));
    assertFalse(testTable.logFilesExist(p0, "000", file1P0, 1, 2));
    assertTrue(testTable.baseFileExists(p0, "001", file1P0));
    assertTrue(testTable.logFileExists(p0, "001", file1P0, 3));
    assertTrue(testTable.baseFileExists(p0, "002", file1P0));
    assertTrue(testTable.logFileExists(p0, "002", file1P0, 4));
  }

  /**
   * Tests cleaning service based on number of hours retained.
   */
  @ParameterizedTest
  @MethodSource("argumentsForTestKeepLatestCommits")
  public void testKeepXHoursWithCleaning(
      boolean simulateFailureRetry, boolean simulateMetadataFailure,
      boolean enableIncrementalClean, boolean enableBootstrapSourceClean) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withIncrementalCleaningMode(enableIncrementalClean)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .withCleanBootstrapBaseFileEnabled(enableBootstrapSourceClean)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS).cleanerNumHoursRetained(2)
            .build())
        .build();

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = enableBootstrapSourceClean ? generateBootstrapIndexAndSourceData(p0, p1) : null;

    String file1P0C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p0).get(0).getFileId()
            : UUID.randomUUID().toString();
    String file1P1C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p1).get(0).getFileId()
            : UUID.randomUUID().toString();
    Instant instant = Instant.now();
    ZonedDateTime commitDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    int minutesForFirstCommit = 150;
    String firstCommitTs = HoodieActiveTimeline.formatDate(Date.from(commitDateTime.minusMinutes(minutesForFirstCommit).toInstant()));
    testTable.addInflightCommit(firstCommitTs).withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);

    HoodieCommitMetadata commitMetadata = generateCommitMetadata(firstCommitTs,
            Collections.unmodifiableMap(new HashMap<String, List<String>>() {
              {
                put(p0, CollectionUtils.createImmutableList(file1P0C0));
                put(p1, CollectionUtils.createImmutableList(file1P1C0));
              }
            })
    );
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, firstCommitTs),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> hoodieCleanStatsOne =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, firstCommitTs, file1P0C0));
    assertTrue(testTable.baseFileExists(p1, firstCommitTs, file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    int minutesForSecondCommit = 90;
    String secondCommitTs = HoodieActiveTimeline.formatDate(Date.from(commitDateTime.minusMinutes(minutesForSecondCommit).toInstant()));
    Map<String, String> partitionAndFileId002 = testTable.addInflightCommit(secondCommitTs).getFileIdsWithBaseFilesInPartitions(p0, p1);
    String file2P0C1 = partitionAndFileId002.get(p0);
    String file2P1C1 = partitionAndFileId002.get(p1);
    testTable.forCommit(secondCommitTs).withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);
    commitMetadata = generateCommitMetadata(secondCommitTs, new HashMap<String, List<String>>() {
      {
        put(p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1));
        put(p1, CollectionUtils.createImmutableList(file1P1C0, file2P1C1));
      }
    });
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, secondCommitTs),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsTwo =
        runCleaner(config, simulateFailureRetry, simulateMetadataFailure);
    assertEquals(2, hoodieCleanStatsTwo.size(), "Should clean one file each from both the partitions");
    assertTrue(testTable.baseFileExists(p0, secondCommitTs, file2P0C1));
    assertTrue(testTable.baseFileExists(p1, secondCommitTs, file2P1C1));
    assertTrue(testTable.baseFileExists(p0, secondCommitTs, file1P0C0));
    assertTrue(testTable.baseFileExists(p1, secondCommitTs, file1P1C0));
    assertFalse(testTable.baseFileExists(p0, firstCommitTs, file1P0C0));
    assertFalse(testTable.baseFileExists(p1, firstCommitTs, file1P1C0));
  }
}
