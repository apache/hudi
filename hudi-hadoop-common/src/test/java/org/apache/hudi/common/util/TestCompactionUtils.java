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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.compaction.CompactionPlanMigrator;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.testutils.CompactionTestUtils.DummyHoodieBaseFile;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.CompactionTestUtils.createCompactionPlan;
import static org.apache.hudi.common.testutils.CompactionTestUtils.scheduleCompaction;
import static org.apache.hudi.common.testutils.CompactionTestUtils.setupAndValidateCompactionOperations;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.apache.hudi.common.util.CompactionUtils.COMPACTION_METADATA_VERSION_1;
import static org.apache.hudi.common.util.CompactionUtils.LATEST_COMPACTION_METADATA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CompactionUtils}.
 */
public class TestCompactionUtils extends HoodieCommonTestHarness {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  private static final Map<String, Double> METRICS = new HashMap<String, Double>() {
    {
      put("key1", 1.0);
      put("key2", 3.0);
    }
  };
  private Function<Pair<String, FileSlice>, Map<String, Double>> metricsCaptureFn = (partitionFileSlice) -> METRICS;

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testUpgradeDowngrade() {
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), inputAndPlan.getValue());

    CompactionPlanMigrator migrator = new CompactionPlanMigrator(metaClient);
    HoodieCompactionPlan plan = inputAndPlan.getRight();
    assertEquals(LATEST_COMPACTION_METADATA_VERSION, plan.getVersion());
    HoodieCompactionPlan oldPlan = migrator.migrateToVersion(plan, plan.getVersion(), COMPACTION_METADATA_VERSION_1);
    // Check with older version of compaction plan
    assertEquals(COMPACTION_METADATA_VERSION_1, oldPlan.getVersion());
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), oldPlan);
    HoodieCompactionPlan newPlan = migrator.upgradeToLatest(plan, plan.getVersion());
    assertEquals(LATEST_COMPACTION_METADATA_VERSION, newPlan.getVersion());
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), newPlan);
    HoodieCompactionPlan latestPlan = migrator.migrateToVersion(oldPlan, oldPlan.getVersion(), newPlan.getVersion());
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), latestPlan);
  }

  @Test
  public void testBuildFromFileSlice() {
    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();

    // Empty File-Slice with no data and log files
    FileSlice emptyFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "empty1");
    HoodieCompactionOperation op =
        CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], emptyFileSlice, Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(emptyFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);

    // File Slice with data-file but no log files
    FileSlice noLogFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noLog1");
    noLogFileSlice.setBaseFile(new DummyHoodieBaseFile("/tmp/noLog_1_000" + extension));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], noLogFileSlice,
        Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noLogFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);
    // File Slice with no data-file but log files present
    FileSlice noDataFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    noDataFileSlice.addLogFile(
        new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("noData1", ".log", "000", 1,
            TEST_WRITE_TOKEN))));
    noDataFileSlice.addLogFile(
        new HoodieLogFile(new StoragePath(
            FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], noDataFileSlice,
        Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noDataFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);

    // File Slice with data-file and log files present
    FileSlice fileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    fileSlice.setBaseFile(new DummyHoodieBaseFile("/tmp/noLog_1_000" + extension));
    fileSlice.addLogFile(
        new HoodieLogFile(new StoragePath(
            FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN))));
    fileSlice.addLogFile(
        new HoodieLogFile(new StoragePath(
            FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], fileSlice,
        Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(fileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);
  }

  /**
   * Generate input for compaction plan tests.
   */
  private Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> buildCompactionPlan() {
    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();

    StoragePath fullPartitionPath =
        new StoragePath(metaClient.getBasePath(), DEFAULT_PARTITION_PATHS[0]);
    FileSlice emptyFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "empty1");
    FileSlice fileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    fileSlice.setBaseFile(
        new DummyHoodieBaseFile(fullPartitionPath.toString() + "/data1_1_000" + extension));
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(fullPartitionPath,
            FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN))));
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(fullPartitionPath,
            FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    FileSlice noLogFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noLog1");
    noLogFileSlice.setBaseFile(
        new DummyHoodieBaseFile(fullPartitionPath.toString() + "/noLog_1_000" + extension));
    FileSlice noDataFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(fullPartitionPath,
            FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN))));
    noDataFileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(fullPartitionPath,
            FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    List<FileSlice> fileSliceList = Arrays.asList(emptyFileSlice, noDataFileSlice, fileSlice, noLogFileSlice);
    List<Pair<String, FileSlice>> input =
        fileSliceList.stream().map(f -> Pair.of(DEFAULT_PARTITION_PATHS[0], f)).collect(Collectors.toList());
    return Pair.of(input, CompactionUtils.buildFromFileSlices(input, Option.empty(), Option.of(metricsCaptureFn)));
  }

  @Test
  public void testBuildFromFileSlices() {
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), inputAndPlan.getValue());
  }

  @Test
  public void testCompactionTransformation() {
    // check HoodieCompactionOperation <=> CompactionOperation transformation function
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    HoodieCompactionPlan plan = inputAndPlan.getRight();
    List<HoodieCompactionOperation> originalOps = plan.getOperations();
    // Convert to CompactionOperation
    // Convert back to HoodieCompactionOperation and check for equality
    List<HoodieCompactionOperation> regeneratedOps = originalOps.stream()
        .map(CompactionUtils::buildCompactionOperation)
        .map(CompactionUtils::buildHoodieCompactionOperation)
        .collect(Collectors.toList());
    assertTrue(originalOps.size() > 0, "Transformation did get tested");
    assertEquals(originalOps, regeneratedOps, "All fields set correctly in transformations");
  }

  @Test
  public void testGetAllPendingCompactionOperationsWithDupFileId() throws IOException {
    // Case where there is duplicate fileIds in compaction requests
    HoodieCompactionPlan plan1 = createCompactionPlan(metaClient, "000", "001", 10, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan(metaClient, "002", "003", 0, false, false);
    scheduleCompaction(metaClient, "001", plan1);
    scheduleCompaction(metaClient, "003", plan2);
    // schedule similar plan again so that there will be duplicates
    plan1.getOperations().get(0).setDataFilePath("bla");
    scheduleCompaction(metaClient, "005", plan1);
    metaClient = createMetaClient(metaClient.getStorageConf(), basePath);
    assertThrows(IllegalStateException.class, () -> {
      CompactionUtils.getAllPendingCompactionOperations(metaClient);
    });
  }

  @Test
  public void testGetAllPendingCompactionOperationsWithFullDupFileId() throws IOException {
    // Case where there is duplicate fileIds in compaction requests
    HoodieCompactionPlan plan1 = createCompactionPlan(metaClient, "000", "001", 10, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan(metaClient, "002", "003", 0, false, false);
    scheduleCompaction(metaClient, "001", plan1);
    scheduleCompaction(metaClient, "003", plan2);
    // schedule same plan again so that there will be duplicates. It should not fail as it is a full duplicate
    scheduleCompaction(metaClient, "005", plan1);
    metaClient = createMetaClient(metaClient.getStorageConf().newInstance(), basePath);
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> res =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
  }

  @Test
  public void testGetAllPendingCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty.
    setupAndValidateCompactionOperations(metaClient, false, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingInflightCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty. All of them are marked inflight
    setupAndValidateCompactionOperations(metaClient, true, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingCompactionOperationsForEmptyCompactions() throws IOException {
    // Case where there are 4 compaction requests and all are empty.
    setupAndValidateCompactionOperations(metaClient, false, 0, 0, 0, 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetDeltaCommitsSinceLatestCompaction(boolean hasCompletedCompaction) {
    HoodieActiveTimeline timeline = prepareTimeline(hasCompletedCompaction);
    Pair<HoodieTimeline, HoodieInstant> actual = CompactionUtils.getDeltaCommitsSinceLatestCompaction(timeline).get();
    if (hasCompletedCompaction) {
      assertEquals(
          Stream.of(
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "700"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "800"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "900"))
              .collect(Collectors.toList()),
          actual.getLeft().getInstants());
      assertEquals(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "600"),
          actual.getRight());
    } else {
      assertEquals(
          Stream.of(
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "100"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "200"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "300"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "400"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "500"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "700"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "800"),
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "900"))
              .collect(Collectors.toList()),
          actual.getLeft().getInstants());
      assertEquals(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "100"),
          actual.getRight());
    }
  }

  @Test
  public void testGetDeltaCommitsSinceLastCompactionWithCompletedReplaceCommits() {
    // 4th replace commit.
    HoodieActiveTimeline timeline = new MockHoodieActiveTimeline(
        Stream.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "01"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "02"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "03"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "04"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "05"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "06"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "07"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "08"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "09")).collect(Collectors.toList()));

    Pair<HoodieTimeline, HoodieInstant> actual =
        CompactionUtils.getDeltaCommitsSinceLatestCompaction(timeline).get();
    assertEquals(
        Stream.of(
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "07"),
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "08"),
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "09"))
            .collect(Collectors.toList()),
        actual.getLeft().getInstants());
    assertEquals(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "06"),
        actual.getRight());

    // mix of compaction commit and replace commit.
    timeline = new MockHoodieActiveTimeline(
        Stream.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "01"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "02"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "03"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "04"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "06"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "07"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "08"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "09"),
            INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, "10")).collect(Collectors.toList()));

    actual =
        CompactionUtils.getDeltaCommitsSinceLatestCompaction(timeline).get();
    assertEquals(
        Stream.of(
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "07"),
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "08"),
                INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "09"))
            .collect(Collectors.toList()),
        actual.getLeft().getInstants());
    assertEquals(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05"),
        actual.getRight());
  }

  @Test
  public void testGetDeltaCommitsSinceLatestCompactionWithEmptyDeltaCommits() {
    HoodieActiveTimeline timeline = new MockHoodieActiveTimeline();
    assertEquals(Option.empty(), CompactionUtils.getDeltaCommitsSinceLatestCompaction(timeline));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetOldestInstantToKeepForCompaction(boolean hasCompletedCompaction) {
    HoodieActiveTimeline timeline = prepareTimeline(hasCompletedCompaction);
    Option<HoodieInstant> actual = CompactionUtils.getEarliestInstantToRetainForCompaction(timeline, 20);

    if (hasCompletedCompaction) {
      assertEquals(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "600"), actual.get());
    } else {
      assertEquals(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "100"), actual.get());
    }

    actual = CompactionUtils.getEarliestInstantToRetainForCompaction(timeline, 3);
    assertEquals(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "700"), actual.get());

    actual = CompactionUtils.getEarliestInstantToRetainForCompaction(timeline, 2);
    assertEquals(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "800"), actual.get());
  }

  @Test
  public void testGetOldestInstantToKeepForCompactionWithEmptyDeltaCommits() {
    HoodieActiveTimeline timeline = new MockHoodieActiveTimeline();
    assertEquals(Option.empty(), CompactionUtils.getEarliestInstantToRetainForCompaction(timeline, 20));
  }

  private HoodieActiveTimeline prepareTimeline(boolean hasCompletedCompaction) {
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "100", "110"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "200", "210"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "300", "310"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "400", "410"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "500", "510"));
    if (hasCompletedCompaction) {
      instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "600", "610"));
    }
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "700", "710"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "800", "810"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "900", "910"));
    return new MockHoodieActiveTimeline(instants);
  }

  /**
   * Validates if generated compaction plan matches with input file-slices.
   *
   * @param input File Slices with partition-path
   * @param plan  Compaction Plan
   */
  private void testFileSlicesCompactionPlanEquality(List<Pair<String, FileSlice>> input, HoodieCompactionPlan plan) {
    assertEquals(input.size(), plan.getOperations().size(), "All file-slices present");
    IntStream.range(0, input.size()).boxed().forEach(idx -> testFileSliceCompactionOpEquality(input.get(idx).getValue(),
        plan.getOperations().get(idx), input.get(idx).getKey(), plan.getVersion()));
  }

  /**
   * Validates if generated compaction operation matches with input file slice and partition path.
   *
   * @param slice            File Slice
   * @param op               HoodieCompactionOperation
   * @param expPartitionPath Partition path
   */
  private void testFileSliceCompactionOpEquality(FileSlice slice, HoodieCompactionOperation op, String expPartitionPath,
                                                 int version) {
    assertEquals(expPartitionPath, op.getPartitionPath(), "Partition path is correct");
    assertEquals(slice.getBaseInstantTime(), op.getBaseInstantTime(), "Same base-instant");
    assertEquals(slice.getFileId(), op.getFileId(), "Same file-id");
    if (slice.getBaseFile().isPresent()) {
      HoodieBaseFile df = slice.getBaseFile().get();
      assertEquals(version == COMPACTION_METADATA_VERSION_1 ? df.getPath() : df.getFileName(),
          op.getDataFilePath(), "Same data-file");
    }
    List<String> paths = slice.getLogFiles().map(l -> l.getPath().toString()).collect(Collectors.toList());
    IntStream.range(0, paths.size()).boxed().forEach(idx -> assertEquals(
        version == COMPACTION_METADATA_VERSION_1 ? paths.get(idx) : new Path(paths.get(idx)).getName(),
        op.getDeltaFilePaths().get(idx), "Log File Index " + idx));
    assertEquals(METRICS, op.getMetrics(), "Metrics set");
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  class MockHoodieActiveTimeline extends ActiveTimelineV2 {

    public MockHoodieActiveTimeline() {
      super();
      this.setInstants(new ArrayList<>());
    }

    public MockHoodieActiveTimeline(List<HoodieInstant> instants) {
      super();
      this.setInstants(instants);
    }
  }
}
