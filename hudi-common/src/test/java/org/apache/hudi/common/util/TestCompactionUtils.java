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
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CompactionTestUtils.TestHoodieDataFile;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.versioning.compaction.CompactionPlanMigrator;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.model.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.util.CompactionTestUtils.createCompactionPlan;
import static org.apache.hudi.common.util.CompactionTestUtils.scheduleCompaction;
import static org.apache.hudi.common.util.CompactionTestUtils.setupAndValidateCompactionOperations;
import static org.apache.hudi.common.util.CompactionUtils.COMPACTION_METADATA_VERSION_1;
import static org.apache.hudi.common.util.CompactionUtils.LATEST_COMPACTION_METADATA_VERSION;

/**
 * The utility class for testing compaction.
 */
public class TestCompactionUtils extends HoodieCommonTestHarness {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  private static final Map<String, Double> METRICS =
      new ImmutableMap.Builder<String, Double>().put("key1", 1.0).put("key2", 3.0).build();
  private Function<Pair<String, FileSlice>, Map<String, Double>> metricsCaptureFn = (partitionFileSlice) -> METRICS;

  @Before
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testUpgradeDowngrade() {
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), inputAndPlan.getValue());

    CompactionPlanMigrator migrator = new CompactionPlanMigrator(metaClient);
    HoodieCompactionPlan plan = inputAndPlan.getRight();
    System.out.println("Plan=" + plan.getOperations());
    Assert.assertEquals(LATEST_COMPACTION_METADATA_VERSION, plan.getVersion());
    HoodieCompactionPlan oldPlan = migrator.migrateToVersion(plan, plan.getVersion(), COMPACTION_METADATA_VERSION_1);
    // Check with older version of compaction plan
    Assert.assertEquals(COMPACTION_METADATA_VERSION_1, oldPlan.getVersion());
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), oldPlan);
    HoodieCompactionPlan newPlan = migrator.upgradeToLatest(plan, plan.getVersion());
    Assert.assertEquals(LATEST_COMPACTION_METADATA_VERSION, newPlan.getVersion());
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), newPlan);
  }

  @Test
  public void testBuildFromFileSlice() {
    // Empty File-Slice with no data and log files
    FileSlice emptyFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "empty1");
    HoodieCompactionOperation op =
        CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], emptyFileSlice, Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(emptyFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);

    // File Slice with data-file but no log files
    FileSlice noLogFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog_1_000.parquet"));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], noLogFileSlice, Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noLogFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);
    // File Slice with no data-file but log files present
    FileSlice noDataFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    noDataFileSlice.addLogFile(
        new HoodieLogFile(new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN))));
    noDataFileSlice.addLogFile(
        new HoodieLogFile(new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], noDataFileSlice, Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noDataFileSlice, op, DEFAULT_PARTITION_PATHS[0],
        LATEST_COMPACTION_METADATA_VERSION);

    // File Slice with data-file and log files present
    FileSlice fileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog_1_000.parquet"));
    fileSlice.addLogFile(
        new HoodieLogFile(new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN))));
    fileSlice.addLogFile(
        new HoodieLogFile(new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN))));
    op = CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], fileSlice, Option.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(fileSlice, op, DEFAULT_PARTITION_PATHS[0], LATEST_COMPACTION_METADATA_VERSION);
  }

  /**
   * Generate input for compaction plan tests.
   */
  private Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> buildCompactionPlan() {
    Path fullPartitionPath = new Path(new Path(metaClient.getBasePath()), DEFAULT_PARTITION_PATHS[0]);
    FileSlice emptyFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "empty1");
    FileSlice fileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile(fullPartitionPath.toString() + "/data1_1_000.parquet"));
    fileSlice.addLogFile(new HoodieLogFile(
        new Path(fullPartitionPath, new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN)))));
    fileSlice.addLogFile(new HoodieLogFile(
        new Path(fullPartitionPath, new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN)))));
    FileSlice noLogFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile(fullPartitionPath.toString() + "/noLog_1_000.parquet"));
    FileSlice noDataFileSlice = new FileSlice(DEFAULT_PARTITION_PATHS[0], "000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(
        new Path(fullPartitionPath, new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 1, TEST_WRITE_TOKEN)))));
    noDataFileSlice.addLogFile(new HoodieLogFile(
        new Path(fullPartitionPath, new Path(FSUtils.makeLogFileName("noData1", ".log", "000", 2, TEST_WRITE_TOKEN)))));
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
    List<HoodieCompactionOperation> regeneratedOps = originalOps.stream().map(op -> {
      // Convert to CompactionOperation
      return CompactionUtils.buildCompactionOperation(op);
    }).map(op2 -> {
      // Convert back to HoodieCompactionOperation and check for equality
      return CompactionUtils.buildHoodieCompactionOperation(op2);
    }).collect(Collectors.toList());
    Assert.assertTrue("Transformation did get tested", originalOps.size() > 0);
    Assert.assertEquals("All fields set correctly in transformations", originalOps, regeneratedOps);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetAllPendingCompactionOperationsWithDupFileId() throws IOException {
    // Case where there is duplicate fileIds in compaction requests
    HoodieCompactionPlan plan1 = createCompactionPlan(metaClient, "000", "001", 10, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan(metaClient, "002", "003", 0, false, false);
    scheduleCompaction(metaClient, "001", plan1);
    scheduleCompaction(metaClient, "003", plan2);
    // schedule similar plan again so that there will be duplicates
    plan1.getOperations().get(0).setDataFilePath("bla");
    scheduleCompaction(metaClient, "005", plan1);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> res =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
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
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
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

  /**
   * Validates if generated compaction plan matches with input file-slices.
   *
   * @param input File Slices with partition-path
   * @param plan Compaction Plan
   */
  private void testFileSlicesCompactionPlanEquality(List<Pair<String, FileSlice>> input, HoodieCompactionPlan plan) {
    Assert.assertEquals("All file-slices present", input.size(), plan.getOperations().size());
    IntStream.range(0, input.size()).boxed().forEach(idx -> testFileSliceCompactionOpEquality(input.get(idx).getValue(),
        plan.getOperations().get(idx), input.get(idx).getKey(), plan.getVersion()));
  }

  /**
   * Validates if generated compaction operation matches with input file slice and partition path.
   *
   * @param slice File Slice
   * @param op HoodieCompactionOperation
   * @param expPartitionPath Partition path
   */
  private void testFileSliceCompactionOpEquality(FileSlice slice, HoodieCompactionOperation op, String expPartitionPath,
      int version) {
    Assert.assertEquals("Partition path is correct", expPartitionPath, op.getPartitionPath());
    Assert.assertEquals("Same base-instant", slice.getBaseInstantTime(), op.getBaseInstantTime());
    Assert.assertEquals("Same file-id", slice.getFileId(), op.getFileId());
    if (slice.getDataFile().isPresent()) {
      HoodieDataFile df = slice.getDataFile().get();
      Assert.assertEquals("Same data-file", version == COMPACTION_METADATA_VERSION_1 ? df.getPath() : df.getFileName(),
          op.getDataFilePath());
    }
    List<String> paths = slice.getLogFiles().map(l -> l.getPath().toString()).collect(Collectors.toList());
    IntStream.range(0, paths.size()).boxed().forEach(idx -> {
      Assert.assertEquals("Log File Index " + idx,
          version == COMPACTION_METADATA_VERSION_1 ? paths.get(idx) : new Path(paths.get(idx)).getName(),
          op.getDeltaFilePaths().get(idx));
    });
    Assert.assertEquals("Metrics set", METRICS, op.getMetrics());
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
