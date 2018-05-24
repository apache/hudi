/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import static com.uber.hoodie.common.model.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCompactionUtils {

  private static final Map<String, Double> metrics =
      new ImmutableMap.Builder<String, Double>()
          .put("key1", 1.0)
          .put("key2", 3.0).build();
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private HoodieTableMetaClient metaClient;
  private String basePath;
  private Function<Pair<String, FileSlice>, Map<String, Double>> metricsCaptureFn = (partitionFileSlice) -> metrics;

  @Before
  public void init() throws IOException {
    metaClient = HoodieTestUtils.init(tmpFolder.getRoot().getAbsolutePath());
    basePath = metaClient.getBasePath();
  }

  @Test
  public void testBuildFromFileSlice() {
    // Empty File-Slice with no data and log files
    FileSlice emptyFileSlice = new FileSlice("000", "empty1");
    HoodieCompactionOperation op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], emptyFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(emptyFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    // File Slice with data-file but no log files
    FileSlice noLogFileSlice = new FileSlice("000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], noLogFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noLogFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    //File Slice with no data-file but log files present
    FileSlice noDataFileSlice = new FileSlice("000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], noDataFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noDataFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    //File Slice with data-file and log files present
    FileSlice fileSlice = new FileSlice("000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], fileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(fileSlice, op, DEFAULT_PARTITION_PATHS[0]);
  }

  /**
   * Generate input for compaction plan tests
   */
  private Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> buildCompactionPlan() {
    FileSlice emptyFileSlice = new FileSlice("000", "empty1");
    FileSlice fileSlice = new FileSlice("000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    FileSlice noLogFileSlice = new FileSlice("000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    FileSlice noDataFileSlice = new FileSlice("000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    List<FileSlice> fileSliceList = Arrays.asList(emptyFileSlice, noDataFileSlice, fileSlice, noLogFileSlice);
    List<Pair<String, FileSlice>> input = fileSliceList.stream().map(f -> Pair.of(DEFAULT_PARTITION_PATHS[0], f))
        .collect(Collectors.toList());
    return Pair.of(input, CompactionUtils.buildFromFileSlices(input, Optional.empty(), Optional.of(metricsCaptureFn)));
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
    List<HoodieCompactionOperation> regeneratedOps =
        originalOps.stream().map(op -> {
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
    HoodieCompactionPlan plan1 = createCompactionPlan("000", 10);
    HoodieCompactionPlan plan2 = createCompactionPlan("001", 10);
    scheduleCompaction("000", plan1);
    scheduleCompaction("001", plan2);
    // schedule same plan again so that there will be duplicates
    scheduleCompaction("003", plan1);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    Map<String, Pair<String, HoodieCompactionOperation>> res =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
  }

  @Test
  public void testGetAllPendingCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty.
    testGetAllPendingCompactionOperations(false, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingInflightCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty. All of them are marked inflight
    testGetAllPendingCompactionOperations(true, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingCompactionOperationsForEmptyCompactions() throws IOException {
    // Case where there are 4 compaction requests and all are empty.
    testGetAllPendingCompactionOperations(false, 0, 0, 0, 0);
  }

  private void testGetAllPendingCompactionOperations(boolean inflight, int numEntriesInPlan1, int numEntriesInPlan2,
      int numEntriesInPlan3, int numEntriesInPlan4) throws IOException {
    HoodieCompactionPlan plan1 = createCompactionPlan("000", numEntriesInPlan1);
    HoodieCompactionPlan plan2 = createCompactionPlan("001", numEntriesInPlan2);
    HoodieCompactionPlan plan3 = createCompactionPlan("002", numEntriesInPlan3);
    HoodieCompactionPlan plan4 = createCompactionPlan("003", numEntriesInPlan4);

    if (inflight) {
      scheduleInflightCompaction("000", plan1);
      scheduleInflightCompaction("001", plan2);
      scheduleInflightCompaction("002", plan3);
      scheduleInflightCompaction("003", plan4);
    } else {
      scheduleCompaction("000", plan1);
      scheduleCompaction("001", plan2);
      scheduleCompaction("002", plan3);
      scheduleCompaction("003", plan4);
    }

    List<Integer> expectedNumEntries =
        Arrays.asList(numEntriesInPlan1, numEntriesInPlan2, numEntriesInPlan3, numEntriesInPlan4);
    List<HoodieCompactionPlan> plans = new ImmutableList.Builder<HoodieCompactionPlan>()
        .add(plan1, plan2, plan3, plan4).build();
    IntStream.range(0, 4).boxed().forEach(idx -> {
      if (expectedNumEntries.get(idx) > 0) {
        Assert.assertEquals("check if plan " + idx + " has exp entries",
            expectedNumEntries.get(idx).longValue(), plans.get(idx).getOperations().size());
      } else {
        Assert.assertNull("Plan " + idx + " has null ops", plans.get(idx).getOperations());
      }
    });

    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    Map<String, Pair<String, HoodieCompactionOperation>> pendingCompactionMap =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);

    Map<String, Pair<String, HoodieCompactionOperation>> expPendingCompactionMap =
        generateExpectedCompactionOperations(Arrays.asList(plan1, plan2, plan3, plan4));

    // Ensure all the
    Assert.assertEquals(expPendingCompactionMap, pendingCompactionMap);
  }

  private Map<String, Pair<String, HoodieCompactionOperation>> generateExpectedCompactionOperations(
      List<HoodieCompactionPlan> plans) {
    return plans.stream()
        .flatMap(plan -> {
          if (plan.getOperations() != null) {
            return plan.getOperations().stream().map(op -> Pair.of(op.getFileId(),
                Pair.of(op.getBaseInstantTime(), op)));
          }
          return Stream.empty();
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private void scheduleCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    metaClient.getActiveTimeline().saveToRequested(new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, instantTime),
        AvroUtils.serializeCompactionWorkload(compactionPlan));
  }

  private void scheduleInflightCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    metaClient.getActiveTimeline().saveToInflight(new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, instantTime),
        AvroUtils.serializeCompactionWorkload(compactionPlan));
  }

  private HoodieCompactionPlan createCompactionPlan(String instantId, int numFileIds) {
    List<HoodieCompactionOperation> ops = IntStream.range(0, numFileIds).boxed().map(idx -> {
      try {
        String fileId =
            HoodieTestUtils.createNewDataFile(basePath, DEFAULT_PARTITION_PATHS[0], instantId);
        HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
            instantId, fileId, Optional.of(1));
        HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
            instantId, fileId, Optional.of(2));
        FileSlice slice = new FileSlice(instantId, fileId);
        slice.setDataFile(new TestHoodieDataFile(HoodieTestUtils.createDataFile(basePath, DEFAULT_PARTITION_PATHS[0],
            instantId, fileId)));
        String logFilePath1 = HoodieTestUtils.getLogFilePath(basePath, DEFAULT_PARTITION_PATHS[0], instantId, fileId,
            Optional.of(1));
        String logFilePath2 = HoodieTestUtils.getLogFilePath(basePath, DEFAULT_PARTITION_PATHS[0], instantId, fileId,
            Optional.of(2));
        slice.addLogFile(new HoodieLogFile(new Path(logFilePath1)));
        slice.addLogFile(new HoodieLogFile(new Path(logFilePath2)));
        return CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], slice, Optional.empty());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }).collect(Collectors.toList());
    return new HoodieCompactionPlan(ops.isEmpty() ? null : ops, new HashMap<>());
  }

  /**
   * Validates if  generated compaction plan matches with input file-slices
   *
   * @param input File Slices with partition-path
   * @param plan  Compaction Plan
   */
  private void testFileSlicesCompactionPlanEquality(List<Pair<String, FileSlice>> input,
      HoodieCompactionPlan plan) {
    Assert.assertEquals("All file-slices present", input.size(), plan.getOperations().size());
    IntStream.range(0, input.size()).boxed().forEach(idx ->
        testFileSliceCompactionOpEquality(input.get(idx).getValue(), plan.getOperations().get(idx),
            input.get(idx).getKey()));
  }

  /**
   * Validates if generated compaction operation matches with input file slice and partition path
   *
   * @param slice            File Slice
   * @param op               HoodieCompactionOperation
   * @param expPartitionPath Partition path
   */
  private void testFileSliceCompactionOpEquality(FileSlice slice, HoodieCompactionOperation op,
      String expPartitionPath) {
    Assert.assertEquals("Partition path is correct", expPartitionPath, op.getPartitionPath());
    Assert.assertEquals("Same base-instant", slice.getBaseInstantTime(), op.getBaseInstantTime());
    Assert.assertEquals("Same file-id", slice.getFileId(), op.getFileId());
    if (slice.getDataFile().isPresent()) {
      Assert.assertEquals("Same data-file", slice.getDataFile().get().getPath(), op.getDataFilePath());
    }
    List<String> paths = slice.getLogFiles().map(l -> l.getPath().toString()).collect(Collectors.toList());
    IntStream.range(0, paths.size()).boxed().forEach(idx -> {
      Assert.assertEquals("Log File Index " + idx, paths.get(idx), op.getDeltaFilePaths().get(idx));
    });
    Assert.assertEquals("Metrics set", metrics, op.getMetrics());
  }


  private static class TestHoodieDataFile extends HoodieDataFile {

    private final String path;

    public TestHoodieDataFile(String path) {
      super(null);
      this.path = path;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public String getFileId() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String getCommitTime() {
      return "100";
    }

    @Override
    public long getFileSize() {
      return 0;
    }
  }
}
