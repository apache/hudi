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

package org.apache.hudi.source;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link IncrementalInputSplits}.
 */
public class TestIncrementalInputSplits extends HoodieCommonTestHarness {

  @BeforeEach
  void init() {
    initPath();
  }

  @Test
  void testFilterInstantsWithRange() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);

    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant commit1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "1");
    HoodieInstant commit2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant commit3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "3");
    timeline.createCompleteInstant(commit1);
    timeline.createCompleteInstant(commit2);
    timeline.createCompleteInstant(commit3);
    timeline = metaClient.reloadActiveTimeline();

    Map<String, String> completionTimeMap = timeline.filterCompletedInstants().getInstantsAsStream()
        .collect(Collectors.toMap(HoodieInstant::requestedTime, HoodieInstant::getCompletionTime));

    IncrementalQueryAnalyzer analyzer1 = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .rangeType(InstantRange.RangeType.OPEN_CLOSED)
        .startCompletionTime(completionTimeMap.get("1"))
        .skipClustering(true)
        .build();
    // previous read iteration read till instant time "1", next read iteration should return ["2", "3"]
    List<HoodieInstant> activeInstants1 = analyzer1.analyze().getActiveInstants();
    assertEquals(2, activeInstants1.size());
    assertIterableEquals(Arrays.asList(commit2, commit3), activeInstants1);

    // simulate first iteration cycle with read from the LATEST commit
    IncrementalQueryAnalyzer analyzer2 = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipClustering(true)
        .build();
    List<HoodieInstant> activeInstants2 = analyzer2.analyze().getActiveInstants();
    assertEquals(1, activeInstants2.size());
    assertIterableEquals(Collections.singletonList(commit3), activeInstants2);

    // specifying a start and end commit
    IncrementalQueryAnalyzer analyzer3 = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .startCompletionTime(completionTimeMap.get("1"))
        .endCompletionTime(completionTimeMap.get("3"))
        .skipClustering(true)
        .build();
    List<HoodieInstant> activeInstants3 = analyzer3.analyze().getActiveInstants();
    assertEquals(3, activeInstants3.size());
    assertIterableEquals(Arrays.asList(commit1, commit2, commit3), activeInstants3);

    // add an inflight instant which should be excluded
    HoodieInstant commit4 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "4");
    timeline.createNewInstant(commit4);
    timeline = metaClient.reloadActiveTimeline();
    assertEquals(4, timeline.getInstants().size());
    List<HoodieInstant> activeInstants4 = analyzer3.analyze().getActiveInstants();
    assertEquals(3, activeInstants4.size());
  }

  @Test
  void testFilterInstantsByConditionForMOR() throws IOException {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieActiveTimeline timelineMOR = metaClient.getActiveTimeline();

    // commit1: delta commit
    HoodieInstant commit1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "1");
    timelineMOR.createCompleteInstant(commit1);
    // commit2: delta commit
    HoodieInstant commit2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    // commit3: clustering
    timelineMOR.createCompleteInstant(commit2);
    HoodieInstant commit3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, "3");
    timelineMOR.createNewInstant(commit3);
    commit3 = timelineMOR.transitionClusterRequestedToInflight(commit3, Option.empty());
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.CLUSTER,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineMOR.transitionClusterInflightToComplete(true, INSTANT_GENERATOR.getClusteringCommitInflightInstant(commit3.requestedTime()),
        (HoodieReplaceCommitMetadata) commitMetadata);
    // commit4: insert overwrite
    HoodieInstant commit4 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "4");
    timelineMOR.createNewInstant(commit4);
    commit4 = timelineMOR.transitionReplaceRequestedToInflight(commit4, Option.empty());
    commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.INSERT_OVERWRITE,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineMOR.transitionReplaceInflightToComplete(true, INSTANT_GENERATOR.getReplaceCommitInflightInstant(commit4.requestedTime()),
        (HoodieReplaceCommitMetadata) commitMetadata);
    // commit5: insert overwrite table
    HoodieInstant commit5 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "5");
    timelineMOR.createNewInstant(commit5);
    commit5 = timelineMOR.transitionReplaceRequestedToInflight(commit5, Option.empty());
    commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.INSERT_OVERWRITE_TABLE,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineMOR.transitionReplaceInflightToComplete(true, INSTANT_GENERATOR.getReplaceCommitInflightInstant(commit5.requestedTime()),
        (HoodieReplaceCommitMetadata) commitMetadata);
    // commit6:  compaction
    HoodieInstant commit6 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "6");
    timelineMOR.createNewInstant(commit6);
    commit6 = timelineMOR.transitionCompactionRequestedToInflight(commit6);
    commit6 = timelineMOR.transitionCompactionInflightToComplete(false, commit6, new HoodieCommitMetadata());
    timelineMOR.createCompleteInstant(commit6);
    timelineMOR = timelineMOR.reload();

    // will not filter commits by default
    HoodieTimeline resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineMOR, false, false, false);
    assertEquals(6, resTimeline.getInstants().size());

    // filter cluster commits
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineMOR, false, true, false);
    assertEquals(5, resTimeline.getInstants().size());
    assertFalse(resTimeline.containsInstant(commit3));

    // filter compaction commits for mor table
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineMOR, true, false, false);
    assertFalse(resTimeline.containsInstant(commit6));

    // filter insert overwriter commits
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineMOR, false, false, true);
    assertEquals(4, resTimeline.getInstants().size());
    assertFalse(resTimeline.containsInstant(commit4));
    assertFalse(resTimeline.containsInstant(commit5));
  }

  @Test
  void testFilterInstantsByConditionForCOW() throws IOException {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieActiveTimeline timelineCOW = metaClient.getActiveTimeline();

    // commit1: commit
    HoodieInstant commit1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    timelineCOW.createCompleteInstant(commit1);
    // commit2: commit
    HoodieInstant commit2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "2");
    // commit3: clustering
    timelineCOW.createCompleteInstant(commit2);
    HoodieInstant commit3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, "3");
    timelineCOW.createNewInstant(commit3);
    commit3 = timelineCOW.transitionClusterRequestedToInflight(commit3, Option.empty());
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.CLUSTER,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineCOW.transitionClusterInflightToComplete(true,
            INSTANT_GENERATOR.getClusteringCommitInflightInstant(commit3.requestedTime()),
            (HoodieReplaceCommitMetadata) commitMetadata);
    // commit4: insert overwrite
    HoodieInstant commit4 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "4");
    timelineCOW.createNewInstant(commit4);
    commit4 = timelineCOW.transitionReplaceRequestedToInflight(commit4, Option.empty());
    commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.INSERT_OVERWRITE,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineCOW.transitionReplaceInflightToComplete(true, INSTANT_GENERATOR.getReplaceCommitInflightInstant(commit4.requestedTime()),
        (HoodieReplaceCommitMetadata) commitMetadata);
    // commit5: insert overwrite table
    HoodieInstant commit5 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "5");
    timelineCOW.createNewInstant(commit5);
    commit5 = timelineCOW.transitionReplaceRequestedToInflight(commit5, Option.empty());
    commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.INSERT_OVERWRITE_TABLE,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timelineCOW.transitionReplaceInflightToComplete(true, INSTANT_GENERATOR.getReplaceCommitInflightInstant(commit5.requestedTime()),
        (HoodieReplaceCommitMetadata) commitMetadata);

    timelineCOW = timelineCOW.reload();

    // will not filter commits by default
    HoodieTimeline resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineCOW, false, false, false);
    assertEquals(5, resTimeline.getInstants().size());

    // filter cluster commits
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineCOW, false, true, false);
    assertEquals(4, resTimeline.getInstants().size());
    assertFalse(resTimeline.containsInstant(commit3));

    // cow table skip-compact does not take effect (because if it take effect will affect normal commits)
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineCOW, true, false, false);
    assertEquals(5, resTimeline.getInstants().size());

    // filter insert overwriter commits
    resTimeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timelineCOW, false, false, true);
    assertEquals(3, resTimeline.getInstants().size());
    assertFalse(resTimeline.containsInstant(commit4));
    assertFalse(resTimeline.containsInstant(commit5));
  }

  @Test
  void testInputSplitsSortedByPartition() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);

    // To enable a full table scan
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, false);
    List<String> partitions = getFilteredPartitions(result);
    assertEquals(Arrays.asList("par1", "par2", "par3", "par4", "par5", "par6"), partitions);
  }

  @ParameterizedTest
  @MethodSource("partitionEvaluators")
  void testInputSplitsWithPartitionPruner(
      ExpressionEvaluators.Evaluator partitionEvaluator,
      List<String> expectedPartitions) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);

    List<RowData> testData = new ArrayList<>();
    testData.addAll(TestData.DATA_SET_INSERT.stream().collect(Collectors.toList()));
    testData.addAll(TestData.DATA_SET_INSERT_PARTITION_IS_NULL.stream().collect(Collectors.toList()));
    TestData.writeData(testData, conf);
    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder()
            .partitionEvaluators(Collections.singletonList(partitionEvaluator))
            .partitionKeys(Collections.singletonList("partition"))
            .partitionTypes(Collections.singletonList(DataTypes.STRING()))
            .defaultParName(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)
            .hivePartition(false)
            .build();
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .partitionPruner(partitionPruner)
        .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, false);
    List<String> partitions = getFilteredPartitions(result);
    assertEquals(expectedPartitions, partitions);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInputSplitsWithPartitionStatsPruner(HoodieTableType tableType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      // enable CSI for MOR table to collect col stats for delta write stats,
      // which will be used to construct partition stats then.
      conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    }
    metaClient = HoodieTestUtils.init(basePath, tableType);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // uuid > 'id5' and age < 30, only column stats of 'par3' matches the filter.
    ColumnStatsProbe columnStatsProbe =
        ColumnStatsProbe.newInstance(Arrays.asList(
            CallExpression.permanent(
                FunctionIdentifier.of("greaterThan"),
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                    new ValueLiteralExpression("id5", DataTypes.STRING().notNull())
                ),
                DataTypes.BOOLEAN()),
            CallExpression.permanent(
                FunctionIdentifier.of("lessThan"),
                BuiltInFunctionDefinitions.LESS_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("age", DataTypes.INT(), 2, 2),
                    new ValueLiteralExpression(30, DataTypes.INT().notNull())
                ),
                DataTypes.BOOLEAN())));

    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder().rowType(TestConfigurations.ROW_TYPE).basePath(basePath).metaClient(metaClient).conf(conf).columnStatsProbe(columnStatsProbe).build();
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .partitionPruner(partitionPruner)
        .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, false);
    List<String> partitions = getFilteredPartitions(result);
    assertEquals(Arrays.asList("par3"), partitions);
  }

  @Test
  void testInputSplitsWithSpeedLimit() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true);
    conf.set(FlinkOptions.READ_COMMITS_LIMIT, 1);
    // insert data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTimeline commitsTimeline = metaClient.reloadActiveTimeline()
            .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    HoodieInstant firstInstant = commitsTimeline.firstInstant().get();
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
            .conf(conf)
            .path(new Path(basePath))
            .rowType(TestConfigurations.ROW_TYPE)
            .partitionPruner(null)
            .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, firstInstant.getCompletionTime(), false);

    String minStartCommit = result.getInputSplits().stream()
            .map(split -> split.getInstantRange().get().getStartInstant().get())
            .min((commit1,commit2) -> compareTimestamps(commit1, LESSER_THAN, commit2) ? 1 : 0)
            .orElse(null);
    String maxEndCommit = result.getInputSplits().stream()
            .map(split -> split.getInstantRange().get().getEndInstant().get())
            .max((commit1,commit2) -> compareTimestamps(commit1, GREATER_THAN, commit2) ? 1 : 0)
            .orElse(null);
    assertEquals(0, intervalBetween2Instants(commitsTimeline, minStartCommit, maxEndCommit), "Should read 1 instant");
  }

  @Test
  void testInputSplitsForSplitLastCommit() throws Exception {
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true);
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());

    // insert data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTimeline commitsTimeline =
        metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = commitsTimeline.getInstants();
    String lastInstant = commitsTimeline.lastInstant().map(HoodieInstant::requestedTime).get();
    List<HoodieCommitMetadata> metadataList = instants.stream()
        .map(instant -> WriteProfiles.getCommitMetadata(tableName, new Path(basePath), instant,
            commitsTimeline)).collect(Collectors.toList());
    List<StoragePathInfo> pathInfoList = WriteProfiles.getFilesFromMetadata(
        new Path(basePath), (org.apache.hadoop.conf.Configuration) metaClient.getStorageConf().unwrap(),
        metadataList, metaClient.getTableType());
    HoodieTableFileSystemView fileSystemView =
        new HoodieTableFileSystemView(metaClient, commitsTimeline, pathInfoList);
    Map<String, String> fileIdToBaseInstant = fileSystemView.getAllFileSlices("par1")
        .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));

    IncrementalInputSplits iis = IncrementalInputSplits.builder()
            .conf(conf)
            .path(new Path(basePath))
            .rowType(TestConfigurations.ROW_TYPE)
            .partitionPruner(null)
            .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, false);
    result.getInputSplits().stream().filter(split -> fileIdToBaseInstant.containsKey(split.getFileId()))
            .forEach(split -> assertEquals(fileIdToBaseInstant.get(split.getFileId()), split.getLatestCommit()));
    assertTrue(result.getInputSplits().stream().anyMatch(split -> split.getLatestCommit().equals(lastInstant)),
            "Some input splits' latest commit time should equal to the last instant");
    assertTrue(result.getInputSplits().stream().anyMatch(split -> !split.getLatestCommit().equals(lastInstant)),
            "The input split latest commit time does not always equal to last instant");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static Stream<Arguments> partitionEvaluators() {
    FieldReferenceExpression partitionFieldRef = new FieldReferenceExpression("partition", DataTypes.STRING(), 0, 0);
    // `partition` != 'par3'
    ExpressionEvaluators.Evaluator notEqualTo = ExpressionEvaluators.NotEqualTo.getInstance()
        .bindVal(new ValueLiteralExpression("par3"))
        .bindFieldReference(partitionFieldRef);

    // `partition` >= 'par2'
    ExpressionEvaluators.Evaluator greaterThan = ExpressionEvaluators.GreaterThanOrEqual.getInstance()
        .bindVal(new ValueLiteralExpression("par2"))
        .bindFieldReference(partitionFieldRef);

    // `partition` != 'par3' and `partition` >= 'par2'
    ExpressionEvaluators.Evaluator and = ExpressionEvaluators.And.getInstance()
        .bindEvaluator(greaterThan, notEqualTo);

    // `partition` in ('par1', 'par4')
    ExpressionEvaluators.In in = ExpressionEvaluators.In.getInstance();
    in.bindFieldReference(partitionFieldRef);
    in.bindVals("par1", "par4");

    // `partition` is not null
    ExpressionEvaluators.IsNotNull isNotNull = ExpressionEvaluators.IsNotNull.getInstance();
    isNotNull.bindFieldReference(partitionFieldRef);

    // `partition` is null
    ExpressionEvaluators.IsNull isNull = ExpressionEvaluators.IsNull.getInstance();
    isNull.bindFieldReference(partitionFieldRef);

    Object[][] data = new Object[][] {
        {notEqualTo, Arrays.asList("par1", "par2", "par4")},
        {greaterThan, Arrays.asList("par2", "par3", "par4")},
        {and, Arrays.asList("par2", "par4")},
        {in, Arrays.asList("par1", "par4")},
        {isNotNull, Arrays.asList("par1", "par2", "par3", "par4")},
        {isNull, Arrays.asList(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)}};
    return Stream.of(data).map(Arguments::of);
  }

  private List<String> getFilteredPartitions(IncrementalInputSplits.Result result) {
    List<String> partitions = new ArrayList<>();
    result.getInputSplits().forEach(split -> {
      split.getBasePath().map(path -> {
        String[] pathParts = path.split("/");
        partitions.add(pathParts[pathParts.length - 2]);
        return null;
      });
      split.getLogPaths().map(paths -> {
        paths.forEach(path -> {
          String[] pathParts = path.split("/");
          partitions.add(pathParts[pathParts.length - 2]);
        });
        return null;
      });
    });
    return partitions;
  }

  private Integer intervalBetween2Instants(HoodieTimeline timeline, String instant1, String instant2) {
    Integer idxInstant1 = getInstantIdxInTimeline(timeline, instant1);
    Integer idxInstant2 = getInstantIdxInTimeline(timeline, instant2);
    return (idxInstant1 != -1 && idxInstant2 != -1) ? Math.abs(idxInstant1 - idxInstant2) : -1;
  }

  private Integer getInstantIdxInTimeline(HoodieTimeline timeline, String instant) {
    List<HoodieInstant> instants = timeline.getInstants();
    return IntStream.range(0, instants.size())
            .filter(i -> instants.get(i).requestedTime().equals(instant))
            .findFirst()
            .orElse(-1);
  }
}
