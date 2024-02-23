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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Test cases for {@link IncrementalInputSplits}.
 */
public class TestIncrementalInputSplits extends HoodieCommonTestHarness {

  @BeforeEach
  void init() throws IOException {
    initPath();
    initMetaClient();
  }

  @Test
  void testFilterInstantsWithRange() {
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient, true);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .skipClustering(conf.getBoolean(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING))
        .build();

    HoodieInstant commit1 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "1");
    HoodieInstant commit2 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant commit3 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "3");
    timeline.createNewInstant(commit1);
    timeline.createNewInstant(commit2);
    timeline.createNewInstant(commit3);
    timeline = timeline.reload();

    // previous read iteration read till instant time "1", next read iteration should return ["2", "3"]
    List<HoodieInstant> instantRange2 = iis.filterInstantsWithRange(timeline, "1");
    assertEquals(2, instantRange2.size());
    assertIterableEquals(Arrays.asList(commit2, commit3), instantRange2);

    // simulate first iteration cycle with read from the LATEST commit
    List<HoodieInstant> instantRange1 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(1, instantRange1.size());
    assertIterableEquals(Collections.singletonList(commit3), instantRange1);

    // specifying a start and end commit
    conf.set(FlinkOptions.READ_START_COMMIT, "1");
    conf.set(FlinkOptions.READ_END_COMMIT, "3");
    List<HoodieInstant> instantRange3 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(3, instantRange3.size());
    assertIterableEquals(Arrays.asList(commit1, commit2, commit3), instantRange3);

    // add an inflight instant which should be excluded
    HoodieInstant commit4 = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "4");
    timeline.createNewInstant(commit4);
    timeline = timeline.reload();
    assertEquals(4, timeline.getInstants().size());
    List<HoodieInstant> instantRange4 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(3, instantRange4.size());
  }

  @Test
  void testFilterInstantsByCondition() throws IOException {
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient, true);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
            .conf(conf)
            .path(new Path(basePath))
            .rowType(TestConfigurations.ROW_TYPE)
            .build();

    HoodieInstant commit1 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "1");
    HoodieInstant commit2 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant commit3 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "3");
    timeline.createNewInstant(commit1);
    timeline.createNewInstant(commit2);
    timeline.createNewInstant(commit3);
    commit3 = timeline.transitionReplaceRequestedToInflight(commit3, Option.empty());
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(
            new ArrayList<>(),
            new HashMap<>(),
            Option.empty(),
            WriteOperationType.CLUSTER,
            "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    timeline.transitionReplaceInflightToComplete(
        HoodieTimeline.getReplaceCommitInflightInstant(commit3.getTimestamp()),
        Option.of(getUTF8Bytes(commitMetadata.toJsonString())));
    timeline = timeline.reload();

    conf.set(FlinkOptions.READ_END_COMMIT, "3");
    HoodieTimeline resTimeline = iis.filterInstantsAsPerUserConfigs(timeline);
    // will not filter cluster commit by default
    assertEquals(3, resTimeline.getInstants().size());
  }

  @Test
  void testInputSplitsSortedByPartition() throws Exception {
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient, true);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    // To enable a full table scan
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, null, false);
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
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    PartitionPruners.PartitionPruner partitionPruner = PartitionPruners.getInstance(
        Collections.singletonList(partitionEvaluator),
        Collections.singletonList("partition"),
        Collections.singletonList(DataTypes.STRING()),
        PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH,
        false);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .partitionPruner(partitionPruner)
        .build();
    IncrementalInputSplits.Result result = iis.inputSplits(metaClient, null, null, false);
    List<String> partitions = getFilteredPartitions(result);
    assertEquals(expectedPartitions, partitions);
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
    Object[][] data = new Object[][] {
        {notEqualTo, Arrays.asList("par1", "par2", "par4")},
        {greaterThan, Arrays.asList("par2", "par3", "par4")},
        {and, Arrays.asList("par2", "par4")},
        {in, Arrays.asList("par1", "par4")}};
    return Stream.of(data).map(Arguments::of);
  }

  private List<String> getFilteredPartitions(IncrementalInputSplits.Result result) {
    return result.getInputSplits().stream().map(split -> {
      Option<String> basePath = split.getBasePath();
      String[] pathParts = basePath.get().split("/");
      return pathParts[pathParts.length - 2];
    }).collect(Collectors.toList());
  }
}
