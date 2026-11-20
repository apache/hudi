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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.plan.generators.HoodieCompactionPlanGenerator;
import org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestScheduleCompactionActionExecutor extends HoodieCommonTestHarness {

  private final HoodieEngineContext context = new HoodieLocalEngineContext(getStorageConf());
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTable table;

  @Test
  void testInitPlanGenerator() throws IOException {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCompactionConfig.COMPACTION_PLAN_GENERATOR.key(), TestCompactionPlanGenerator.class.getName());
    when(config.getProps()).thenReturn(properties);
    when(config.getCompactionStrategy()).thenReturn(new LogFileNumBasedCompactionStrategy());
    Assertions.assertDoesNotThrow(() -> new ScheduleCompactionActionExecutor(
        context, config, table, "001", Option.empty(), WriteOperationType.COMPACT
    ));
    Assertions.assertEquals(1, TestCompactionPlanGenerator.getCount());
  }

  @Test
  void testEmptyDeltaCommitsAreNotCountedTowardsTheTrigger() throws Exception {
    // 3 delta commits wrote files and 7 were empty: with max.delta.commits = 10, compaction must wait.
    assertEquals(3, newExecutorOnDeltaCommits(3, 7, 10, 50).countDeltaCommitsForTrigger(completedDeltaCommits()));
  }

  @Test
  void testEmptyDeltaCommitsCountOnceTheTotalReachesTheCap() throws Exception {
    assertEquals(10, newExecutorOnDeltaCommits(3, 7, 10, 10).countDeltaCommitsForTrigger(completedDeltaCommits()));
  }

  @Test
  void testNonEmptyDeltaCommitsAreCountedUpToMaxDeltaCommits() throws Exception {
    assertEquals(3, newExecutorOnDeltaCommits(5, 2, 3, 50).countDeltaCommitsForTrigger(completedDeltaCommits()));
  }

  @Test
  void testNumCommitsAfterLastRequestIgnoresEmptyDeltaCommits() throws Exception {
    when(config.isCompactionIgnoreEmptyCommits()).thenReturn(true);
    ScheduleCompactionActionExecutor executor = newExecutorOnDeltaCommits(2, 5, 3, 50);
    when(table.getActiveTimeline()).thenReturn(metaClient.getActiveTimeline());
    assertFalse(executor.needCompact(CompactionTriggerStrategy.NUM_COMMITS_AFTER_LAST_REQUEST));

    HoodieTestTable.of(metaClient).addDeltaCommit("100", commitMetadataWithWrites());
    metaClient.reloadActiveTimeline();
    when(table.getActiveTimeline()).thenReturn(metaClient.getActiveTimeline());
    assertTrue(executor.needCompact(CompactionTriggerStrategy.NUM_COMMITS_AFTER_LAST_REQUEST));
  }

  private ScheduleCompactionActionExecutor newExecutorOnDeltaCommits(int nonEmpty, int empty, int maxDeltaCommits,
                                                                     int maxTotalDeltaCommits) throws Exception {
    initPath();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    int instant = 1;
    for (int i = 0; i < nonEmpty; i++) {
      testTable.addDeltaCommit(String.format("%03d", instant++), commitMetadataWithWrites());
    }
    for (int i = 0; i < empty; i++) {
      testTable.addDeltaCommit(String.format("%03d", instant++), new HoodieCommitMetadata());
    }
    metaClient.reloadActiveTimeline();
    when(config.getProps()).thenReturn(new TypedProperties());
    when(config.getCompactionStrategy()).thenReturn(new LogFileNumBasedCompactionStrategy());
    lenient().when(config.getInlineCompactDeltaCommitMax()).thenReturn(maxDeltaCommits);
    lenient().when(config.getInlineCompactMaxTotalDeltaCommits()).thenReturn(maxTotalDeltaCommits);
    lenient().when(table.getMetaClient()).thenReturn(metaClient);
    return new ScheduleCompactionActionExecutor(context, config, table, "200", Option.empty(), WriteOperationType.COMPACT);
  }

  private HoodieTimeline completedDeltaCommits() {
    return metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
  }

  private static HoodieCommitMetadata commitMetadataWithWrites() {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p1");
    stat.setFileId("f1");
    stat.setNumWrites(10);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat("p1", stat);
    return metadata;
  }

  @Test
  void testMergeExtraMetadataKeepsPlanGeneratorEntries() {
    HoodieCompactionPlan plan = planWithExtraMetadata(generatorMetadata());
    Map<String, String> provided = new HashMap<>();
    provided.put("caller.key", "caller.value");
    ScheduleCompactionActionExecutor.mergeExtraMetadata(plan, Option.of(provided));
    Map<String, String> expected = new HashMap<>(generatorMetadata());
    expected.putAll(provided);
    Assertions.assertEquals(expected, plan.getExtraMetadata());
  }

  @Test
  void testMergeExtraMetadataGeneratorWinsOnCollision() {
    HoodieCompactionPlan plan = planWithExtraMetadata(generatorMetadata());
    ScheduleCompactionActionExecutor.mergeExtraMetadata(plan, Option.of(Collections.singletonMap("generator.mode", "other")));
    Assertions.assertEquals(generatorMetadata(), plan.getExtraMetadata());
  }

  @Test
  void testMergeExtraMetadataWithoutCallerEntriesLeavesPlanUntouched() {
    HoodieCompactionPlan plan = planWithExtraMetadata(generatorMetadata());
    ScheduleCompactionActionExecutor.mergeExtraMetadata(plan, Option.empty());
    Assertions.assertEquals(generatorMetadata(), plan.getExtraMetadata());
  }

  @Test
  void testMergeExtraMetadataOntoPlanWithoutEntries() {
    HoodieCompactionPlan plan = planWithExtraMetadata(null);
    Map<String, String> provided = Collections.singletonMap("caller.key", "caller.value");
    ScheduleCompactionActionExecutor.mergeExtraMetadata(plan, Option.of(provided));
    Assertions.assertEquals(provided, plan.getExtraMetadata());
  }

  private static Map<String, String> generatorMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("generator.mode", "planned");
    metadata.put("generator.state", "[\"a\",\"b\"]");
    return metadata;
  }

  private static HoodieCompactionPlan planWithExtraMetadata(Map<String, String> extraMetadata) {
    return HoodieCompactionPlan.newBuilder()
        .setOperations(Collections.emptyList())
        .setExtraMetadata(extraMetadata)
        .build();
  }

  public static class TestCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends HoodieCompactionPlanGenerator<T, I, K, O> {
    private static int count = 0;

    public TestCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig, BaseTableServicePlanActionExecutor executor) {
      super(table, engineContext, writeConfig, executor);
      count++;
    }

    public static int getCount() {
      return count;
    }
  }
}
