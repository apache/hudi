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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.storage.HoodieStorageLayout;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieTable extends HoodieCommonTestHarness {
  @Test
  void getIndexReturnsCachedInstance() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    HoodieIndex<?, ?> index = hoodieTable.getIndex();
    assertSame(index, hoodieTable.getIndex());
  }

  @Test
  void getStorageLayoutReturnsCachedInstance() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    HoodieStorageLayout storageLayout = hoodieTable.getStorageLayout();
    assertSame(storageLayout, hoodieTable.getStorageLayout());
  }

  @Test
  void testGetEngineContext() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    // before serialization, context is the same one that is passed in
    assertSame(context, hoodieTable.getContext());
    // after serialization, we expect a local context to be made
    HoodieTable deserializedTable = SerializationUtils.deserialize(SerializationUtils.serialize(hoodieTable));
    assertTrue(deserializedTable.getContext() instanceof HoodieLocalEngineContext);
  }

  @Test
  void testRollbackInflightInstant() throws IOException {
    // Setup.
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable =
        new TestBaseHoodieTable(writeConfig, context, metaClient);
    // Prepare test inputs.
    HoodieInstant inflightInstant = new HoodieInstant(
        HoodieInstant.State.INFLIGHT,
        COMPACTION_ACTION,
        "123",
        InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    // Mock getPendingRollbackInstantFunc behavior.
    Function<String, Option<HoodiePendingRollbackInfo>>
        getPendingRollbackInstantFunc = mock(Function.class);
    HoodiePendingRollbackInfo pendingRollbackInfo = new HoodiePendingRollbackInfo(
        inflightInstant, new HoodieRollbackPlan());
    when(getPendingRollbackInstantFunc.apply("123"))
        .thenReturn(Option.of(pendingRollbackInfo));

    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    timeline.createNewInstant(inflightInstant);
    // Case 1: Execute the method with pending rollback instant.
    hoodieTable.rollbackInflightInstant(
        inflightInstant, getPendingRollbackInstantFunc);
    // Validate that function scheduleRollback is not called.
    assertEquals(0, ((TestBaseHoodieTable)hoodieTable).getCountOfScheduleRollbackFunctionCalls());

    // Reset the parameters.
    when(getPendingRollbackInstantFunc.apply("123"))
        .thenReturn(Option.empty());
    timeline.createNewInstant(inflightInstant);
    // Case 2: Execute the method without pending rollback instant.
    hoodieTable.rollbackInflightInstant(
        inflightInstant, getPendingRollbackInstantFunc);
    // Validate that function scheduleRollback is called.
    assertEquals(1, ((TestBaseHoodieTable)hoodieTable).getCountOfScheduleRollbackFunctionCalls());
  }

  private static class TestBaseHoodieTable extends HoodieTable {
    protected TestBaseHoodieTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
      super(config, context, metaClient);
    }

    private int countOfScheduleRollbackFunctionCalls = 0;

    public int getCountOfScheduleRollbackFunctionCalls() {
      return countOfScheduleRollbackFunctionCalls;
    }

    @Override
    protected HoodieIndex<?, ?> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata delete(HoodieEngineContext context, String instantTime, Object keys) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwriteTable(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata managePartitionTTL(HoodieEngineContext context, String instantTime) {
      return null;
    }

    @Override
    public HoodieWriteMetadata compact(HoodieEngineContext context, String compactionInstantTime) {
      return null;
    }

    @Override
    public HoodieWriteMetadata cluster(HoodieEngineContext context, String clusteringInstantTime) {
      return null;
    }

    @Override
    public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    }

    @Override
    public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
      return null;
    }

    @Override
    public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                       boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore) {
      countOfScheduleRollbackFunctionCalls++;
      return null;
    }

    @Override
    public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
      return null;
    }

    @Override
    public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List partitionsToIndex, List partitionPaths) {
      return null;
    }

    @Override
    public Option<HoodieIndexCommitMetadata> index(HoodieEngineContext context, String indexInstantTime) {
      return null;
    }

    @Override
    public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
      return null;
    }

    @Override
    public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieBootstrapWriteMetadata bootstrap(HoodieEngineContext context, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords, Option bulkInsertPartitioner) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List partitions) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsert(HoodieEngineContext context, String instantTime, Object records, Option bulkInsertPartitioner) {
      return null;
    }
  }
}
