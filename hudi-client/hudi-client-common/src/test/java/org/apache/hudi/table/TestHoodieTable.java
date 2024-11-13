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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class TestHoodieTable extends HoodieCommonTestHarness {
  @ParameterizedTest
  @MethodSource("generateParametersForValidateTimestampInternal")
  void testValidateForLatestTimestampInterval(
      boolean shouldEnableTimestampOrderingValidation,
      boolean supportsOcc,
      boolean shouldValidateForLatestTimestamp
  ) throws IOException {
    initMetaClient();
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY).build())
        .withEnableTimestampOrderingValidation(shouldEnableTimestampOrderingValidation);
    if (supportsOcc) {
      writeConfigBuilder.withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    }

    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfigBuilder.build(), context, metaClient);

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);
    if (shouldValidateForLatestTimestamp) {
      HoodieInstant lastEntry = metaClient.getActiveTimeline().getWriteTimeline().lastInstant().get();
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> hoodieTable.validateForLatestTimestampInternal("001"));
      assertEquals(String.format("Found later commit time %s, compared to the current instant 001, hence failing to create requested commit meta file", lastEntry), exception.getMessage());
    } else {
      assertDoesNotThrow(() -> hoodieTable.validateForLatestTimestampInternal("001"));
    }
  }

  private static Stream<Arguments> generateParametersForValidateTimestampInternal() {
    return Stream.of(
      Arguments.of(true, true, true),
      Arguments.of(true, false, false),
      Arguments.of(false, false, false)
    );
  }

  private static class TestBaseHoodieTable extends HoodieTable {
    protected TestBaseHoodieTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
      super(config, context, metaClient);
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
      return null;
    }

    @Override
    public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
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
    public void validateForLatestTimestamp(String instantTime) {
    }

    @Override
    public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List partitionsToIndex) {
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
