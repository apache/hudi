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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRollbackPlanActionExecutor extends HoodieClientRollbackTestBase {
  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initHoodieStorage();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testCopyOnWriteRollbackActionExecutorForFileListingAsGenerateFile() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";
    // Let's create some commit files and base files
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11").getLeft()
        .withBaseFilesInPartition(p2, "id12").getLeft()
        .withLogFile(p1, "id11", 3).getLeft();

    Map<String, String> partitionAndFileId1 = new HashMap<String, String>() {
      {
        put(p1, "id21");
        put(p2, "id22");
      }
    };
    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap1 = new HashMap<>();
    partitionAndFileId1.forEach((k, v) -> partitionToFilesNameLengthMap1.put(k, Collections.singletonList(Pair.of(v, 100))));
    HoodieCommitMetadata commitMetadata = testTable.doWriteOperation("002", WriteOperationType.UPSERT, Arrays.asList(p1, p2, p3), partitionToFilesNameLengthMap1,
        false, true);

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true).withEmbeddedTimelineServerEnabled(false)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build();
    HoodieTable table = this.getHoodieTable(metaClient, writeConfig);

    // create markers for inflight commit 002
    WriteMarkers writeMarkers = WriteMarkersFactory.get(MarkerType.DIRECT, table, "002");
    writeMarkers.createIfNotExists(p1, commitMetadata.getPartitionToWriteStats().get(p1).get(0).getPath().substring(11), IOType.CREATE);
    writeMarkers.createIfNotExists(p1, commitMetadata.getPartitionToWriteStats().get(p2).get(0).getPath().substring(11), IOType.CREATE);

    HoodieInstant needRollBackInstant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "002");
    String rollbackInstant1 = "003";
    String rollbackInstant2 = "004";
    CountDownLatch countDownLatch = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CompletableFuture<Boolean> future1 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          // trigger rollback planning by writer 1.
          BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
              new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant2, needRollBackInstant, false,
                  table.getConfig().shouldRollbackUsingMarkers(), false);
          HoodieRollbackPlan rollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();
          CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, rollbackInstant2, needRollBackInstant, true,
              false);
          HoodieRollbackMetadata hoodieRollbackMetadata = copyOnWriteRollbackActionExecutor.execute();
          assertTrue(hoodieRollbackMetadata.getTotalFilesDeleted() > 0);
          // once rollback planning is complete, count down the latch
          countDownLatch.countDown();
          return true;
        }
      }, executor);

      CompletableFuture<Boolean> future2 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          MockRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
              new MockRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant1, needRollBackInstant, false,
                  table.getConfig().shouldRollbackUsingMarkers(), false, countDownLatch);
          // this execute will wait until count down latch reaches 0 successfully before proceeding.
          Option<HoodieRollbackPlan> rollbackPlan = (Option<HoodieRollbackPlan>) copyOnWriteRollbackPlanActionExecutor.execute();
          assertTrue(rollbackPlan.isEmpty());
          return true;
        }
      }, executor);

      CompletableFuture.allOf(future1, future2).join();
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Mock RollbackPlanActionExecutor to await on count down latch before calling super.execute() for rollback planning.
   */
  class MockRollbackPlanActionExecutor extends BaseRollbackPlanActionExecutor {

    private CountDownLatch countDownLatch;

    public MockRollbackPlanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table, String instantTime,
                                          HoodieInstant instantToRollback, boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore,
                                          CountDownLatch countDownLatch) {
      super(context, config, table, instantTime, instantToRollback, skipTimelinePublish, shouldRollbackUsingMarkers, isRestore);
      this.countDownLatch = countDownLatch;
    }

    @Override
    public Option<HoodieRollbackPlan> execute() {
      try {
        if (countDownLatch.await(120, TimeUnit.SECONDS)) {
          return super.execute();
        } else {
          throw new HoodieException("Hitting timeout waiting for countdown latch to reach 0.");
        }
      } catch (InterruptedException e) {
        throw new HoodieException("Interrupted exception thrown waiting for countdown latch to reach 0.");
      }
    }
  }
}
