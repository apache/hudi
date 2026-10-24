/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.compact;

import org.apache.hudi.async.BaseCompactor;
import org.apache.hudi.async.SparkAsyncCompactService;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Async compaction when the writer replaces the service's write client while a compaction runs, as a writer does on a
 * schema change: it switches the service to its new client and closes the previous one.
 */
public class TestAsyncCompactionWriteClientSwap extends CompactionTestBase {

  private static final String COMPACTION_INSTANT = "005";
  private static final String FAILURE_MESSAGE = "Simulated failure of a compaction on a closed write client";

  /**
   * A compaction that fails after the service was switched to a new write client is retried with the new client: the
   * failed attempt is rolled back, the instant is compacted again and committed, and the service keeps running.
   */
  @Test
  public void testCompactionFailedAfterWriteClientSwapIsRetried() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).build();
    scheduleCompactionAfterDeltaCommits(cfg);

    CountDownLatch firstRunStarted = new CountDownLatch(1);
    CountDownLatch clientSwapped = new CountDownLatch(1);
    AtomicInteger runs = new AtomicInteger();
    AtomicBoolean stoppedWithError = new AtomicBoolean();
    // each client gets its own write config, as a writer builds a new config for the client it re-creates
    SparkRDDWriteClient previousClient = new SparkRDDWriteClient(context, getConfigBuilder(false).build());
    SparkRDDWriteClient newClient = new SparkRDDWriteClient(context, getConfigBuilder(false).build());
    SparkAsyncCompactService service = compactService(previousClient, runs, firstRunStarted, clientSwapped, true);
    try {
      service.enqueuePendingAsyncServiceInstant(COMPACTION_INSTANT);
      service.start(error -> {
        stoppedWithError.set(error);
        return true;
      });
      assertTrue(firstRunStarted.await(2, TimeUnit.MINUTES), "the first compaction run must start");

      // the writer replaces its write client: switch the service to the new client, then close the previous one
      service.updateWriteClient(newClient);
      previousClient.close();
      clientSwapped.countDown();

      waitForCompletedCompaction(cfg);
      assertEquals(2, runs.get(), "the failed compaction must run again with the new write client");
      assertFalse(service.hasError());
      assertFalse(service.isShutdown(), "the service must keep running");
      assertFalse(stoppedWithError.get());
    } finally {
      service.shutdown(true);
      newClient.close();
    }
  }

  /**
   * Any other compaction failure stops the service with that failure, for the writer to act on.
   */
  @Test
  public void testCompactionFailedWithoutWriteClientSwapStopsService() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).build();
    scheduleCompactionAfterDeltaCommits(cfg);

    AtomicInteger runs = new AtomicInteger();
    CountDownLatch stopped = new CountDownLatch(1);
    AtomicBoolean stoppedWithError = new AtomicBoolean();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, cfg);
    SparkAsyncCompactService service = compactService(client, runs, new CountDownLatch(1), new CountDownLatch(0), false);
    try {
      service.enqueuePendingAsyncServiceInstant(COMPACTION_INSTANT);
      service.start(error -> {
        stoppedWithError.set(error);
        stopped.countDown();
        return true;
      });
      assertTrue(stopped.await(2, TimeUnit.MINUTES), "the service must stop");

      assertEquals(1, runs.get(), "the compaction must not be retried");
      assertTrue(service.hasError());
      assertTrue(stoppedWithError.get());
      assertTrue(service.getFailure().isPresent());
      assertEquals(FAILURE_MESSAGE, service.getFailure().get().getMessage());
      assertTrue(createMetaClient(cfg.getBasePath()).getActiveTimeline().filterPendingCompactionTimeline()
          .containsInstant(COMPACTION_INSTANT), "the failed compaction must stay pending");
    } finally {
      service.shutdown(true);
      client.close();
    }
  }

  private void scheduleCompactionAfterDeltaCommits(HoodieWriteConfig cfg) throws Exception {
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, cfg)) {
      SparkRDDReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<HoodieRecord> records = dataGen.generateInserts("001", 100);
      runNextDeltaCommits(client, readClient, Arrays.asList("001", "004"), records, cfg, true, new ArrayList<>());
      scheduleCompaction(COMPACTION_INSTANT, client, cfg);
    }
  }

  /**
   * Compaction service whose first run compacts with the write client it started with and then, instead of committing,
   * waits for {@code clientSwapped} and fails. Later runs compact and commit with the client the service then uses.
   */
  private SparkAsyncCompactService compactService(SparkRDDWriteClient client, AtomicInteger runs, CountDownLatch firstRunStarted,
                                                  CountDownLatch clientSwapped, boolean onlyFirstRunFails) {
    return new SparkAsyncCompactService(context, client) {
      @Override
      protected BaseCompactor createCompactor(BaseHoodieWriteClient writeClient) {
        return new BaseCompactor(writeClient) {
          @Override
          public void compact(String instantTime) {
            SparkRDDWriteClient compactionWriteClient = (SparkRDDWriteClient) compactionClient;
            HoodieWriteMetadata metadata = compactionWriteClient.compact(instantTime);
            if (runs.incrementAndGet() == 1 || !onlyFirstRunFails) {
              firstRunStarted.countDown();
              try {
                clientSwapped.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              throw new HoodieException(FAILURE_MESSAGE);
            }
            compactionWriteClient.commitCompaction(instantTime, metadata, Option.empty());
          }
        };
      }
    };
  }

  private void waitForCompletedCompaction(HoodieWriteConfig cfg) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
    while (System.currentTimeMillis() < deadline) {
      HoodieTableMetaClient metaClient = createMetaClient(cfg.getBasePath());
      if (metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().containsInstant(COMPACTION_INSTANT)) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("compaction " + COMPACTION_INSTANT + " did not complete");
  }
}
