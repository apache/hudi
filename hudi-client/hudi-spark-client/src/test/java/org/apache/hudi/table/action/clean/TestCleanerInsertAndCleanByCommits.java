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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.table.TestCleaner.insertFirstBigBatchForClientCleanerTest;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieClientTestBase.Function2;
import static org.apache.hudi.testutils.HoodieClientTestBase.Function3;
import static org.apache.hudi.testutils.HoodieClientTestBase.wrapRecordsGenFunctionForPreppedCalls;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCleanerInsertAndCleanByCommits extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestCleanerInsertAndCleanByCommits.class);
  private static final int BATCH_SIZE = 100;
  private static final int PARALLELISM = 2;

  /**
   * Test Clean-By-Commits using insert/upsert API.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInsertAndCleanByCommits(boolean isAsync) throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::insert, SparkRDDWriteClient::upsert, false, isAsync);
  }

  /**
   * Test Clean-By-Commits using prepped version of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::insertPreppedRecords, SparkRDDWriteClient::upsertPreppedRecords,
        true, false);
  }

  /**
   * Test Clean-By-Commits using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(
        (client, recordRDD, instantTime) -> client.bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty()),
        SparkRDDWriteClient::upsertPreppedRecords, true, false);
  }

  /**
   * Test Clean-By-Commits using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::bulkInsert, SparkRDDWriteClient::upsert, false, false);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn     Insert API to be tested
   * @param upsertFn     Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *                     record generation to also tag the regards (de-dupe is implicit as we use unique record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI, boolean isAsync)
      throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withAsyncClean(isAsync).retainCommits(maxCommits).build())
        .withParallelism(PARALLELISM, PARALLELISM)
        .withBulkInsertParallelism(PARALLELISM)
        .withFinalizeWriteParallelism(PARALLELISM)
        .withDeleteParallelism(PARALLELISM)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .build();
    try (final SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(System.nanoTime());
      final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction = isPreppedAPI
          ? wrapRecordsGenFunctionForPreppedCalls(basePath(), hadoopConf(), context(), cfg, dataGen::generateInserts)
          : dataGen::generateInserts;
      final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction = isPreppedAPI
          ? wrapRecordsGenFunctionForPreppedCalls(basePath(), hadoopConf(), context(), cfg, dataGen::generateUniqueUpdates)
          : dataGen::generateUniqueUpdates;

      HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
      insertFirstBigBatchForClientCleanerTest(context(), metaClient, client, recordInsertGenWrappedFunction, insertFn);

      // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
      for (int i = 0; i < 8; i++) {
        String newCommitTime = makeNewCommitTime();
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newCommitTime, BATCH_SIZE);

        List<WriteStatus> statuses = upsertFn.apply(client, jsc().parallelize(records, PARALLELISM), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);
        HoodieTable table1 = HoodieSparkTable.create(cfg, context(), metaClient);
        HoodieTimeline activeTimeline = table1.getCompletedCommitsTimeline();
        HoodieInstant lastInstant = activeTimeline.lastInstant().get();
        if (cfg.isAsyncClean()) {
          activeTimeline = activeTimeline.findInstantsBefore(lastInstant.getTimestamp());
        }
        // NOTE: See CleanPlanner#getFilesToCleanKeepingLatestCommits. We explicitly keep one commit before earliest
        // commit
        Option<HoodieInstant> earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits);
        Set<HoodieInstant> acceptableCommits = activeTimeline.getInstants().collect(Collectors.toSet());
        if (earliestRetainedCommit.isPresent()) {
          acceptableCommits
              .removeAll(activeTimeline.findInstantsInRange("000", earliestRetainedCommit.get().getTimestamp())
                  .getInstants().collect(Collectors.toSet()));
          acceptableCommits.add(earliestRetainedCommit.get());
        }

        TableFileSystemView fsView = table1.getFileSystemView();
        // Need to ensure the following
        for (String partitionPath : dataGen.getPartitionPaths()) {
          List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
          for (HoodieFileGroup fileGroup : fileGroups) {
            Set<String> commitTimes = new HashSet<>();
            fileGroup.getAllBaseFiles().forEach(value -> {
              LOG.debug("Data File - " + value);
              commitTimes.add(value.getCommitTime());
            });
            if (cfg.isAsyncClean()) {
              commitTimes.remove(lastInstant.getTimestamp());
            }
            assertEquals(acceptableCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()), commitTimes,
                "Only contain acceptable versions of file should be present");
          }
        }
      }
    }
  }
}
