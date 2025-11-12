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
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieCleaningPolicy.KEEP_LATEST_COMMITS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.table.TestCleaner.insertFirstBigBatchForClientCleanerTest;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieClientTestBase.Function2;
import static org.apache.hudi.testutils.HoodieClientTestBase.Function3;
import static org.apache.hudi.testutils.HoodieClientTestBase.wrapRecordsGenFunctionForPreppedCalls;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCleanerInsertAndCleanByCommits extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestCleanerInsertAndCleanByCommits.class);
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
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(KEEP_LATEST_COMMITS)
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
          ? wrapRecordsGenFunctionForPreppedCalls(basePath(), storageConf(), context(), cfg, dataGen::generateInserts)
          : dataGen::generateInserts;
      final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction = isPreppedAPI
          ? wrapRecordsGenFunctionForPreppedCalls(basePath(), storageConf(), context(), cfg, dataGen::generateUniqueUpdates)
          : dataGen::generateUniqueUpdates;

      HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
      insertFirstBigBatchForClientCleanerTest(context(), metaClient, client, recordInsertGenWrappedFunction, insertFn);

      Map<String, List<HoodieWriteStat>> commitWriteStatsMap = new HashMap<>();
      // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
      for (int i = 0; i < 8; i++) {
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newCommitTime, BATCH_SIZE);
        List<WriteStatus> statusList = upsertFn.apply(client, jsc().parallelize(records, PARALLELISM), newCommitTime).collect();
        client.commit(newCommitTime, jsc().parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
        assertNoWriteErrors(statusList);
        commitWriteStatsMap.put(
            newCommitTime,
            statusList.stream().map(WriteStatus::getStat).collect(Collectors.toList()));

        metaClient = HoodieTableMetaClient.reload(metaClient);
        validateFilesAfterCleaning(
            HoodieSparkTable.create(cfg, context(), metaClient),
            commitWriteStatsMap,
            dataGen.getPartitionPaths());
      }
    }
  }

  /**
   * Validates the data files in a Hudi table based on the `KEEP_LATEST_COMMITS` cleaner policy.
   *
   * @param table               {@link HoodieTable} instance.
   * @param commitWriteStatsMap The cache for the list of write stats of each commit.
   * @param partitionPaths      List of partitions to validate.
   */
  private void validateFilesAfterCleaning(
      HoodieTable table,
      Map<String, List<HoodieWriteStat>> commitWriteStatsMap,
      String[] partitionPaths) {
    assertEquals(KEEP_LATEST_COMMITS, table.getConfig().getCleanerPolicy());
    boolean isAsyncClean = table.getConfig().isAsyncClean();
    int maxCommitsToRetain = table.getConfig().getCleanerCommitsRetained();
    HoodieTimeline commitsTimeline = table.getCompletedCommitsTimeline();
    HoodieInstant lastInstant = commitsTimeline.lastInstant().get();

    if (isAsyncClean) {
      commitsTimeline = commitsTimeline.findInstantsBefore(lastInstant.requestedTime());
    }
    // This corresponds to the `earliestCommitToRetain` in {@code CleanPlanner::getFilesToCleanKeepingLatestCommits}
    Option<HoodieInstant> earliestRetainedCommit = commitsTimeline.nthFromLastInstant(maxCommitsToRetain - 1);
    // A final timeline to be used in Lambda function
    HoodieTimeline timeline = commitsTimeline;
    // Mapping of <partition path, file group ID> to expected set of instant timestamps
    Map<Pair<String, String>, Set<String>> expectedInstantTimeMap = new HashMap<>();
    TableFileSystemView fsView = table.getFileSystemView();
    // Remaining file groups to figure out the one version before earliestRetainedCommit
    Set<Pair<String, String>> remainingFileGroupSet = new HashSet<>();

    for (String partitionPath : partitionPaths) {
      remainingFileGroupSet.addAll(
          fsView.getAllFileGroups(partitionPath)
              .map(fileGroup -> Pair.of(partitionPath, fileGroup.getFileGroupId().getFileId()))
              .collect(Collectors.toList()));
    }
    // With KEEP_LATEST_COMMITS cleaner policy, for each file group, we need to figure out
    // the latest version before earliestCommitToRetain, which is also kept from cleaning.
    // The timeline of commits is traversed in reverse order to achieve this.
    for (HoodieInstant instant : commitsTimeline.getReverseOrderedInstants().collect(Collectors.toList())) {
      TimelineLayout layout = TimelineLayout.fromVersion(commitsTimeline.getTimelineLayoutVersion());
      List<HoodieWriteStat> hoodieWriteStatList = commitWriteStatsMap.computeIfAbsent(instant.requestedTime(), newInstant -> {
        try {
          HoodieInstant instant1 = timeline.filter(inst -> inst.requestedTime().equals(newInstant))
              .firstInstant().get();
          return timeline.readCommitMetadata(instant1).getWriteStats();
        } catch (IOException e) {
          return Collections.EMPTY_LIST;
        }
      });
      hoodieWriteStatList.forEach(writeStat -> {
        Pair<String, String> partitionFileIdPair = Pair.of(writeStat.getPartitionPath(), writeStat.getFileId());
        if (remainingFileGroupSet.contains(partitionFileIdPair)) {
          if (earliestRetainedCommit.isPresent()
              && compareTimestamps(
              instant.requestedTime(), LESSER_THAN, earliestRetainedCommit.get().requestedTime())) {
            remainingFileGroupSet.remove(partitionFileIdPair);
          }
          expectedInstantTimeMap.computeIfAbsent(partitionFileIdPair, k -> new HashSet<>())
              .add(instant.requestedTime());
        }
      });
      if (remainingFileGroupSet.isEmpty()) {
        break;
      }
    }

    // Need to ensure the following
    for (String partitionPath : partitionPaths) {
      List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
      for (HoodieFileGroup fileGroup : fileGroups) {
        Set<String> commitTimes = new HashSet<>();
        fileGroup.getAllBaseFiles().forEach(value -> {
          LOG.debug("Data File - {}", value);
          commitTimes.add(value.getCommitTime());
        });
        if (isAsyncClean) {
          commitTimes.remove(lastInstant.requestedTime());
        }

        assertEquals(
            expectedInstantTimeMap.get(
                Pair.of(partitionPath, fileGroup.getFileGroupId().getFileId())),
            commitTimes,
            "Only contain acceptable versions of file should be present");
      }
    }
  }
}
