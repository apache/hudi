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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utils.HoodieWriterClientTestHarness.Function2;
import org.apache.hudi.utils.HoodieWriterClientTestHarness.Function3;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeIncrementalCommitTimes;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.table.TestCleaner.insertFirstBigBatchForClientCleanerTest;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieClientTestBase.wrapRecordsGenFunctionForPreppedCalls;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCleanerInsertAndCleanByVersions extends SparkClientFunctionalTestHarness {

  private static final int BATCH_SIZE = 100;
  private static final int PARALLELISM = 2;

  /**
   * Test Clean-By-Versions using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::insert, SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::insertPreppedRecords, SparkRDDWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::bulkInsert, SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(
        (client, recordRDD, instantTime) -> client.bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty()),
        SparkRDDWriteClient::upsertPreppedRecords, true);
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
  private void testInsertAndCleanByVersions(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI)
      throws Exception {
    int maxVersions = 2; // keep upto 2 versions for each file
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .retainFileVersions(maxVersions).build())
        .withParallelism(PARALLELISM, PARALLELISM)
        .withBulkInsertParallelism(PARALLELISM)
        .withFinalizeWriteParallelism(PARALLELISM)
        .withDeleteParallelism(PARALLELISM)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
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

      Map<HoodieFileGroupId, FileSlice> compactionFileIdToLatestFileSlice = new HashMap<>();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.createForReads(cfg, context(), metaClient);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        TableFileSystemView fsView = table.getFileSystemView();
        Option<Boolean> added = Option.fromJavaOptional(fsView.getAllFileGroups(partitionPath).findFirst().map(fg -> {
          fg.getLatestFileSlice().map(fs -> compactionFileIdToLatestFileSlice.put(fg.getFileGroupId(), fs));
          return true;
        }));
        if (added.isPresent()) {
          // Select only one file-group for compaction
          break;
        }
      }

      // Create workload with selected file-slices
      List<Pair<String, FileSlice>> partitionFileSlicePairs = compactionFileIdToLatestFileSlice.entrySet().stream()
          .map(e -> Pair.of(e.getKey().getPartitionPath(), e.getValue())).collect(Collectors.toList());
      HoodieCompactionPlan compactionPlan =
          CompactionUtils.buildFromFileSlices(partitionFileSlicePairs, Option.empty(), Option.empty());
      List<String> instantTimes = makeIncrementalCommitTimes(9, 1, 10);
      String compactionTime = instantTimes.get(0);
      table.getActiveTimeline().saveToCompactionRequested(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime),
          compactionPlan);

      instantTimes = instantTimes.subList(1, instantTimes.size());
      // Keep doing some writes and clean inline. Make sure we have expected number of files
      // remaining.
      for (String newInstantTime : instantTimes) {
        WriteClientTestUtils.startCommitWithTime(client, newInstantTime);
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newInstantTime, BATCH_SIZE);

        List<WriteStatus> statusList = upsertFn.apply(client, jsc().parallelize(records, PARALLELISM), newInstantTime).collect();
        client.commit(newInstantTime, jsc().parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
        assertNoWriteErrors(statusList);

        metaClient = HoodieTableMetaClient.reload(metaClient);
        table = HoodieSparkTable.createForReads(cfg, context(), metaClient);
        HoodieTimeline timeline = table.getMetaClient().getCommitsTimeline();

        TableFileSystemView fsView = table.getFileSystemView();
        // Need to ensure the following
        for (String partitionPath : dataGen.getPartitionPaths()) {
          // compute all the versions of all files, from time 0
          HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
          for (HoodieInstant entry : timeline.getInstants()) {
            HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(entry);

            for (HoodieWriteStat wstat : commitMetadata.getWriteStats(partitionPath)) {
              if (!fileIdToVersions.containsKey(wstat.getFileId())) {
                fileIdToVersions.put(wstat.getFileId(), new TreeSet<>());
              }
              fileIdToVersions.get(wstat.getFileId()).add(FSUtils.getCommitTime(new Path(wstat.getPath()).getName()));
            }
          }

          List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());

          for (HoodieFileGroup fileGroup : fileGroups) {
            if (compactionFileIdToLatestFileSlice.containsKey(fileGroup.getFileGroupId())) {
              // Ensure latest file-slice selected for compaction is retained
              Option<HoodieBaseFile> dataFileForCompactionPresent =
                  Option.fromJavaOptional(fileGroup.getAllBaseFiles().filter(df -> {
                    return compactionFileIdToLatestFileSlice.get(fileGroup.getFileGroupId()).getBaseInstantTime()
                        .equals(df.getCommitTime());
                  }).findAny());
              assertTrue(dataFileForCompactionPresent.isPresent(),
                  "Data File selected for compaction is retained");
            } else {
              // file has no more than max versions
              String fileId = fileGroup.getFileGroupId().getFileId();
              List<HoodieBaseFile> dataFiles = fileGroup.getAllBaseFiles().collect(Collectors.toList());

              assertTrue(dataFiles.size() <= maxVersions,
                  "fileId " + fileId + " has more than " + maxVersions + " versions");

              // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
              List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
              for (int i = 0; i < dataFiles.size(); i++) {
                assertEquals((dataFiles.get(i)).getCommitTime(),
                    commitedVersions.get(commitedVersions.size() - 1 - i),
                    "File " + fileId + " does not have latest versions on commits" + commitedVersions);
              }
            }
          }
        }
      }
    }
  }
}
