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

package org.apache.hudi;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple3;

import static org.apache.hudi.common.model.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test Cleaning related logic.
 */
public class TestCleaner extends TestHoodieClientBase {

  private static final int BIG_BATCH_INSERT_SIZE = 500;
  private static Logger logger = LogManager.getLogger(TestHoodieClientBase.class);

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests.
   *
   * @param cfg Hoodie Write Config
   * @param client Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn Insertion API for testing
   * @throws Exception in case of error
   */
  private String insertFirstBigBatchForClientCleanerTest(HoodieWriteConfig cfg, HoodieWriteClient client,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn) throws Exception {

    /**
     * do a big insert (this is basically same as insert part of upsert, just adding it here so we can catch breakages
     * in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

    List<WriteStatus> statuses = insertFn.apply(client, writeRecords, newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    // verify that there is a commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting a single commit.", 1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
    // Should have 100 records in table (check using Index), all in locations marked at commit
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, client.config, jsc);

    assertFalse(table.getCompletedCommitsTimeline().empty());
    String commitTime = table.getCompletedCommitsTimeline().getInstants().findFirst().get().getTimestamp();
    assertFalse(table.getCompletedCleanTimeline().empty());
    assertEquals("The clean instant should be the same as the commit instant", commitTime,
        table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

    HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
    List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), jsc, table).collect();
    checkTaggedRecords(taggedRecords, newCommitTime);
    return newCommitTime;
  }

  /**
   * Test Clean-By-Versions using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::insertPreppedRecords, HoodieWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::bulkInsert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(
        (client, recordRDD, commitTime) -> client.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *        record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByVersions(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI)
      throws Exception {
    int maxVersions = 2; // keep upto 2 versions for each file
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(maxVersions).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    try (HoodieWriteClient client = getHoodieWriteClient(cfg);) {

      final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
          generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);

      final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction =
          generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateUniqueUpdates);

      insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn);

      Map<HoodieFileGroupId, FileSlice> compactionFileIdToLatestFileSlice = new HashMap<>();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
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
      List<String> instantTimes = HoodieTestUtils.monotonicIncreasingCommitTimestamps(9, 1);
      String compactionTime = instantTimes.get(0);
      table.getActiveTimeline().saveToCompactionRequested(
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime),
          AvroUtils.serializeCompactionPlan(compactionPlan));

      instantTimes = instantTimes.subList(1, instantTimes.size());
      // Keep doing some writes and clean inline. Make sure we have expected number of files
      // remaining.
      for (String newInstantTime : instantTimes) {
        try {
          client.startCommitWithTime(newInstantTime);
          List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newInstantTime, 100);

          List<WriteStatus> statuses = upsertFn.apply(client, jsc.parallelize(records, 1), newInstantTime).collect();
          // Verify there are no errors
          assertNoWriteErrors(statuses);

          metaClient = HoodieTableMetaClient.reload(metaClient);
          table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
          HoodieTimeline timeline = table.getMetaClient().getCommitsTimeline();

          TableFileSystemView fsView = table.getFileSystemView();
          // Need to ensure the following
          for (String partitionPath : dataGen.getPartitionPaths()) {
            // compute all the versions of all files, from time 0
            HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
            for (HoodieInstant entry : timeline.getInstants().collect(Collectors.toList())) {
              HoodieCommitMetadata commitMetadata =
                  HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(entry).get(), HoodieCommitMetadata.class);

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
                Option<HoodieDataFile> dataFileForCompactionPresent =
                    Option.fromJavaOptional(fileGroup.getAllDataFiles().filter(df -> {
                      return compactionFileIdToLatestFileSlice.get(fileGroup.getFileGroupId()).getBaseInstantTime()
                          .equals(df.getCommitTime());
                    }).findAny());
                Assert.assertTrue("Data File selected for compaction is retained",
                    dataFileForCompactionPresent.isPresent());
              } else {
                // file has no more than max versions
                String fileId = fileGroup.getFileGroupId().getFileId();
                List<HoodieDataFile> dataFiles = fileGroup.getAllDataFiles().collect(Collectors.toList());

                assertTrue("fileId " + fileId + " has more than " + maxVersions + " versions",
                    dataFiles.size() <= maxVersions);

                // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
                List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
                for (int i = 0; i < dataFiles.size(); i++) {
                  assertEquals("File " + fileId + " does not have latest versions on commits" + commitedVersions,
                      Iterables.get(dataFiles, i).getCommitTime(),
                      commitedVersions.get(commitedVersions.size() - 1 - i));
                }
              }
            }
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }

  /**
   * Test Clean-By-Versions using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped version of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::insertPreppedRecords, HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(
        (client, recordRDD, commitTime) -> client.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::bulkInsert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *        record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI)
      throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainCommits(maxCommits).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);

    final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);

    final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateUniqueUpdates);

    insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn);

    // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
    HoodieTestUtils.monotonicIncreasingCommitTimestamps(8, 1).stream().forEach(newCommitTime -> {
      try {
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newCommitTime, 100);

        List<WriteStatus> statuses = upsertFn.apply(client, jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);
        HoodieTable table1 = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
        HoodieTimeline activeTimeline = table1.getCompletedCommitsTimeline();
        Option<HoodieInstant> earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits - 1);
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
            fileGroup.getAllDataFiles().forEach(value -> {
              logger.debug("Data File - " + value);
              commitTimes.add(value.getCommitTime());
            });
            assertEquals("Only contain acceptable versions of file should be present",
                acceptableCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()), commitTimes);
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    });
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  private List<HoodieCleanStat> runCleaner(HoodieWriteConfig config) {
    return runCleaner(config, false);
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  private List<HoodieCleanStat> runCleaner(HoodieWriteConfig config, boolean simulateRetryFailure) {
    HoodieCleanClient writeClient = getHoodieCleanClient(config);

    String cleanInstantTs = getNextInstant();
    HoodieCleanMetadata cleanMetadata1 = writeClient.clean(cleanInstantTs);

    if (null == cleanMetadata1) {
      return new ArrayList<>();
    }

    if (simulateRetryFailure) {
      metaClient.reloadActiveTimeline()
          .revertToInflight(new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, cleanInstantTs));
      final HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
      HoodieCleanMetadata cleanMetadata2 = writeClient.runClean(table, cleanInstantTs);
      Assert.assertTrue(
          Objects.equals(cleanMetadata1.getEarliestCommitToRetain(), cleanMetadata2.getEarliestCommitToRetain()));
      Assert.assertEquals(new Integer(0), cleanMetadata2.getTotalFilesDeleted());
      Assert.assertEquals(cleanMetadata1.getPartitionMetadata().keySet(),
          cleanMetadata2.getPartitionMetadata().keySet());
      cleanMetadata1.getPartitionMetadata().keySet().stream().forEach(k -> {
        HoodieCleanPartitionMetadata p1 = cleanMetadata1.getPartitionMetadata().get(k);
        HoodieCleanPartitionMetadata p2 = cleanMetadata2.getPartitionMetadata().get(k);
        Assert.assertEquals(p1.getDeletePathPatterns(), p2.getDeletePathPatterns());
        Assert.assertEquals(p1.getSuccessDeleteFiles(), p2.getFailedDeleteFiles());
        Assert.assertEquals(p1.getPartitionPath(), p2.getPartitionPath());
        Assert.assertEquals(k, p1.getPartitionPath());
      });
    }
    List<HoodieCleanStat> stats = cleanMetadata1.getPartitionMetadata().values().stream()
        .map(x -> new HoodieCleanStat.Builder().withPartitionPath(x.getPartitionPath())
            .withFailedDeletes(x.getFailedDeleteFiles()).withSuccessfulDeletes(x.getSuccessDeleteFiles())
            .withPolicy(HoodieCleaningPolicy.valueOf(x.getPolicy())).withDeletePathPattern(x.getDeletePathPatterns())
            .withEarliestCommitRetained(Option.ofNullable(cleanMetadata1.getEarliestCommitToRetain() != null
                ? new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "000")
                : null))
            .build())
        .collect(Collectors.toList());

    return stats;
  }

  /**
   * Test HoodieTable.clean() Cleaning by versions logic.
   */
  @Test
  public void testKeepLatestFileVersions() throws IOException {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    // make 1 commit, with 1 file per partition
    HoodieTestUtils.createCommitFiles(basePath, "000");

    String file1P0C0 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000");
    String file1P1C0 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "000");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "000",
        file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    HoodieTestUtils.createCommitFiles(basePath, "001");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    String file2P0C1 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001"); // insert
    String file2P1C1 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001"); // insert
    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001", file1P1C0); // update

    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleaner(config);
    assertEquals("Must clean 1 file", 1,
        getCleanStat(hoodieCleanStatsTwo, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertEquals("Must clean 1 file", 1,
        getCleanStat(hoodieCleanStatsTwo, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001",
        file2P1C1));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        "000", file1P1C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "002");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002", file2P0C1); // update
    String file3P0C2 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002");

    List<HoodieCleanStat> hoodieCleanStatsThree = runCleaner(config);
    assertEquals("Must clean two files", 2,
        getCleanStat(hoodieCleanStatsThree, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
            .getSuccessDeleteFiles().size());
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file1P0C0));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002",
        file3P0C2));

    // No cleaning on partially written file, with no commit.
    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "003", file3P0C2); // update
    List<HoodieCleanStat> hoodieCleanStatsFour = runCleaner(config);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsFour, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002",
        file3P0C2));
  }

  /**
   * Test HoodieTable.clean() Cleaning by versions logic for MOR table with Log files.
   */
  @Test
  public void testKeepLatestFileVersionsMOR() throws IOException {

    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    HoodieTableMetaClient metaClient =
        HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);

    // Make 3 files, one base file and 2 log files associated with base file
    String file1P0 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000");
    String file2P0L0 = HoodieTestUtils.createNewLogFile(fs, basePath,
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000", file1P0, Option.empty());
    String file2P0L1 = HoodieTestUtils.createNewLogFile(fs, basePath,
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000", file1P0, Option.of(2));
    // make 1 compaction commit
    HoodieTestUtils.createCompactionCommitFiles(fs, basePath, "000");

    // Make 4 files, one base file and 3 log files associated with base file
    HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001", file1P0);
    file2P0L0 = HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        "001", file1P0, Option.empty());
    file2P0L0 = HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        "001", file1P0, Option.of(2));
    file2P0L0 = HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        "001", file1P0, Option.of(3));
    // make 1 compaction commit
    HoodieTestUtils.createCompactionCommitFiles(fs, basePath, "001");

    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config);
    assertEquals("Must clean three files, one parquet and 2 log files", 3,
        getCleanStat(hoodieCleanStats, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0));
    assertFalse(HoodieTestUtils.doesLogFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file2P0L0, Option.empty()));
    assertFalse(HoodieTestUtils.doesLogFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file2P0L0, Option.of(2)));
  }

  @Test
  public void testUpgradeDowngrade() {
    String commitTime = "000";

    String partition1 = DEFAULT_PARTITION_PATHS[0];
    String partition2 = DEFAULT_PARTITION_PATHS[1];

    String fileName1 = "data1_1_000.parquet";
    String fileName2 = "data2_1_000.parquet";

    String filePath1 = metaClient.getBasePath() + "/" + partition1 + "/" + fileName1;
    String filePath2 = metaClient.getBasePath() + "/" + partition1 + "/" + fileName2;

    List<String> deletePathPatterns1 = Arrays.asList(filePath1, filePath2);
    List<String> successDeleteFiles1 = Arrays.asList(filePath1);
    List<String> failedDeleteFiles1 = Arrays.asList(filePath2);

    // create partition1 clean stat.
    HoodieCleanStat cleanStat1 = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        partition1, deletePathPatterns1, successDeleteFiles1,
        failedDeleteFiles1, commitTime);

    List<String> deletePathPatterns2 = new ArrayList<>();
    List<String> successDeleteFiles2 = new ArrayList<>();
    List<String> failedDeleteFiles2 = new ArrayList<>();

    // create partition2 empty clean stat.
    HoodieCleanStat cleanStat2 = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        partition2, deletePathPatterns2, successDeleteFiles2,
        failedDeleteFiles2, commitTime);

    // map with absolute file path.
    Map<String, Tuple3> oldExpected = new HashMap<>();
    oldExpected.put(partition1, new Tuple3<>(deletePathPatterns1, successDeleteFiles1, failedDeleteFiles1));
    oldExpected.put(partition2, new Tuple3<>(deletePathPatterns2, successDeleteFiles2, failedDeleteFiles2));

    // map with relative path.
    Map<String, Tuple3> newExpected = new HashMap<>();
    newExpected.put(partition1, new Tuple3<>(Arrays.asList(fileName1, fileName2), Arrays.asList(fileName1), Arrays.asList(fileName2)));
    newExpected.put(partition2, new Tuple3<>(deletePathPatterns2, successDeleteFiles2, failedDeleteFiles2));

    HoodieCleanMetadata metadata =
        CleanerUtils.convertCleanMetadata(metaClient, commitTime, Option.of(0L), Arrays.asList(cleanStat1, cleanStat2));

    Assert.assertEquals(CleanerUtils.LATEST_CLEAN_METADATA_VERSION, metadata.getVersion());
    testCleanMetadataPathEquality(metadata, newExpected);

    CleanMetadataMigrator migrator = new CleanMetadataMigrator(metaClient);
    HoodieCleanMetadata oldMetadata =
        migrator.migrateToVersion(metadata, metadata.getVersion(), CleanerUtils.CLEAN_METADATA_VERSION_1);
    Assert.assertEquals(CleanerUtils.CLEAN_METADATA_VERSION_1, oldMetadata.getVersion());
    testCleanMetadataEquality(metadata, oldMetadata);
    testCleanMetadataPathEquality(oldMetadata, oldExpected);

    HoodieCleanMetadata newMetadata = migrator.upgradeToLatest(oldMetadata, oldMetadata.getVersion());
    Assert.assertEquals(CleanerUtils.LATEST_CLEAN_METADATA_VERSION, newMetadata.getVersion());
    testCleanMetadataEquality(oldMetadata, newMetadata);
    testCleanMetadataPathEquality(newMetadata, newExpected);
    testCleanMetadataPathEquality(oldMetadata, oldExpected);
  }

  public void testCleanMetadataEquality(HoodieCleanMetadata input1, HoodieCleanMetadata input2) {
    Assert.assertEquals(input1.getEarliestCommitToRetain(), input2.getEarliestCommitToRetain());
    Assert.assertEquals(input1.getStartCleanTime(), input2.getStartCleanTime());
    Assert.assertEquals(input1.getTimeTakenInMillis(), input2.getTimeTakenInMillis());
    Assert.assertEquals(input1.getTotalFilesDeleted(), input2.getTotalFilesDeleted());

    Map<String, HoodieCleanPartitionMetadata> map1 = input1.getPartitionMetadata();
    Map<String, HoodieCleanPartitionMetadata> map2 = input2.getPartitionMetadata();

    Assert.assertEquals(map1.keySet(), map2.keySet());

    List<String> partitions1 = map1.values().stream().map(m -> m.getPartitionPath()).collect(
        Collectors.toList());
    List<String> partitions2 = map2.values().stream().map(m -> m.getPartitionPath()).collect(
        Collectors.toList());
    Assert.assertEquals(partitions1, partitions2);

    List<String> policies1 = map1.values().stream().map(m -> m.getPolicy()).collect(Collectors.toList());
    List<String> policies2 = map2.values().stream().map(m -> m.getPolicy()).collect(Collectors.toList());
    Assert.assertEquals(policies1, policies2);
  }

  private void testCleanMetadataPathEquality(HoodieCleanMetadata metadata, Map<String, Tuple3> expected) {

    Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = metadata.getPartitionMetadata();

    for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : partitionMetadataMap.entrySet()) {
      String partitionPath = entry.getKey();
      HoodieCleanPartitionMetadata partitionMetadata = entry.getValue();

      Assert.assertEquals(expected.get(partitionPath)._1(), partitionMetadata.getDeletePathPatterns());
      Assert.assertEquals(expected.get(partitionPath)._2(), partitionMetadata.getSuccessDeleteFiles());
      Assert.assertEquals(expected.get(partitionPath)._3(), partitionMetadata.getFailedDeleteFiles());
    }
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files.
   */
  @Test
  public void testKeepLatestCommits() throws IOException {
    testKeepLatestCommits(false, false);
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files. Here the operations are simulated
   * such that first clean attempt failed after files were cleaned and a subsequent cleanup succeeds.
   */
  @Test
  public void testKeepLatestCommitsWithFailureRetry() throws IOException {
    testKeepLatestCommits(true, false);
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files.
   */
  @Test
  public void testKeepLatestCommitsIncrMode() throws IOException {
    testKeepLatestCommits(false, true);
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files.
   */
  private void testKeepLatestCommits(boolean simulateFailureRetry, boolean enableIncrementalClean) throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withIncrementalCleaningMode(enableIncrementalClean)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    // make 1 commit, with 1 file per partition
    HoodieTestUtils.createCommitFiles(basePath, "000");

    String file1P0C0 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000");
    String file1P1C0 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "000");

    HoodieCommitMetadata commitMetadata = generateCommitMetadata(new ImmutableMap.Builder()
        .put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P0C0).build())
        .put(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P1C0).build())
        .build());
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "000"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config, simulateFailureRetry);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "000",
        file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    HoodieTestUtils.createCommitFiles(basePath, "001");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    String file2P0C1 =
        HoodieTestUtils
            .createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001"); // insert
    String file2P1C1 =
        HoodieTestUtils
            .createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001"); // insert
    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0); // update
    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001", file1P1C0); // update
    commitMetadata = generateCommitMetadata(new ImmutableMap.Builder()
        .put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P0C0).add(file2P0C1).build())
        .put(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P1C0).add(file2P1C1).build())
        .build());
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleaner(config, simulateFailureRetry);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsTwo, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsTwo, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles()
            .size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "001",
        file2P1C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "000",
        file1P1C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "002");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002", file1P0C0); // update
    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002", file2P0C1); // update
    String file3P0C2 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002");

    commitMetadata = generateCommitMetadata(new ImmutableMap.Builder()
        .put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P0C0).add(file2P0C1).add(file3P0C2).build())
        .build());
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "002"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    List<HoodieCleanStat> hoodieCleanStatsThree = runCleaner(config, simulateFailureRetry);
    assertEquals("Must not clean any file. We have to keep 1 version before the latest commit time to keep", 0,
        getCleanStat(hoodieCleanStatsThree, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
            .getSuccessDeleteFiles().size());

    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "003");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "003", file1P0C0); // update
    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "003", file2P0C1); // update
    String file4P0C3 =
        HoodieTestUtils.createNewDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "003");
    commitMetadata = generateCommitMetadata(new ImmutableMap.Builder()
        .put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file1P0C0).add(file2P0C1).add(file4P0C3).build())
        .build());
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "003"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    List<HoodieCleanStat> hoodieCleanStatsFour = runCleaner(config, simulateFailureRetry);
    assertEquals("Must not clean one old file", 1,
        getCleanStat(hoodieCleanStatsFour, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles()
            .size());

    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "000",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002",
        file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "002",
        file3P0C2));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "003",
        file4P0C3));

    // No cleaning on partially written file, with no commit.
    HoodieTestUtils
        .createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "004", file3P0C2); // update
    commitMetadata = generateCommitMetadata(new ImmutableMap.Builder()
        .put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            new ImmutableList.Builder<>().add(file3P0C2).build())
        .build());
    metaClient.getActiveTimeline().saveToInflight(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "004"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsFive = runCleaner(config, simulateFailureRetry);
    HoodieCleanStat cleanStat = getCleanStat(hoodieCleanStatsFive, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    assertEquals("Must not clean any files", 0,
        cleanStat != null ? cleanStat.getSuccessDeleteFiles().size() : 0);
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "001",
        file2P0C1));
  }

  /**
   * Test Cleaning functionality of table.rollback() API.
   */
  @Test
  public void testCleanMarkerDataFilesOnRollback() throws IOException {
    HoodieTestUtils.createCommitFiles(basePath, "000");
    List<String> markerFiles = createMarkerFiles("000", 10);
    assertEquals("Some marker files are created.", 10, markerFiles.size());
    assertEquals("Some marker files are created.", markerFiles.size(), getTotalTempFiles());

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    table.rollback(jsc, "000", true);
    assertEquals("All temp files are deleted.", 0, getTotalTempFiles());
  }

  /**
   * Test CLeaner Stat when there are no partition paths.
   */
  @Test
  public void testCleaningWithZeroPartitonPaths() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    // Make a commit, although there are no partitionPaths.
    // Example use-case of this is when a client wants to create a table
    // with just some commit metadata, but no data/partitionPaths.
    HoodieTestUtils.createCommitFiles(basePath, "000");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config);
    assertTrue("HoodieCleanStats should be empty for a table with empty partitionPaths", hoodieCleanStatsOne.isEmpty());
  }

  /**
   * Test Keep Latest Commits when there are pending compactions.
   */
  @Test
  public void testKeepLatestCommitsWithPendingCompactions() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();
    // Deletions:
    // . FileId Parquet Logs Total Retained Commits
    // FileId7 5 10 15 009, 011
    // FileId6 5 10 15 009
    // FileId5 3 6 9 005
    // FileId4 2 4 6 003
    // FileId3 1 2 3 001
    // FileId2 0 0 0 000
    // FileId1 0 0 0 000
    testPendingCompactions(config, 48, 18, false);
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files. Here the operations are simulated
   * such that first clean attempt failed after files were cleaned and a subsequent cleanup succeeds.
   */
  @Test
  public void testKeepLatestVersionsWithPendingCompactions() throws IOException {
    testKeepLatestVersionsWithPendingCompactions(false);
  }


  /**
   * Test Keep Latest Versions when there are pending compactions.
   */
  @Test
  public void testKeepLatestVersionsWithPendingCompactionsAndFailureRetry() throws IOException {
    testKeepLatestVersionsWithPendingCompactions(true);
  }

  private void testKeepLatestVersionsWithPendingCompactions(boolean retryFailure) throws IOException {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(2).build())
            .build();
    // Deletions:
    // . FileId Parquet Logs Total Retained Commits
    // FileId7 5 10 15 009, 011
    // FileId6 4 8 12 007, 009
    // FileId5 2 4 6 003 005
    // FileId4 1 2 3 001, 003
    // FileId3 0 0 0 000, 001
    // FileId2 0 0 0 000
    // FileId1 0 0 0 000
    testPendingCompactions(config, 36, 9, retryFailure);
  }

  /**
   * Common test method for validating pending compactions.
   *
   * @param config Hoodie Write Config
   * @param expNumFilesDeleted Number of files deleted
   */
  private void testPendingCompactions(HoodieWriteConfig config, int expNumFilesDeleted,
      int expNumFilesUnderCompactionDeleted, boolean retryFailure) throws IOException {
    HoodieTableMetaClient metaClient =
        HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);
    String[] instants = new String[] {"000", "001", "003", "005", "007", "009", "011", "013"};
    String[] compactionInstants = new String[] {"002", "004", "006", "008", "010"};
    Map<String, String> expFileIdToPendingCompaction = new HashMap<>();
    Map<String, String> fileIdToLatestInstantBeforeCompaction = new HashMap<>();
    Map<String, List<FileSlice>> compactionInstantsToFileSlices = new HashMap<>();

    for (String instant : instants) {
      HoodieTestUtils.createCommitFiles(basePath, instant);
    }

    // Generate 7 file-groups. First one has only one slice and no pending compaction. File Slices (2 - 5) has
    // multiple versions with pending compaction. File Slices (6 - 7) have multiple file-slices but not under
    // compactions
    // FileIds 2-5 will be under compaction
    int maxNumFileIds = 7;
    String[] fileIds = new String[] {"fileId1", "fileId2", "fileId3", "fileId4", "fileId5", "fileId6", "fileId7"};
    int maxNumFileIdsForCompaction = 4;
    for (int i = 0; i < maxNumFileIds; i++) {
      final String fileId = HoodieTestUtils.createDataFile(basePath,
          HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, instants[0], fileIds[i]);
      HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, instants[0],
          fileId, Option.empty());
      HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, instants[0],
          fileId, Option.of(2));
      fileIdToLatestInstantBeforeCompaction.put(fileId, instants[0]);
      for (int j = 1; j <= i; j++) {
        if (j == i && j <= maxNumFileIdsForCompaction) {
          expFileIdToPendingCompaction.put(fileId, compactionInstants[j]);
          metaClient = HoodieTableMetaClient.reload(metaClient);
          HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
          FileSlice slice =
              table.getRTFileSystemView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
                  .filter(fs -> fs.getFileId().equals(fileId)).findFirst().get();
          List<FileSlice> slices = new ArrayList<>();
          if (compactionInstantsToFileSlices.containsKey(compactionInstants[j])) {
            slices = compactionInstantsToFileSlices.get(compactionInstants[j]);
          }
          slices.add(slice);
          compactionInstantsToFileSlices.put(compactionInstants[j], slices);
          // Add log-files to simulate delta-commits after pending compaction
          HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
              compactionInstants[j], fileId, Option.empty());
          HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
              compactionInstants[j], fileId, Option.of(2));
        } else {
          HoodieTestUtils.createDataFile(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, instants[j],
              fileId);
          HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
              instants[j], fileId, Option.empty());
          HoodieTestUtils.createNewLogFile(fs, basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
              instants[j], fileId, Option.of(2));
          fileIdToLatestInstantBeforeCompaction.put(fileId, instants[j]);
        }
      }
    }

    // Setup pending compaction plans
    for (String instant : compactionInstants) {
      List<FileSlice> fileSliceList = compactionInstantsToFileSlices.get(instant);
      if (null != fileSliceList) {
        HoodieTestUtils.createCompactionRequest(metaClient, instant, fileSliceList.stream()
            .map(fs -> Pair.of(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fs)).collect(Collectors.toList()));
      }
    }

    // Clean now
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config, retryFailure);

    // Test for safety
    final HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

    expFileIdToPendingCompaction.entrySet().stream().forEach(entry -> {
      String fileId = entry.getKey();
      String baseInstantForCompaction = fileIdToLatestInstantBeforeCompaction.get(fileId);
      Option<FileSlice> fileSliceForCompaction = Option.fromJavaOptional(hoodieTable.getRTFileSystemView()
          .getLatestFileSlicesBeforeOrOn(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, baseInstantForCompaction,
              true)
          .filter(fs -> fs.getFileId().equals(fileId)).findFirst());
      Assert.assertTrue("Base Instant for Compaction must be preserved", fileSliceForCompaction.isPresent());
      Assert.assertTrue("FileSlice has data-file", fileSliceForCompaction.get().getDataFile().isPresent());
      Assert.assertEquals("FileSlice has log-files", 2, fileSliceForCompaction.get().getLogFiles().count());
    });

    // Test for progress (Did we clean some files ?)
    long numFilesUnderCompactionDeleted = hoodieCleanStats.stream().flatMap(cleanStat -> {
      return convertPathToFileIdWithCommitTime(newMetaClient, cleanStat.getDeletePathPatterns())
          .map(fileIdWithCommitTime -> {
            if (expFileIdToPendingCompaction.containsKey(fileIdWithCommitTime.getKey())) {
              Assert.assertTrue("Deleted instant time must be less than pending compaction",
                  HoodieTimeline.compareTimestamps(
                      fileIdToLatestInstantBeforeCompaction.get(fileIdWithCommitTime.getKey()),
                      fileIdWithCommitTime.getValue(), HoodieTimeline.GREATER));
              return true;
            }
            return false;
          });
    }).filter(x -> x).count();
    long numDeleted =
        hoodieCleanStats.stream().flatMap(cleanStat -> cleanStat.getDeletePathPatterns().stream()).count();
    // Tighter check for regression
    Assert.assertEquals("Correct number of files deleted", expNumFilesDeleted, numDeleted);
    Assert.assertEquals("Correct number of files under compaction deleted", expNumFilesUnderCompactionDeleted,
        numFilesUnderCompactionDeleted);
  }

  /**
   * Utility method to create temporary data files.
   *
   * @param commitTime Commit Timestamp
   * @param numFiles Number for files to be generated
   * @return generated files
   * @throws IOException in case of error
   */
  private List<String> createMarkerFiles(String commitTime, int numFiles) throws IOException {
    List<String> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(HoodieTestUtils.createNewMarkerFile(basePath, "2019/03/29", commitTime));
    }
    return files;
  }

  /***
   * Helper method to return temporary files count.
   * 
   * @return Number of temporary files found
   * @throws IOException in case of error
   */
  private int getTotalTempFiles() throws IOException {
    RemoteIterator itr = fs.listFiles(new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME), true);
    int count = 0;
    while (itr.hasNext()) {
      count++;
      itr.next();
    }
    return count;
  }

  private Stream<Pair<String, String>> convertPathToFileIdWithCommitTime(final HoodieTableMetaClient metaClient,
      List<String> paths) {
    Predicate<String> roFilePredicate =
        path -> path.contains(metaClient.getTableConfig().getROFileFormat().getFileExtension());
    Predicate<String> rtFilePredicate =
        path -> path.contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension());
    Stream<Pair<String, String>> stream1 = paths.stream().filter(roFilePredicate).map(fullPath -> {
      String fileName = Paths.get(fullPath).getFileName().toString();
      return Pair.of(FSUtils.getFileId(fileName), FSUtils.getCommitTime(fileName));
    });
    Stream<Pair<String, String>> stream2 = paths.stream().filter(rtFilePredicate).map(path -> {
      return Pair.of(FSUtils.getFileIdFromLogPath(new Path(path)),
          FSUtils.getBaseCommitTimeFromLogPath(new Path(path)));
    });
    return Stream.concat(stream1, stream2);
  }

  private static HoodieCommitMetadata generateCommitMetadata(Map<String, List<String>> partitionToFilePaths) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    partitionToFilePaths.entrySet().forEach(e -> {
      e.getValue().forEach(f -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(e.getKey());
        writeStat.setPath(f);
        writeStat.setFileId(f);
        metadata.addWriteStat(e.getKey(), writeStat);
      });
    });
    return metadata;
  }
}
