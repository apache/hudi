/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie;

import static com.uber.hoodie.common.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static com.uber.hoodie.common.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static com.uber.hoodie.common.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.GREATER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.util.AccumulatorV2;
import org.junit.Assert;
import org.junit.Test;
import scala.Option;
import scala.collection.Iterator;

/**
 * Test Cleaning related logic
 */
public class TestCleaner extends TestHoodieClientBase {

  private static final int BIG_BATCH_INSERT_SIZE = 500;
  private static Logger logger = LogManager.getLogger(TestHoodieClientBase.class);

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests
   *
   * @param cfg Hoodie Write Config
   * @param client Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn Insertion API for testing
   * @throws Exception in case of error
   */
  private String insertFirstBigBatchForClientCleanerTest(
      HoodieWriteConfig cfg,
      HoodieWriteClient client,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn) throws Exception {

    /**
     * do a big insert
     * (this is basically same as insert part of upsert, just adding it here so we can
     * catch breakages in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

    List<WriteStatus> statuses = insertFn.apply(client, writeRecords, newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting a single commit.", 1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
    // Should have 100 records in table (check using Index), all in locations marked at commit
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);

    assertFalse(table.getCompletedCommitTimeline().empty());
    String commitTime = table.getCompletedCommitTimeline().getInstants().findFirst().get().getTimestamp();
    assertFalse(table.getCompletedCleanTimeline().empty());
    assertEquals("The clean instant should be the same as the commit instant", commitTime,
        table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

    HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
    List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), jsc, table).collect();
    checkTaggedRecords(taggedRecords, newCommitTime);
    return newCommitTime;
  }

  /**
   * Test Clean-By-Versions using insert/upsert API
   */
  @Test
  public void testInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of insert/upsert API
   */
  @Test
  public void testInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::insertPreppedRecords,
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API
   */
  @Test
  public void testBulkInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(HoodieWriteClient::bulkInsert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API
   */
  @Test
  public void testBulkInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(
        (client, recordRDD, commitTime) -> client.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   * record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByVersions(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> upsertFn,
      boolean isPreppedAPI
  ) throws Exception {
    int maxVersions = 2; // keep upto 2 versions for each file
    HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
        HoodieCompactionConfig.newBuilder().withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .retainFileVersions(maxVersions).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1)
        .build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

    final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);

    final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateUniqueUpdates);

    insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn);

    Map<String, String> selectedFileIdForCompaction = new HashMap<>();
    Map<String, FileSlice> compactionFileIdToLatestFileSlice = new HashMap<>();
    HoodieTableMetaClient metadata = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metadata, getConfig(), jsc);
    for (String partitionPath : dataGen.getPartitionPaths()) {
      TableFileSystemView fsView = table.getFileSystemView();
      Optional<Boolean> added = fsView.getAllFileGroups(partitionPath).findFirst()
          .map(fg -> {
            selectedFileIdForCompaction.put(fg.getId(), partitionPath);
            fg.getLatestFileSlice().map(fs -> compactionFileIdToLatestFileSlice.put(fg.getId(), fs));
            return true;
          });
      if (added.isPresent()) {
        // Select only one file-group for compaction
        break;
      }
    }

    // Create workload with selected file-slices
    List<Pair<String, FileSlice>> partitionFileSlicePairs = compactionFileIdToLatestFileSlice.entrySet().stream()
        .map(e -> Pair.of(selectedFileIdForCompaction.get(e.getKey()), e.getValue())).collect(Collectors.toList());
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicePairs, Optional.empty(), Optional.empty());
    List<String> instantTimes = HoodieTestUtils.monotonicIncreasingCommitTimestamps(9, 1);
    String compactionTime = instantTimes.get(0);
    table.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionTime),
        AvroUtils.serializeCompactionPlan(compactionPlan));

    instantTimes = instantTimes.subList(1, instantTimes.size());
    // Keep doing some writes and clean inline. Make sure we have expected number of files
    // remaining.
    for (String newInstantTime : instantTimes) {
      try {
        client.startCommitWithTime(newInstantTime);
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newInstantTime, 100);

        List<WriteStatus> statuses =
            upsertFn.apply(client, jsc.parallelize(records, 1), newInstantTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metadata = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
        table = HoodieTable.getHoodieTable(metadata, getConfig(), jsc);
        HoodieTimeline timeline = table.getMetaClient().getCommitsTimeline();

        TableFileSystemView fsView = table.getFileSystemView();
        // Need to ensure the following
        for (String partitionPath : dataGen.getPartitionPaths()) {
          // compute all the versions of all files, from time 0
          HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
          for (HoodieInstant entry : timeline.getInstants().collect(Collectors.toList())) {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(timeline.getInstantDetails(entry).get());

            for (HoodieWriteStat wstat : commitMetadata.getWriteStats(partitionPath)) {
              if (!fileIdToVersions.containsKey(wstat.getFileId())) {
                fileIdToVersions.put(wstat.getFileId(), new TreeSet<>());
              }
              fileIdToVersions.get(wstat.getFileId()).add(FSUtils.getCommitTime(new Path(wstat.getPath()).getName()));
            }
          }

          List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());

          for (HoodieFileGroup fileGroup : fileGroups) {
            if (selectedFileIdForCompaction.containsKey(fileGroup.getId())) {
              // Ensure latest file-slice selected for compaction is retained
              String oldestCommitRetained =
                  fileGroup.getAllDataFiles().map(HoodieDataFile::getCommitTime).sorted().findFirst().get();

              Optional<HoodieDataFile> dataFileForCompactionPresent =
                  fileGroup.getAllDataFiles().filter(df -> {
                    return compactionFileIdToLatestFileSlice.get(fileGroup.getId())
                        .getBaseInstantTime().equals(df.getCommitTime());
                  }).findAny();
              Assert.assertTrue("Data File selected for compaction is retained",
                  dataFileForCompactionPresent.isPresent());
            } else {
              // file has no more than max versions
              String fileId = fileGroup.getId();
              List<HoodieDataFile> dataFiles = fileGroup.getAllDataFiles().collect(Collectors.toList());

              assertTrue("fileId " + fileId + " has more than " + maxVersions + " versions",
                  dataFiles.size() <= maxVersions);

              // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
              List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
              for (int i = 0; i < dataFiles.size(); i++) {
                assertEquals("File " + fileId + " does not have latest versions on commits" + commitedVersions,
                    Iterables.get(dataFiles, i).getCommitTime(), commitedVersions.get(commitedVersions.size() - 1 - i));
              }
            }
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Test Clean-By-Versions using insert/upsert API
   */
  @Test
  public void testInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped version of insert/upsert API
   */
  @Test
  public void testInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::insertPreppedRecords,
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API
   */
  @Test
  public void testBulkInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(
        (client, recordRDD, commitTime) -> client.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API
   */
  @Test
  public void testBulkInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(HoodieWriteClient::bulkInsert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   * record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> upsertFn,
      boolean isPreppedAPI
  ) throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
        HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainCommits(maxCommits).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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

        List<WriteStatus> statuses =
            upsertFn.apply(client, jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        HoodieTableMetaClient metadata = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
        HoodieTable table1 = HoodieTable.getHoodieTable(metadata, cfg, jsc);
        HoodieTimeline activeTimeline = table1.getCompletedCommitTimeline();
        Optional<HoodieInstant> earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits - 1);
        Set<HoodieInstant> acceptableCommits = activeTimeline.getInstants().collect(Collectors.toSet());
        if (earliestRetainedCommit.isPresent()) {
          acceptableCommits.removeAll(
              activeTimeline.findInstantsInRange("000", earliestRetainedCommit.get().getTimestamp()).getInstants()
                  .collect(Collectors.toSet()));
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
   * Test HoodieTable.clean() Cleaning by versions logic
   */
  @Test
  public void testKeepLatestFileVersions() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
        .build();

    // make 1 commit, with 1 file per partition
    HoodieTestUtils.createCommitFiles(basePath, "000");

    String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "000");
    String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "000");
    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "000", file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    HoodieTestUtils.createCommitFiles(basePath, "001");
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath, true), config,
        jsc);

    String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "001"); // insert
    String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "001"); // insert
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "001", file1P1C0); // update

    List<HoodieCleanStat> hoodieCleanStatsTwo = table.clean(jsc);
    assertEquals("Must clean 1 file", 1,
        getCleanStat(hoodieCleanStatsTwo, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals("Must clean 1 file", 1,
        getCleanStat(hoodieCleanStatsTwo, DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "001", file2P1C1));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "000", file1P1C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "002");
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true),
        config, jsc);

    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file2P0C1); // update
    String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002");

    List<HoodieCleanStat> hoodieCleanStatsThree = table.clean(jsc);
    assertEquals("Must clean two files", 2,
        getCleanStat(hoodieCleanStatsThree, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file3P0C2));

    // No cleaning on partially written file, with no commit.
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "003", file3P0C2); // update
    List<HoodieCleanStat> hoodieCleanStatsFour = table.clean(jsc);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsFour, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file3P0C2));
  }

  /**
   * Test HoodieTable.clean() Cleaning by versions logic for MOR table with Log files
   */
  @Test
  public void testKeepLatestFileVersionsMOR() throws IOException {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
        .build();

    HoodieTableMetaClient metaClient = HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath,
        HoodieTableType.MERGE_ON_READ);

    // Make 3 files, one base file and 2 log files associated with base file
    String file1P0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "000");
    String file2P0L0 = HoodieTestUtils
        .createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0, Optional.empty());
    String file2P0L1 = HoodieTestUtils
        .createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0, Optional.of(2));
    // make 1 compaction commit
    HoodieTestUtils.createCompactionCommitFiles(fs, basePath, "000");

    // Make 4 files, one base file and 3 log files associated with base file
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0);
    file2P0L0 = HoodieTestUtils
        .createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0, Optional.empty());
    file2P0L0 = HoodieTestUtils
        .createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0, Optional.of(2));
    file2P0L0 = HoodieTestUtils
        .createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0, Optional.of(3));
    // make 1 compaction commit
    HoodieTestUtils.createCompactionCommitFiles(fs, basePath, "001");

    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    List<HoodieCleanStat> hoodieCleanStats = table.clean(jsc);
    assertEquals("Must clean three files, one parquet and 2 log files", 3,
        getCleanStat(hoodieCleanStats, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0));
    assertFalse(
        HoodieTestUtils.doesLogFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file2P0L0, Optional.empty()));
    assertFalse(
        HoodieTestUtils.doesLogFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file2P0L0, Optional.of(2)));
  }

  /**
   * Test HoodieTable.clean() Cleaning by commit logic for MOR table with Log files
   */
  @Test
  public void testKeepLatestCommits() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build()).build();

    // make 1 commit, with 1 file per partition
    HoodieTestUtils.createCommitFiles(basePath, "000");

    String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "000");
    String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "000");

    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsOne, DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "000", file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    HoodieTestUtils.createCommitFiles(basePath, "001");
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true),
        config, jsc);

    String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "001"); // insert
    String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "001"); // insert
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, "001", file1P1C0); // update

    List<HoodieCleanStat> hoodieCleanStatsTwo = table.clean(jsc);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsTwo, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsTwo, DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "001", file2P1C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_SECOND_PARTITION_PATH, "000", file1P1C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "002");
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true),
        config, jsc);

    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file2P0C1); // update
    String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "002");

    List<HoodieCleanStat> hoodieCleanStatsThree = table.clean(jsc);
    assertEquals("Must not clean any file. We have to keep 1 version before the latest commit time to keep", 0,
        getCleanStat(hoodieCleanStatsThree, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());

    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));

    // make next commit, with 2 updates to existing files, and 1 insert
    HoodieTestUtils.createCommitFiles(basePath, "003");
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true),
        config, jsc);

    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "003", file1P0C0); // update
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "003", file2P0C1); // update
    String file4P0C3 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "003");

    List<HoodieCleanStat> hoodieCleanStatsFour = table.clean(jsc);
    assertEquals("Must not clean one old file", 1,
        getCleanStat(hoodieCleanStatsFour, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());

    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "000", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file2P0C1));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "002", file3P0C2));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "003", file4P0C3));

    // No cleaning on partially written file, with no commit.
    HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, "004", file3P0C2); // update
    List<HoodieCleanStat> hoodieCleanStatsFive = table.clean(jsc);
    assertEquals("Must not clean any files", 0,
        getCleanStat(hoodieCleanStatsFive, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file1P0C0));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, DEFAULT_FIRST_PARTITION_PATH, "001", file2P0C1));
  }

  /**
   * Test Cleaning functionality of table.rollback() API.
   */
  @Test
  public void testCleanTemporaryDataFilesOnRollback() throws IOException {
    HoodieTestUtils.createCommitFiles(basePath, "000");
    List<String> tempFiles = createTempFiles("000", 10);
    assertEquals("Some temp files are created.", 10, tempFiles.size());
    assertEquals("Some temp files are created.", tempFiles.size(), getTotalTempFiles());

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withUseTempFolderCopyOnWriteForCreate(false)
        .withUseTempFolderCopyOnWriteForMerge(false).build();
    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);
    table.rollback(jsc, Collections.emptyList());
    assertEquals("Some temp files are created.", tempFiles.size(), getTotalTempFiles());

    config = HoodieWriteConfig.newBuilder().withPath(basePath).withUseTempFolderCopyOnWriteForCreate(true)
        .withUseTempFolderCopyOnWriteForMerge(false).build();
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true),
        config, jsc);
    table.rollback(jsc, Collections.emptyList());
    assertEquals("All temp files are deleted.", 0, getTotalTempFiles());
  }

  /**
   * Test CLeaner Stat when there are no partition paths.
   */
  @Test
  public void testCleaningWithZeroPartitonPaths() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build()).build();

    // Make a commit, although there are no partitionPaths.
    // Example use-case of this is when a client wants to create a table
    // with just some commit metadata, but no data/partitionPaths.
    HoodieTestUtils.createCommitFiles(basePath, "000");

    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);

    List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
    assertTrue("HoodieCleanStats should be empty for a table with empty partitionPaths", hoodieCleanStatsOne.isEmpty());
  }

  /**
   * Test Clean-by-commits behavior in the presence of skewed partitions
   */
  @Test
  public void testCleaningSkewedPartitons() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build()).build();
    Map<Long, Long> stageOneShuffleReadTaskRecordsCountMap = new HashMap<>();

    // Since clean involves repartition in order to uniformly distribute data,
    // we can inspect the number of records read by various tasks in stage 1.
    // There should not be skew in the number of records read in the task.

    // SparkListener below listens to the stage end events and captures number of
    // records read by various tasks in stage-1.
    jsc.sc().addSparkListener(new SparkListener() {

      @Override
      public void onTaskEnd(SparkListenerTaskEnd taskEnd) {

        Iterator<AccumulatorV2<?, ?>> iterator = taskEnd.taskMetrics().accumulators().iterator();
        while (iterator.hasNext()) {
          AccumulatorV2 accumulator = iterator.next();
          if (taskEnd.stageId() == 1 && accumulator.isRegistered() && accumulator.name().isDefined()
              && accumulator.name().get().equals("internal.metrics.shuffle.read.recordsRead")) {
            stageOneShuffleReadTaskRecordsCountMap.put(taskEnd.taskInfo().taskId(), (Long) accumulator.value());
          }
        }
      }
    });

    // make 1 commit, with 100 files in one partition and 10 in other two
    HoodieTestUtils.createCommitFiles(basePath, "000");
    List<String> filesP0C0 = createFilesInPartition(DEFAULT_FIRST_PARTITION_PATH, "000", 100);
    List<String> filesP1C0 = createFilesInPartition(DEFAULT_SECOND_PARTITION_PATH, "000", 10);
    List<String> filesP2C0 = createFilesInPartition(DEFAULT_THIRD_PARTITION_PATH, "000", 10);

    HoodieTestUtils.createCommitFiles(basePath, "001");
    updateAllFilesInPartition(filesP0C0, DEFAULT_FIRST_PARTITION_PATH, "001");
    updateAllFilesInPartition(filesP1C0, DEFAULT_SECOND_PARTITION_PATH, "001");
    updateAllFilesInPartition(filesP2C0, DEFAULT_THIRD_PARTITION_PATH, "001");

    HoodieTestUtils.createCommitFiles(basePath, "002");
    updateAllFilesInPartition(filesP0C0, DEFAULT_FIRST_PARTITION_PATH, "002");
    updateAllFilesInPartition(filesP1C0, DEFAULT_SECOND_PARTITION_PATH, "002");
    updateAllFilesInPartition(filesP2C0, DEFAULT_THIRD_PARTITION_PATH, "002");

    HoodieTestUtils.createCommitFiles(basePath, "003");
    updateAllFilesInPartition(filesP0C0, DEFAULT_FIRST_PARTITION_PATH, "003");
    updateAllFilesInPartition(filesP1C0, DEFAULT_SECOND_PARTITION_PATH, "003");
    updateAllFilesInPartition(filesP2C0, DEFAULT_THIRD_PARTITION_PATH, "003");

    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);
    List<HoodieCleanStat> hoodieCleanStats = table.clean(jsc);

    assertEquals(100, getCleanStat(hoodieCleanStats, DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals(10, getCleanStat(hoodieCleanStats, DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().size());
    assertEquals(10, getCleanStat(hoodieCleanStats, DEFAULT_THIRD_PARTITION_PATH).getSuccessDeleteFiles().size());

    // 3 tasks are expected since the number of partitions is 3
    assertEquals(3, stageOneShuffleReadTaskRecordsCountMap.keySet().size());
    // Sum of all records processed = total number of files to clean
    assertEquals(120,
        stageOneShuffleReadTaskRecordsCountMap.values().stream().reduce((a, b) -> a + b).get().intValue());
    assertTrue("The skew in handling files to clean is not removed. "
            + "Each task should handle more records than the partitionPath with least files "
            + "and less records than the partitionPath with most files.",
        stageOneShuffleReadTaskRecordsCountMap.values().stream().filter(a -> a > 10 && a < 100).count() == 3);
  }


  /**
   * Test Keep Latest Commits when there are pending compactions
   */
  @Test
  public void testKeepLatestCommitsWithPendingCompactions() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build()).build();
    // Deletions:
    // .  FileId     Parquet   Logs     Total     Retained Commits
    //      FileId7   5         10        15         009, 011
    //      FileId6   5         10        15         009
    //      FileId5   3         6          9         005
    //      FileId4   2         4          6         003
    //      FileId3   1         2          3         001
    //      FileId2   0         0          0         000
    //      FileId1   0         0          0         000
    testPendingCompactions(config, 48, 18);
  }

  /**
   * Test Keep Latest Versions when there are pending compactions
   */
  @Test
  public void testKeepLatestVersionsWithPendingCompactions() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCleanerPolicy(
            HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(2).build()).build();
    // Deletions:
    // .  FileId     Parquet   Logs     Total     Retained Commits
    //      FileId7   5         10        15         009, 011
    //      FileId6   4         8         12         007, 009
    //      FileId5   2         4          6         003  005
    //      FileId4   1         2          3         001, 003
    //      FileId3   0         0          0         000, 001
    //      FileId2   0         0          0         000
    //      FileId1   0         0          0         000
    testPendingCompactions(config, 36, 9);
  }

  /**
   * Common test method for validating pending compactions
   *
   * @param config             Hoodie Write Config
   * @param expNumFilesDeleted Number of files deleted
   */
  public void testPendingCompactions(HoodieWriteConfig config, int expNumFilesDeleted,
      int expNumFilesUnderCompactionDeleted) throws IOException {
    HoodieTableMetaClient metaClient = HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath,
        HoodieTableType.MERGE_ON_READ);
    String[] instants = new String[]{"000", "001", "003", "005", "007", "009", "011", "013"};
    String[] compactionInstants = new String[]{"002", "004", "006", "008", "010"};
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
    String[] fileIds = new String[]
        {"fileId1", "fileId2", "fileId3", "fileId4", "fileId5", "fileId6", "fileId7"};
    int maxNumFileIdsForCompaction = 4;
    for (int i = 0; i < maxNumFileIds; i++) {
      final String fileId = HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, instants[0],
          fileIds[i]);
      HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, instants[0],
          fileId, Optional.empty());
      HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, instants[0],
          fileId, Optional.of(2));
      fileIdToLatestInstantBeforeCompaction.put(fileId, instants[0]);
      for (int j = 1; j <= i; j++) {
        if (j == i && j <= maxNumFileIdsForCompaction) {
          expFileIdToPendingCompaction.put(fileId, compactionInstants[j]);
          HoodieTable table = HoodieTable.getHoodieTable(
              new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
              jsc);
          FileSlice slice = table.getRTFileSystemView().getLatestFileSlices(DEFAULT_FIRST_PARTITION_PATH)
              .filter(fs -> fs.getFileId().equals(fileId)).findFirst().get();
          List<FileSlice> slices = new ArrayList<>();
          if (compactionInstantsToFileSlices.containsKey(compactionInstants[j])) {
            slices = compactionInstantsToFileSlices.get(compactionInstants[j]);
          }
          slices.add(slice);
          compactionInstantsToFileSlices.put(compactionInstants[j], slices);
          // Add log-files to simulate delta-commits after pending compaction
          HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, compactionInstants[j],
              fileId, Optional.empty());
          HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, compactionInstants[j],
              fileId, Optional.of(2));
        } else {
          HoodieTestUtils.createDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, instants[j], fileId);
          HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, instants[j], fileId,
              Optional.empty());
          HoodieTestUtils.createNewLogFile(fs, basePath, DEFAULT_FIRST_PARTITION_PATH, instants[j], fileId,
              Optional.of(2));
          fileIdToLatestInstantBeforeCompaction.put(fileId, instants[j]);
        }
      }
    }

    // Setup pending compaction plans
    for (String instant : compactionInstants) {
      List<FileSlice> fileSliceList = compactionInstantsToFileSlices.get(instant);
      if (null != fileSliceList) {
        HoodieTestUtils.createCompactionRequest(metaClient, instant,
            fileSliceList.stream().map(fs -> Pair.of(DEFAULT_FIRST_PARTITION_PATH, fs)).collect(Collectors.toList()));
      }
    }

    // Clean now
    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);
    List<HoodieCleanStat> hoodieCleanStats = table.clean(jsc);

    // Test for safety
    final HoodieTable hoodieTable = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config,
        jsc);

    expFileIdToPendingCompaction.entrySet().stream().forEach(entry -> {
      String fileId = entry.getKey();
      String baseInstantForCompaction = fileIdToLatestInstantBeforeCompaction.get(fileId);
      Optional<FileSlice> fileSliceForCompaction =
          hoodieTable.getRTFileSystemView().getLatestFileSlicesBeforeOrOn(DEFAULT_FIRST_PARTITION_PATH,
              baseInstantForCompaction).filter(fs -> fs.getFileId().equals(fileId)).findFirst();
      Assert.assertTrue("Base Instant for Compaction must be preserved", fileSliceForCompaction.isPresent());
      Assert.assertTrue("FileSlice has data-file", fileSliceForCompaction.get().getDataFile().isPresent());
      Assert.assertEquals("FileSlice has log-files", 2,
          fileSliceForCompaction.get().getLogFiles().count());
    });

    // Test for progress (Did we clean some files ?)
    long numFilesUnderCompactionDeleted =
        hoodieCleanStats.stream().flatMap(cleanStat -> {
          return convertPathToFileIdWithCommitTime(metaClient, cleanStat.getDeletePathPatterns()).map(
              fileIdWithCommitTime -> {
                if (expFileIdToPendingCompaction.containsKey(fileIdWithCommitTime.getKey())) {
                  Assert.assertTrue("Deleted instant time must be less than pending compaction",
                      HoodieTimeline.compareTimestamps(
                          fileIdToLatestInstantBeforeCompaction.get(fileIdWithCommitTime.getKey()),
                          fileIdWithCommitTime.getValue(), GREATER));
                  return true;
                }
                return false;
              });
        }).filter(x -> x).count();
    long numDeleted = hoodieCleanStats.stream()
        .flatMap(cleanStat -> cleanStat.getDeletePathPatterns().stream()).count();
    // Tighter check for regression
    Assert.assertEquals("Correct number of files deleted", expNumFilesDeleted, numDeleted);
    Assert.assertEquals("Correct number of files under compaction deleted",
        expNumFilesUnderCompactionDeleted, numFilesUnderCompactionDeleted);
  }

  /**
   * Utility method to create temporary data files
   *
   * @param commitTime Commit Timestamp
   * @param numFiles Number for files to be generated
   * @return generated files
   * @throws IOException in case of error
   */
  private List<String> createTempFiles(String commitTime, int numFiles) throws IOException {
    List<String> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(HoodieTestUtils.createNewDataFile(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, commitTime));
    }
    return files;
  }

  /***
   * Helper method to return temporary files count
   * @return Number of temporary files found
   * @throws IOException in case of error
   */
  private int getTotalTempFiles() throws IOException {
    return fs.listStatus(new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME)).length;
  }

  private Stream<Pair<String, String>> convertPathToFileIdWithCommitTime(
      final HoodieTableMetaClient metaClient, List<String> paths) {
    Predicate<String> roFilePredicate = path ->
        path.contains(metaClient.getTableConfig().getROFileFormat().getFileExtension());
    Predicate<String> rtFilePredicate = path ->
        path.contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension());
    Stream<Pair<String, String>> stream1 = paths.stream().filter(roFilePredicate)
        .map(fullPath -> {
          String fileName = Paths.get(fullPath).getFileName().toString();
          return Pair.of(FSUtils.getFileId(fileName), FSUtils.getCommitTime(fileName));
        });
    Stream<Pair<String, String>> stream2 = paths.stream().filter(rtFilePredicate)
        .map(path -> {
          return Pair.of(FSUtils.getFileIdFromLogPath(new Path(path)),
              FSUtils.getBaseCommitTimeFromLogPath(new Path(path)));
        });
    return Stream.concat(stream1, stream2);
  }
}
