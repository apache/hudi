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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieMergeOnReadTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload.MetadataMergeWriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieHiveUtil;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.action.deltacommit.DeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.DeltaCommitActionExecutor;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMergeOnReadTable extends HoodieClientTestHarness {

  private HoodieParquetInputFormat roInputFormat;
  private JobConf roJobConf;

  private HoodieParquetRealtimeInputFormat rtInputFormat;
  private JobConf rtJobConf;

  @BeforeEach
  public void init() throws IOException {
    initDFS();
    initSparkContexts("TestHoodieMergeOnReadTable");
    jsc.hadoopConfiguration().addResource(dfs.getConf());
    initPath();
    dfs.mkdirs(new Path(basePath));
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();

    // initialize parquet input format
    roInputFormat = new HoodieParquetInputFormat();
    roJobConf = new JobConf(jsc.hadoopConfiguration());
    roInputFormat.setConf(roJobConf);

    rtInputFormat = new HoodieParquetRealtimeInputFormat();
    rtJobConf = new JobConf(jsc.hadoopConfiguration());
    rtInputFormat.setConf(rtJobConf);
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupDFS();
    cleanupSparkContexts();
    cleanupTestDataGenerator();
  }

  private HoodieWriteClient getWriteClient(HoodieWriteConfig config) {
    return new HoodieWriteClient(jsc, config);
  }

  @Test
  public void testSimpleInsertAndUpdate() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      insertAndGetFilePaths(records, client, cfg, newCommitTime);

      /**
       * Write 2 (updates)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateAndGetFilePaths(records, client, cfg, newCommitTime);

      String compactionCommitTime = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(compactionCommitTime);

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(dfs, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);
      HoodieTableFileSystemView roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent());

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
      assertTrue(HoodieTimeline.compareTimestamps("000", HoodieTimeline.LESSER_THAN, latestCompactionCommitTime));

      assertEquals(200, HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, "000").count(),
          "Must contain 200 records");
    }
  }

  // test incremental read does not go past compaction instant for RO views
  // For RT views, incremental read can go past compaction
  @Test
  public void testIncrementalReadsWithCompaction() throws Exception {
    String partitionPath = "2020/02/20"; // use only one partition for this test
    dataGen = new HoodieTestDataGenerator(new String[] { partitionPath });
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String commitTime1 = "001";
      client.startCommitWithTime(commitTime1);

      List<HoodieRecord> records001 = dataGen.generateInserts(commitTime1, 200);
      insertAndGetFilePaths(records001, client, cfg, commitTime1);

      // verify only one parquet file shows up with commit time 001
      FileStatus[] incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateIncrementalFiles(partitionPath, 1, incrementalROFiles, roInputFormat,
              roJobConf,200, commitTime1);
      Path firstFilePath = incrementalROFiles[0].getPath();

      FileStatus[] incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateIncrementalFiles(partitionPath, 1, incrementalRTFiles, rtInputFormat,
              rtJobConf,200, commitTime1);
      assertEquals(firstFilePath, incrementalRTFiles[0].getPath());

      /**
       * Write 2 (updates)
       */
      String updateTime = "004";
      client.startCommitWithTime(updateTime);
      List<HoodieRecord> records004 = dataGen.generateUpdates(updateTime, 100);
      updateAndGetFilePaths(records004, client, cfg, updateTime);

      // verify RO incremental reads - only one parquet file shows up because updates to into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateIncrementalFiles(partitionPath, 1, incrementalROFiles, roInputFormat,
              roJobConf, 200, commitTime1);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateIncrementalFiles(partitionPath, 1, incrementalRTFiles, rtInputFormat,
              rtJobConf, 200, commitTime1, updateTime);

      // request compaction, but do not perform compaction
      String compactionCommitTime = "005";
      client.scheduleCompactionAtInstant("005", Option.empty());

      // verify RO incremental reads - only one parquet file shows up because updates go into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateIncrementalFiles(partitionPath,1, incrementalROFiles, roInputFormat,
              roJobConf, 200, commitTime1);

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateIncrementalFiles(partitionPath, 1, incrementalRTFiles, rtInputFormat,
              rtJobConf, 200, commitTime1, updateTime);

      // write 3 - more inserts
      String insertsTime = "006";
      List<HoodieRecord> records006 = dataGen.generateInserts(insertsTime, 200);
      client.startCommitWithTime(insertsTime);
      insertAndGetFilePaths(records006, client, cfg, insertsTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());
      // verify 006 does not show up in RO mode because of pending compaction
      validateIncrementalFiles(partitionPath, 1, incrementalROFiles, roInputFormat,
              roJobConf, 200, commitTime1);

      // verify that if stopAtCompaction is disabled, inserts from "insertsTime" show up
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateIncrementalFiles(partitionPath,2, incrementalROFiles, roInputFormat,
          roJobConf, 400, commitTime1, insertsTime);

      // verify 006 shows up in RT views
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateIncrementalFiles(partitionPath, 2, incrementalRTFiles, rtInputFormat,
              rtJobConf, 400, commitTime1, updateTime, insertsTime);

      // perform the scheduled compaction
      client.compact(compactionCommitTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, "002", -1, true);
      assertTrue(incrementalROFiles.length == 2);
      // verify 006 shows up because of pending compaction
      validateIncrementalFiles(partitionPath, 2, incrementalROFiles, roInputFormat,
              roJobConf, 400, commitTime1, compactionCommitTime, insertsTime);
    }
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() {
    HoodieWriteConfig cfg = getConfigBuilder(false).withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    try (HoodieWriteClient client = getWriteClient(cfg)) {

      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      client.startCommitWithTime(newCommitTime);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      Map<String, String> allWriteStatusMergedMetadataMap =
          MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(statuses);
      assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
      // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
      // should be 2 * records.size()
      assertEquals(String.valueOf(2 * records.size()),
          allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
    }
  }

  @Test
  public void testSimpleInsertUpdateAndDelete() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as parquet file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      BaseFileOnlyView roView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      assertFalse(baseFilesToRead.findAny().isPresent());

      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent(),
          "should list the parquet files we wrote in the delta commit");

      /**
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      /**
       * Write 2 (only deletes, written to .log file)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);

      statuses = client.upsert(jsc.parallelize(fewRecordsForDelete, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("004", deltaCommit.get().getTimestamp(), "Latest Delta commit should be 004");

      commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = HoodieTestUtils.listAllBaseFilesInPath(dfs, cfg.getBasePath());
      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent());

      List<String> baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
      // Wrote 20 records and deleted 20 records, so remaining 20-20 = 0
      assertEquals(0, recordsRead.size(), "Must contain 0 records");
    }
  }

  @Test
  public void testCOWToMORConvertedTableRollback() throws Exception {

    // Set TableType to COW
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      // verify there are no errors
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertTrue(commit.isPresent());
      assertEquals("001", commit.get().getTimestamp(), "commit should be 001");

      /**
       * Write 2 (updates)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);

      statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      // Set TableType to MOR
      HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);

      // rollback a COW commit when TableType is MOR
      client.rollback(newCommitTime);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);
      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      HoodieTableFileSystemView roView =
          new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);

      final String absentCommit = newCommitTime;
      assertFalse(roView.getLatestBaseFiles().anyMatch(file -> absentCommit.equals(file.getCommitTime())));
    }
  }

  @Test
  public void testRollbackWithDeltaAndCompactionCommit() throws Exception {

    HoodieWriteConfig cfg = getConfig(false);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      // Test delta commit rollback
      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      BaseFileOnlyView roView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(!baseFilesToRead.findAny().isPresent());

      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent(),
          "should list the parquet files we wrote in the delta commit");

      /**
       * Write 2 (inserts + updates - testing failed delta commit)
       */
      final String commitTime1 = "002";
      // WriteClient with custom config (disable small file handling)
      try (HoodieWriteClient secondClient = getWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());) {
        secondClient.startCommitWithTime(commitTime1);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime1, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime1, 200));

        List<String> baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
        assertEquals(recordsRead.size(), 200);

        statuses = secondClient.upsert(jsc.parallelize(copyOfRecords, 1), commitTime1).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // Test failed delta commit rollback
        secondClient.rollback(commitTime1);
        allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
        // After rollback, there should be no parquet file with the failed commit time
        assertEquals(0, Arrays.stream(allFiles)
            .filter(file -> file.getPath().getName().contains(commitTime1)).count());
        baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
        assertEquals(200, recordsRead.size());
      }

      /**
       * Write 3 (inserts + updates - testing successful delta commit)
       */
      final String commitTime2 = "002";
      try (HoodieWriteClient thirdClient = getWriteClient(cfg);) {
        thirdClient.startCommitWithTime(commitTime2);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

        List<String> baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
        assertEquals(200, recordsRead.size());

        writeRecords = jsc.parallelize(copyOfRecords, 1);
        writeStatusJavaRDD = thirdClient.upsert(writeRecords, commitTime2);
        thirdClient.commit(commitTime2, writeStatusJavaRDD);
        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // Test successful delta commit rollback
        thirdClient.rollback(commitTime2);
        allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
        // After rollback, there should be no parquet file with the failed commit time
        assertEquals(0, Arrays.stream(allFiles)
            .filter(file -> file.getPath().getName().contains(commitTime2)).count());

        metaClient = HoodieTableMetaClient.reload(metaClient);
        hoodieTable = HoodieTable.create(metaClient, cfg, jsc);
        roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
        // check that the number of records read is still correct after rollback operation
        assertEquals(200, recordsRead.size());

        // Test compaction commit rollback
        /**
         * Write 4 (updates)
         */
        newCommitTime = "003";
        thirdClient.startCommitWithTime(newCommitTime);

        writeStatusJavaRDD = thirdClient.upsert(writeRecords, newCommitTime);
        thirdClient.commit(newCommitTime, writeStatusJavaRDD);
        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);

        String compactionInstantTime = thirdClient.scheduleCompaction(Option.empty()).get().toString();
        JavaRDD<WriteStatus> ws = thirdClient.compact(compactionInstantTime);
        thirdClient.commitCompaction(compactionInstantTime, ws, Option.empty());

        allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
        metaClient = HoodieTableMetaClient.reload(metaClient);
        roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        final String compactedCommitTime =
            metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get().getTimestamp();

        assertTrue(roView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));

        thirdClient.rollback(compactedCommitTime);

        allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
        metaClient = HoodieTableMetaClient.reload(metaClient);
        roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        assertFalse(roView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));
      }
    }
  }

  @Test
  public void testMultiRollbackWithDeltaAndCompactionCommit() throws Exception {

    HoodieWriteConfig cfg = getConfig(false);
    try (final HoodieWriteClient client = getWriteClient(cfg);) {
      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      BaseFileOnlyView roView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      assertFalse(baseFilesToRead.findAny().isPresent());

      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent(),
          "Should list the parquet files we wrote in the delta commit");

      /**
       * Write 2 (inserts + updates)
       */
      newCommitTime = "002";
      // WriteClient with custom config (disable small file handling)
      HoodieWriteClient nClient = getWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());
      nClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
      copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
      copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

      List<String> baseFiles = roView.getLatestBaseFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
      assertEquals(200, recordsRead.size());

      statuses = nClient.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      nClient.commit(newCommitTime, writeStatusJavaRDD);
      copyOfRecords.clear();

      // Schedule a compaction
      /**
       * Write 3 (inserts + updates)
       */
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newInserts = dataGen.generateInserts(newCommitTime, 100);
      records = dataGen.generateUpdates(newCommitTime, records);
      records.addAll(newInserts);
      writeRecords = jsc.parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      String compactionInstantTime = "004";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());

      // Compaction commit
      /**
       * Write 4 (updates)
       */
      newCommitTime = "005";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      compactionInstantTime = "006";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
      JavaRDD<WriteStatus> ws = client.compact(compactionInstantTime);
      client.commitCompaction(compactionInstantTime, ws, Option.empty());

      allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      metaClient = HoodieTableMetaClient.reload(metaClient);
      roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

      final String compactedCommitTime =
          metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get().getTimestamp();

      assertTrue(roView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));

      /**
       * Write 5 (updates)
       */
      newCommitTime = "007";
      client.startCommitWithTime(newCommitTime);
      copyOfRecords = new ArrayList<>(records);
      copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
      copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

      statuses = client.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, writeStatusJavaRDD);
      copyOfRecords.clear();

      // Rollback latest commit first
      client.restoreToInstant("000");

      metaClient = HoodieTableMetaClient.reload(metaClient);
      allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      roView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertFalse(baseFilesToRead.findAny().isPresent());
      SliceView rtView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      List<HoodieFileGroup> fileGroups =
          ((HoodieTableFileSystemView) rtView).getAllFileGroups().collect(Collectors.toList());
      assertTrue(fileGroups.isEmpty());

      // make sure there are no log files remaining
      assertEquals(0L, ((HoodieTableFileSystemView) rtView).getAllFileGroups()
          .filter(fileGroup -> fileGroup.getAllRawFileSlices().noneMatch(f -> f.getLogFiles().count() == 0))
          .count());

    }
  }

  protected HoodieWriteConfig getHoodieWriteConfigWithSmallFileHandlingOff() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(false).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024).build()).forTable("test-trip-table")
        .build();
  }

  @Test
  public void testUpsertPartitioner() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as parquet file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      BaseFileOnlyView roView = new HoodieTableFileSystemView(metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      Map<String, Long> parquetFileIdToSize =
          baseFilesToRead.collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> baseFilesList = baseFilesToRead.collect(Collectors.toList());
      assertTrue(baseFilesList.size() > 0,
          "Should list the parquet files we wrote in the delta commit");

      /**
       * Write 2 (only updates + inserts, written to .log file + correction of existing parquet file size)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, records);
      newRecords.addAll(dataGen.generateInserts(newCommitTime, 20));

      statuses = client.upsert(jsc.parallelize(newRecords), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("002", deltaCommit.get().getTimestamp(), "Latest Delta commit should be 002");

      commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      roView = new HoodieTableFileSystemView(metaClient,
          hoodieTable.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> newBaseFilesList = baseFilesToRead.collect(Collectors.toList());
      Map<String, Long> parquetFileIdToNewSize =
          newBaseFilesList.stream().collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      assertTrue(parquetFileIdToNewSize.entrySet().stream().anyMatch(entry -> parquetFileIdToSize.get(entry.getKey()) < entry.getValue()));

      List<String> baseFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(baseFiles, basePath);
      // Wrote 20 records in 2 batches
      assertEquals(40, recordsRead.size(), "Must contain 40 records");
    }
  }

  @Test
  public void testLogFileCountsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig(true);
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);

      HoodieReadClient readClient = new HoodieReadClient(jsc, config);
      updatedRecords = readClient.tagLocation(updatedRecordsRDD).collect();

      // Write them to corresponding avro logfiles
      HoodieTestUtils.writeRecordsToLogFiles(metaClient.getFs(), metaClient.getBasePath(),
          HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS, updatedRecords);

      // Verify that all base file has one log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieTable.create(metaClient, config, jsc);
      // In writeRecordsToLogFiles, no commit files are getting added, so resetting file-system view state
      ((SyncableFileSystemView) (table.getSliceView())).reset();

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(), "There should be 1 log file written for every base file");
        }
      }

      // Mark 2nd delta-instant as completed
      metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(State.INFLIGHT,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime));
      metaClient.getActiveTimeline().saveAsComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // Do a compaction
      String compactionInstantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      JavaRDD<WriteStatus> result = writeClient.compact(compactionInstantTime);

      // Verify that recently written compacted base file has no log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieTable.create(metaClient, config, jsc);
      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

      assertTrue(HoodieTimeline
              .compareTimestamps(timeline.lastInstant().get().getTimestamp(), HoodieTimeline.GREATER_THAN, newCommitTime),
          "Compaction commit should be > than last insert");

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice slice : groupedLogFiles) {
          assertEquals(0, slice.getLogFiles().count(), "After compaction there should be no log files visible on a full view");
        }
        List<WriteStatus> writeStatuses = result.collect();
        assertTrue(writeStatuses.stream().anyMatch(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath)));
      }
    }
  }

  @Test
  public void testSimpleInsertsGeneratedIntoLogFiles() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      HoodieTable table =
          HoodieTable.create(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config, jsc);
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertEquals(0, tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getBaseFile().isPresent()).count());
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }

      assertTrue(numLogFiles > 0);
      // Do a compaction
      String instantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      statuses = writeClient.compact(instantTime);
      assertEquals(statuses.map(status -> status.getStat().getPath().contains("parquet")).count(), numLogFiles);
      assertEquals(statuses.count(), numLogFiles);
      writeClient.commitCompaction(instantTime, statuses, Option.empty());
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollback(@TempDir java.nio.file.Path tempFolder) throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      // trigger an action
      List<WriteStatus> writeStatuses = statuses.collect();

      // Ensure that inserts are written to only log files
      assertEquals(0,
          writeStatuses.stream().filter(writeStatus -> !writeStatus.getStat().getPath().contains("log")).count());
      assertTrue(
          writeStatuses.stream().anyMatch(writeStatus -> writeStatus.getStat().getPath().contains("log")));

      // rollback a failed commit
      boolean rollback = writeClient.rollback(newCommitTime);
      assertTrue(rollback);
      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      // insert 100 records
      records = dataGen.generateInserts(newCommitTime, 100);
      recordsRDD = jsc.parallelize(records, 1);
      statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      // Sleep for small interval (at least 1 second) to force a new rollback start time.
      Thread.sleep(1000);

      // We will test HUDI-204 here. We will simulate rollback happening twice by copying the commit file to local fs
      // and calling rollback twice
      final String lastCommitTime = newCommitTime;
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
      HoodieInstant last = metaClient.getCommitsTimeline().getInstants()
          .filter(instant -> instant.getTimestamp().equals(lastCommitTime)).findFirst().get();
      String fileName = last.getFileName();
      // Save the .commit file to local directory.
      // Rollback will be called twice to test the case where rollback failed first time and retried.
      // We got the "BaseCommitTime cannot be null" exception before the fix
      File file = Files.createTempFile(tempFolder, null, null).toFile();
      metaClient.getFs().copyToLocalFile(new Path(metaClient.getMetaPath(), fileName),
          new Path(file.getAbsolutePath()));
      writeClient.rollback(newCommitTime);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieTable.create(metaClient, config, jsc);
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }
      assertEquals(0, numLogFiles);
      metaClient.getFs().copyFromLocalFile(new Path(file.getAbsolutePath()),
          new Path(metaClient.getMetaPath(), fileName));
      Thread.sleep(1000);
      // Rollback again to pretend the first rollback failed partially. This should not error our
      writeClient.rollback(newCommitTime);
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompaction() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);
      // trigger an action
      statuses.collect();

      HoodieTable table =
          HoodieTable.create(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config, jsc);
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }

      assertTrue(numLogFiles > 0);
      // Do a compaction
      newCommitTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      statuses = writeClient.compact(newCommitTime);
      // Ensure all log files have been compacted into parquet files
      assertEquals(statuses.map(status -> status.getStat().getPath().contains("parquet")).count(), numLogFiles);
      assertEquals(statuses.count(), numLogFiles);
      writeClient.commitCompaction(newCommitTime, statuses, Option.empty());
      // Trigger a rollback of compaction
      writeClient.rollback(newCommitTime);
      table = HoodieTable.create(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config, jsc);
      tableRTFileSystemView = table.getSliceView();
      ((SyncableFileSystemView) tableRTFileSystemView).reset();
      Option<HoodieInstant> lastInstant = ((SyncableFileSystemView) tableRTFileSystemView).getLastInstant();
      System.out.println("Last Instant =" + lastInstant);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
      }
    }
  }

  /**
   * Test to ensure rolling stats are correctly written to metadata file.
   */
  @Test
  public void testRollingStatsInMetadata() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    try (HoodieWriteClient client = getWriteClient(cfg);) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
      HoodieTable table = HoodieTable.create(metaClient, cfg, jsc);

      // Create a commit without rolling stats in metadata to test backwards compatibility
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant instant = new HoodieInstant(State.REQUESTED, commitActionType, "000");
      activeTimeline.createNewInstant(instant);
      activeTimeline.transitionRequestedToInflight(instant, Option.empty());
      instant = new HoodieInstant(State.INFLIGHT, commitActionType, "000");
      activeTimeline.saveAsComplete(instant, Option.empty());

      String instantTime = "001";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline().getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          inserts += stat.getValue().getInserts();
        }
      }
      assertEquals(200, inserts);

      instantTime = "002";
      client.startCommitWithTime(instantTime);
      records = dataGen.generateUpdates(instantTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      inserts = 0;
      int upserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          inserts += stat.getValue().getInserts();
          upserts += stat.getValue().getUpserts();
        }
      }

      assertEquals(200, inserts);
      assertEquals(200, upserts);

      client.rollback(instantTime);

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          inserts += stat.getValue().getInserts();
          upserts += stat.getValue().getUpserts();
        }
      }
      assertEquals(200, inserts);
      assertEquals(0, upserts);
    }
  }

  /**
   * Test to ensure rolling stats are correctly written to the metadata file, identifies small files and corrects them.
   */
  @Test
  public void testRollingStatsWithSmallFileHandling() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    try (HoodieWriteClient client = getWriteClient(cfg);) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
      Map<String, Long> fileIdToInsertsMap = new HashMap<>();
      Map<String, Long> fileIdToUpsertsMap = new HashMap<>();

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      HoodieTable table = HoodieTable.create(cfg, jsc);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          inserts += stat.getValue().getInserts();
          fileIdToInsertsMap.put(stat.getKey(), stat.getValue().getInserts());
          fileIdToUpsertsMap.put(stat.getKey(), stat.getValue().getUpserts());
        }
      }
      assertEquals(200, inserts);

      instantTime = "001";
      client.startCommitWithTime(instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      inserts = 0;
      int upserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          // No new file id should be created, all the data should be written to small files already there
          assertTrue(fileIdToInsertsMap.containsKey(stat.getKey()));
          assertTrue(fileIdToUpsertsMap.containsKey(stat.getKey()));
          inserts += stat.getValue().getInserts();
          upserts += stat.getValue().getUpserts();
        }
      }

      assertEquals(400, inserts);
      assertEquals(200, upserts);

      // Test small file handling after compaction
      instantTime = "002";
      client.scheduleCompactionAtInstant(instantTime, Option.of(metadata.getExtraMetadata()));
      statuses = client.compact(instantTime);
      client.commitCompaction(instantTime, statuses, Option.empty());

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getCommitsTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      HoodieRollingStatMetadata rollingStatMetadata1 = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);

      // Ensure that the rolling stats from the extra metadata of delta commits is copied over to the compaction commit
      for (Map.Entry<String, Map<String, HoodieRollingStat>> entry : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        assertTrue(rollingStatMetadata1.getPartitionToRollingStats().containsKey(entry.getKey()));
        assertEquals(rollingStatMetadata1.getPartitionToRollingStats().get(entry.getKey()).size(),
            entry.getValue().size());
      }

      // Write inserts + updates
      instantTime = "003";
      client.startCommitWithTime(instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieTable.create(cfg, jsc);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      rollingStatMetadata = HoodieCommitMetadata.fromBytes(
          metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(),
          HoodieRollingStatMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
          .entrySet()) {
        for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
          // No new file id should be created, all the data should be written to small files already there
          assertTrue(fileIdToInsertsMap.containsKey(stat.getKey()));
          inserts += stat.getValue().getInserts();
          upserts += stat.getValue().getUpserts();
        }
      }

      assertEquals(600, inserts);
      assertEquals(600, upserts);
    }
  }

  /**
   * Test to validate invoking table.handleUpdate() with input records from multiple partitions will fail.
   */
  @Test
  public void testHandleUpdateWithMultiplePartitions() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (HoodieWriteClient client = getWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as parquet file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
      HoodieMergeOnReadTable hoodieTable = (HoodieMergeOnReadTable) HoodieTable.create(metaClient, cfg, jsc);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
      BaseFileOnlyView roView =
          new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
      assertFalse(baseFilesToRead.findAny().isPresent());

      roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      baseFilesToRead = roView.getLatestBaseFiles();
      assertTrue(baseFilesToRead.findAny().isPresent(),
          "should list the parquet files we wrote in the delta commit");

      /**
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      /**
       * Write 3 (only deletes, written to .log file)
       */
      final String newDeleteTime = "004";
      final String partitionPath = records.get(0).getPartitionPath();
      final String fileId = statuses.get(0).getFileId();
      client.startCommitWithTime(newDeleteTime);

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);
      JavaRDD<HoodieRecord> deleteRDD = jsc.parallelize(fewRecordsForDelete, 1);

      // initialize partitioner
      DeltaCommitActionExecutor actionExecutor = new DeleteDeltaCommitActionExecutor(jsc, cfg, hoodieTable,
          newDeleteTime, deleteRDD);
      actionExecutor.getUpsertPartitioner(new WorkloadProfile(deleteRDD));
      final List<List<WriteStatus>> deleteStatus = jsc.parallelize(Arrays.asList(1)).map(x -> {
        return actionExecutor.handleUpdate(partitionPath, fileId, fewRecordsForDelete.iterator());
      }).map(x -> (List<WriteStatus>) HoodieClientTestUtils.collectStatuses(x)).collect();

      // Verify there are  errors because records are from multiple partitions (but handleUpdate is invoked for
      // specific partition)
      WriteStatus status = deleteStatus.get(0).get(0);
      assertTrue(status.hasErrors());
      long numRecordsInPartition = fewRecordsForDelete.stream().filter(u ->
              u.getPartitionPath().equals(partitionPath)).count();
      assertEquals(fewRecordsForDelete.size() - numRecordsInPartition, status.getTotalErrorRecords());
    }
  }

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, IndexType.BLOOM);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build());
  }

  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId());
    }
  }
  
  private FileStatus[] insertAndGetFilePaths(List<HoodieRecord> records, HoodieWriteClient client,
                                             HoodieWriteConfig cfg, String commitTime) throws IOException {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.insert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.create(metaClient, cfg, jsc);

    Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(), "Delta commit should be specified value");

    Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().lastInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
    BaseFileOnlyView roView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieBaseFile> baseFilesToRead = roView.getLatestBaseFiles();
    assertTrue(!baseFilesToRead.findAny().isPresent());

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    baseFilesToRead = roView.getLatestBaseFiles();
    assertTrue(baseFilesToRead.findAny().isPresent(),
        "should list the parquet files we wrote in the delta commit");
    return allFiles;
  }

  private FileStatus[] updateAndGetFilePaths(List<HoodieRecord> records, HoodieWriteClient client,
                                             HoodieWriteConfig cfg, String commitTime) throws IOException {
    Map<HoodieKey, HoodieRecord> recordsMap = new HashMap<>();
    for (HoodieRecord rec : records) {
      if (!recordsMap.containsKey(rec.getKey())) {
        recordsMap.put(rec.getKey(), rec);
      }
    }

    List<WriteStatus> statuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(),
        "Latest Delta commit should match specified time");

    Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());
    return HoodieTestUtils.listAllBaseFilesInPath(metaClient.getFs(), cfg.getBasePath());
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, boolean stopAtCompaction)
      throws Exception {
    return getROIncrementalFiles(partitionPath, "000", -1, stopAtCompaction);
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull, boolean stopAtCompaction)
          throws Exception {
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);
    setupIncremental(roJobConf, startCommitTime, numCommitsToPull, stopAtCompaction);
    FileInputFormat.setInputPaths(roJobConf, Paths.get(basePath, partitionPath).toString());
    return roInputFormat.listStatus(roJobConf);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath)
          throws Exception {
    return getRTIncrementalFiles(partitionPath, "000", -1);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
          throws Exception {
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);
    setupIncremental(rtJobConf, startCommitTime, numCommitsToPull, false);
    FileInputFormat.setInputPaths(rtJobConf, Paths.get(basePath, partitionPath).toString());
    return rtInputFormat.listStatus(rtJobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull, boolean stopAtCompaction) {
    String modePropertyName =
            String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtil.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
            String.format(HoodieHiveUtil.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtil.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);

    String stopAtCompactionPropName =
        String.format(HoodieHiveUtil.HOODIE_STOP_AT_COMPACTION_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setBoolean(stopAtCompactionPropName, stopAtCompaction);
  }

  private void validateIncrementalFiles(String partitionPath, int expectedNumFiles,
                                        FileStatus[] files, HoodieParquetInputFormat inputFormat,
                                        JobConf jobConf, int expectedRecords, String... expectedCommits) {

    assertEquals(expectedNumFiles, files.length);
    Set<String> expectedCommitsSet = Arrays.stream(expectedCommits).collect(Collectors.toSet());
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        Collections.singletonList(Paths.get(basePath, partitionPath).toString()), basePath, jobConf, inputFormat);
    assertEquals(expectedRecords, records.size());
    Set<String> actualCommits = records.stream().map(r ->
            r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()).collect(Collectors.toSet());
    assertEquals(expectedCommitsSet, actualCommits);
  }
}
