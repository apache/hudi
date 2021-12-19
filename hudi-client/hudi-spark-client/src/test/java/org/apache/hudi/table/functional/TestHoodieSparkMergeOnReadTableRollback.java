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

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableRollback extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCOWToMORConvertedTableRollback(boolean rollbackUsingMarkers) throws Exception {
    // Set TableType to COW
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = getConfig(false, rollbackUsingMarkers);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      // verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, jsc().parallelize(statuses));

      metaClient = HoodieTableMetaClient.reload(metaClient);
      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertTrue(commit.isPresent());
      assertEquals("001", commit.get().getTimestamp(), "commit should be 001");

      /*
       * Write 2 (updates)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);

      statuses = client.upsert(jsc().parallelize(records, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      // Set TableType to MOR
      metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);

      // rollback a COW commit when TableType is MOR
      client.rollback(newCommitTime);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);

      final String absentCommit = newCommitTime;
      assertAll(tableView.getLatestBaseFiles().map(file -> () -> assertNotEquals(absentCommit, file.getCommitTime())));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRollbackWithDeltaAndCompactionCommit(boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false, rollbackUsingMarkers, HoodieIndex.IndexType.SIMPLE)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build());
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

      // Test delta commit rollback
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (inserts + updates - testing failed delta commit)
       */
      final String commitTime1 = "002";
      // WriteClient with custom config (disable small file handling)
      try (SparkRDDWriteClient secondClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff(false));) {
        secondClient.startCommitWithTime(commitTime1);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime1, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime1, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles,
            basePath());
        assertEquals(200, recordsRead.size());

        statuses = secondClient.upsert(jsc().parallelize(copyOfRecords, 1), commitTime1).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // Test failed delta commit rollback
        secondClient.rollback(commitTime1);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        // After rollback, there should be no base file with the failed commit time
        List<String> remainingFiles = Arrays.stream(allFiles).filter(file -> file.getPath().getName()
            .contains(commitTime1)).map(fileStatus -> fileStatus.getPath().toString()).collect(Collectors.toList());
        assertEquals(0, remainingFiles.size(), "There files should have been rolled-back "
            + "when rolling back commit " + commitTime1 + " but are still remaining. Files: " + remainingFiles);
        dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles, basePath());
        assertEquals(200, recordsRead.size());
      }

      /*
       * Write 3 (inserts + updates - testing successful delta commit)
       */
      final String commitTime2 = "002";
      try (SparkRDDWriteClient thirdClient = getHoodieWriteClient(cfg);) {
        thirdClient.startCommitWithTime(commitTime2);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles,
            basePath());
        assertEquals(200, recordsRead.size());

        writeRecords = jsc().parallelize(copyOfRecords, 1);
        writeStatusJavaRDD = thirdClient.upsert(writeRecords, commitTime2);
        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // Test successful delta commit rollback
        thirdClient.rollback(commitTime2);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        // After rollback, there should be no base file with the failed commit time
        assertEquals(0, Arrays.stream(allFiles)
            .filter(file -> file.getPath().getName().contains(commitTime2)).count());

        metaClient = HoodieTableMetaClient.reload(metaClient);
        hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles, basePath());
        // check that the number of records read is still correct after rollback operation
        assertEquals(200, recordsRead.size());

        // Test compaction commit rollback
        /*
         * Write 4 (updates)
         */
        newCommitTime = "003";
        thirdClient.startCommitWithTime(newCommitTime);

        writeStatusJavaRDD = thirdClient.upsert(writeRecords, newCommitTime);
        statuses = writeStatusJavaRDD.collect();
        thirdClient.commit(newCommitTime, writeStatusJavaRDD);
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);

        String compactionInstantTime = thirdClient.scheduleCompaction(Option.empty()).get().toString();
        thirdClient.compact(compactionInstantTime);

        metaClient = HoodieTableMetaClient.reload(metaClient);

        final String compactedCommitTime = metaClient.getActiveTimeline().reload().lastInstant().get().getTimestamp();
        assertTrue(Arrays.stream(listAllBaseFilesInPath(hoodieTable))
            .anyMatch(file -> compactedCommitTime.equals(new HoodieBaseFile(file).getCommitTime())));
        hoodieTable.rollbackInflightCompaction(new HoodieInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactedCommitTime));
        allFiles = listAllBaseFilesInPath(hoodieTable);
        metaClient = HoodieTableMetaClient.reload(metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        assertFalse(tableView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));
        assertAll(tableView.getLatestBaseFiles().map(file -> () -> assertNotEquals(compactedCommitTime, file.getCommitTime())));
      }
    }
  }

  @Test
  void testMultiRollbackWithDeltaAndCompactionCommit() throws Exception {
    boolean populateMetaFields = true;
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false)
        // Timeline-server-based markers are not used for multi-rollback tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build());
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (final SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);
      client.close();

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "Should list the base files we wrote in the delta commit");
      /*
       * Write 2 (inserts + updates)
       */
      newCommitTime = "002";
      // WriteClient with custom config (disable small file handling)
      HoodieWriteConfig smallFileWriteConfig = getHoodieWriteConfigWithSmallFileHandlingOffBuilder(populateMetaFields)
          // Timeline-server-based markers are not used for multi-rollback tests
          .withMarkersType(MarkerType.DIRECT.name())
          .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
      try (SparkRDDWriteClient nClient = getHoodieWriteClient(smallFileWriteConfig)) {
        nClient.startCommitWithTime(newCommitTime);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles,
            basePath());
        assertEquals(200, recordsRead.size());

        statuses = nClient.upsert(jsc().parallelize(copyOfRecords, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);
        nClient.commit(newCommitTime, writeStatusJavaRDD);
        copyOfRecords.clear();
      }

      // Schedule a compaction
      /*
       * Write 3 (inserts + updates)
       */
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newInserts = dataGen.generateInserts(newCommitTime, 100);
      records = dataGen.generateUpdates(newCommitTime, records);
      records.addAll(newInserts);
      writeRecords = jsc().parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      String compactionInstantTime = "004";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());

      // Compaction commit
      /*
       * Write 4 (updates)
       */
      newCommitTime = "005";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc().parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      compactionInstantTime = "006";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
      JavaRDD<WriteStatus> ws = (JavaRDD<WriteStatus>) client.compact(compactionInstantTime);
      client.commitCompaction(compactionInstantTime, ws, Option.empty());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

      final String compactedCommitTime =
          metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get().getTimestamp();

      assertTrue(tableView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));

      /*
       * Write 5 (updates)
       */
      newCommitTime = "007";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
      copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
      copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

      statuses = client.upsert(jsc().parallelize(copyOfRecords, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, writeStatusJavaRDD);
      copyOfRecords.clear();

      // Rollback latest commit first
      client.restoreToInstant("000");

      metaClient = HoodieTableMetaClient.reload(metaClient);
      allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());
      TableFileSystemView.SliceView rtView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      List<HoodieFileGroup> fileGroups =
          ((HoodieTableFileSystemView) rtView).getAllFileGroups().collect(Collectors.toList());
      assertTrue(fileGroups.isEmpty());

      // make sure there are no log files remaining
      assertEquals(0L, ((HoodieTableFileSystemView) rtView).getAllFileGroups()
          .filter(fileGroup -> fileGroup.getAllRawFileSlices().noneMatch(f -> f.getLogFiles().count() == 0))
          .count());

    }
  }

  private HoodieWriteConfig getHoodieWriteConfigWithSmallFileHandlingOff(boolean populateMetaFields) {
    return getHoodieWriteConfigWithSmallFileHandlingOffBuilder(populateMetaFields).build();
  }

  private HoodieWriteConfig.Builder getHoodieWriteConfigWithSmallFileHandlingOffBuilder(boolean populateMetaFields) {
    HoodieWriteConfig.Builder cfgBuilder = HoodieWriteConfig.newBuilder().withPath(basePath()).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024).parquetMaxFileSize(1024).build()).forTable("test-trip-table");

    if (!populateMetaFields) {
      addConfigsForPopulateMetaFields(cfgBuilder, false);
    }
    return cfgBuilder;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInsertsGeneratedIntoLogFilesRollback(boolean rollbackUsingMarkers) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, HoodieIndex.IndexType.INMEMORY).build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
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

      // insert 100 records
      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 100);
      recordsRDD = jsc().parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Sleep for small interval (at least 1 second) to force a new rollback start time.
      Thread.sleep(1000);

      // We will test HUDI-204 here. We will simulate rollback happening twice by copying the commit file to local fs
      // and calling rollback twice
      final String lastCommitTime = newCommitTime;

      // Save the .commit file to local directory.
      // Rollback will be called twice to test the case where rollback failed first time and retried.
      // We got the "BaseCommitTime cannot be null" exception before the fix
      java.nio.file.Path tempFolder = Files.createTempDirectory(this.getClass().getCanonicalName());
      Map<String, String> fileNameMap = new HashMap<>();
      for (HoodieInstant.State state : Arrays.asList(HoodieInstant.State.REQUESTED, HoodieInstant.State.INFLIGHT)) {
        HoodieInstant toCopy = new HoodieInstant(state, HoodieTimeline.DELTA_COMMIT_ACTION, lastCommitTime);
        File file = Files.createTempFile(tempFolder, null, null).toFile();
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMetaPath(), toCopy.getFileName()),
            new Path(file.getAbsolutePath()));
        fileNameMap.put(file.getAbsolutePath(), toCopy.getFileName());
      }
      Path markerDir = new Path(Files.createTempDirectory(tempFolder, null).toAbsolutePath().toString());
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMarkerFolderPath(lastCommitTime)),
            markerDir);
      }

      writeClient.rollback(newCommitTime);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context());
      TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }
      assertEquals(0, numLogFiles);
      for (Map.Entry<String, String> entry : fileNameMap.entrySet()) {
        try {
          metaClient.getFs().copyFromLocalFile(new Path(entry.getKey()),
              new Path(metaClient.getMetaPath(), entry.getValue()));
        } catch (IOException e) {
          throw new HoodieIOException("Error copying state from local disk.", e);
        }
      }
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyFromLocalFile(new Path(markerDir, lastCommitTime),
            new Path(metaClient.getMarkerFolderPath(lastCommitTime)));
      }
      Thread.sleep(1000);
      // Rollback again to pretend the first rollback failed partially. This should not error out
      writeClient.rollback(newCommitTime);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(boolean rollbackUsingMarkers) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, HoodieIndex.IndexType.INMEMORY).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      table.getHoodieView().sync();
      TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

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
      statuses = (JavaRDD<WriteStatus>) writeClient.compact(newCommitTime);
      // Ensure all log files have been compacted into base files
      String extension = table.getBaseFileExtension();
      assertEquals(numLogFiles, statuses.map(status -> status.getStat().getPath().contains(extension)).count());
      assertEquals(numLogFiles, statuses.count());
      //writeClient.commitCompaction(newCommitTime, statuses, Option.empty());
      // Trigger a rollback of compaction
      table.getActiveTimeline().reload();
      table.rollbackInflightCompaction(new HoodieInstant(
          HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, newCommitTime));

      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(config, context(), metaClient);
      tableRTFileSystemView = table.getSliceView();
      ((SyncableFileSystemView) tableRTFileSystemView).reset();

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> fileSlices = getFileSystemViewWithUnCommittedSlices(metaClient)
            .getAllFileSlices(partitionPath).filter(fs -> fs.getBaseInstantTime().equals("100")).collect(Collectors.toList());
        assertTrue(fileSlices.stream().noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(fileSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLazyRollbackOfFailedCommit(boolean rollbackUsingMarkers) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig cfg = getWriteConfig(true, rollbackUsingMarkers);
    HoodieWriteConfig autoCommitFalseCfg = getWriteConfig(false, rollbackUsingMarkers);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // commit 1
    List<HoodieRecord> records = insertRecords(client, dataGen, "001");
    // commit 2 to create log files
    List<HoodieRecord> updates1 = updateRecords(client, dataGen, "002", records, metaClient, cfg, true);

    // trigger a inflight commit 3 which will be later be rolled back explicitly.
    SparkRDDWriteClient autoCommitFalseClient = getHoodieWriteClient(autoCommitFalseCfg);
    List<HoodieRecord> updates2 = updateRecords(autoCommitFalseClient, dataGen, "003", records, metaClient, autoCommitFalseCfg, false);

    // commit 4 successful (mimic multi-writer scenario)
    List<HoodieRecord> updates3 = updateRecords(client, dataGen, "004", records, metaClient, cfg, false);

    // trigger compaction
    long numLogFiles = getNumLogFilesInLatestFileSlice(metaClient, cfg, dataGen);
    doCompaction(autoCommitFalseClient, metaClient, cfg, numLogFiles);
    long numLogFilesAfterCompaction = getNumLogFilesInLatestFileSlice(metaClient, cfg, dataGen);
    assertNotEquals(numLogFiles, numLogFilesAfterCompaction);

    // rollback 3rd commit.
    client.rollback("003");
    long numLogFilesAfterRollback = getNumLogFilesInLatestFileSlice(metaClient, cfg, dataGen);
    // lazy rollback should have added the rollback block to previous file slice and not the latest. And so the latest slice's log file count should
    // remain the same.
    assertEquals(numLogFilesAfterRollback, numLogFilesAfterCompaction);
  }

  private List<HoodieRecord> insertRecords(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String commitTime) {
    /*
     * Write 1 (only inserts, written as base file)
     */
    client.startCommitWithTime(commitTime);

    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);
    return records;
  }

  private List<HoodieRecord> updateRecords(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String commitTime,
                                           List<HoodieRecord> records, HoodieTableMetaClient metaClient, HoodieWriteConfig cfg,
                                           boolean assertLogFiles) throws IOException {
    client.startCommitWithTime(commitTime);

    records = dataGen.generateUpdates(commitTime, records);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);

    if (assertLogFiles) {
      HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);
      table.getHoodieView().sync();
      TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> allSlices = tableRTFileSystemView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
        assertEquals(1, allSlices.stream().filter(fileSlice -> fileSlice.getBaseFile().isPresent()).count());
        assertTrue(allSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += allSlices.stream().filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }
      assertTrue(numLogFiles > 0);
    }
    return records;
  }

  private long doCompaction(SparkRDDWriteClient client, HoodieTableMetaClient metaClient, HoodieWriteConfig cfg, long numLogFiles) throws IOException {
    // Do a compaction
    String instantTime = client.scheduleCompaction(Option.empty()).get().toString();
    JavaRDD<WriteStatus> writeStatuses = (JavaRDD<WriteStatus>) client.compact(instantTime);

    metaClient.reloadActiveTimeline();
    HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    String extension = table.getBaseFileExtension();
    assertEquals(numLogFiles, writeStatuses.map(status -> status.getStat().getPath().contains(extension)).count());
    assertEquals(numLogFiles, writeStatuses.count());
    client.commitCompaction(instantTime, writeStatuses, Option.empty());
    return numLogFiles;
  }

  private long getNumLogFilesInLatestFileSlice(HoodieTableMetaClient metaClient, HoodieWriteConfig cfg, HoodieTestDataGenerator dataGen) {
    metaClient.reloadActiveTimeline();
    HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    table.getHoodieView().sync();
    TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

    long numLogFiles = 0;
    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> allSlices = tableRTFileSystemView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
      numLogFiles += allSlices.stream().filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
    }
    return numLogFiles;
  }

  private HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean rollbackUsingMarkers) {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(autoCommit).withRollbackUsingMarkers(rollbackUsingMarkers)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024L)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(3)
            .withAutoClean(false)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build());
    return cfgBuilder.build();
  }

  private SyncableFileSystemView getFileSystemViewWithUnCommittedSlices(HoodieTableMetaClient metaClient) {
    try {
      return new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline(),
          HoodieTestTable.of(metaClient).listAllBaseAndLogFiles()
      );
    } catch (IOException ioe) {
      throw new HoodieIOException("Error getting file system view", ioe);
    }
  }

}
