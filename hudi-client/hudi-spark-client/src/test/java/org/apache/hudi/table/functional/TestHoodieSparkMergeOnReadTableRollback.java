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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  private static Stream<Arguments> testRollbackWithDeltaAndCompactionCommit() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void testRollbackWithDeltaAndCompactionCommit(boolean rollbackUsingMarkers, boolean populateMetaFields) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false, rollbackUsingMarkers, HoodieIndex.IndexType.SIMPLE);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
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
        thirdClient.rollbackInflightCompaction(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactedCommitTime),
            hoodieTable);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        metaClient = HoodieTableMetaClient.reload(metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        assertFalse(tableView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));
        assertAll(tableView.getLatestBaseFiles().map(file -> () -> assertNotEquals(compactedCommitTime, file.getCommitTime())));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiRollbackWithDeltaAndCompactionCommit(boolean populateMetaFields) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false);
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
      try (SparkRDDWriteClient nClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff(populateMetaFields))) {
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
    HoodieWriteConfig.Builder cfgBuilder = HoodieWriteConfig.newBuilder().withPath(basePath()).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024).parquetMaxFileSize(1024).build()).forTable("test-trip-table");

    if (!populateMetaFields) {
      addConfigsForPopulateMetaFields(cfgBuilder, false);
    }
    return cfgBuilder.build();
  }

}
