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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
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
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
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
import java.util.Collection;
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
    // NOTE: First writer will have Metadata table DISABLED
    HoodieWriteConfig.Builder cfgBuilder =
        getConfigBuilder(false, rollbackUsingMarkers, HoodieIndex.IndexType.SIMPLE);

    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = CollectionUtils.copy(cfg.getProps());
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

      // Test delta commit rollback
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "000000001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);

      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      client.commit(newCommitTime, jsc().parallelize(statuses));

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("000000001", deltaCommit.get().getTimestamp(), "Delta commit should be 000000001");

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
      final String commitTime1 = "000000002";
      // WriteClient with custom config (disable small file handling)
      // NOTE: Second writer will have Metadata table ENABLED
      try (SparkRDDWriteClient secondClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff(true));) {
        secondClient.startCommitWithTime(commitTime1);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime1, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime1, 200));

        List<String> inputPaths = tableView.getLatestBaseFiles()
            .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
            .collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths,
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
            .contains("_" + commitTime1)).map(fileStatus -> fileStatus.getPath().toString()).collect(Collectors.toList());
        assertEquals(0, remainingFiles.size(), "These files should have been rolled-back "
            + "when rolling back commit " + commitTime1 + " but are still remaining. Files: " + remainingFiles);
        inputPaths = tableView.getLatestBaseFiles()
            .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
            .collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths, basePath());
        assertEquals(200, recordsRead.size());
      }

      /*
       * Write 3 (inserts + updates - testing successful delta commit)
       */
      final String commitTime2 = "000000003";
      try (SparkRDDWriteClient thirdClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff(true));) {
        thirdClient.startCommitWithTime(commitTime2);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

        List<String> inputPaths = tableView.getLatestBaseFiles()
            .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
            .collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths,
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
        List<String> remainingFiles = Arrays.stream(allFiles).filter(file -> file.getPath().getName()
            .contains("_" + commitTime2)).map(fileStatus -> fileStatus.getPath().toString()).collect(Collectors.toList());
        assertEquals(0, remainingFiles.size(), "These files should have been rolled-back "
            + "when rolling back commit " + commitTime2 + " but are still remaining. Files: " + remainingFiles);

        metaClient = HoodieTableMetaClient.reload(metaClient);
        hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        inputPaths = tableView.getLatestBaseFiles()
            .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
            .collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths, basePath());
        // check that the number of records read is still correct after rollback operation
        assertEquals(200, recordsRead.size());

        // Test compaction commit rollback
        /*
         * Write 4 (updates)
         */
        newCommitTime = "000000004";
        thirdClient.startCommitWithTime(newCommitTime);

        writeStatusJavaRDD = thirdClient.upsert(writeRecords, newCommitTime);

        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        thirdClient.commit(newCommitTime, jsc().parallelize(statuses));

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
        .withMarkersType(MarkerType.DIRECT.name());
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = getPropertiesForKeyGen(populateMetaFields);
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

      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      client.commit(newCommitTime, jsc().parallelize(statuses));
      client.close();

      Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantCommitMetadataPairOpt =
          metaClient.getActiveTimeline().getLastCommitMetadataWithValidData();

      assertTrue(instantCommitMetadataPairOpt.isPresent());

      HoodieInstant commitInstant = instantCommitMetadataPairOpt.get().getKey();

      assertEquals("001", commitInstant.getTimestamp());
      assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, commitInstant.getAction());
      assertEquals(200, getTotalRecordsWritten(instantCommitMetadataPairOpt.get().getValue()));

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

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
          .withMarkersType(MarkerType.DIRECT.name()).build();
      try (SparkRDDWriteClient nClient = getHoodieWriteClient(smallFileWriteConfig)) {
        nClient.startCommitWithTime(newCommitTime);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles()
            .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
            .collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles,
            basePath());
        assertEquals(200, recordsRead.size());

        statuses = nClient.upsert(jsc().parallelize(copyOfRecords, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        nClient.commit(newCommitTime, jsc().parallelize(statuses));

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
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      client.commit(newCommitTime, jsc().parallelize(statuses));

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
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      client.commit(newCommitTime, jsc().parallelize(statuses));

      metaClient = HoodieTableMetaClient.reload(metaClient);

      compactionInstantTime = "006";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(compactionInstantTime);
      client.commitCompaction(compactionInstantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());

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

      client.commit(newCommitTime, jsc().parallelize(statuses));

      copyOfRecords.clear();

      // Rollback latest commit first
      client.restoreToInstant("000", cfg.isMetadataTableEnabled());

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

  @Test
  void testRestoreWithCleanedUpCommits() throws Exception {
    boolean populateMetaFields = true;
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false)
        // Timeline-server-based markers are not used for multi-rollback tests
        .withMarkersType(MarkerType.DIRECT.name());
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
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, jsc().parallelize(statuses));

      upsertRecords(client, "002", records, dataGen);

      client.savepoint("002","user1","comment1");

      upsertRecords(client, "003", records, dataGen);
      upsertRecords(client, "004", records, dataGen);

      // Compaction commit
      String compactionInstantTime = "006";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(compactionInstantTime);
      client.commitCompaction(compactionInstantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());

      upsertRecords(client, "007", records, dataGen);
      upsertRecords(client, "008", records, dataGen);

      // Compaction commit
      String compactionInstantTime1 = "009";
      client.scheduleCompactionAtInstant(compactionInstantTime1, Option.empty());
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata1 = client.compact(compactionInstantTime1);
      client.commitCompaction(compactionInstantTime1, compactionMetadata1.getCommitMetadata().get(), Option.empty());

      upsertRecords(client, "010", records, dataGen);

      // trigger clean. creating a new client with aggresive cleaner configs so that clean will kick in immediately.
      cfgBuilder = getConfigBuilder(false)
          .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
          // Timeline-server-based markers are not used for multi-rollback tests
          .withMarkersType(MarkerType.DIRECT.name());
      addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
      HoodieWriteConfig cfg1 = cfgBuilder.build();
      final SparkRDDWriteClient client1 = getHoodieWriteClient(cfg1);
      client1.clean();
      client1.close();

      metaClient = HoodieTableMetaClient.reload(metaClient);
      upsertRecords(client, "011", records, dataGen);

      // Rollback to 002
      client.restoreToInstant("002", cfg.isMetadataTableEnabled());

      // verify that no files are present after 002. every data file should have been cleaned up
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.anyMatch(file -> HoodieTimeline.compareTimestamps("002", HoodieTimeline.GREATER_THAN, file.getCommitTime())));

      client.deleteSavepoint("002");
      assertFalse(metaClient.reloadActiveTimeline().getSavePointTimeline().containsInstant("002"));
    }
  }

  private void upsertRecords(SparkRDDWriteClient client, String commitTime, List<HoodieRecord> records, HoodieTestDataGenerator dataGen) throws IOException {
    client.startCommitWithTime(commitTime);
    List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(commitTime, copyOfRecords);
    List<WriteStatus>  statuses = client.upsert(jsc().parallelize(copyOfRecords, 1), commitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    client.commit(commitTime, jsc().parallelize(statuses));
  }

  private long getTotalRecordsWritten(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream)
        .map(stat -> stat.getNumWrites() + stat.getNumUpdateWrites())
        .reduce(0L, Long::sum);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMORTableRestore(boolean restoreAfterCompaction) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(false)
        // Timeline-server-based markers are not used for multi-rollback tests
        .withMarkersType(MarkerType.DIRECT.name());
    HoodieWriteConfig cfg = cfgBuilder.build();

    Properties properties = getPropertiesForKeyGen(true);
    properties.putAll(cfg.getProps());
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());

    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (final SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      List<HoodieRecord> records = insertAndGetRecords("001", client, dataGen, 200);
      List<HoodieRecord> updates1 = updateAndGetRecords("002", client, dataGen, records);
      List<HoodieRecord> updates2 = updateAndGetRecords("003", client, dataGen, records);
      List<HoodieRecord> updates3 = updateAndGetRecords("004", client, dataGen, records);
      validateRecords(cfg, metaClient, updates3);

      if (!restoreAfterCompaction) {
        // restore to 002 and validate records.
        client.restoreToInstant("002", cfg.isMetadataTableEnabled());
        validateRecords(cfg, metaClient, updates1);
      } else {
        // trigger compaction and then trigger couple of upserts followed by restore.
        metaClient = HoodieTableMetaClient.reload(metaClient);
        String compactionInstantTime = "005";
        client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
        HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(compactionInstantTime);
        client.commitCompaction(compactionInstantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());

        validateRecords(cfg, metaClient, updates3);
        List<HoodieRecord> updates4 = updateAndGetRecords("006", client, dataGen, records);
        List<HoodieRecord> updates5 = updateAndGetRecords("007", client, dataGen, records);
        validateRecords(cfg, metaClient, updates5);

        // restore to 003 and validate records.
        client.restoreToInstant("003", cfg.isMetadataTableEnabled());
        validateRecords(cfg, metaClient, updates2);
      }
    }
  }

  private List<HoodieRecord> insertAndGetRecords(String newCommitTime, SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, int count) {
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, count);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    return records;
  }

  private List<HoodieRecord> updateAndGetRecords(String newCommitTime, SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, List<HoodieRecord> records) throws IOException {
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> updates = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(jsc().parallelize(updates, 1), newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    return updates;
  }

  private void validateRecords(HoodieWriteConfig cfg, HoodieTableMetaClient metaClient, List<HoodieRecord> expectedRecords) throws IOException {

    HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
    FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
    HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    List<String> inputPaths = tableView.getLatestBaseFiles()
        .map(hf -> new Path(hf.getPath()).getParent().toString())
        .collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths,
        basePath());
    assertRecords(expectedRecords, recordsRead);
  }

  private void assertRecords(List<HoodieRecord> inputRecords, List<GenericRecord> recordsRead) {
    assertEquals(recordsRead.size(), inputRecords.size());
    Map<String, GenericRecord> expectedRecords = new HashMap<>();
    inputRecords.forEach(entry -> {
      try {
        expectedRecords.put(entry.getRecordKey(), (GenericRecord) ((HoodieRecordPayload) entry.getData()).getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA).get());
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    Map<String, GenericRecord> actualRecords = new HashMap<>();
    recordsRead.forEach(entry -> actualRecords.put(String.valueOf(entry.get("_row_key")), entry));
    for (Map.Entry<String, GenericRecord> entry : expectedRecords.entrySet()) {
      assertEquals(String.valueOf(entry.getValue().get("driver")), String.valueOf(actualRecords.get(entry.getKey()).get("driver")));
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
      // trigger an action
      List<WriteStatus> writeStatuses = ((JavaRDD<WriteStatus>) writeClient.insert(recordsRDD, newCommitTime)).collect();

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
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = writeClient.compact(newCommitTime);
      statuses = compactionMetadata.getWriteStatuses();
      // Ensure all log files have been compacted into base files
      String extension = table.getBaseFileExtension();
      Collection<List<HoodieWriteStat>> stats = compactionMetadata.getCommitMetadata().get().getPartitionToWriteStats().values();
      assertEquals(numLogFiles, stats.stream().flatMap(Collection::stream).filter(state -> state.getPath().contains(extension)).count());
      assertEquals(numLogFiles, stats.stream().mapToLong(Collection::size).sum());

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
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(instantTime);

    metaClient.reloadActiveTimeline();
    HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    String extension = table.getBaseFileExtension();
    Collection<List<HoodieWriteStat>> stats = compactionMetadata.getCommitMetadata().get().getPartitionToWriteStats().values();
    assertEquals(numLogFiles, stats.stream().flatMap(Collection::stream).filter(state -> state.getPath().contains(extension)).count());
    assertEquals(numLogFiles, stats.stream().mapToLong(Collection::size).sum());
    client.commitCompaction(instantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());
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
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024L)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(3)
            .build());
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
