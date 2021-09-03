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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.action.deltacommit.AbstractSparkDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadTable extends HoodieClientTestHarness {

  @TempDir
  public java.nio.file.Path tempFolder;

  public void init(HoodieFileFormat baseFileFormat, boolean populateMetaFields) throws IOException {
    initDFS();
    initSparkContexts("TestHoodieMergeOnReadTable");
    hadoopConf.addResource(dfs.getConf());
    jsc.hadoopConfiguration().addResource(dfs.getConf());
    context = new HoodieSparkEngineContext(jsc);
    initPath();
    dfs.mkdirs(new Path(basePath));

    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.toString());

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, properties);
    initTestDataGenerator();
  }

  @BeforeEach
  public void init() throws IOException {
    init(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue(), true);
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  private static Stream<Arguments> populateMetaFieldsParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

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

  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testUpsertPartitioner(boolean populateMetaFields) throws Exception {
    clean();
    init(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue(), populateMetaFields);
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      BaseFileOnlyView roView = getHoodieTableFileSystemView(metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
      Map<String, Long> fileIdToSize =
          dataFilesToRead.collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      roView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> dataFilesList = dataFilesToRead.collect(Collectors.toList());
      assertTrue(dataFilesList.size() > 0,
          "Should list the base files we wrote in the delta commit");

      /**
       * Write 2 (only updates + inserts, written to .log file + correction of existing base file size)
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

      allFiles = listAllBaseFilesInPath(hoodieTable);
      roView = getHoodieTableFileSystemView(metaClient,
          hoodieTable.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> newDataFilesList = dataFilesToRead.collect(Collectors.toList());
      Map<String, Long> fileIdToNewSize =
          newDataFilesList.stream().collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      assertTrue(fileIdToNewSize.entrySet().stream().anyMatch(entry -> fileIdToSize.get(entry.getKey()) < entry.getValue()));

      List<String> dataFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles,
          basePath, new JobConf(hadoopConf), true, false);
      // Wrote 20 records in 2 batches
      assertEquals(40, recordsRead.size(), "Must contain 40 records");
    }
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testLogFileCountsAfterCompaction(boolean populateMetaFields) throws Exception {
    // insert 100 records
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig config = cfgBuilder.build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      metaClient = getHoodieMetaClient(hadoopConf, basePath);

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);

      HoodieReadClient readClient = new HoodieReadClient(context, config);
      updatedRecords = readClient.tagLocation(updatedRecordsRDD).collect();

      // Write them to corresponding avro logfiles
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
      HoodieSparkWriteableTestTable.of(table, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS)
          .withLogAppends(updatedRecords);
      // In writeRecordsToLogFiles, no commit files are getting added, so resetting file-system view state
      ((SyncableFileSystemView) (table.getSliceView())).reset();

      // Verify that all data file has one log file
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(), "There should be 1 log file written for every data file");
        }
      }

      // Mark 2nd delta-instant as completed
      metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(State.INFLIGHT,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime));
      metaClient.getActiveTimeline().saveAsComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // Do a compaction
      String compactionInstantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      JavaRDD<WriteStatus> result = (JavaRDD<WriteStatus>) writeClient.compact(compactionInstantTime);

      // Verify that recently written compacted data file has no log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(config, context, metaClient);
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

  private void testInsertsGeneratedIntoLogFilesRollback(Boolean rollbackUsingMarkers) throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY).build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
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

      // insert 100 records
      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 100);
      recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Sleep for small interval (at least 1 second) to force a new rollback start time.
      Thread.sleep(1000);

      // We will test HUDI-204 here. We will simulate rollback happening twice by copying the commit file to local fs
      // and calling rollback twice
      final String lastCommitTime = newCommitTime;
      metaClient = getHoodieMetaClient(hadoopConf, basePath);

      // Save the .commit file to local directory.
      // Rollback will be called twice to test the case where rollback failed first time and retried.
      // We got the "BaseCommitTime cannot be null" exception before the fix
      Map<String, String> fileNameMap = new HashMap<>();
      for (State state : Arrays.asList(State.REQUESTED, State.INFLIGHT)) {
        HoodieInstant toCopy = new HoodieInstant(state, HoodieTimeline.DELTA_COMMIT_ACTION, lastCommitTime);
        File file = Files.createTempFile(tempFolder, null, null).toFile();
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMetaPath(), toCopy.getFileName()),
                new Path(file.getAbsolutePath()));
        fileNameMap.put(file.getAbsolutePath(), toCopy.getFileName());
      }
      Path markerDir = new Path(Files.createTempDirectory(tempFolder,null).toAbsolutePath().toString());
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMarkerFolderPath(lastCommitTime)),
            markerDir);
      }

      writeClient.rollback(newCommitTime);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context);
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }
      assertEquals(0, numLogFiles);
      fileNameMap.forEach((key, value) -> {
        try {
          metaClient.getFs().copyFromLocalFile(new Path(key),
                  new Path(metaClient.getMetaPath(), value));
        } catch (IOException e) {
          throw new HoodieIOException("Error copying state from local disk.", e);
        }
      });
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyFromLocalFile(markerDir,
            new Path(metaClient.getMarkerFolderPath(lastCommitTime)));
      }
      Thread.sleep(1000);
      // Rollback again to pretend the first rollback failed partially. This should not error out
      writeClient.rollback(newCommitTime);
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackUsingFileList() throws Exception {
    testInsertsGeneratedIntoLogFilesRollback(false);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackUsingMarkers() throws Exception {
    testInsertsGeneratedIntoLogFilesRollback(true);
  }

  private void testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(Boolean rollbackUsingMarkers) throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      HoodieTable table = HoodieSparkTable.create(config, context, getHoodieMetaClient(hadoopConf, basePath));
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
      statuses = (JavaRDD<WriteStatus>) writeClient.compact(newCommitTime);
      // Ensure all log files have been compacted into base files
      String extension = table.getBaseFileExtension();
      assertEquals(numLogFiles, statuses.map(status -> status.getStat().getPath().contains(extension)).count());
      assertEquals(numLogFiles, statuses.count());
      //writeClient.commitCompaction(newCommitTime, statuses, Option.empty());
      // Trigger a rollback of compaction
      table.getActiveTimeline().reload();
      writeClient.rollbackInflightCompaction(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, newCommitTime), table);

      table = HoodieSparkTable.create(config, context, getHoodieMetaClient(hadoopConf, basePath));
      tableRTFileSystemView = table.getSliceView();
      ((SyncableFileSystemView) tableRTFileSystemView).reset();

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> fileSlices =  getFileSystemViewWithUnCommittedSlices(getHoodieMetaClient(hadoopConf, basePath))
                .getAllFileSlices(partitionPath).filter(fs -> fs.getBaseInstantTime().equals("100")).collect(Collectors.toList());
        assertTrue(fileSlices.stream().noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(fileSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
      }
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompactionUsingFileList() throws Exception {
    testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(false);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompactionUsingMarkers() throws Exception {
    testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(true);
  }

  /**
   * Test to ensure metadata stats are correctly written to metadata file.
   */
  public void testMetadataStatsOnCommit(Boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY)
        .withAutoCommit(false).build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      metaClient = getHoodieMetaClient(hadoopConf, basePath);
      HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);

      // Create a commit without metadata stats in metadata to test backwards compatibility
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
      table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline().getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
        }
      }
      assertEquals(200, inserts);

      instantTime = "002";
      client.startCommitWithTime(instantTime);
      records = dataGen.generateUpdates(instantTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      //assertTrue(client.commit(instantTime, statuses), "Commit should succeed");
      inserts = 0;
      int upserts = 0;
      List<WriteStatus> writeStatusList = statuses.collect();
      for (WriteStatus ws: writeStatusList) {
        inserts += ws.getStat().getNumInserts();
        upserts += ws.getStat().getNumUpdateWrites();
      }

      // Read from commit file
      assertEquals(0, inserts);
      assertEquals(200, upserts);

      client.rollback(instantTime);

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }
      assertEquals(200, inserts);
      assertEquals(0, upserts);
    }
  }

  /**
   * Test to ensure rolling stats are correctly written to metadata file.
   */
  @Test
  public void testMetadataStatsOnCommitUsingFileList() throws Exception {
    testMetadataStatsOnCommit(false);
  }

  @Test
  public void testMetadataStatsOnCommitUsingMarkers() throws Exception {
    testMetadataStatsOnCommit(true);
  }

  /**
   * Test to ensure rolling stats are correctly written to the metadata file, identifies small files and corrects them.
   */
  @Test
  public void testRollingStatsWithSmallFileHandling() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      Map<String, Long> fileIdToInsertsMap = new HashMap<>();
      Map<String, Long> fileIdToUpsertsMap = new HashMap<>();

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      HoodieTable table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
          fileIdToInsertsMap.put(stat.getFileId(), stat.getNumInserts());
          fileIdToUpsertsMap.put(stat.getFileId(), stat.getNumUpdateWrites());
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
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      int upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          assertTrue(fileIdToInsertsMap.containsKey(stat.getFileId()));
          assertTrue(fileIdToUpsertsMap.containsKey(stat.getFileId()));
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }

      assertEquals(200, inserts);
      assertEquals(200, upserts);

      // Test small file handling after compaction
      instantTime = "002";
      client.scheduleCompactionAtInstant(instantTime, Option.of(metadata.getExtraMetadata()));
      statuses = (JavaRDD<WriteStatus>) client.compact(instantTime);
      client.commitCompaction(instantTime, statuses, Option.empty());

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata1 = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getCommitsTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);

      // Ensure that the metadata stats from the extra metadata of delta commits is copied over to the compaction commit
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        assertTrue(metadata1.getPartitionToWriteStats().containsKey(pstat.getKey()));
        assertEquals(metadata1.getPartitionToWriteStats().get(pstat.getKey()).size(),
            pstat.getValue().size());
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
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          assertTrue(fileIdToInsertsMap.containsKey(stat.getFileId()));
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }

      assertEquals(200, inserts);
      assertEquals(400, upserts);
    }
  }

  /**
   * Test to validate invoking table.handleUpdate() with input records from multiple partitions will fail.
   */
  @Test
  public void testHandleUpdateWithMultiplePartitions() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieSparkMergeOnReadTable hoodieTable = (HoodieSparkMergeOnReadTable) HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      BaseFileOnlyView roView =
          getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      roView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /**
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      metaClient.reloadActiveTimeline();
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
      metaClient.reloadActiveTimeline();

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);
      JavaRDD<HoodieRecord> deleteRDD = jsc.parallelize(fewRecordsForDelete, 1);

      // initialize partitioner
      AbstractSparkDeltaCommitActionExecutor actionExecutor = new SparkDeleteDeltaCommitActionExecutor(context, cfg, hoodieTable,
          newDeleteTime, deleteRDD);
      actionExecutor.getUpsertPartitioner(new WorkloadProfile(buildProfile(deleteRDD)));
      final List<List<WriteStatus>> deleteStatus = jsc.parallelize(Arrays.asList(1)).map(x -> {
        return actionExecutor.handleUpdate(partitionPath, fileId, fewRecordsForDelete.iterator());
      }).map(Transformations::flatten).collect();

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

  private HoodieWriteConfig getConfig(Boolean autoCommit, Boolean rollbackUsingMarkers) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, IndexType.BLOOM).build();
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, IndexType.BLOOM);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, false, indexType);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return getConfigBuilder(autoCommit, false, IndexType.BLOOM, compactionSmallFileSize, clusteringConfig);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, indexType, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType,
                                                       long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(compactionSmallFileSize)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(rollbackUsingMarkers);
  }

  private FileStatus[] listAllBaseFilesInPath(HoodieTable table) throws IOException {
    return HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
  }
}

