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
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableInsertUpdateDelete extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> testSimpleInsertAndUpdate() {
    return Stream.of(
        Arguments.of(HoodieFileFormat.PARQUET, true),
        Arguments.of(HoodieFileFormat.PARQUET, false),
        Arguments.of(HoodieFileFormat.HFILE, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSimpleInsertAndUpdate(HoodieFileFormat fileFormat, boolean populateMetaFields) throws Exception {
    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records, client, cfg, newCommitTime);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (updates)
       */
      newCommitTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime, false);

      String compactionCommitTime = client.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata result = client.compact(compactionCommitTime);
      client.commitCompaction(compactionCommitTime, result, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionCommitTime));

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      hoodieTable.getHoodieView().sync();
      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView =
          getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().requestedTime();
      assertTrue(compareTimestamps("000", LESSER_THAN, latestCompactionCommitTime));

      if (cfg.populateMetaFields()) {
        assertEquals(200, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.of("000")),
            "Must contain 200 records");
      } else {
        assertEquals(200, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.empty()));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInlineScheduleCompaction(boolean scheduleInlineCompaction) throws Exception {
    HoodieFileFormat fileFormat = HoodieFileFormat.PARQUET;
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(2).withScheduleInlineCompaction(scheduleInlineCompaction).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records, client, cfg, newCommitTime, true);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (updates)
       */
      newCommitTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime, true);

      // verify that there is a commit
      if (scheduleInlineCompaction) {
        assertEquals(metaClient.reloadActiveTimeline().getAllCommitsTimeline().filterPendingCompactionTimeline().countInstants(), 1);
      } else {
        assertEquals(metaClient.reloadActiveTimeline().getAllCommitsTimeline().filterPendingCompactionTimeline().countInstants(), 0);
      }
    }
  }

  @Test
  public void testRepeatedRollbackOfCompaction() throws Exception {
    boolean scheduleInlineCompaction = false;
    HoodieFileFormat fileFormat = HoodieFileFormat.PARQUET;
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(2).withScheduleInlineCompaction(scheduleInlineCompaction).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records, client, cfg, newCommitTime, true);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (updates)
       */
      newCommitTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime, true);

      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      client.compact(compactionInstant.get());

      // trigger compaction again.
      client.compact(compactionInstant.get());

      metaClient.reloadActiveTimeline();
      // verify that there is no new rollback instant generated
      HoodieInstant rollbackInstant = metaClient.getActiveTimeline().getRollbackTimeline().lastInstant().get();
      String basePathStr = metaClient.getBasePath().toString();
      FileCreateUtilsLegacy.deleteRollbackCommit(basePathStr.substring(basePathStr.indexOf(":") + 1),
          rollbackInstant.requestedTime());
      metaClient.reloadActiveTimeline();
      try (SparkRDDWriteClient client1 = getHoodieWriteClient(cfg)) {
        // trigger compaction again.
        HoodieWriteMetadata result = client1.compact(compactionInstant.get());
        client.commitCompaction(compactionInstant.get(), result, Option.empty());
        assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstant.get()));
        // verify that there is no new rollback instant generated
        HoodieInstant newRollbackInstant = metaClient.getActiveTimeline().getRollbackTimeline().lastInstant().get();
        assertEquals(rollbackInstant.requestedTime(), newRollbackInstant.requestedTime());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSimpleInsertUpdateAndDelete(boolean populateMetaFields) throws Exception {
    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc().parallelize(statuses));
      assertNoWriteErrors(statuses);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().requestedTime(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
      assertFalse(commit.isPresent());

      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView =
          getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(),
              allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc().parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, jsc().parallelize(statuses));

      /*
       * Write 2 (only deletes, written to .log file)
       */
      newCommitTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);

      statuses = client.upsert(jsc().parallelize(fewRecordsForDelete, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, jsc().parallelize(statuses));

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("004", deltaCommit.get().requestedTime(), "Latest Delta commit should be 004");

      commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      List<String> inputPaths = tableView.getLatestBaseFiles()
          .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
          .collect(Collectors.toList());
      List<GenericRecord> recordsRead =
          HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf(), inputPaths, basePath(), new JobConf(storageConf().unwrap()), true, populateMetaFields);
      // Wrote 20 records and deleted 20 records, so remaining 20-20 = 0
      assertEquals(0, recordsRead.size(), "Must contain 0 records");
    }
  }

  @Test
  public void testSimpleInsertsGeneratedIntoLogFiles() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, HoodieIndex.IndexType.INMEMORY).build();
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      long expectedLogFileNum =
          statuses.map(writeStatus -> (HoodieDeltaWriteStat) writeStatus.getStat())
              .flatMap(deltaWriteStat -> deltaWriteStat.getLogFiles().iterator())
              .count();
      // inject a fake log file to test marker file for log file
      HoodieDeltaWriteStat correctWriteStat =
          (HoodieDeltaWriteStat) statuses.map(WriteStatus::getStat).take(1).get(0);
      assertTrue(HadoopFSUtils.isLogFile(new Path(correctWriteStat.getPath())));
      HoodieLogFile correctLogFile = new HoodieLogFile(correctWriteStat.getPath());
      String correctWriteToken = FSUtils.getWriteTokenFromLogPath(correctLogFile.getPath());

      final String fakeToken = generateFakeWriteToken(correctWriteToken);

      final WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(),
          HoodieSparkTable.create(config, context()), newCommitTime);
      HoodieLogFormat.Writer fakeLogWriter = HoodieLogFormat.newWriterBuilder()
          .onParentPath(
              FSUtils.constructAbsolutePath(config.getBasePath(),
                  correctWriteStat.getPartitionPath()))
          .withFileId(correctWriteStat.getFileId())
          .withInstantTime(newCommitTime)
          .withLogVersion(correctLogFile.getLogVersion())
          .withFileSize(0L)
          .withSizeThreshold(config.getLogFileMaxSize()).withStorage(hoodieStorage())
          .withLogWriteToken(fakeToken)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
          .withFileCreationCallback(new LogFileCreationCallback() {
            @Override
            public boolean preFileCreation(HoodieLogFile logFile) {
              return writeMarkers.create(correctWriteStat.getPartitionPath(), logFile.getFileName(), IOType.CREATE).isPresent();
            }
          }).build();
      AppendResult fakeAppendResult = fakeLogWriter.appendBlock(getLogBlock());
      // check marker for fake log generated
      assertTrue(writeMarkers.allMarkerFilePaths().stream().anyMatch(marker -> marker.contains(fakeToken)));
      SyncableFileSystemView unCommittedFsView = getFileSystemViewWithUnCommittedSlices(metaClient);
      // check fake log generated
      assertTrue(unCommittedFsView.getAllFileSlices(correctWriteStat.getPartitionPath())
          .flatMap(FileSlice::getLogFiles).map(HoodieLogFile::getPath)
          .anyMatch(
              path -> path.getName().equals(fakeAppendResult.logFile().getPath().getName())));
      writeClient.commit(newCommitTime, statuses);

      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      table.getHoodieView().sync();
      TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> allSlices = tableRTFileSystemView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
        assertEquals(0, allSlices.stream().filter(fileSlice -> fileSlice.getBaseFile().isPresent()).count());
        assertTrue(allSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        long logFileCount = allSlices.stream().filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
        if (logFileCount > 0) {
          // check the log versions start from the base version
          assertTrue(allSlices.stream().map(slice -> slice.getLogFiles().findFirst().get().getLogVersion())
              .allMatch(version -> version.equals(HoodieLogFile.LOGFILE_BASE_VERSION)));
          // check fake log file is deleted
          assertFalse(allSlices.stream().flatMap(slice -> slice.getLogFiles().map(HoodieLogFile::getLogWriteToken))
              .anyMatch(s -> s.equals(fakeToken)));
        }
        numLogFiles += logFileCount;
      }
      assertEquals(expectedLogFileNum, numLogFiles);
      // Do a compaction
      String instantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = writeClient.compact(instantTime);
      String extension = table.getBaseFileExtension();
      writeClient.commitCompaction(instantTime, compactionMetadata, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(instantTime));
      Collection<List<HoodieWriteStat>> stats = compactionMetadata.getCommitMetadata().get().getPartitionToWriteStats().values();
      assertEquals(numLogFiles, stats.stream().flatMap(Collection::stream).filter(state -> state.getPath().contains(extension)).count());
      assertEquals(numLogFiles, stats.stream().mapToLong(Collection::size).sum());
    }
  }

  private HoodieDataBlock getLogBlock() throws IOException, URISyntaxException {
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    List<HoodieRecord> hoodieRecords = records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
    return new HoodieAvroDataBlock(hoodieRecords, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  private String generateFakeWriteToken(String correctWriteToken) {
    Random random = new Random();
    String fakeToken = "";
    do {
      fakeToken = random.nextLong() + "-" + random.nextLong() + "-" + random.nextLong();
    } while (fakeToken.equals(correctWriteToken));
    return fakeToken;
  }
}
