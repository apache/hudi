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

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.BaseSparkDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieClientTestHarness.buildProfile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadTable extends SparkClientFunctionalTestHarness {

  private HoodieTableMetaClient metaClient;
  private HoodieTestDataGenerator dataGen;

  void setUp(Properties props) throws IOException {
    Properties properties = CollectionUtils.copy(props);
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);
    dataGen = new HoodieTestDataGenerator();
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).withWriteStatusClass(MetadataMergeWriteStatus.class).build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

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
  @ValueSource(booleans = {true, false})
  public void testUpsertPartitioner(boolean populateMetaFields) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

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

      statuses = client.upsert(jsc().parallelize(newRecords), newCommitTime).collect();
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

      List<String> inputPaths = roView.getLatestBaseFiles()
          .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
          .collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), inputPaths,
          basePath(), new JobConf(hadoopConf()), true, false);
      // Wrote 20 records in 2 batches
      assertEquals(40, recordsRead.size(), "Must contain 40 records");
    }
  }

  // TODO: Enable metadata virtual keys in this test once the feature HUDI-2593 is completed
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLogFileCountsAfterCompaction(boolean preserveCommitMeta) throws Exception {
    boolean populateMetaFields = true;
    // insert 100 records
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true, false, HoodieIndex.IndexType.BLOOM,
        1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build(), preserveCommitMeta);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig config = cfgBuilder.build();

    setUp(config.getProps());

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      newCommitTime = "101";
      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc().parallelize(updatedRecords, 1);

      SparkRDDReadClient readClient = new SparkRDDReadClient(context(), config);
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = readClient.tagLocation(updatedRecordsRDD);

      writeClient.startCommitWithTime(newCommitTime);
      writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();

      // Write them to corresponding avro logfiles
      metaClient = HoodieTableMetaClient.reload(metaClient);

      HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          writeClient.getEngineContext().getHadoopConf().get(), config, writeClient.getEngineContext());
      HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable
          .of(metaClient, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS, metadataWriter);

      Set<String> allPartitions = updatedRecords.stream()
          .map(record -> record.getPartitionPath())
          .collect(Collectors.groupingBy(partitionPath -> partitionPath))
          .keySet();
      assertEquals(allPartitions.size(), testTable.listAllBaseFiles().length);

      // Verify that all data file has one log file
      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(),
              "There should be 1 log file written for the latest data file - " + fileSlice);
        }
      }

      // Do a compaction
      String compactionInstantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata<JavaRDD<WriteStatus>> result = writeClient.compact(compactionInstantTime);

      // Verify that recently written compacted data file has no log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(config, context(), metaClient);
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
        assertTrue(result.getCommitMetadata().get().getWritePartitionPaths().stream().anyMatch(part -> part.contentEquals(partitionPath)));
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath(), dataGen.getPartitionPaths()[i]);
      }
      Dataset<Row> actual = HoodieClientTestUtils.read(jsc(), basePath(), sqlContext(), fs(), fullPartitionPaths);
      List<Row> rows = actual.collectAsList();
      assertEquals(updatedRecords.size(), rows.size());
      for (Row row: rows) {
        assertEquals(row.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD), preserveCommitMeta ? newCommitTime : compactionInstantTime);
      }
    }
  }

  /**
   * Test to ensure metadata stats are correctly written to metadata file.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMetadataStatsOnCommit(Boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY)
        .withAutoCommit(false).build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);

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
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
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
      writeRecords = jsc().parallelize(records, 1);
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
      table = HoodieSparkTable.create(cfg, context());
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
   * Test to ensure rolling stats are correctly written to the metadata file, identifies small files and corrects them.
   */
  @Test
  public void testRollingStatsWithSmallFileHandling() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      Map<String, Long> fileIdToInsertsMap = new HashMap<>();
      Map<String, Long> fileIdToUpsertsMap = new HashMap<>();

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      HoodieTable table = HoodieSparkTable.create(cfg, context());
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
      writeRecords = jsc().parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
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
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(instantTime);
      statuses = compactionMetadata.getWriteStatuses();
      client.commitCompaction(instantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
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
      writeRecords = jsc().parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
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

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieSparkMergeOnReadTable hoodieTable = (HoodieSparkMergeOnReadTable) HoodieSparkTable.create(cfg, context(), metaClient);

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
      writeRecords = jsc().parallelize(records, 1);
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
      JavaRDD<HoodieRecord> deleteRDD = jsc().parallelize(fewRecordsForDelete, 1);

      // initialize partitioner
      hoodieTable.getHoodieView().sync();
      BaseSparkDeltaCommitActionExecutor actionExecutor = new SparkDeleteDeltaCommitActionExecutor(context(), cfg, hoodieTable,
          newDeleteTime, HoodieJavaRDD.of(deleteRDD));
      actionExecutor.getUpsertPartitioner(new WorkloadProfile(buildProfile(deleteRDD)));
      final List<List<WriteStatus>> deleteStatus = jsc().parallelize(Arrays.asList(1)).map(x -> {
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

  @Test
  public void testReleaseResource() throws Exception {
    HoodieWriteConfig.Builder builder = getConfigBuilder(true);
    builder.withReleaseResourceEnabled(true);
    builder.withAutoCommit(false);

    setUp(builder.build().getProps());

    /**
     * Write 1 (test when RELEASE_RESOURCE_ENABLE is true)
     */
    try (SparkRDDWriteClient client = getHoodieWriteClient(builder.build())) {

      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
      writeRecords.persist(StorageLevel.MEMORY_AND_DISK());
      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      client.commitStats(newCommitTime, statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
      assertEquals(spark().sparkContext().persistentRdds().size(), 0);
    }

    builder.withReleaseResourceEnabled(false);

    /**
     * Write 2 (test when RELEASE_RESOURCE_ENABLE is false)
     */
    try (SparkRDDWriteClient client = getHoodieWriteClient(builder.build())) {
      String newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      writeRecords.persist(StorageLevel.MEMORY_AND_DISK());
      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      client.commitStats(newCommitTime, statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
      assertTrue(spark().sparkContext().persistentRdds().size() > 0);
    }

  }
}

