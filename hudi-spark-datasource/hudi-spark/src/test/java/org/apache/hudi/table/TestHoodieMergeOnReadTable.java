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
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.BaseSparkDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieSparkClientTestHarness.buildProfile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

  @BeforeEach
  void beforeEach() {
    jsc().getPersistentRDDs().values().forEach(JavaRDD::unpersist);
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

      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<WriteStatus> rawStatuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(rawStatuses);
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      Map<String, String> allWriteStatusMergedMetadataMap =
          MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(rawStatuses);
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
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> rawStatuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(rawStatuses);
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().requestedTime(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
      assertFalse(commit.isPresent());

      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
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
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, records);
      newRecords.addAll(dataGen.generateInserts(newCommitTime, 20));

      rawStatuses = client.upsert(jsc().parallelize(newRecords), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(rawStatuses);
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("002", deltaCommit.get().requestedTime(), "Latest Delta commit should be 002");

      commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
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
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf(), inputPaths,
          basePath(), new JobConf(storageConf().unwrap()), true, populateMetaFields);
      // Wrote 20 records in 2 batches
      assertEquals(40, recordsRead.size(), "Must contain 40 records");
    }
  }

  @Test
  public void testUpsertPartitionerWithTableVersionSix() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    cfgBuilder.withWriteTableVersion(6);
    HoodieWriteConfig cfg = cfgBuilder.build();

    // create meta client w/ the table version 6
    Properties props = getPropertiesForKeyGen(true);
    props.put(WRITE_TABLE_VERSION.key(), "6");
    metaClient = getHoodieMetaClient(storageConf(), basePath(), props, HoodieTableType.MERGE_ON_READ);
    dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      // batch 1 insert
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
      List<WriteStatus> rawStatuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(rawStatuses);
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().requestedTime(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
      assertFalse(commit.isPresent());

      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
      BaseFileOnlyView roView = getHoodieTableFileSystemView(metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(), allFiles);

      Map<String, String> baseFileMapping = new HashMap<>();
      Map<String, List<String>> baseFileToLogFileMapping = new HashMap<>();
      BaseFileOnlyView finalRoView = roView;
      Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).forEach(partitionPath -> {
        String baseFileName = finalRoView.getLatestBaseFiles(partitionPath).collect(Collectors.toList()).get(0).getFileName();
        baseFileMapping.put(partitionPath, baseFileName);
        baseFileToLogFileMapping.put(baseFileName, new ArrayList<>());
      });

      writeAndValidateLogFileBaseInstantTimeMatches(client, "002", records, cfg, baseFileMapping, baseFileToLogFileMapping);
      writeAndValidateLogFileBaseInstantTimeMatches(client, "003", records, cfg, baseFileMapping, baseFileToLogFileMapping);
      writeAndValidateLogFileBaseInstantTimeMatches(client, "004", records, cfg, baseFileMapping, baseFileToLogFileMapping);
    }
  }

  private void writeAndValidateLogFileBaseInstantTimeMatches(SparkRDDWriteClient client, String newCommitTime, List<HoodieRecord> records,
                                                             HoodieWriteConfig cfg, Map<String, String> baseFileMapping,
                                                             Map<String, List<String>> baseFileToLogFileMapping) throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, records);
    List<WriteStatus> rawStatuses = client.upsert(jsc().parallelize(newRecords), newCommitTime).collect();
    assertNoWriteErrors(rawStatuses);
    client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
    // validate the data itself
    validateNewData(newRecords);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(newCommitTime, deltaCommit.get().requestedTime(), "Latest Delta commit should be 002");

    HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTable finalHoodieTable = hoodieTable;
    baseFileMapping.entrySet().forEach(entry -> {
          FileSlice fileSlice = finalHoodieTable.getSliceView().getLatestFileSlices(entry.getKey()).collect(Collectors.toList()).get(0);
          String baseFileName = entry.getValue();
          String baseInstantTime = FSUtils.getCommitTime(baseFileName);
          // validate the base instant time matches
          List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
          // except latest log file, all other files should be present in the tracking map.
          int counter = 0;
          while (counter < logFiles.size()) {
            HoodieLogFile logFile = logFiles.get(counter);
            if (counter == logFiles.size() - 1) {
              // latest log file may not be present in the tracking map. lets add it to assist w/ for next round of validation.
              baseFileToLogFileMapping.get(baseFileName).add(logFile.getFileName());
            } else {
              // all previous log files are expected to be matching
              baseFileToLogFileMapping.get(baseFileName).contains(logFile.getFileName());
            }
            // validate that base instant time matches
            assertEquals(baseInstantTime, FSUtils.getDeltaCommitTimeFromLogPath(logFile.getPath()));
            counter++;
          }
        }
    );
  }

  private void validateNewData(List<HoodieRecord> newRecords) {
    Dataset<Row> inputDf = spark().read().json(jsc().parallelize(recordsToStrings(newRecords), 2)).drop("partition");
    // get keys from the dataframe
    List<String> updatedKeys = inputDf.select("_row_key").as(Encoders.STRING()).collectAsList();
    Dataset<Row> outputDf = spark().read().format("hudi").load(basePath());
    // drop metadata columns
    outputDf = outputDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HoodieRecord.FILENAME_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    // filter the dataframe for updatedKeys only
    outputDf = outputDf.filter(outputDf.col("_row_key").isin(updatedKeys.toArray()));
    // assert that the dataframe is equal to the expected dataframe
    // NOTE: we have excluded some columns from comparison such as map, date and array type fields as they were incompatible
    // For example below is what data generated looks like vs what is read from the table (check `city_to_state` map: [CA] vs Map(LA -> CA))
    //  [false,029c1e56-3c03-42e3-a2eb-a45addd5b671,0.5550830309956531,0.013823731501093062,[CA],15,1322460250,1053705246,driver-002,0.8563083971473885,0.7050871729430999,
    //         [39.649862113946796,USD], WrappedArray(0, 0, 8, 19, -72),Canada,2015/03/17,rider-002,-5190452608208752867,0,WrappedArray([88.29247239885966,USD]),BLACK,0.7458226]
    //  [false,029c1e56-3c03-42e3-a2eb-a45addd5b671,0.5550830309956531,0.013823731501093062,Map(LA -> CA),1970-01-16,1322460250,1053705246,driver-002,0.8563083971473885,0.7050871729430999,
    //         [39.649862113946796,USD],0.529336,[B@372d7420,2015/03/17,rider-002,-5190452608208752867,0,WrappedArray([88.29247239885966,USD]),BLACK,0.7458226]
    assertTrue(areDataframesEqual(inputDf, outputDf, new HashSet<>(Arrays.asList("_hoodie_is_deleted", "_row_key", "begin_lat", "begin_lon",
        "current_ts", "distance_in_meters", "driver", "end_lat", "end_lon", "fare"))), "Dataframe mismatch");
  }

  // TODO: Enable metadata virtual keys in this test once the feature HUDI-2593 is completed
  @Test
  public void testLogFileCountsAfterCompaction() throws Exception {
    boolean populateMetaFields = true;
    // insert 100 records
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true, false, HoodieIndex.IndexType.BLOOM,
        1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig config = cfgBuilder.build();

    setUp(config.getProps());

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc().parallelize(statuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      // Update all the 100 records
      newCommitTime = "101";
      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc().parallelize(updatedRecords, 1);

      SparkRDDReadClient readClient = new SparkRDDReadClient(context(), config);
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = readClient.tagLocation(updatedRecordsRDD);

      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
      statuses = writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc().parallelize(statuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      // Write them to corresponding avro logfiles
      metaClient = HoodieTableMetaClient.reload(metaClient);

      try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          writeClient.getEngineContext().getStorageConf(), config, writeClient.getEngineContext())) {
        HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable
            .of(metaClient, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS, metadataWriter);

        Set<String> allPartitions = updatedRecords.stream()
            .map(record -> record.getPartitionPath())
            .collect(Collectors.groupingBy(partitionPath -> partitionPath))
            .keySet();
        assertEquals(allPartitions.size(), testTable.listAllBaseFiles().size());

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
        writeClient.commitCompaction(compactionInstantTime, result, Option.of(table));
        assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantTime));

        // Verify that recently written compacted data file has no log file
        metaClient = HoodieTableMetaClient.reload(metaClient);
        table = HoodieSparkTable.create(config, context(), metaClient);
        HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

        assertTrue(compareTimestamps(timeline.lastInstant().get().requestedTime(), GREATER_THAN, newCommitTime),
            "Compaction commit should be > than last insert");

        for (String partitionPath : dataGen.getPartitionPaths()) {
          List<FileSlice> groupedLogFiles =
              table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
          for (FileSlice slice : groupedLogFiles) {
            assertEquals(0, slice.getLogFiles().count(),
                "After compaction there should be no log files visible on a full view");
          }
          assertTrue(result.getCommitMetadata().get().getWritePartitionPaths().stream()
              .anyMatch(part -> part.contentEquals(partitionPath)));
        }

        // Check the entire dataset has all records still
        String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
        for (int i = 0; i < fullPartitionPaths.length; i++) {
          fullPartitionPaths[i] =
              String.format("%s/%s/*", basePath(), dataGen.getPartitionPaths()[i]);
        }
        Dataset<Row> actual = HoodieClientTestUtils.read(
            jsc(), basePath(), sqlContext(), hoodieStorage(), fullPartitionPaths);
        List<Row> rows = actual.collectAsList();
        assertEquals(updatedRecords.size(), rows.size());
        for (Row row : rows) {
          assertEquals(row.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD), newCommitTime);
          // check that file names metadata is updated
          assertTrue(row.getString(HoodieRecord.FILENAME_META_FIELD_ORD).contains(compactionInstantTime));
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"true,avro", "true,parquet", "false,avro", "false,parquet"})
  public void testLogBlocksCountsAfterLogCompaction(boolean populateMetaFields, String logFileFormat) throws Exception {

    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    // insert 100 records
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .withCompactionConfig(compactionConfig);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig config = cfgBuilder
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024)
            .parquetMaxFileSize(1024 * 1024 * 1024)
            .logFileDataBlockFormat(logFileFormat)
            .build())
        .build();
    setUp(config.getProps());

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc().parallelize(statuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
      // Update all the 100 records
      newCommitTime = "101";
      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc().parallelize(updatedRecords, 1);

      HoodieReadClient readClient = new HoodieReadClient(context(), config);
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = readClient.tagLocation(updatedRecordsRDD);

      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
      statuses = writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc().parallelize(statuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      newCommitTime = "102";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
      statuses = writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc().parallelize(statuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      // Write them to corresponding avro logfiles
      metaClient = HoodieTableMetaClient.reload(metaClient);

      try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          writeClient.getEngineContext().getStorageConf(), config, writeClient.getEngineContext())) {
        HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable
            .of(metaClient, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS, metadataWriter);

        Set<String> allPartitions = updatedRecords.stream()
            .map(record -> record.getPartitionPath())
            .collect(Collectors.groupingBy(partitionPath -> partitionPath))
            .keySet();
        assertEquals(allPartitions.size(), testTable.listAllBaseFiles().size());

        // Verify that all data file has one log file
        HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
        for (String partitionPath : dataGen.getPartitionPaths()) {
          List<FileSlice> groupedLogFiles =
              table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
          for (FileSlice fileSlice : groupedLogFiles) {
            assertEquals(2, fileSlice.getLogFiles().count(),
                "There should be 1 log file written for the latest data file - " + fileSlice);
          }
        }

        // Do a log compaction
        String logCompactionInstantTime = writeClient.scheduleLogCompaction(Option.empty()).get().toString();
        HoodieWriteMetadata<JavaRDD<WriteStatus>> result = writeClient.logCompact(logCompactionInstantTime, true);

        // Verify that recently written compacted data file has no log file
        metaClient = HoodieTableMetaClient.reload(metaClient);
        table = HoodieSparkTable.create(config, context(), metaClient);
        HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

        assertTrue(compareTimestamps(timeline.lastInstant().get().requestedTime(), GREATER_THAN, newCommitTime),
            "Compaction commit should be > than last insert");

        for (String partitionPath : dataGen.getPartitionPaths()) {
          List<FileSlice> fileSlices =
              table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
          assertEquals(1, fileSlices.size());
          for (FileSlice slice : fileSlices) {
            assertEquals(3, slice.getLogFiles().count(), "After compaction there will still be one log file.");
            assertNotNull(slice.getBaseFile(), "Base file is not created by log compaction operation.");
          }
          assertTrue(result.getCommitMetadata().get().getWritePartitionPaths().stream().anyMatch(part -> part.contentEquals(partitionPath)));
        }
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
        .withAvroSchemaValidate(false)
        .withAllowAutoEvolutionColumnDrop(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(false).build())
        // in this test we mock few entries in timeline. hence col stats initialization does not work.
        .build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);

      // Create a commit without metadata stats in metadata to test backwards compatibility
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      List<String> instants = new ArrayList<>();
      String instant0 = metaClient.createNewInstantTime();
      HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, commitActionType, instant0);
      activeTimeline.createNewInstant(instant);
      activeTimeline.transitionRequestedToInflight(instant, Option.empty());
      instant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, commitActionType, instant0);
      activeTimeline.saveAsComplete(instant, Option.empty());

      String instant1 = metaClient.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant1);

      List<HoodieRecord> records = dataGen.generateInserts(instant1, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> rawStatuses = client.insert(writeRecords, instant1).collect();
      assertTrue(client.commit(instant1, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap()), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instantOne = table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
      HoodieCommitMetadata metadata =
          table.getActiveTimeline().readCommitMetadata(instantOne);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
        }
      }
      assertEquals(200, inserts);

      String instant2 = metaClient.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant2);
      records = dataGen.generateUpdates(instant2, records);
      writeRecords = jsc().parallelize(records, 1);
      rawStatuses = client.upsert(writeRecords, instant2).collect();
      //assertTrue(client.commit(instantTime, statuses), "Commit should succeed");
      inserts = 0;
      int upserts = 0;
      List<WriteStatus> writeStatusList = rawStatuses;
      for (WriteStatus ws : writeStatusList) {
        inserts += ws.getStat().getNumInserts();
        upserts += ws.getStat().getNumUpdateWrites();
      }

      // Read from commit file
      assertEquals(0, inserts);
      assertEquals(200, upserts);

      if (!rollbackUsingMarkers) {
        // we can do listing based rollback only when commit is completed
        assertTrue(client.commit(instant2, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap()), "Commit should succeed");
      }
      client.rollback(instant2);

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instant3 = table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
      metadata = table.getActiveTimeline().readCommitMetadata(instant3);
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
    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).build();

    setUp(cfg.getProps());

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      Map<String, Long> fileIdToInsertsMap = new HashMap<>();
      Map<String, Long> fileIdToUpsertsMap = new HashMap<>();

      String instantTime = "000";
      WriteClientTestUtils.startCommitWithTime(client, instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> rawStatuses = client.insert(writeRecords, instantTime).collect();
      assertNoWriteErrors(rawStatuses);
      assertTrue(client.commit(instantTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap()));

      // Read from commit file
      HoodieTable table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instantOne = table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
      HoodieCommitMetadata metadata =
          table.getActiveTimeline().readCommitMetadata(instantOne);
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
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc().parallelize(records, 1);
      rawStatuses = client.upsert(writeRecords, instantTime).collect();
      assertTrue(client.commit(instantTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap()),"Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instantTwo = table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
      metadata = table.getActiveTimeline().readCommitMetadata(instantTwo);
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
      WriteClientTestUtils.scheduleTableService(client, instantTime, Option.of(metadata.getExtraMetadata()), TableServiceType.COMPACT);
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(instantTime);
      client.commitCompaction(instantTime, compactionMetadata, Option.of(table));
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(instantTime));

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instantThree = table.getActiveTimeline().getCommitsTimeline().lastInstant().get();
      HoodieCommitMetadata metadata1 =
          table.getActiveTimeline().readCommitMetadata(instantThree);

      // Ensure that the metadata stats from the extra metadata of delta commits is copied over to the compaction commit
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        assertTrue(metadata1.getPartitionToWriteStats().containsKey(pstat.getKey()));
        assertEquals(metadata1.getPartitionToWriteStats().get(pstat.getKey()).size(),
            pstat.getValue().size());
      }

      // Write inserts + updates
      instantTime = "003";
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc().parallelize(records, 1);
      rawStatuses = client.upsert(writeRecords, instantTime).collect();
      assertTrue(client.commit(instantTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap()), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context());
      HoodieInstant instant = table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
      metadata = table.getActiveTimeline().readCommitMetadata(instant);
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
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> rawStatuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(rawStatuses);
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());

      HoodieSparkMergeOnReadTable hoodieTable = (HoodieSparkMergeOnReadTable) HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().requestedTime(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
      assertFalse(commit.isPresent());

      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
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
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      metaClient.reloadActiveTimeline();
      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc().parallelize(records, 1);
      rawStatuses = client.upsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc().parallelize(rawStatuses, 1), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
      assertNoWriteErrors(rawStatuses);

      /**
       * Write 3 (only deletes, written to .log file)
       */
      final String newDeleteTime = "004";
      final String partitionPath = records.get(0).getPartitionPath();
      final String fileId = rawStatuses.get(0).getFileId();
      WriteClientTestUtils.startCommitWithTime(client, newDeleteTime);
      metaClient.reloadActiveTimeline();

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);
      JavaRDD<HoodieRecord> deleteRDD = jsc().parallelize(fewRecordsForDelete, 1);

      // initialize partitioner
      hoodieTable.getHoodieView().sync();
      BaseSparkDeltaCommitActionExecutor actionExecutor = new SparkDeleteDeltaCommitActionExecutor(context(), cfg, hoodieTable,
          newDeleteTime, HoodieJavaRDD.of(deleteRDD));
      actionExecutor.getUpsertPartitioner(new WorkloadProfile(buildProfile(deleteRDD)));
      final List<List<WriteStatus>> deleteStatus = jsc().parallelize(Arrays.asList(1))
          .map(x -> (Iterator<List<WriteStatus>>)
              actionExecutor.handleUpdate(partitionPath, fileId, fewRecordsForDelete.iterator()))
          .map(Transformations::flatten).collect();

      // Verify there are  errors because records are from multiple partitions (but handleUpdate is invoked for
      // specific partition)
      WriteStatus status = deleteStatus.get(0).get(0);
      assertTrue(status.hasErrors());
      long numRecordsInPartition = fewRecordsForDelete.stream().filter(u ->
          u.getPartitionPath().equals(partitionPath)).count();
      assertEquals(fewRecordsForDelete.size() - numRecordsInPartition, status.getTotalErrorRecords());
    }
  }
}
