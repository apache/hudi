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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndexHelper;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCompactor extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts();

    // Create a temp folder as the base path
    initPath();
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test
  public void testCompactionOnCopyOnWriteFail() throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();
    assertThrows(HoodieNotSupportedException.class, () -> {
      table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      table.compact(context, compactionInstantTime);
    });
  }

  @Test
  public void testCompactionEmpty() throws Exception {
    HoodieWriteConfig config = getConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {

      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();
      Option<HoodieCompactionPlan> plan = table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      assertFalse(plan.isPresent(), "If there is nothing to compact, result will be empty");
    }
  }

  @Test
  public void testScheduleCompactionWithInflightInstant() {
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // create one inflight instance.
      newCommitTime = "102";
      writeClient.startCommitWithTime(newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());
      // create one compaction instance before exist inflight instance.
      String compactionTime = "101";
      writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty());
    }
  }

  @Test
  public void testWriteStatusContentsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      HoodieTable table = HoodieSparkTable.create(config, context);
      newCommitTime = "101";

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

      writeClient.startCommitWithTime(newCommitTime);
      writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
      metaClient.reloadActiveTimeline();

      // Verify that all data file has one log file
      table = HoodieSparkTable.create(config, context);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(), "There should be 1 log file written for every data file");
        }
      }

      // Do a compaction
      table = HoodieSparkTable.create(config, context);
      String compactionInstantTime = "102";
      table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      table.getMetaClient().reloadActiveTimeline();
      JavaRDD<WriteStatus> result = (JavaRDD<WriteStatus>) table.compact(
          context, compactionInstantTime).getWriteStatuses();

      // Verify that all partition paths are present in the WriteStatus result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<WriteStatus> writeStatuses = result.collect();
        assertTrue(writeStatuses.stream()
            .filter(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath)).count() > 0);
      }
    }
  }

  @Test
  public void testCompactionWithOrderedTs() throws Exception {
    String partition = "2022";
    String preCombineField = "timestamp";
    dataGen = new HoodieTestDataGenerator(new String[] {partition});
    // create table with payload = DefaultHoodieRecordPayload, PreCombineField = timestamp
    metaClient = HoodieTestUtils.initWithPayLoad(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, DefaultHoodieRecordPayload.class, preCombineField, new Properties());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withPreCombineField(preCombineField)
        .combineInput(false, true)
        .withWritePayLoad(DefaultHoodieRecordPayload.class.getName())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withPayloadClass(DefaultHoodieRecordPayload.class.getName())
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    /*
     * Update the records twice with different ordered timestamps
     * the first step : insert 100 records
     * the second step : first update 100 records with timestamp 2
     * the third step : second update 100 records with timestamp 3
     * the forth step : do a compact
     * the final step : check 100 records with timestamp 3
     */
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String firstCommitTime = "100";
      writeClient.startCommitWithTime(firstCommitTime);
      // first step
      List<HoodieRecord> records = dataGen.generateInserts(firstCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, firstCommitTime).collect();
      HoodieTable table = HoodieSparkTable.create(config, context);
      // second step : do first update
      String firstUpdateTime = "101";
      // init ts = 2
      Integer firstUpdateTimeStamp = 2;
      List<HoodieRecord> updatedRecords = dataGen.generateUpdatesWithTS(firstUpdateTime, records, firstUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

      writeClient.startCommitWithTime(firstUpdateTime);
      writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, firstUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // second update
      String secondUpdateTime = "102";
      Integer secondUpdateTimeStamp = firstUpdateTimeStamp + 1;
      List<HoodieRecord> updatedTwiceRecords = dataGen.generateUpdatesWithTS(secondUpdateTime, records, secondUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedTwiceRecordsRDD = jsc.parallelize(updatedTwiceRecords, 1);
      JavaRDD<HoodieRecord> updatedTwiceTaggedRecordsRDD = tagLocation(index, updatedTwiceRecordsRDD, table);

      writeClient.startCommitWithTime(secondUpdateTime);
      writeClient.upsertPreppedRecords(updatedTwiceTaggedRecordsRDD, secondUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // Do a compaction
      String compactionCommitTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata writeMetadata = writeClient.compact(compactionCommitTime);
      HoodieCommitMetadata commitMetadata = (HoodieCommitMetadata) writeMetadata.getCommitMetadata().get();
      // Verify that  partition paths are present in the commitMetadata result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(commitMetadata.getWritePartitionPaths().contains(partitionPath));
      }

      metaClient.reloadActiveTimeline();
      // Verify that latest timeline instant is the compact
      assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());


      table.getHoodieView().sync();
      FileStatus[] allFiles = HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, table.getCompletedCommitsTimeline(), allFiles);

      List<String> inputPaths = tableView.getLatestBaseFiles()
          .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
          .collect(Collectors.toList());
      List<GenericRecord> recordsRead =
          HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, inputPaths, basePath, new JobConf(hadoopConf), true, false);
      // check table records size = 100
      assertEquals(100, recordsRead.size());
      // after compaction , check all records timestamp = secondUpdateTimeStamp, _hoodie_commit_time = secondUpdateTime
      for (GenericRecord genericRecord : recordsRead) {
        assertEquals(String.valueOf(secondUpdateTimeStamp), genericRecord.get("timestamp").toString());
        assertEquals(secondUpdateTime, genericRecord.get("_hoodie_commit_time").toString());
      }
    }

  }

  @Test
  public void testCompactionWithUnOrderedTs() throws Exception {
    String partition = "2022";
    String preCombineField = "timestamp";
    dataGen = new HoodieTestDataGenerator(new String[] {partition});
    // create table with payload = DefaultHoodieRecordPayload, PreCombineField = timestamp
    metaClient = HoodieTestUtils.initWithPayLoad(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, DefaultHoodieRecordPayload.class, preCombineField, new Properties());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withPreCombineField(preCombineField)
        .combineInput(false, true)
        .withWritePayLoad(DefaultHoodieRecordPayload.class.getName())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withPayloadClass(DefaultHoodieRecordPayload.class.getName())
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    /*
     * Update the records twice with different unordered timestamps
     * the first step : insert 100 records
     * the second step : first update 100 records with timestamp Integer.MAX_VALUE
     * the third step : second update 100 records with timestamp 5
     * the forth step : do a compact
     * the final step : check 100 records with timestamp Integer.MAX_VALUE
     */
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String firstCommitTime = "100";
      writeClient.startCommitWithTime(firstCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(firstCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, firstCommitTime).collect();
      // Update the records twice with different ordered timestamps
      HoodieTable table = HoodieSparkTable.create(config, context);
      // first update
      String firstUpdateTime = "101";
      // init ts = Integer.MAX_VALUE
      Integer firstUpdateTimeStamp = Integer.MAX_VALUE;
      List<HoodieRecord> updatedRecords = dataGen.generateUpdatesWithTS(firstUpdateTime, records, firstUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

      writeClient.startCommitWithTime(firstUpdateTime);
      writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, firstUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // second update
      String secondUpdateTime = "102";
      // init secondTimeStamp less than firstTimeStamp
      Integer secondUpdateTimeStamp = 5;
      List<HoodieRecord> updatedTwiceRecords = dataGen.generateUpdatesWithTS(secondUpdateTime, records, secondUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedTwiceRecordsRDD = jsc.parallelize(updatedTwiceRecords, 1);
      JavaRDD<HoodieRecord> updatedTwiceTaggedRecordsRDD = tagLocation(index, updatedTwiceRecordsRDD, table);

      writeClient.startCommitWithTime(secondUpdateTime);
      writeClient.upsertPreppedRecords(updatedTwiceTaggedRecordsRDD, secondUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // Do a compaction
      String compactionCommitTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata writeMetadata = writeClient.compact(compactionCommitTime);
      HoodieCommitMetadata commitMetadata = (HoodieCommitMetadata) writeMetadata.getCommitMetadata().get();
      // Verify that  partition paths are present in the commitMetadata result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(commitMetadata.getWritePartitionPaths().contains(partitionPath));
      }

      metaClient.reloadActiveTimeline();
      // Verify that latest timeline instant is the compact
      assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());


      table.getHoodieView().sync();
      FileStatus[] allFiles = HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, table.getCompletedCommitsTimeline(), allFiles);

      List<String> inputPaths = tableView.getLatestBaseFiles()
          .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
          .collect(Collectors.toList());
      List<GenericRecord> recordsRead =
          HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, inputPaths, basePath, new JobConf(hadoopConf), true, false);
      // check table records size = 100
      assertEquals(100, recordsRead.size());
      // after compaction , check all records timestamp = firstUpdateTime, _hoodie_commit_time = firstUpdateTime
      for (GenericRecord genericRecord : recordsRead) {
        assertEquals(String.valueOf(firstUpdateTimeStamp), genericRecord.get("timestamp").toString());
        assertEquals(firstUpdateTime, genericRecord.get("_hoodie_commit_time").toString());
      }
    }
  }

  @Test
  public void testCompactionWithSameTs() throws Exception {
    String partition = "2022";
    String preCombineField = "timestamp";
    dataGen = new HoodieTestDataGenerator(new String[] {partition});
    // create table with payload = DefaultHoodieRecordPayload, PreCombineField = timestamp
    metaClient = HoodieTestUtils.initWithPayLoad(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, DefaultHoodieRecordPayload.class, preCombineField, new Properties());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withPreCombineField(preCombineField)
        .combineInput(false, true)
        .withWritePayLoad(DefaultHoodieRecordPayload.class.getName())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withPayloadClass(DefaultHoodieRecordPayload.class.getName())
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
      /*
       * Update the records twice with different same timestamps
       * the first step : insert 100 records (default timestamp = 0)
       * the second step : first update 100 records with timestamp 0
       * the third step : second update 100 records with timestamp 0
       * the forth step : do a compact
       * the final step : check 100 records with timestamp=0 and _hoodie_commit_time = final_commit_time
       */
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String firstCommitTime = "100";
      writeClient.startCommitWithTime(firstCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(firstCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, firstCommitTime).collect();
      // Update the records twice with different ordered timestamps
      HoodieTable table = HoodieSparkTable.create(config, context);
      // first update
      String firstUpdateTime = "101";
      // init firstUpdateTimeStamp = 0
      Integer firstUpdateTimeStamp = 0;
      List<HoodieRecord> updatedRecords = dataGen.generateUpdatesWithTS(firstUpdateTime, records, firstUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

      writeClient.startCommitWithTime(firstUpdateTime);
      writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, firstUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // second update
      String secondUpdateTime = "102";
      // init secondUpdateTimeStamp same with firstUpdateTimeStamp
      Integer secondUpdateTimeStamp = 0;
      List<HoodieRecord> updatedTwiceRecords = dataGen.generateUpdatesWithTS(secondUpdateTime, records, secondUpdateTimeStamp);
      JavaRDD<HoodieRecord> updatedTwiceRecordsRDD = jsc.parallelize(updatedTwiceRecords, 1);
      JavaRDD<HoodieRecord> updatedTwiceTaggedRecordsRDD = tagLocation(index, updatedTwiceRecordsRDD, table);

      writeClient.startCommitWithTime(secondUpdateTime);
      writeClient.upsertPreppedRecords(updatedTwiceTaggedRecordsRDD, secondUpdateTime).collect();
      metaClient.reloadActiveTimeline();

      // Do a compaction
      String compactionCommitTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      HoodieWriteMetadata writeMetadata = writeClient.compact(compactionCommitTime);
      HoodieCommitMetadata commitMetadata = (HoodieCommitMetadata) writeMetadata.getCommitMetadata().get();
      // Verify that  partition paths are present in the commitMetadata result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(commitMetadata.getWritePartitionPaths().contains(partitionPath));
      }

      metaClient.reloadActiveTimeline();
      // Verify that latest timeline instant is the compact
      assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());


      table.getHoodieView().sync();
      FileStatus[] allFiles = HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, table.getCompletedCommitsTimeline(), allFiles);

      List<String> inputPaths = tableView.getLatestBaseFiles()
          .map(baseFile -> new Path(baseFile.getPath()).getParent().toString())
          .collect(Collectors.toList());
      List<GenericRecord> recordsRead =
          HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, inputPaths, basePath, new JobConf(hadoopConf), true, false);
      // check table records size = 100
      assertEquals(100, recordsRead.size());
      // after compaction , check all records timestamp = secondUpdateTimeStamp, _hoodie_commit_time = secondUpdateTime
      for (GenericRecord genericRecord : recordsRead) {
        assertEquals(String.valueOf(secondUpdateTimeStamp), genericRecord.get("timestamp").toString());
        assertEquals(secondUpdateTime, genericRecord.get("_hoodie_commit_time").toString());
      }
    }
  }


  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
