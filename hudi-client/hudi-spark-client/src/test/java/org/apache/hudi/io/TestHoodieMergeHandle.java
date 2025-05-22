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

package org.apache.hudi.io;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unchecked")
public class TestHoodieMergeHandle extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testUpsertsForMultipleRecordsInSameFile(ExternalSpillableMap.DiskMapType diskMapType,
                                                      boolean isCompressionEnabled) throws Exception {
    // Create records in a single partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});

    // Build a common config with diff configs
    Properties properties = new Properties();
    properties.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType.name());
    properties.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), String.valueOf(isCompressionEnabled));

    // Build a write config with bulkinsertparallelism set
    HoodieWriteConfig cfg = getConfigBuilder()
        .withProperties(properties)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts) This will do a bulk insert of 44 records of which there are 2 records repeated 21 times
       * each. id1 (21 records), id2 (21 records), id3, id4
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 4);
      HoodieRecord record1 = records.get(0);
      HoodieRecord record2 = records.get(1);
      for (int i = 0; i < 20; i++) {
        HoodieRecord dup = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
        records.add(dup);
      }
      for (int i = 0; i < 20; i++) {
        HoodieRecord dup = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
        records.add(dup);
      }
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      List<WriteStatus> statusList = client.bulkInsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      assertEquals(newCommitTime, timeline.lastInstant().get().requestedTime(), "Latest commit should be 001");
      assertEquals(records.size(),
          HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, INSTANT_GENERATOR).count(),
          "Must contain 44 records");

      /**
       * Write 2 (insert) This will do a bulk insert of 1 record with the same row_key as record1 in the previous insert
       * - id1. At this point, we will have 2 files with the row_keys as shown here - File 1 - id1 (21 records), id2 (21
       * records), id3, id4 File 2 - id1
       */
      newCommitTime = "002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      // Do 1 more bulk insert with the same dup record1
      List<HoodieRecord> newRecords = new ArrayList<>();
      HoodieRecord sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
      newRecords.add(sameAsRecord1);
      writeRecords = jsc.parallelize(newRecords, 1);
      statusList = client.bulkInsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      // verify that there are 2 commits
      metaClient = HoodieTableMetaClient.reload(metaClient);
      timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();
      assertEquals(2, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(), "Expecting two commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().requestedTime(), "Latest commit should be 002");
      Dataset<Row> dataSet = getRecords();
      assertEquals(45, dataSet.count(), "Must contain 45 records");

      /**
       * Write 3 (insert) This will bulk insert 2 new completely new records. At this point, we will have 2 files with
       * the row_keys as shown here - File 1 - id1 (21 records), id2 (21 records), id3, id4 File 2 - id1 File 3 - id5,
       * id6
       */
      newCommitTime = "003";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      newRecords = dataGen.generateInserts(newCommitTime, 2);
      writeRecords = jsc.parallelize(newRecords, 1);
      statusList = client.bulkInsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      // verify that there are now 3 commits
      metaClient = HoodieTableMetaClient.reload(metaClient);
      timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();
      assertEquals(3, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(), "Expecting three commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().requestedTime(), "Latest commit should be 003");
      dataSet = getRecords();
      assertEquals(47, dataSet.count(), "Must contain 47 records");

      /**
       * Write 4 (updates) This will generate 2 upsert records with id1 and id2. The rider and driver names in the
       * update records will be rider-004 and driver-004. After the upsert is complete, all the records with id1 in File
       * 1 and File 2 must be updated, all the records with id2 in File 2 must also be updated. Also, none of the other
       * records in File 1, File 2 and File 3 must be updated.
       */
      newCommitTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> updateRecords = new ArrayList<>();

      // This exists in 001 and 002 and should be updated in both
      sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
      updateRecords.add(sameAsRecord1);

      // This exists in 001 and should be updated
      HoodieRecord sameAsRecord2 = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
      updateRecords.add(sameAsRecord2);
      JavaRDD<HoodieRecord> updateRecordsRDD = jsc.parallelize(updateRecords, 1);
      statusList = client.upsert(updateRecordsRDD, newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      // verify there are now 4 commits
      timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();
      assertEquals(4, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(), "Expecting four commits.");
      assertEquals(timeline.lastInstant().get().requestedTime(), newCommitTime, "Latest commit should be 004");

      // Check the entire dataset has 47 records still
      dataSet = getRecords();
      assertEquals(47, dataSet.count(), "Must contain 47 records");

      Row[] rows = (Row[]) dataSet.collect();
      int record1Count = 0;
      int record2Count = 0;
      for (Row row : rows) {
        if (row.getAs("_hoodie_record_key").equals(record1.getKey().getRecordKey())) {
          record1Count++;
          // assert each duplicate record is updated
          assertEquals(row.getAs("rider"), "rider-004");
          assertEquals(row.getAs("driver"), "driver-004");
        } else if (row.getAs("_hoodie_record_key").equals(record2.getKey().getRecordKey())) {
          record2Count++;
          // assert each duplicate record is updated
          assertEquals(row.getAs("rider"), "rider-004");
          assertEquals(row.getAs("driver"), "driver-004");
        } else {
          assertNotEquals(row.getAs("rider"), "rider-004");
          assertNotEquals(row.getAs("driver"), "rider-004");
        }
      }
      // Assert that id1 record count which has been updated to rider-004 and driver-004 is 22, which is the total
      // number of records with row_key id1
      assertEquals(22, record1Count);

      // Assert that id2 record count which has been updated to rider-004 and driver-004 is 21, which is the total
      // number of records with row_key id2
      assertEquals(21, record2Count);

      // Validate that all the records only reference the _latest_ base files as part of the
      // FILENAME_METADATA_FIELD payload (entailing that corresponding metadata is in-sync with
      // the state of the table
      HoodieTableFileSystemView tableView =
          getHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), HoodieTestTable.of(metaClient).listAllBaseFiles());

      Set<String> latestBaseFileNames = tableView.getLatestBaseFiles()
          .map(BaseFile::getFileName)
          .collect(Collectors.toSet());

      Set<Object> metadataFilenameFieldRefs = dataSet.collectAsList().stream()
          .map(row -> row.getAs(HoodieRecord.FILENAME_METADATA_FIELD))
          .collect(Collectors.toSet());

      assertEquals(latestBaseFileNames, metadataFilenameFieldRefs);
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testHoodieMergeHandleWriteStatMetrics(ExternalSpillableMap.DiskMapType diskMapType,
                                                    boolean isCompressionEnabled) throws Exception {
    // insert 100 records
    // Build a common config with diff configs
    Properties properties = new Properties();
    properties.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType.name());
    properties.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), String.valueOf(isCompressionEnabled));

    HoodieWriteConfig config = getConfigBuilder()
        .withProperties(properties)
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      List<WriteStatus> statusList = writeClient.insert(recordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc.parallelize(statusList, 1), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      // All records should be inserts into new parquet
      assertTrue(statusList.stream()
          .filter(status -> status.getStat().getPrevCommit() != HoodieWriteStat.NULL_COMMIT).count() > 0);
      // Num writes should be equal to the number of records inserted
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of records updated
      assertEquals(0,
          (long) statusList.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get());

      // Update all the 100 records
      metaClient = HoodieTableMetaClient.reload(metaClient);

      newCommitTime = "101";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      statusList = writeClient.upsert(updatedRecordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc.parallelize(statusList, 1), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      // All records should be upserts into existing parquet
      assertEquals(0,
          statusList.stream().filter(status -> status.getStat().getPrevCommit() == HoodieWriteStat.NULL_COMMIT).count());
      // Num writes should be equal to the number of records inserted
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of records updated
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      assertEquals(0,
          (long) statusList.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get());

      newCommitTime = "102";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> allRecords = dataGen.generateInserts(newCommitTime, 100);
      allRecords.addAll(updatedRecords);
      JavaRDD<HoodieRecord> allRecordsRDD = jsc.parallelize(allRecords, 1);
      statusList = writeClient.upsert(allRecordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc.parallelize(statusList, 1), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      // All records should be upserts into existing parquet (with inserts as updates small file handled)
      assertEquals(0, (long) statusList.stream()
          .filter(status -> status.getStat().getPrevCommit() == HoodieWriteStat.NULL_COMMIT).count());
      // Num writes should be equal to the total number of records written
      assertEquals(200,
          (long) statusList.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of records updated (including inserts converted as updates)
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get());
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      assertEquals(100,
          (long) statusList.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get());
      // Verify all records have location set
      statusList.forEach(writeStatus -> {
        writeStatus.getWrittenRecordDelegates().forEach(r -> {
          // Ensure New Location is set
          assertTrue(r.getNewLocation().isPresent());
        });
      });
    }
  }

  private Dataset<Row> getRecords() {
    // Check the entire dataset has 8 records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = Paths.get(basePath, dataGen.getPartitionPaths()[i], "*").toString();
    }
    Dataset<Row> dataSet =
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths);
    return dataSet;
  }

  protected HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withBulkInsertParallelism(2).withWriteStatusClass(TestWriteStatus.class);
  }

  private static Stream<Arguments> testArguments() {
    // Arg1: ExternalSpillableMap Type, Arg2: isDiskMapCompressionEnabled
    return Stream.of(
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true)
    );
  }

  /**
   * Overridden so that we can capture and inspect all success records.
   */
  public static class TestWriteStatus extends WriteStatus {

    public TestWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
      // Track Success Records
      super(true, failureFraction);
    }
  }
}
