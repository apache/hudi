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

package org.apache.hudi.index.redis;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieRedisIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.TestRedisIndexUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.TestRedisIndexUtils.cleanRedisServer;
import static org.apache.hudi.testutils.TestRedisIndexUtils.initRedisServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRedisIndex extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieRedisIndex.class);

  private static final String TABLE_NAME = "test_table";

  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    initMetaClient();
    // init redis server
    initRedisServer();
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    // close redis server
    cleanRedisServer();
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testSimpleTagLocationAndUpdate(HoodieTableType tableType) throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);

    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = makeConfig();
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // auto commit
      JavaRDD<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(10, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now tagLocation for these records, redisIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records3 = tagLocation(index, writeRecords, hoodieTable).collect();
      assertEquals(numRecords, records3.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records3.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(numRecords, records3.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    }
  }

  @Test
  public void testTagLocationAndPartitionPathUpdate() {
    final String newCommitTime = "001";
    final int numRecords = 10;
    final String oldPartitionPath = "1970/01/01";

    List<HoodieRecord> newRecords = dataGen.generateInserts(newCommitTime, numRecords);
    List<HoodieRecord> oldRecords = new LinkedList();
    for (HoodieRecord newRecord : newRecords) {
      HoodieKey key = new HoodieKey(newRecord.getRecordKey(), oldPartitionPath);
      HoodieRecord hoodieRecord = new HoodieAvroRecord(key, (HoodieRecordPayload) newRecord.getData());
      oldRecords.add(hoodieRecord);
    }

    JavaRDD<HoodieRecord> newWriteRecords = jsc.parallelize(newRecords, 1);
    JavaRDD<HoodieRecord> oldWriteRecords = jsc.parallelize(oldRecords, 1);

    HoodieWriteConfig config = makeConfig();
    HoodieRedisIndex index = new HoodieRedisIndex(config);

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      // allowed path change test
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      JavaRDD<HoodieRecord> oldHoodieRecord = tagLocation(index, oldWriteRecords, hoodieTable);
      assertEquals(0, oldHoodieRecord.filter(record -> record.isCurrentLocationKnown()).count());
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(oldWriteRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());
      updateLocation(index, writeStatues, hoodieTable);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> taggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      assertEquals(numRecords, taggedRecords.size());

      // not allowed path change test
      index = new HoodieRedisIndex(makeConfig());
      List<HoodieRecord> notAllowPathChangeRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      assertEquals(numRecords, (long) notAllowPathChangeRecords.size());
      assertEquals(0, taggedRecords.stream().filter(hoodieRecord -> hoodieRecord.isCurrentLocationKnown()
          && hoodieRecord.getKey().getPartitionPath().equals(oldPartitionPath)).count());
    }
  }

  @Test
  public void testTagLocationAndDuplicateUpdate() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = makeConfig();
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(newCommitTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    tagLocation(index, writeRecords, hoodieTable);

    // Duplicate upsert and ensure correctness is maintained
    // We are trying to approximately imitate the case when the RDD is recomputed. For RDD creating, driver code is not
    // recomputed. This includes the state transitions. We need to delete the inflight instance so that subsequent
    // upsert will not run into conflicts.
    metaClient.getFs().delete(new Path(metaClient.getMetaPath(), "001.inflight"));

    // Now tagLocation for these records, redisIndex should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    List<HoodieRecord> taggedRecords = tagLocation(index, writeRecords, hoodieTable).collect();
    assertEquals(numRecords, taggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(numRecords, taggedRecords.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(numRecords, taggedRecords.stream().filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
  }

  @Test
  public void testTagLocationAndPartitionPathUpdateWithExplicitRollback() {
    final int numRecords = 10;
    final String oldPartitionPath = "1970/01/01";
    final String emptyHoodieRecordPayloadClasssName = EmptyHoodieRecordPayload.class.getName();
    HoodieWriteConfig config = makeConfig(false);
    HoodieRedisIndex index = new HoodieRedisIndex(config);

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      final String firstCommitTime = writeClient.startCommit();
      List<HoodieRecord> newRecords = dataGen.generateInserts(firstCommitTime, numRecords);
      List<HoodieRecord> oldRecords = new LinkedList();
      for (HoodieRecord newRecord : newRecords) {
        HoodieKey key = new HoodieKey(newRecord.getRecordKey(), oldPartitionPath);
        HoodieRecord hoodieRecord = new HoodieAvroRecord(key, (HoodieRecordPayload) newRecord.getData());
        oldRecords.add(hoodieRecord);
      }
      JavaRDD<HoodieRecord> newWriteRecords = jsc.parallelize(newRecords, 1);
      JavaRDD<HoodieRecord> oldWriteRecords = jsc.parallelize(oldRecords, 1);
      // first commit old record
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> beforeFirstTaggedRecords = tagLocation(index, oldWriteRecords, hoodieTable).collect();
      JavaRDD<WriteStatus> oldWriteStatues = writeClient.upsert(oldWriteRecords, firstCommitTime);
      updateLocation(index, oldWriteStatues, hoodieTable);
      List<HoodieRecord> afterFirstTaggedRecords = tagLocation(index, oldWriteRecords, hoodieTable).collect();

      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      final String secondCommitTime = writeClient.startCommit();
      List<HoodieRecord> beforeSecondTaggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      JavaRDD<WriteStatus> newWriteStatues = writeClient.upsert(newWriteRecords, secondCommitTime);
      updateLocation(index, newWriteStatues, hoodieTable);
      List<HoodieRecord> afterSecondTaggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      writeClient.rollback(secondCommitTime);
      List<HoodieRecord> afterRollback = tagLocation(index, newWriteRecords, hoodieTable).collect();

      // Verify the first commit
      assertEquals(numRecords, beforeFirstTaggedRecords.stream().filter(record -> record.getCurrentLocation() == null).count());
      assertEquals(numRecords, afterFirstTaggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
      // Verify the second commit
      assertEquals(0, beforeSecondTaggedRecords.stream()
          .filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
              && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClasssName)).count());
      assertEquals(numRecords, (long) beforeSecondTaggedRecords.size());
      assertEquals(numRecords, (long) afterSecondTaggedRecords.size());
      assertEquals(numRecords, afterSecondTaggedRecords.stream().filter(record -> !record.getKey().getPartitionPath().equals(oldPartitionPath)).count());
      // Verify the rollback
      // If an exception occurs after redis writes the index and the index does not roll back,
      // the currentLocation information will not be returned.
      assertEquals(0, afterRollback.stream().filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
          && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClasssName)).count());
      assertEquals(numRecords, (long) beforeSecondTaggedRecords.size());
      assertEquals(0, afterRollback.stream().filter(HoodieRecord::isCurrentLocationKnown)
          .filter(record -> record.getCurrentLocation().getInstantTime().equals(firstCommitTime)).count());
    }
  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() {
    // Load to memory
    HoodieWriteConfig config = makeConfig(false);
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    final String newCommitTime = writeClient.startCommit();
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    // Now tagLocation for these records, redisIndex should tag them
    List<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable).collect();
    assertEquals(numRecords, records2.stream().filter(HoodieRecord::isCurrentLocationKnown).count());

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(WriteStatus::getFileId).collect();
    assertEquals(0, records2.stream().filter(record -> record.getCurrentLocation().getFileId() == null).count());
    List<String> taggedFileIds = records2.stream().map(record -> record.getCurrentLocation().getFileId()).distinct().collect(Collectors.toList());

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    // Now tagLocation for these records, redisIndex should not tag them since it was a rolled
    // back commit
    List<HoodieRecord> records3 = tagLocation(index, writeRecords, hoodieTable).collect();
    assertEquals(numRecords, records3.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(numRecords, records3.stream().filter(record -> record.getCurrentLocation() != null).count());
  }

  /*
   * Test case to verify that for taglocation entries present in HBase, if the corresponding commit instant is missing
   * in timeline and the commit is not archived, taglocation would reset the current record location to null.
   */
  @Test
  public void testSimpleTagLocationWithInvalidCommit() throws Exception {
    // Load to memory
    HoodieWriteConfig config = makeConfig(false);
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    String newCommitTime = writeClient.startCommit();
    // make a commit with 199 records
    JavaRDD<HoodieRecord> writeRecords = generateAndCommitRecords(writeClient, 199, newCommitTime);

    // make a second commit with a single record
    String invalidCommit = writeClient.startCommit();
    JavaRDD<HoodieRecord> invalidWriteRecords = generateAndCommitRecords(writeClient, 1, invalidCommit);

    // verify location is tagged.
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD0 = tagLocation(index, invalidWriteRecords, hoodieTable);
    assert (javaRDD0.collect().size() == 1);   // one record present
    assert (javaRDD0.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 1); // it is tagged
    assert (javaRDD0.collect().get(0).getCurrentLocation().getInstantTime().equals(invalidCommit));

    // rollback the invalid commit, so that hbase will be left with a stale entry.
    writeClient.rollback(invalidCommit);

    // Now tagLocation for the valid records, redisIndex should tag them
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 199);

    // tagLocation for the invalid record - commit is not present in timeline due to rollback.
    JavaRDD<HoodieRecord> javaRDD2 = tagLocation(index, invalidWriteRecords, hoodieTable);
    assert (javaRDD2.collect().size() == 1);   // one record present
    assert (javaRDD2.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 1); // it is not tagged
  }

  /*
   * Test case to verify that taglocation() uses the commit timeline to validate the commitTS stored in hbase.
   * When CheckIfValidCommit() in redisIndex uses the incorrect timeline filtering, this test would fail.
   */
  @Test
  public void testEnsureTagLocationUsesCommitTimeline() throws Exception {
    // Load to memory
    HoodieWriteConfig config = makeConfig(false);
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    String commitTime1 = writeClient.startCommit();
    JavaRDD<HoodieRecord> writeRecords1 = generateAndCommitRecords(writeClient, 20, commitTime1);

    // rollback the commit - leaves a clean file in timeline.
    writeClient.rollback(commitTime1);

    // create a second commit with 20 records
    metaClient = HoodieTableMetaClient.reload(metaClient);
    generateAndCommitRecords(writeClient, 20);

    // Now tagLocation for the first set of rolledback records, redisIndex should tag them
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords1, hoodieTable);
    assert (javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 20);
  }

  private JavaRDD<HoodieRecord> generateAndCommitRecords(SparkRDDWriteClient writeClient, int numRecs) throws Exception {
    String commitTime = writeClient.startCommit();
    return generateAndCommitRecords(writeClient, numRecs, commitTime);
  }

  private JavaRDD<HoodieRecord> generateAndCommitRecords(SparkRDDWriteClient writeClient,
                                                         int numRecs, String commitTime) throws Exception {
    // first batch of records
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, numRecs);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, commitTime);
    assertNoWriteErrors(writeStatues.collect());

    return writeRecords;
  }

  // Verify redis is tagging records belonging to an archived commit as valid.
  @Test
  public void testRedisTagLocationForArchivedCommits() throws Exception {
    // Load to memory
    Map<String, String> params = new HashMap<String, String>();
    params.put(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED.key(), "1");
    params.put(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP.key(), "3");
    params.put(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP.key(), "2");
    HoodieWriteConfig config = makeConfig(params);

    HoodieRedisIndex index = new HoodieRedisIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    // make first commit with 20 records
    JavaRDD<HoodieRecord> writeRecords1 = generateAndCommitRecords(writeClient, 20);

    // Make 3 additional commits, so that first commit is archived
    for (int nCommit = 0; nCommit < 3; nCommit++) {
      generateAndCommitRecords(writeClient, 20);
    }

    // tagLocation for the first set of records (for the archived commit), redisIndex should tag them as valid
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords1, hoodieTable);
    assertEquals(20, javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
  }

  @Test
  public void testSmallBatchSize() {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = makeConfig();
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());
      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, redisIndex should not tag them since it was a failed
      // commit
      JavaRDD<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(10, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now tagLocation for these records, redisIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records3 = tagLocation(index, writeRecords, hoodieTable).collect();
      assertEquals(numRecords, records3.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records3.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(numRecords, records3.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    }
  }

  @Test
  public void testDelete() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = makeConfig();
    HoodieRedisIndex index = new HoodieRedisIndex(config);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, redisIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable).collect();
      assertEquals(numRecords, records2.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records2.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(numRecords, records2.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());

      // Delete all records. This has to be done directly as deleting index entries
      // is not implemented via HoodieWriteClient
      JavaRDD<WriteStatus> deleteWriteStatues = writeStatues.map(w -> {
        WriteStatus newWriteStatus = new WriteStatus(true, 1.0);
        w.getWrittenRecords().forEach(r -> newWriteStatus.markSuccess(new HoodieAvroRecord(r.getKey(), null), Option.empty()));
        assertEquals(w.getTotalRecords(), newWriteStatus.getTotalRecords());
        newWriteStatus.setStat(new HoodieWriteStat());
        return newWriteStatus;
      });
      // if not for this caching, due to RDD chaining/lineage, first time update is called again when subsequent update is called.
      // So caching here to break the chain and so future update does not re-trigger update of older Rdd.
      deleteWriteStatues.cache();
      JavaRDD<WriteStatus> deleteStatus = updateLocation(index, deleteWriteStatues, hoodieTable);
      assertEquals(deleteStatus.count(), deleteWriteStatues.count());
      assertNoWriteErrors(deleteStatus.collect());

      // Ensure no records can be tagged
      List<HoodieRecord> records3 = tagLocation(index, writeRecords, hoodieTable).collect();
      assertEquals(0, records3.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records3.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(0, records3.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    }
  }

  private HoodieWriteConfig makeConfig() {
    return makeConfig(true, new HashMap<>());
  }

  private HoodieWriteConfig makeConfig(Boolean rollbackUsingMarkers) {
    return makeConfig(rollbackUsingMarkers, new HashMap<>());
  }

  private HoodieWriteConfig makeConfig(Map<String, String> params) {
    return makeConfig(true, params);
  }

  private HoodieWriteConfig makeConfig(boolean rollbackUsingMarkers, Map<String, String> params) {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    String address = TestRedisIndexUtils.HOST + ":" + TestRedisIndexUtils.PORT;
    if (params.isEmpty()) {
      return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .withParallelism(1, 1).withDeleteParallelism(1)
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
              .withInlineCompaction(false).build())
          .withRollbackUsingMarkers(rollbackUsingMarkers)
          .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
              .withIndexType(HoodieIndex.IndexType.REDIS)
              .withRedisIndexConfig(new HoodieRedisIndexConfig.Builder().address(address).build())
              .withIndexKeyField("_row_key").build()).build();
    } else {
      return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .withParallelism(1, 1).withDeleteParallelism(1)
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
              .withInlineCompaction(false).build())
          .withRollbackUsingMarkers(rollbackUsingMarkers)
          .withAutoCommit(true)
          .withProps(params)
          .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
              .withIndexType(HoodieIndex.IndexType.REDIS)
              .withRedisIndexConfig(new HoodieRedisIndexConfig.Builder().address(address).build())
              .withIndexKeyField("_row_key").build()).build();
    }
  }

  protected JavaRDD<WriteStatus> updateLocation(
      HoodieIndex index, JavaRDD<WriteStatus> writeStatus, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(
        index.updateLocation(HoodieJavaRDD.of(writeStatus), context, table));
  }
}
