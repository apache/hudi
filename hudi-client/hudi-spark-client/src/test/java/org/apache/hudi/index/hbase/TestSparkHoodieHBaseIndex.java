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

package org.apache.hudi.index.hbase;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
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
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Note :: HBaseTestingUtility is really flaky with issues where the HbaseMiniCluster fails to shutdown across tests,
 * (see one problem here : https://issues.apache.org/jira/browse/HBASE-15835). Hence, the need to use
 * {@link MethodOrderer.Alphanumeric} to make sure the tests run in order. Please alter the order of tests running carefully.
 */
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
@Tag("functional")
public class TestSparkHoodieHBaseIndex extends SparkClientFunctionalTestHarness {

  private static final String TABLE_NAME = "test_table";
  private static HBaseTestingUtility utility;
  private static Configuration hbaseConfig;

  private Configuration hadoopConf;
  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;
  private HoodieSparkEngineContext context;
  private String basePath;

  @BeforeAll
  public static void init() throws Exception {
    // Initialize HbaseMiniCluster
    hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("zookeeper.znode.parent", "/hudi-hbase-test");

    utility = new HBaseTestingUtility(hbaseConfig);
    utility.startMiniCluster();
    hbaseConfig = utility.getConnection().getConfiguration();
    utility.createTable(TableName.valueOf(TABLE_NAME), Bytes.toBytes("_s"),2);
  }

  @AfterAll
  public static void clean() throws Exception {
    utility.shutdownMiniHBaseCluster();
    utility.shutdownMiniDFSCluster();
    utility.shutdownMiniMapReduceCluster();
    // skip shutdownZkCluster due to localhost connection refused issue
    utility = null;
  }

  @BeforeEach
  public void setUp() throws Exception {
    hadoopConf = jsc().hadoopConfiguration();
    hadoopConf.addResource(utility.getConfiguration());
    // reInit the context here to keep the hadoopConf the same with that in this class
    context = new HoodieSparkEngineContext(jsc());
    basePath = utility.getDataTestDirOnTestFS(TABLE_NAME).toString();
    metaClient = getHoodieMetaClient(hadoopConf, basePath);
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void cleanUpTableData() throws IOException {
    utility.cleanupDataTestDirOnTestFS(TABLE_NAME);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testSimpleTagLocationAndUpdate(HoodieTableType tableType) throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);

    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
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

      // Now tagLocation for these records, hbaseIndex should not tag them since commit never occurred
      JavaRDD<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
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
  public void testTagLocationAndPartitionPathUpdate() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    final String oldPartitionPath = "1970/01/01";
    final String emptyHoodieRecordPayloadClassName = EmptyHoodieRecordPayload.class.getName();

    List<HoodieRecord> newRecords = dataGen.generateInserts(newCommitTime, numRecords);
    List<HoodieRecord> oldRecords = new LinkedList();
    for (HoodieRecord newRecord: newRecords) {
      HoodieKey key = new HoodieKey(newRecord.getRecordKey(), oldPartitionPath);
      HoodieRecord hoodieRecord = new HoodieAvroRecord(key, (HoodieRecordPayload) newRecord.getData());
      oldRecords.add(hoodieRecord);
    }

    JavaRDD<HoodieRecord> newWriteRecords = jsc().parallelize(newRecords, 1);
    JavaRDD<HoodieRecord> oldWriteRecords = jsc().parallelize(oldRecords, 1);

    HoodieWriteConfig config = getConfig(true, false);
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(getConfig(true, false));

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      // allowed path change test
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      JavaRDD<HoodieRecord> oldHoodieRecord = tagLocation(index, oldWriteRecords, hoodieTable);
      assertEquals(0, oldHoodieRecord.filter(record -> record.isCurrentLocationKnown()).count());
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(oldWriteRecords, newCommitTime);
      writeClient.commit(newCommitTime, writeStatues);
      assertNoWriteErrors(writeStatues.collect());
      updateLocation(index, writeStatues, hoodieTable);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> taggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      assertEquals(numRecords * 2L, taggedRecords.stream().count());
      // Verify the number of deleted records
      assertEquals(numRecords, taggedRecords.stream().filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
          && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClassName)).count());
      // Verify the number of inserted records
      assertEquals(numRecords, taggedRecords.stream().filter(record -> !record.getKey().getPartitionPath().equals(oldPartitionPath)).count());

      // not allowed path change test
      index = new SparkHoodieHBaseIndex(getConfig(false, false));
      List<HoodieRecord> notAllowPathChangeRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      assertEquals(numRecords, notAllowPathChangeRecords.stream().count());
      assertEquals(numRecords, taggedRecords.stream().filter(hoodieRecord -> hoodieRecord.isCurrentLocationKnown()
          && hoodieRecord.getKey().getPartitionPath().equals(oldPartitionPath)).count());
    }
  }

  @Test
  public void testTagLocationAndDuplicateUpdate() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
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

    writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, hbaseIndex should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    List<HoodieRecord> taggedRecords = tagLocation(index, writeRecords, hoodieTable).collect();
    assertEquals(numRecords, taggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(numRecords, taggedRecords.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(numRecords, taggedRecords.stream().filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
  }

  @Test
  public void testTagLocationAndPartitionPathUpdateWithExplicitRollback() throws Exception {
    final int numRecords = 10;
    final String oldPartitionPath = "1970/01/01";
    final String emptyHoodieRecordPayloadClasssName = EmptyHoodieRecordPayload.class.getName();
    HoodieWriteConfig config = getConfigBuilder(100,  true, true).withRollbackUsingMarkers(false).build();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      final String firstCommitTime = writeClient.startCommit();
      List<HoodieRecord> newRecords = dataGen.generateInserts(firstCommitTime, numRecords);
      List<HoodieRecord> oldRecords = new LinkedList();
      for (HoodieRecord newRecord: newRecords) {
        HoodieKey key = new HoodieKey(newRecord.getRecordKey(), oldPartitionPath);
        HoodieRecord hoodieRecord = new HoodieAvroRecord(key, (HoodieRecordPayload) newRecord.getData());
        oldRecords.add(hoodieRecord);
      }
      JavaRDD<HoodieRecord> newWriteRecords = jsc().parallelize(newRecords, 1);
      JavaRDD<HoodieRecord> oldWriteRecords = jsc().parallelize(oldRecords, 1);
      // first commit old record
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> beforeFirstTaggedRecords = tagLocation(index, oldWriteRecords, hoodieTable).collect();
      JavaRDD<WriteStatus> oldWriteStatues = writeClient.upsert(oldWriteRecords, firstCommitTime);
      updateLocation(index, oldWriteStatues, hoodieTable);
      writeClient.commit(firstCommitTime, oldWriteStatues);
      List<HoodieRecord> afterFirstTaggedRecords = tagLocation(index, oldWriteRecords, hoodieTable).collect();

      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      final String secondCommitTime = writeClient.startCommit();
      List<HoodieRecord> beforeSecondTaggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      JavaRDD<WriteStatus> newWriteStatues = writeClient.upsert(newWriteRecords, secondCommitTime);
      updateLocation(index, newWriteStatues, hoodieTable);
      writeClient.commit(secondCommitTime, newWriteStatues);
      List<HoodieRecord> afterSecondTaggedRecords = tagLocation(index, newWriteRecords, hoodieTable).collect();
      writeClient.rollback(secondCommitTime);
      List<HoodieRecord> afterRollback = tagLocation(index, newWriteRecords, hoodieTable).collect();

      // Verify the first commit
      assertEquals(numRecords, beforeFirstTaggedRecords.stream().filter(record -> record.getCurrentLocation() == null).count());
      assertEquals(numRecords, afterFirstTaggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
      // Verify the second commit
      assertEquals(numRecords, beforeSecondTaggedRecords.stream()
              .filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
                      && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClasssName)).count());
      assertEquals(numRecords * 2, beforeSecondTaggedRecords.stream().count());
      assertEquals(numRecords, afterSecondTaggedRecords.stream().count());
      assertEquals(numRecords, afterSecondTaggedRecords.stream().filter(record -> !record.getKey().getPartitionPath().equals(oldPartitionPath)).count());
      // Verify the rollback
      // If an exception occurs after hbase writes the index and the index does not roll back,
      // the currentLocation information will not be returned.
      assertEquals(numRecords, afterRollback.stream().filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
              && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClasssName)).count());
      assertEquals(numRecords * 2, beforeSecondTaggedRecords.stream().count());
      assertEquals(numRecords, afterRollback.stream().filter(HoodieRecord::isCurrentLocationKnown)
              .filter(record -> record.getCurrentLocation().getInstantTime().equals(firstCommitTime)).count());
    }
  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfigBuilder(100, false, false)
        .withRollbackUsingMarkers(false).build();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    final String newCommitTime = writeClient.startCommit();
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    // Now tagLocation for these records, hbaseIndex should tag them
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
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    List<HoodieRecord> records3 = tagLocation(index, writeRecords, hoodieTable).collect();
    assertEquals(0, records3.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(0, records3.stream().filter(record -> record.getCurrentLocation() != null).count());
  }

  /*
   * Test case to verify that for taglocation entries present in HBase, if the corresponding commit instant is missing
   * in timeline and the commit is not archived, taglocation would reset the current record location to null.
   */
  @Test
  public void testSimpleTagLocationWithInvalidCommit() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfigBuilder(100, false, false).withRollbackUsingMarkers(false).build();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
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

    // Now tagLocation for the valid records, hbaseIndex should tag them
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 199);

    // tagLocation for the invalid record - commit is not present in timeline due to rollback.
    JavaRDD<HoodieRecord> javaRDD2 = tagLocation(index, invalidWriteRecords, hoodieTable);
    assert (javaRDD2.collect().size() == 1);   // one record present
    assert (javaRDD2.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 0); // it is not tagged
  }

  /*
   * Test case to verify that taglocation() uses the commit timeline to validate the commitTS stored in hbase.
   * When CheckIfValidCommit() in HbaseIndex uses the incorrect timeline filtering, this test would fail.
   */
  @Test
  public void testEnsureTagLocationUsesCommitTimeline() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfigBuilder(100, false, false)
        .withRollbackUsingMarkers(false).build();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    String commitTime1 = writeClient.startCommit();
    JavaRDD<HoodieRecord> writeRecords1 = generateAndCommitRecords(writeClient, 20, commitTime1);

    // rollback the commit - leaves a clean file in timeline.
    writeClient.rollback(commitTime1);

    // create a second commit with 20 records
    metaClient = HoodieTableMetaClient.reload(metaClient);
    generateAndCommitRecords(writeClient, 20);

    // Now tagLocation for the first set of rolledback records, hbaseIndex should tag them
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords1, hoodieTable);
    assert (javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 20);
  }

  private JavaRDD<HoodieRecord>  generateAndCommitRecords(SparkRDDWriteClient writeClient, int numRecs) throws Exception {
    String commitTime = writeClient.startCommit();
    return generateAndCommitRecords(writeClient, numRecs, commitTime);
  }

  private JavaRDD<HoodieRecord> generateAndCommitRecords(SparkRDDWriteClient writeClient,
                                                         int numRecs, String commitTime) throws Exception {
    // first batch of records
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, numRecs);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, commitTime);
    assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    writeClient.commit(commitTime, writeStatues);

    return writeRecords;
  }

  // Verify hbase is tagging records belonging to an archived commit as valid.
  @Test
  public void testHbaseTagLocationForArchivedCommits() throws Exception {
    // Load to memory
    Map<String, String> params = new HashMap<String, String>();
    params.put(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "1");
    params.put(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3");
    params.put(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2");
    HoodieWriteConfig config = getConfigBuilder(100, false, false).withProps(params).build();

    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    // make first commit with 20 records
    JavaRDD<HoodieRecord> writeRecords1 = generateAndCommitRecords(writeClient, 20);

    // Make 3 additional commits, so that first commit is archived
    for (int nCommit = 0; nCommit < 3; nCommit++) {
      generateAndCommitRecords(writeClient, 20);
    }

    // tagLocation for the first set of records (for the archived commit), hbaseIndex should tag them as valid
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords1, hoodieTable);
    assertEquals(20, javaRDD1.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
  }

  @Test
  public void testTotalGetsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = mock(Connection.class);
    HTable table = mock(HTable.class);
    when(hbaseConnection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);
    when(table.get((List<Get>) any())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Insert 250 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, hbaseIndex should tag them
    tagLocation(index, writeRecords, hoodieTable);

    // 3 batches should be executed given batchSize = 100 and parallelism = 1
    verify(table, times(3)).get((List<Get>) any());

  }

  @Test
  public void testTotalPutsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = mock(Connection.class);
    HTable table = mock(HTable.class);
    when(hbaseConnection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);
    when(table.get((List<Get>) any())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    // Get all the files generated
    int numberOfDataFileIds = (int) writeStatues.map(status -> status.getFileId()).distinct().count();

    updateLocation(index, writeStatues, hoodieTable);
    // 3 batches should be executed given batchSize = 100 and <=numberOfDataFileIds getting updated,
    // so each fileId ideally gets updates
    verify(table, atMost(numberOfDataFileIds)).put((List<Put>) any());
  }

  @Test
  public void testsHBasePutAccessParallelism() {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    final JavaRDD<WriteStatus> writeStatusRDD = jsc().parallelize(
        Arrays.asList(
            getSampleWriteStatus(0, 2),
            getSampleWriteStatus(2, 3),
            getSampleWriteStatus(4, 3),
            getSampleWriteStatus(6, 3),
            getSampleWriteStatus(8, 0)),
        10);
    final Tuple2<Long, Integer> tuple = index.getHBasePutAccessParallelism(writeStatusRDD);
    final int hbasePutAccessParallelism = Integer.parseInt(tuple._2.toString());
    final int hbaseNumPuts = Integer.parseInt(tuple._1.toString());
    assertEquals(10, writeStatusRDD.getNumPartitions());
    assertEquals(4, hbasePutAccessParallelism);
    assertEquals(20, hbaseNumPuts);
  }

  @Test
  public void testsWriteStatusPartitioner() {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    int parallelism = 4;
    final JavaRDD<WriteStatus> writeStatusRDD = jsc().parallelize(
        Arrays.asList(
            getSampleWriteStatusWithFileId(0, 2),
            getSampleWriteStatusWithFileId(2, 3),
            getSampleWriteStatusWithFileId(4, 3),
            getSampleWriteStatusWithFileId(0, 3),
            getSampleWriteStatusWithFileId(11, 0)), parallelism);

    final Map<String, Integer> fileIdPartitionMap = index.mapFileWithInsertsToUniquePartition(writeStatusRDD);
    int numWriteStatusWithInserts = (int) index.getHBasePutAccessParallelism(writeStatusRDD)._2;
    JavaRDD<WriteStatus> partitionedRDD = writeStatusRDD.mapToPair(w -> new Tuple2<>(w.getFileId(), w))
        .partitionBy(new SparkHoodieHBaseIndex
            .WriteStatusPartitioner(fileIdPartitionMap,
            numWriteStatusWithInserts)).map(w -> w._2());
    assertEquals(numWriteStatusWithInserts, partitionedRDD.getNumPartitions());
    int[] partitionIndexesBeforeRepartition = writeStatusRDD.partitions().stream().mapToInt(p -> p.index()).toArray();
    assertEquals(parallelism, partitionIndexesBeforeRepartition.length);

    int[] partitionIndexesAfterRepartition = partitionedRDD.partitions().stream().mapToInt(p -> p.index()).toArray();
    // there should be 3 partitions after repartition, because only 3 writestatus has
    // inserts (numWriteStatusWithInserts)
    assertEquals(numWriteStatusWithInserts, partitionIndexesAfterRepartition.length);

    List<WriteStatus>[] writeStatuses = partitionedRDD.collectPartitions(partitionIndexesAfterRepartition);
    for (List<WriteStatus> list : writeStatuses) {
      int count = 0;
      for (WriteStatus w: list) {
        if (w.getStat().getNumInserts() > 0)   {
          count++;
        }
      }
      assertEquals(1, count);
    }
  }

  @Test
  public void testsWriteStatusPartitionerWithNoInserts() {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    int parallelism = 3;
    final JavaRDD<WriteStatus> writeStatusRDD = jsc().parallelize(
        Arrays.asList(
            getSampleWriteStatusWithFileId(0, 2),
            getSampleWriteStatusWithFileId(0, 3),
            getSampleWriteStatusWithFileId(0, 0)), parallelism);

    final Map<String, Integer> fileIdPartitionMap = index.mapFileWithInsertsToUniquePartition(writeStatusRDD);
    int numWriteStatusWithInserts = (int) index.getHBasePutAccessParallelism(writeStatusRDD)._2;
    JavaRDD<WriteStatus> partitionedRDD = writeStatusRDD.mapToPair(w -> new Tuple2<>(w.getFileId(), w))
        .partitionBy(new SparkHoodieHBaseIndex
            .WriteStatusPartitioner(fileIdPartitionMap,
            numWriteStatusWithInserts)).map(w -> w._2());
    assertEquals(numWriteStatusWithInserts, partitionedRDD.getNumPartitions());
    int[] partitionIndexesBeforeRepartition = writeStatusRDD.partitions().stream().mapToInt(p -> p.index()).toArray();
    assertEquals(parallelism, partitionIndexesBeforeRepartition.length);

    int[] partitionIndexesAfterRepartition = partitionedRDD.partitions().stream().mapToInt(p -> p.index()).toArray();
    // there should be 3 partitions after repartition, because only 3 writestatus has inserts
    // (numWriteStatusWithInserts)
    assertEquals(numWriteStatusWithInserts, partitionIndexesAfterRepartition.length);
    assertEquals(partitionIndexesBeforeRepartition.length, parallelism);

  }

  private WriteStatus getSampleWriteStatusWithFileId(final int numInserts, final int numUpdateWrites) {
    final WriteStatus writeStatus = new WriteStatus(false, 0.0);
    HoodieWriteStat hoodieWriteStat = new HoodieWriteStat();
    hoodieWriteStat.setNumInserts(numInserts);
    hoodieWriteStat.setNumUpdateWrites(numUpdateWrites);
    writeStatus.setStat(hoodieWriteStat);
    writeStatus.setFileId(UUID.randomUUID().toString());
    return writeStatus;
  }

  @Test
  public void testsHBasePutAccessParallelismWithNoInserts() {
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    final JavaRDD<WriteStatus> writeStatusRDD =
        jsc().parallelize(Arrays.asList(getSampleWriteStatus(0, 2), getSampleWriteStatus(0, 1)), 10);
    final Tuple2<Long, Integer> tuple = index.getHBasePutAccessParallelism(writeStatusRDD);
    final int hbasePutAccessParallelism = Integer.parseInt(tuple._2.toString());
    final int hbaseNumPuts = Integer.parseInt(tuple._1.toString());
    assertEquals(10, writeStatusRDD.getNumPartitions());
    assertEquals(0, hbasePutAccessParallelism);
    assertEquals(0, hbaseNumPuts);
  }

  @Test
  public void testSmallBatchSize() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig(2);
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
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

      // Now tagLocation for these records, hbaseIndex should not tag them since it was a failed
      // commit
      JavaRDD<HoodieRecord> records2 = tagLocation(index, writeRecords, hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
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
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
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
      writeClient.commit(newCommitTime, writeStatues);

      // Now tagLocation for these records, hbaseIndex should tag them correctly
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

  private WriteStatus getSampleWriteStatus(final int numInserts, final int numUpdateWrites) {
    final WriteStatus writeStatus = new WriteStatus(false, 0.1);
    HoodieWriteStat hoodieWriteStat = new HoodieWriteStat();
    hoodieWriteStat.setNumInserts(numInserts);
    hoodieWriteStat.setNumUpdateWrites(numUpdateWrites);
    writeStatus.setStat(hoodieWriteStat);
    return writeStatus;
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder(100, false, false).build();
  }

  private HoodieWriteConfig getConfig(int hbaseIndexBatchSize) {
    return getConfigBuilder(hbaseIndexBatchSize, false, false).build();
  }

  private HoodieWriteConfig getConfig(boolean updatePartitionPath, boolean rollbackSync) {
    return getConfigBuilder(100,  updatePartitionPath, rollbackSync).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(int hbaseIndexBatchSize, boolean updatePartitionPath, boolean rollbackSync) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1).withDeleteParallelism(1)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withAutoCommit(false).withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
            .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder()
                .hbaseZkPort(Integer.parseInt(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
                .hbaseIndexPutBatchSizeAutoCompute(true)
                .hbaseZkZnodeParent(hbaseConfig.get("zookeeper.znode.parent", ""))
                .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(TABLE_NAME)
                .hbaseIndexUpdatePartitionPath(updatePartitionPath)
                .hbaseIndexRollbackSync(rollbackSync)
                .hbaseIndexGetBatchSize(hbaseIndexBatchSize).build())
            .build());
  }
}
