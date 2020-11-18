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
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.FunctionalTestHarness;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
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
public class TestHBaseIndex extends FunctionalTestHarness {

  private static final String TABLE_NAME = "test_table";
  private static HBaseTestingUtility utility;
  private static Configuration hbaseConfig;

  private Configuration hadoopConf;
  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;

  @AfterAll
  public static void clean() throws Exception {
    if (utility != null) {
      utility.deleteTable(TABLE_NAME);
      utility.shutdownMiniCluster();
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    // Initialize HbaseMiniCluster
    hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("zookeeper.znode.parent", "/hudi-hbase-test");

    utility = new HBaseTestingUtility(hbaseConfig);
    utility.startMiniCluster();
    hbaseConfig = utility.getConnection().getConfiguration();
    utility.createTable(TableName.valueOf(TABLE_NAME), Bytes.toBytes("_s"));
  }

  @BeforeEach
  public void setUp() throws Exception {
    hadoopConf = jsc().hadoopConfiguration();
    hadoopConf.addResource(utility.getConfiguration());
    metaClient = getHoodieMetaClient(hadoopConf, basePath());
    dataGen = new HoodieTestDataGenerator();
  }

  @Test
  public void testSimpleTagLocationAndUpdateCOW() throws Exception {
    testSimpleTagLocationAndUpdate(HoodieTableType.COPY_ON_WRITE);
  }

  @Test void testSimpleTagLocationAndUpdateMOR() throws Exception {
    testSimpleTagLocationAndUpdate(HoodieTableType.MERGE_ON_READ);
  }

  public void testSimpleTagLocationAndUpdate(HoodieTableType tableType) throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath(), tableType);

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
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, context(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, hbaseIndex should not tag them since commit never occurred
      JavaRDD<HoodieRecord> records2 = index.tagLocation(writeRecords, context(), hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
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
    final String emptyHoodieRecordPayloadClasssName = EmptyHoodieRecordPayload.class.getName();

    List<HoodieRecord> newRecords = dataGen.generateInserts(newCommitTime, numRecords);
    List<HoodieRecord> oldRecords = new LinkedList();
    for (HoodieRecord newRecord: newRecords) {
      HoodieKey key = new HoodieKey(newRecord.getRecordKey(), oldPartitionPath);
      HoodieRecord hoodieRecord = new HoodieRecord(key, newRecord.getData());
      oldRecords.add(hoodieRecord);
    }

    JavaRDD<HoodieRecord> newWriteRecords = jsc().parallelize(newRecords, 1);
    JavaRDD<HoodieRecord> oldWriteRecords = jsc().parallelize(oldRecords, 1);

    HoodieWriteConfig config = getConfig(true);
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(getConfig(true));

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      // allowed path change test
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      JavaRDD<HoodieRecord> oldHoodieRecord = index.tagLocation(oldWriteRecords, context, hoodieTable);
      assertEquals(0, oldHoodieRecord.filter(record -> record.isCurrentLocationKnown()).count());
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(oldWriteRecords, newCommitTime);
      writeClient.commit(newCommitTime, writeStatues);
      assertNoWriteErrors(writeStatues.collect());
      index.updateLocation(writeStatues, context, hoodieTable);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> taggedRecords = index.tagLocation(newWriteRecords, context, hoodieTable).collect();
      assertEquals(numRecords * 2L, taggedRecords.stream().count());
      // Verify the number of deleted records
      assertEquals(numRecords, taggedRecords.stream().filter(record -> record.getKey().getPartitionPath().equals(oldPartitionPath)
          && record.getData().getClass().getName().equals(emptyHoodieRecordPayloadClasssName)).count());
      // Verify the number of inserted records
      assertEquals(numRecords, taggedRecords.stream().filter(record -> !record.getKey().getPartitionPath().equals(oldPartitionPath)).count());

      // not allowed path change test
      index = new SparkHoodieHBaseIndex<>(getConfig(false));
      List<HoodieRecord> notAllowPathChangeRecords = index.tagLocation(newWriteRecords, context, hoodieTable).collect();
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
    index.tagLocation(writeRecords, context(), hoodieTable);

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
    List<HoodieRecord> taggedRecords = index.tagLocation(writeRecords, context(), hoodieTable).collect();
    assertEquals(numRecords, taggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(numRecords, taggedRecords.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(numRecords, taggedRecords.stream().filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfig();
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
    List<HoodieRecord> records2 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
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
    List<HoodieRecord> records3 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
    assertEquals(0, records3.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(0, records3.stream().filter(record -> record.getCurrentLocation() != null).count());
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
    index.tagLocation(writeRecords, context(), hoodieTable);

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

    index.updateLocation(writeStatues, context(), hoodieTable);
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
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, context(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());
      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, hbaseIndex should not tag them since it was a failed
      // commit
      JavaRDD<HoodieRecord> records2 = index.tagLocation(writeRecords, context(), hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
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
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, context(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());
      writeClient.commit(newCommitTime, writeStatues);

      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(config, context, metaClient);
      List<HoodieRecord> records2 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
      assertEquals(numRecords, records2.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records2.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(numRecords, records2.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());

      // Delete all records. This has to be done directly as deleting index entries
      // is not implemented via HoodieWriteClient
      JavaRDD<WriteStatus> deleteWriteStatues = writeStatues.map(w -> {
        WriteStatus newWriteStatus = new WriteStatus(true, 1.0);
        w.getWrittenRecords().forEach(r -> newWriteStatus.markSuccess(new HoodieRecord(r.getKey(), null), Option.empty()));
        assertEquals(w.getTotalRecords(), newWriteStatus.getTotalRecords());
        newWriteStatus.setStat(new HoodieWriteStat());
        return newWriteStatus;
      });
      JavaRDD<WriteStatus> deleteStatus = index.updateLocation(deleteWriteStatues, context(), hoodieTable);
      assertEquals(deleteStatus.count(), deleteWriteStatues.count());
      assertNoWriteErrors(deleteStatus.collect());

      // Ensure no records can be tagged
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, context(), hoodieTable).collect();
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
    return getConfigBuilder(100, false).build();
  }

  private HoodieWriteConfig getConfig(int hbaseIndexBatchSize) {
    return getConfigBuilder(hbaseIndexBatchSize, false).build();
  }

  private HoodieWriteConfig getConfig(boolean updatePartitionPath) {
    return getConfigBuilder(100,  updatePartitionPath).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(int hbaseIndexBatchSize, boolean updatePartitionPath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath()).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
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
                .hbaseIndexGetBatchSize(hbaseIndexBatchSize).build())
            .build());
  }
}
