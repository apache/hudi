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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
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
import java.util.List;
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
    HBaseIndex index = new HBaseIndex(config);
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, jsc(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, hbaseIndex should not tag them since commit never occurred
      JavaRDD<HoodieRecord> records2 = index.tagLocation(writeRecords, jsc(), hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
      assertEquals(numRecords, records3.stream().filter(record -> record.isCurrentLocationKnown()).count());
      assertEquals(numRecords, records3.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
      assertEquals(numRecords, records3.stream().filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
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
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(newCommitTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    index.tagLocation(writeRecords, jsc(), hoodieTable);

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
    hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
    List<HoodieRecord> taggedRecords = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
    assertEquals(numRecords, taggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(numRecords, taggedRecords.stream().map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(numRecords, taggedRecords.stream().filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = getHoodieWriteClient(config);

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
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
    // Now tagLocation for these records, hbaseIndex should tag them
    List<HoodieRecord> records2 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
    assertEquals(numRecords, records2.stream().filter(HoodieRecord::isCurrentLocationKnown).count());

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(WriteStatus::getFileId).collect();
    assertEquals(0, records2.stream().filter(record -> record.getCurrentLocation().getFileId() == null).count());
    List<String> taggedFileIds = records2.stream().map(record -> record.getCurrentLocation().getFileId()).distinct().collect(Collectors.toList());

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    List<HoodieRecord> records3 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
    assertEquals(0, records3.stream().filter(HoodieRecord::isCurrentLocationKnown).count());
    assertEquals(0, records3.stream().filter(record -> record.getCurrentLocation() != null).count());
  }

  @Test
  public void testTotalGetsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = mock(Connection.class);
    HTable table = mock(HTable.class);
    when(hbaseConnection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);
    when(table.get((List<Get>) any())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    HoodieWriteClient writeClient = getHoodieWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

    // Insert 250 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, hbaseIndex should tag them
    index.tagLocation(writeRecords, jsc(), hoodieTable);

    // 3 batches should be executed given batchSize = 100 and parallelism = 1
    verify(table, times(3)).get((List<Get>) any());

  }

  @Test
  public void testTotalPutsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = getHoodieWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

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

    index.updateLocation(writeStatues, jsc(), hoodieTable);
    // 3 batches should be executed given batchSize = 100 and <=numberOfDataFileIds getting updated,
    // so each fileId ideally gets updates
    verify(table, atMost(numberOfDataFileIds)).put((List<Put>) any());
  }

  @Test
  public void testsHBasePutAccessParallelism() {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    final JavaRDD<WriteStatus> writeStatusRDD = jsc().parallelize(
        Arrays.asList(getSampleWriteStatus(1, 2), getSampleWriteStatus(0, 3), getSampleWriteStatus(10, 0)), 10);
    final Tuple2<Long, Integer> tuple = index.getHBasePutAccessParallelism(writeStatusRDD);
    final int hbasePutAccessParallelism = Integer.parseInt(tuple._2.toString());
    final int hbaseNumPuts = Integer.parseInt(tuple._1.toString());
    assertEquals(10, writeStatusRDD.getNumPartitions());
    assertEquals(2, hbasePutAccessParallelism);
    assertEquals(11, hbaseNumPuts);
  }

  @Test
  public void testsHBasePutAccessParallelismWithNoInserts() {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
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
    HBaseIndex index = new HBaseIndex(config);
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, jsc(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());
      // Insert 200 records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, hbaseIndex should not tag them since it was a failed
      // commit
      JavaRDD<HoodieRecord> records2 = index.tagLocation(writeRecords, jsc(), hoodieTable);
      assertEquals(0, records2.filter(record -> record.isCurrentLocationKnown()).count());

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
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
    HBaseIndex index = new HBaseIndex(config);
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config);) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> records1 = index.tagLocation(writeRecords, jsc(), hoodieTable);
      assertEquals(0, records1.filter(record -> record.isCurrentLocationKnown()).count());

      // Insert records
      writeClient.startCommitWithTime(newCommitTime);
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());
      writeClient.commit(newCommitTime, writeStatues);

      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieTable.create(metaClient, config, hadoopConf);
      List<HoodieRecord> records2 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
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
      JavaRDD<WriteStatus> deleteStatus = index.updateLocation(deleteWriteStatues, jsc(), hoodieTable);
      assertEquals(deleteStatus.count(), deleteWriteStatues.count());
      assertNoWriteErrors(deleteStatus.collect());

      // Ensure no records can be tagged
      List<HoodieRecord> records3 = index.tagLocation(writeRecords, jsc(), hoodieTable).collect();
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
    return getConfigBuilder(100).build();
  }

  private HoodieWriteConfig getConfig(int hbaseIndexBatchSize) {
    return getConfigBuilder(hbaseIndexBatchSize).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(int hbaseIndexBatchSize) {
    return HoodieWriteConfig.newBuilder().withPath(basePath()).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1).withDeleteParallelism(1)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withAutoCommit(false).withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
            .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder()
                .hbaseZkPort(Integer.parseInt(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
                .hbaseIndexPutBatchSizeAutoCompute(true)
                .hbaseZkZnodeParent(hbaseConfig.get("zookeeper.znode.parent", ""))
                .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(TABLE_NAME)
                .hbaseIndexGetBatchSize(hbaseIndexBatchSize).build())
            .build());
  }
}
