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

package org.apache.hudi.index;

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator;
import org.apache.hudi.index.hbase.HBaseIndex;
import org.apache.hudi.index.hbase.HBaseIndex.HbasePutBatchSizeCalculator;
import org.apache.hudi.index.hbase.HBaseIndexQPSResourceAllocator;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.times;

/**
 * Note :: HBaseTestingUtility is really flaky with issues where the HbaseMiniCluster fails to shutdown across tests,
 * (see one problem here : https://issues.apache .org/jira/browse/HBASE-15835). Hence, the need to use
 * MethodSorters.NAME_ASCENDING to make sure the tests run in order. Please alter the order of tests running carefully.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHbaseIndex extends HoodieClientTestHarness {

  private static HBaseTestingUtility utility;
  private static Configuration hbaseConfig;
  private static String tableName = "test_table";

  public TestHbaseIndex() throws Exception {}

  @AfterClass
  public static void clean() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    // Initialize HbaseMiniCluster
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    hbaseConfig = utility.getConnection().getConfiguration();
    utility.createTable(TableName.valueOf(tableName), Bytes.toBytes("_s"));
  }

  @Before
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts("TestHbaseIndex");
    jsc.hadoopConfiguration().addResource(utility.getConfiguration());

    // Create a temp folder as the base path
    initPath();
    initTestDataGenerator();
    initMetaClient();
  }

  @After
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupMetaClient();
  }

  private HoodieWriteClient getWriteClient(HoodieWriteConfig config) throws Exception {
    return new HoodieWriteClient(jsc, config);
  }

  @Test
  public void testSimpleTagLocationAndUpdate() throws Exception {

    String newCommitTime = "001";
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      writeClient.startCommit();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

      // Test tagLocation without any entries in index
      JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
      assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

      // Insert 200 records
      JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
      assertNoWriteErrors(writeStatues.collect());

      // Now tagLocation for these records, hbaseIndex should not tag them since it was a failed
      // commit
      javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
      assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

      // Now commit this & update location of records inserted and validate no errors
      writeClient.commit(newCommitTime, writeStatues);
      // Now tagLocation for these records, hbaseIndex should tag them correctly
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);
      javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
      assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 200);
      assertTrue(javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count() == 200);
      assertTrue(javaRDD.filter(record -> (record.getCurrentLocation() != null
          && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count() == 200);
    }
  }

  @Test
  public void testTagLocationAndDuplicateUpdate() throws Exception {
    String newCommitTime = "001";
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
    writeClient.startCommit();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    JavaRDD<HoodieRecord> javaRDD1 = index.tagLocation(writeRecords, jsc, hoodieTable);
    // Duplicate upsert and ensure correctness is maintained
    writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, hbaseIndex should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);
    JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 10);
    assertTrue(javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count() == 10);
    assertTrue(javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count() == 10);
  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() throws Exception {
    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = getWriteClient(config);

    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);
    hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);
    // Now tagLocation for these records, hbaseIndex should tag them
    JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 200);

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(status -> status.getFileId()).collect();
    assert (javaRDD.filter(record -> record.getCurrentLocation().getFileId() == null).collect().size() == 0);
    List<String> taggedFileIds = javaRDD.map(record -> record.getCurrentLocation().getFileId()).distinct().collect();

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);
    assert (javaRDD.filter(record -> record.getCurrentLocation() != null).collect().size() == 0);
  }

  @Test
  public void testTotalGetsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = Mockito.mock(Connection.class);
    HTable table = Mockito.mock(HTable.class);
    Mockito.when(hbaseConnection.getTable(TableName.valueOf(tableName))).thenReturn(table);
    Mockito.when(table.get((List<Get>) anyObject())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    HoodieWriteClient writeClient = getWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Insert 250 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, hbaseIndex should tag them
    index.tagLocation(writeRecords, jsc, hoodieTable);

    // 3 batches should be executed given batchSize = 100 and parallelism = 1
    Mockito.verify(table, times(3)).get((List<Get>) anyObject());

  }

  @Test
  public void testTotalPutsBatching() throws Exception {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = getWriteClient(config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = Mockito.mock(Connection.class);
    HTable table = Mockito.mock(HTable.class);
    Mockito.when(hbaseConnection.getTable(TableName.valueOf(tableName))).thenReturn(table);
    Mockito.when(table.get((List<Get>) anyObject())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    // Get all the files generated
    int numberOfDataFileIds = (int) writeStatues.map(status -> status.getFileId()).distinct().count();

    index.updateLocation(writeStatues, jsc, hoodieTable);
    // 3 batches should be executed given batchSize = 100 and <=numberOfDataFileIds getting updated,
    // so each fileId ideally gets updates
    Mockito.verify(table, atMost(numberOfDataFileIds)).put((List<Put>) anyObject());
  }

  @Test
  public void testPutBatchSizeCalculation() {
    HbasePutBatchSizeCalculator batchSizeCalculator = new HbasePutBatchSizeCalculator();

    // All asserts cases below are derived out of the first
    // example below, with change in one parameter at a time.

    int putBatchSize = batchSizeCalculator.getBatchSize(10, 16667, 1200, 200, 100, 0.1f);
    // Expected batchSize is 8 because in that case, total request sent in one second is below
    // 8 (batchSize) * 200 (parallelism) * 10 (maxReqsInOneSecond) * 10 (numRegionServers) * 0.1 (qpsFraction)) => 16000
    // We assume requests get distributed to Region Servers uniformly, so each RS gets 1600 request
    // 1600 happens to be 10% of 16667 (maxQPSPerRegionServer) as expected.
    Assert.assertEquals(putBatchSize, 8);

    // Number of Region Servers are halved, total requests sent in a second are also halved, so batchSize is also halved
    int putBatchSize2 = batchSizeCalculator.getBatchSize(5, 16667, 1200, 200, 100, 0.1f);
    Assert.assertEquals(putBatchSize2, 4);

    // If the parallelism is halved, batchSize has to double
    int putBatchSize3 = batchSizeCalculator.getBatchSize(10, 16667, 1200, 100, 100, 0.1f);
    Assert.assertEquals(putBatchSize3, 16);

    // If the parallelism is halved, batchSize has to double.
    // This time parallelism is driven by numTasks rather than numExecutors
    int putBatchSize4 = batchSizeCalculator.getBatchSize(10, 16667, 100, 200, 100, 0.1f);
    Assert.assertEquals(putBatchSize4, 16);

    // If sleepTimeMs is halved, batchSize has to halve
    int putBatchSize5 = batchSizeCalculator.getBatchSize(10, 16667, 1200, 200, 100, 0.05f);
    Assert.assertEquals(putBatchSize5, 4);

    // If maxQPSPerRegionServer is doubled, batchSize also doubles
    int putBatchSize6 = batchSizeCalculator.getBatchSize(10, 33334, 1200, 200, 100, 0.1f);
    Assert.assertEquals(putBatchSize6, 16);
  }

  @Test
  public void testsHBasePutAccessParallelism() {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    final JavaRDD<WriteStatus> writeStatusRDD = jsc.parallelize(
        Arrays.asList(getSampleWriteStatus(1, 2), getSampleWriteStatus(0, 3), getSampleWriteStatus(10, 0)), 10);
    final Tuple2<Long, Integer> tuple = index.getHBasePutAccessParallelism(writeStatusRDD);
    final int hbasePutAccessParallelism = Integer.parseInt(tuple._2.toString());
    final int hbaseNumPuts = Integer.parseInt(tuple._1.toString());
    Assert.assertEquals(10, writeStatusRDD.getNumPartitions());
    Assert.assertEquals(2, hbasePutAccessParallelism);
    Assert.assertEquals(11, hbaseNumPuts);
  }

  @Test
  public void testsHBasePutAccessParallelismWithNoInserts() {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    final JavaRDD<WriteStatus> writeStatusRDD =
        jsc.parallelize(Arrays.asList(getSampleWriteStatus(0, 2), getSampleWriteStatus(0, 1)), 10);
    final Tuple2<Long, Integer> tuple = index.getHBasePutAccessParallelism(writeStatusRDD);
    final int hbasePutAccessParallelism = Integer.parseInt(tuple._2.toString());
    final int hbaseNumPuts = Integer.parseInt(tuple._1.toString());
    Assert.assertEquals(10, writeStatusRDD.getNumPartitions());
    Assert.assertEquals(0, hbasePutAccessParallelism);
    Assert.assertEquals(0, hbaseNumPuts);
  }

  @Test
  public void testsHBaseIndexDefaultQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    Assert.assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    Assert.assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  private WriteStatus getSampleWriteStatus(final int numInserts, final int numUpdateWrites) {
    final WriteStatus writeStatus = new WriteStatus(false, 0.1);
    HoodieWriteStat hoodieWriteStat = new HoodieWriteStat();
    hoodieWriteStat.setNumInserts(numInserts);
    hoodieWriteStat.setNumUpdateWrites(numUpdateWrites);
    writeStatus.setStat(hoodieWriteStat);
    return writeStatus;
  }

  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
    }
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withAutoCommit(false).withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
            .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder()
                .hbaseZkPort(Integer.valueOf(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
                .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(tableName)
                .hbaseIndexGetBatchSize(100).build())
            .build());
  }
}
