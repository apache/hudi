/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.index;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.times;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.hbase.HBaseIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;

/**
 * Note :: HBaseTestingUtility is really flaky with issues where the HbaseMiniCluster fails to shutdown across tests,
 * (see one problem here : https://issues.apache .org/jira/browse/HBASE-15835). Hence, the need to use
 * MethodSorters.NAME_ASCENDING to make sure the tests run in order. Please alter the order of tests running carefully.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHbaseIndex {

  private static JavaSparkContext jsc = null;
  private static HBaseTestingUtility utility;
  private static Configuration hbaseConfig;
  private static String tableName = "test_table";
  private String basePath = null;
  private transient FileSystem fs;

  public TestHbaseIndex() throws Exception {
  }

  @AfterClass
  public static void clean() throws Exception {
    if (jsc != null) {
      jsc.stop();
    }
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
    // Initialize a local spark env
    SparkConf sparkConf = new SparkConf().setAppName("TestHbaseIndex").setMaster("local[1]");
    jsc = new JavaSparkContext(sparkConf);
    jsc.hadoopConfiguration().addResource(utility.getConfiguration());
  }

  @After
  public void clear() throws Exception {
    if (basePath != null) {
      new File(basePath).delete();
    }
  }

  @Before
  public void before() throws Exception {
    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    // Initialize table
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
  }

  @Test
  public void testSimpleTagLocationAndUpdate() throws Exception {

    String newCommitTime = "001";
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
    writeClient.startCommit();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
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
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    hoodieTable = HoodieTable.getHoodieTable(metaClient, config, jsc);
    javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 200);
    assertTrue(javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count() == 200);
    assertTrue(javaRDD.filter(
        record -> (record.getCurrentLocation() != null && record.getCurrentLocation().getCommitTime()
            .equals(newCommitTime))).distinct().count() == 200);

  }

  @Test
  public void testSimpleTagLocationAndUpdateWithRollback() throws Exception {

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    // Load to memory
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);

    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
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

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);

    // Mock hbaseConnection and related entities
    Connection hbaseConnection = Mockito.mock(Connection.class);
    HTable table = Mockito.mock(HTable.class);
    Mockito.when(hbaseConnection.getTable(TableName.valueOf(tableName))).thenReturn(table);
    Mockito.when(table.get((List<Get>) anyObject())).thenReturn(new Result[0]);

    // only for test, set the hbaseConnection to mocked object
    index.setHbaseConnection(hbaseConnection);

    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
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

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    HoodieWriteConfig config = getConfig();
    HBaseIndex index = new HBaseIndex(config);
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);

    // start a commit and generate test data
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 250);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
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
        .withParallelism(1, 1).withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).withInlineCompaction(false)
                .build()).withAutoCommit(false)
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table").withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
                .hbaseZkPort(Integer.valueOf(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
                .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(tableName)
                .hbaseIndexGetBatchSize(100).hbaseIndexPutBatchSize(100).build());
  }
}