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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.TestRawTripPayload.MetadataMergeWriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieHiveUtil;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.TaskContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCopyOnWriteActionExecutor extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestCopyOnWriteActionExecutor.class);

  @Before
  public void setUp() throws Exception {
    initSparkContexts("TestCopyOnWriteActionExecutor");
    initPath();
    initMetaClient();
    initTestDataGenerator();
    initFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupMetaClient();
    cleanupFileSystem();
    cleanupTestDataGenerator();
  }

  @Test
  public void testMakeNewPath() throws Exception {
    String fileName = UUID.randomUUID().toString();
    String partitionPath = "2016/05/04";

    String instantTime = HoodieTestUtils.makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc);

    Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
      HoodieRecord record = mock(HoodieRecord.class);
      when(record.getPartitionPath()).thenReturn(partitionPath);
      String writeToken = FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
          TaskContext.get().taskAttemptId());
      HoodieCreateHandle io = new HoodieCreateHandle(config, instantTime, table, partitionPath, fileName, supplier);
      return Pair.of(io.makeNewPath(record.getPartitionPath()), writeToken);
    }).collect().get(0);

    Assert.assertEquals(newPathWithWriteToken.getKey().toString(), this.basePath + "/" + partitionPath + "/"
        + FSUtils.makeDataFileName(instantTime, newPathWithWriteToken.getRight(), fileName));
  }

  private HoodieWriteConfig makeHoodieClientConfig() throws Exception {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    // Prepare the AvroParquetIO
    String schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr);
  }

  // TODO (weiy): Add testcases for crossing file writing.
  @Test
  public void testUpdateRecords() throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config = makeHoodieClientConfig();
    String firstCommitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
    writeClient.startCommitWithTime(firstCommitTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    String partitionPath = "/2016/01/31";
    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"8eb5b87d-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":51}";

    List<HoodieRecord> records = new ArrayList<>();
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    records.add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    records.add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    records.add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    final HoodieCopyOnWriteTable cowTable = table;
    writeClient.insert(jsc.parallelize(records, 1), firstCommitTime);

    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path parquetFilePath = allFiles[0].getPath();
    BloomFilter filter = ParquetUtils.readBloomFilterFromParquetMetadata(jsc.hadoopConfiguration(), parquetFilePath);
    for (HoodieRecord record : records) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    // Read the parquet file, check the record content
    List<GenericRecord> fileRecords = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), parquetFilePath);
    GenericRecord newRecord;
    int index = 0;
    for (GenericRecord record : fileRecords) {
      System.out.println("Got :" + record.get("_row_key").toString() + ", Exp :" + records.get(index).getRecordKey());
      assertEquals(records.get(index).getRecordKey(), record.get("_row_key").toString());
      index++;
    }

    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    TestRawTripPayload updateRowChanges1 = new TestRawTripPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);

    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord insertedRecord1 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeClient.upsert(jsc.parallelize(updatedRecords), newCommitTime).collect();

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(parquetFilePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    // Check whether the record has been updated
    Path updatedParquetFilePath = allFiles[0].getPath();
    BloomFilter updatedFilter =
        ParquetUtils.readBloomFilterFromParquetMetadata(jsc.hadoopConfiguration(), updatedParquetFilePath);
    for (HoodieRecord record : records) {
      // No change to the _row_key
      assertTrue(updatedFilter.mightContain(record.getRecordKey()));
    }

    assertTrue(updatedFilter.mightContain(insertedRecord1.getRecordKey()));
    records.add(insertedRecord1);// add this so it can further check below

    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), updatedParquetFilePath).build();
    index = 0;
    while ((newRecord = (GenericRecord) updatedReader.read()) != null) {
      assertEquals(newRecord.get("_row_key").toString(), records.get(index).getRecordKey());
      if (index == 0) {
        assertEquals("15", newRecord.get("number").toString());
      }
      index++;
    }
    updatedReader.close();
    // Also check the numRecordsWritten
    WriteStatus writeStatus = statuses.get(0);
    assertEquals("Should be only one file generated", 1, statuses.size());
    assertEquals(4, writeStatus.getStat().getNumWrites());// 3 rewritten records + 1 new record
  }

  private FileStatus[] getIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
          throws Exception {
    // initialize parquet input format
    HoodieParquetInputFormat hoodieInputFormat = new HoodieParquetInputFormat();
    JobConf jobConf = new JobConf(jsc.hadoopConfiguration());
    hoodieInputFormat.setConf(jobConf);
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath, HoodieTableType.COPY_ON_WRITE);
    setupIncremental(jobConf, startCommitTime, numCommitsToPull);
    FileInputFormat.setInputPaths(jobConf, basePath + partitionPath);
    return hoodieInputFormat.listStatus(jobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    String modePropertyName =
            String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtil.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
            String.format(HoodieHiveUtil.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
            String.format(HoodieHiveUtil.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  private List<HoodieRecord> newHoodieRecords(int n, String time) throws Exception {
    List<HoodieRecord> records = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String recordStr =
          String.format("{\"_row_key\":\"%s\",\"time\":\"%s\",\"number\":%d}", UUID.randomUUID().toString(), time, i);
      TestRawTripPayload rowChange = new TestRawTripPayload(recordStr);
      records.add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }
    return records;
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config =
        makeHoodieClientConfigBuilder().withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    String firstCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";

    List<HoodieRecord> records = new ArrayList<>();
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    records.add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    records.add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    records.add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    CommitActionExecutor actionExecutor = new InsertCommitActionExecutor(jsc, config, table,
        firstCommitTime, jsc.parallelize(records));
    List<WriteStatus> writeStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), records.iterator());
    }).flatMap(x -> HoodieClientTestUtils.collectStatuses(x).iterator()).collect();

    Map<String, String> allWriteStatusMergedMetadataMap =
        MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(writeStatuses);
    assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
    // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
    // should be 2 * 3
    assertEquals("6", allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
  }

  @Test
  public void testInsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfig();
    String instantTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);

    // Case 1:
    // 10 records for partition 1, 1 record for partition 2.
    List<HoodieRecord> records = newHoodieRecords(10, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(1, "2016-02-01T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs2 = records;
    CommitActionExecutor actionExecutor = new InsertPreppedCommitActionExecutor(jsc, config, table,
        instantTime, jsc.parallelize(recs2));
    List<WriteStatus> returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), recs2.iterator());
    }).flatMap(x -> HoodieClientTestUtils.collectStatuses(x).iterator()).collect();

    // TODO: check the actual files and make sure 11 records, total were written.
    assertEquals(2, returnedStatuses.size());
    assertEquals("2016/01/31", returnedStatuses.get(0).getPartitionPath());
    assertEquals(0, returnedStatuses.get(0).getFailedRecords().size());
    assertEquals(10, returnedStatuses.get(0).getTotalRecords());
    assertEquals("2016/02/01", returnedStatuses.get(1).getPartitionPath());
    assertEquals(0, returnedStatuses.get(0).getFailedRecords().size());
    assertEquals(1, returnedStatuses.get(1).getTotalRecords());

    // Case 2:
    // 1 record for partition 1, 5 record for partition 2, 1 records for partition 3.
    records = newHoodieRecords(1, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(5, "2016-02-01T03:16:41.415Z"));
    records.addAll(newHoodieRecords(1, "2016-02-02T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs3 = records;
    CommitActionExecutor newActionExecutor = new UpsertPreppedCommitActionExecutor(jsc, config, table,
        instantTime, jsc.parallelize(recs3));
    returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return newActionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), recs3.iterator());
    }).flatMap(x -> HoodieClientTestUtils.collectStatuses(x).iterator()).collect();

    assertEquals(3, returnedStatuses.size());
    assertEquals("2016/01/31", returnedStatuses.get(0).getPartitionPath());
    assertEquals(1, returnedStatuses.get(0).getTotalRecords());

    assertEquals("2016/02/01", returnedStatuses.get(1).getPartitionPath());
    assertEquals(5, returnedStatuses.get(1).getTotalRecords());

    assertEquals("2016/02/02", returnedStatuses.get(2).getPartitionPath());
    assertEquals(1, returnedStatuses.get(2).getTotalRecords());
  }

  @Test
  public void testFileSizeUpsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder().withStorageConfig(HoodieStorageConfig.newBuilder()
        .limitFileSize(64 * 1024).parquetBlockSize(64 * 1024).parquetPageSize(64 * 1024).build()).build();
    String instantTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);

    List<HoodieRecord> records = new ArrayList<>();
    // Approx 1150 records are written for block size of 64KB
    for (int i = 0; i < 2000; i++) {
      String recordStr = "{\"_row_key\":\"" + UUID.randomUUID().toString()
          + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":" + i + "}";
      TestRawTripPayload rowChange = new TestRawTripPayload(recordStr);
      records.add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }

    // Insert new records
    CommitActionExecutor actionExecutor = new UpsertCommitActionExecutor(jsc, config, table,
        instantTime, jsc.parallelize(records));
    jsc.parallelize(Arrays.asList(1))
        .map(i -> actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), records.iterator()))
        .map(x -> HoodieClientTestUtils.collectStatuses(x)).collect();

    // Check the updated file
    int counts = 0;
    for (File file : new File(basePath + "/2016/01/31").listFiles()) {
      if (file.getName().endsWith(".parquet") && FSUtils.getCommitTime(file.getName()).equals(instantTime)) {
        LOG.info(file.getName() + "-" + file.length());
        counts++;
      }
    }
    assertEquals("If the number of records are more than 1150, then there should be a new file", 3, counts);
  }

  @Test
  public void testInsertUpsertWithHoodieAvroPayload() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
            .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);
    String instantTime = "000";
    // Perform inserts of 100 records to test CreateHandle and BufferedExecutor
    final List<HoodieRecord> inserts = dataGen.generateInsertsWithHoodieAvroPayload(instantTime, 100);
    CommitActionExecutor actionExecutor = new InsertCommitActionExecutor(jsc, config, table,
        instantTime, jsc.parallelize(inserts));
    final List<List<WriteStatus>> ws = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(UUID.randomUUID().toString(), inserts.iterator());
    }).map(x -> (List<WriteStatus>) HoodieClientTestUtils.collectStatuses(x)).collect();

    WriteStatus writeStatus = ws.get(0).get(0);
    String fileId = writeStatus.getFileId();
    metaClient.getFs().create(new Path(basePath + "/.hoodie/000.commit")).close();
    final List<HoodieRecord> updates = dataGen.generateUpdatesWithHoodieAvroPayload(instantTime, inserts);

    String partitionPath = updates.get(0).getPartitionPath();
    long numRecordsInPartition = updates.stream().filter(u -> u.getPartitionPath().equals(partitionPath)).count();
    CommitActionExecutor newActionExecutor = new UpsertCommitActionExecutor(jsc, config, table,
        instantTime, jsc.parallelize(updates));
    final List<List<WriteStatus>> updateStatus = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return newActionExecutor.handleUpdate(partitionPath, fileId, updates.iterator());
    }).map(x -> (List<WriteStatus>) HoodieClientTestUtils.collectStatuses(x)).collect();
    assertEquals(updates.size() - numRecordsInPartition, updateStatus.get(0).get(0).getTotalErrorRecords());
  }

  @After
  public void cleanup() {
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }
}
