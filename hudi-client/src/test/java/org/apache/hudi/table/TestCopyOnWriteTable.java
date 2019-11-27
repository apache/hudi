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

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.BloomFilter;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.TestRawTripPayload.MetadataMergeWriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.table.HoodieCopyOnWriteTable.UpsertPartitioner;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
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

import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCopyOnWriteTable extends HoodieClientTestHarness {

  protected static Logger log = LogManager.getLogger(TestCopyOnWriteTable.class);

  @Before
  public void setUp() throws Exception {
    initSparkContexts("TestCopyOnWriteTable");
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

    String commitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
      HoodieRecord record = mock(HoodieRecord.class);
      when(record.getPartitionPath()).thenReturn(partitionPath);
      String writeToken = FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
          TaskContext.get().taskAttemptId());
      HoodieCreateHandle io = new HoodieCreateHandle(config, commitTime, table, partitionPath, fileName);
      return Pair.of(io.makeNewPath(record.getPartitionPath()), writeToken);
    }).collect().get(0);

    Assert.assertEquals(newPathWithWriteToken.getKey().toString(), this.basePath + "/" + partitionPath + "/"
        + FSUtils.makeDataFileName(commitTime, newPathWithWriteToken.getRight(), fileName));
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
    metaClient = HoodieTableMetaClient.reload(metaClient);

    String partitionPath = "/2016/01/31";
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);

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
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      return cowTable.handleInsert(firstCommitTime, FSUtils.createNewFileIdPfx(), records.iterator());
    }).map(x -> HoodieClientTestUtils.collectStatuses(x)).collect();

    // We should have a parquet file generated (TODO: better control # files after we revise
    // AvroParquetIO)
    File parquetFile = null;
    for (File file : new File(this.basePath + partitionPath).listFiles()) {
      if (file.getName().endsWith(".parquet")) {
        parquetFile = file;
        break;
      }
    }
    assertTrue(parquetFile != null);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path parquetFilePath = new Path(parquetFile.getAbsolutePath());
    BloomFilter filter = ParquetUtils.readBloomFilterFromParquetMetadata(jsc.hadoopConfiguration(), parquetFilePath);
    for (HoodieRecord record : records) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }
    // Create a commit file
    new File(this.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + FSUtils.getCommitTime(parquetFile.getName()) + ".commit").createNewFile();

    // Read the parquet file, check the record content
    List<GenericRecord> fileRecords = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), parquetFilePath);
    GenericRecord newRecord;
    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertTrue(record.get("_row_key").toString().equals(records.get(index).getRecordKey()));
      index++;
    }

    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    TestRawTripPayload updateRowChanges1 = new TestRawTripPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);
    updatedRecord1.unseal();
    updatedRecord1.setCurrentLocation(new HoodieRecordLocation(null, FSUtils.getFileId(parquetFile.getName())));
    updatedRecord1.seal();

    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord insertedRecord1 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieCopyOnWriteTable newTable = new HoodieCopyOnWriteTable(config, jsc);
    List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return newTable.handleUpdate(newCommitTime, updatedRecord1.getCurrentLocation().getFileId(),
          updatedRecords.iterator());
    }).flatMap(x -> HoodieClientTestUtils.collectStatuses(x).iterator()).collect();

    // Check the updated file
    File updatedParquetFile = null;
    for (File file : new File(basePath + "/2016/01/31").listFiles()) {
      if (file.getName().endsWith(".parquet")) {
        if (FSUtils.getFileId(file.getName()).equals(FSUtils.getFileId(parquetFile.getName()))
            && HoodieTimeline.compareTimestamps(FSUtils.getCommitTime(file.getName()),
                FSUtils.getCommitTime(parquetFile.getName()), HoodieTimeline.GREATER)) {
          updatedParquetFile = file;
          break;
        }
      }
    }
    assertTrue(updatedParquetFile != null);
    // Check whether the record has been updated
    Path updatedParquetFilePath = new Path(updatedParquetFile.getAbsolutePath());
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
      assertTrue(newRecord.get("_row_key").toString().equals(records.get(index).getRecordKey()));
      if (index == 0) {
        assertTrue(newRecord.get("number").toString().equals("15"));
      }
      index++;
    }
    updatedReader.close();
    // Also check the numRecordsWritten
    WriteStatus writeStatus = statuses.get(0);
    assertTrue("Should be only one file generated", statuses.size() == 1);
    assertEquals(4, writeStatus.getStat().getNumWrites());// 3 rewritten records + 1 new record
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

    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);

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
    List<WriteStatus> writeStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table.handleInsert(firstCommitTime, FSUtils.createNewFileIdPfx(), records.iterator());
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
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Case 1:
    // 10 records for partition 1, 1 record for partition 2.
    List<HoodieRecord> records = newHoodieRecords(10, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(1, "2016-02-01T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs2 = records;
    List<WriteStatus> returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), recs2.iterator());
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

    returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), recs3.iterator());
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
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);

    List<HoodieRecord> records = new ArrayList<>();
    // Approx 1150 records are written for block size of 64KB
    for (int i = 0; i < 2000; i++) {
      String recordStr = "{\"_row_key\":\"" + UUID.randomUUID().toString()
          + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":" + i + "}";
      TestRawTripPayload rowChange = new TestRawTripPayload(recordStr);
      records.add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }

    // Insert new records
    jsc.parallelize(Arrays.asList(1))
        .map(i -> table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), records.iterator()))
        .map(x -> HoodieClientTestUtils.collectStatuses(x)).collect();

    // Check the updated file
    int counts = 0;
    for (File file : new File(basePath + "/2016/01/31").listFiles()) {
      if (file.getName().endsWith(".parquet") && FSUtils.getCommitTime(file.getName()).equals(commitTime)) {
        log.info(file.getName() + "-" + file.length());
        counts++;
      }
    }
    assertEquals("If the number of records are more than 1150, then there should be a new file", 3, counts);
  }

  private UpsertPartitioner getUpsertPartitioner(int smallFileSize, int numInserts, int numUpdates, int fileSize,
      String testPartitionPath, boolean autoSplitInserts) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize)
            .insertSplitSize(100).autoTuneInsertSplits(autoSplitInserts).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();

    HoodieClientTestUtils.fakeCommitFile(basePath, "001");
    HoodieClientTestUtils.fakeDataFile(basePath, testPartitionPath, "001", "file1", fileSize);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("001", numInserts);
    List<HoodieRecord> updateRecords = dataGenerator.generateUpdates("001", numUpdates);
    for (HoodieRecord updateRec : updateRecords) {
      updateRec.unseal();
      updateRec.setCurrentLocation(new HoodieRecordLocation("001", "file1"));
      updateRec.seal();
    }
    List<HoodieRecord> records = new ArrayList<>();
    records.addAll(insertRecords);
    records.addAll(updateRecords);
    WorkloadProfile profile = new WorkloadProfile(jsc.parallelize(records));
    HoodieCopyOnWriteTable.UpsertPartitioner partitioner =
        (HoodieCopyOnWriteTable.UpsertPartitioner) table.getUpsertPartitioner(profile);
    assertEquals("Update record should have gone to the 1 update partiton", 0, partitioner.getPartition(
        new Tuple2<>(updateRecords.get(0).getKey(), Option.ofNullable(updateRecords.get(0).getCurrentLocation()))));
    return partitioner;
  }

  @Test
  public void testUpsertPartitioner() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates... Check all updates go together & inserts subsplit
    UpsertPartitioner partitioner = getUpsertPartitioner(0, 200, 100, 1024, testPartitionPath, false);
    List<HoodieCopyOnWriteTable.InsertBucket> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);
    assertEquals("Total of 2 insert buckets", 2, insertBuckets.size());
  }

  @Test
  public void testUpsertPartitionerWithSmallInsertHandling() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates .. Check updates go together & inserts subsplit, after expanding
    // smallest file
    UpsertPartitioner partitioner = getUpsertPartitioner(1000 * 1024, 400, 100, 800 * 1024, testPartitionPath, false);
    List<HoodieCopyOnWriteTable.InsertBucket> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals("Should have 3 partitions", 3, partitioner.numPartitions());
    assertEquals("Bucket 0 is UPDATE", HoodieCopyOnWriteTable.BucketType.UPDATE,
        partitioner.getBucketInfo(0).bucketType);
    assertEquals("Bucket 1 is INSERT", HoodieCopyOnWriteTable.BucketType.INSERT,
        partitioner.getBucketInfo(1).bucketType);
    assertEquals("Bucket 2 is INSERT", HoodieCopyOnWriteTable.BucketType.INSERT,
        partitioner.getBucketInfo(2).bucketType);
    assertEquals("Total of 3 insert buckets", 3, insertBuckets.size());
    assertEquals("First insert bucket must be same as update bucket", 0, insertBuckets.get(0).bucketNumber);
    assertEquals("First insert bucket should have weight 0.5", 0.5, insertBuckets.get(0).weight, 0.01);

    // Now with insert split size auto tuned
    partitioner = getUpsertPartitioner(1000 * 1024, 2400, 100, 800 * 1024, testPartitionPath, true);
    insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals("Should have 4 partitions", 4, partitioner.numPartitions());
    assertEquals("Bucket 0 is UPDATE", HoodieCopyOnWriteTable.BucketType.UPDATE,
        partitioner.getBucketInfo(0).bucketType);
    assertEquals("Bucket 1 is INSERT", HoodieCopyOnWriteTable.BucketType.INSERT,
        partitioner.getBucketInfo(1).bucketType);
    assertEquals("Bucket 2 is INSERT", HoodieCopyOnWriteTable.BucketType.INSERT,
        partitioner.getBucketInfo(2).bucketType);
    assertEquals("Bucket 3 is INSERT", HoodieCopyOnWriteTable.BucketType.INSERT,
        partitioner.getBucketInfo(3).bucketType);
    assertEquals("Total of 4 insert buckets", 4, insertBuckets.size());
    assertEquals("First insert bucket must be same as update bucket", 0, insertBuckets.get(0).bucketNumber);
    assertEquals("First insert bucket should have weight 0.5", 200.0 / 2400, insertBuckets.get(0).weight, 0.01);
  }

  @Test
  public void testInsertUpsertWithHoodieAvroPayload() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);
    String commitTime = "000";
    // Perform inserts of 100 records to test CreateHandle and BufferedExecutor
    final List<HoodieRecord> inserts = dataGen.generateInsertsWithHoodieAvroPayload(commitTime, 100);
    final List<List<WriteStatus>> ws = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table.handleInsert(commitTime, UUID.randomUUID().toString(), inserts.iterator());
    }).map(x -> (List<WriteStatus>) HoodieClientTestUtils.collectStatuses(x)).collect();

    WriteStatus writeStatus = ws.get(0).get(0);
    String fileId = writeStatus.getFileId();
    metaClient.getFs().create(new Path(basePath + "/.hoodie/000.commit")).close();
    final HoodieCopyOnWriteTable table2 = new HoodieCopyOnWriteTable(config, jsc);

    final List<HoodieRecord> updates =
        dataGen.generateUpdatesWithHoodieAvroPayload(commitTime, writeStatus.getWrittenRecords());

    jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table2.handleUpdate("001", fileId, updates.iterator());
    }).map(x -> (List<WriteStatus>) HoodieClientTestUtils.collectStatuses(x)).collect();
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
