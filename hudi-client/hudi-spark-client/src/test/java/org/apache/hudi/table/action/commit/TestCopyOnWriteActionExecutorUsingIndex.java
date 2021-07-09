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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.utils.BucketUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCopyOnWriteActionExecutorUsingIndex extends HoodieClientTestBase {

  private static final Logger LOG = LogManager
      .getLogger(TestCopyOnWriteActionExecutorUsingIndex.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestCopyOnWriteActionExecutor.class,
      "/exampleSchema.avsc");
  private static final int NUM_BUCKET = 8;

  private HoodieWriteConfig getConfigOfIndexType(IndexType indexType) {
    if (indexType == IndexType.BUCKET_INDEX) {
      return makeHoodieClientConfig(
          HoodieIndexConfig.newBuilder().withIndexType(IndexType.BUCKET_INDEX)
              .withBucketNum(String.valueOf(NUM_BUCKET)).build());
    } else {
      throw new HoodieException("Index type " + indexType + " is not supported.");
    }
  }

  @Test
  public void testUpdateRecordsReusingRecordKey() throws Exception {
    HoodieWriteConfig config = getConfigOfIndexType(IndexType.BUCKET_INDEX);

    String firstCommitTime = HoodieTestTable.makeNewCommitTime();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(firstCommitTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    String partitionPath = "2016/01/31";
    HoodieSparkTable table = (HoodieSparkTable) HoodieSparkTable
        .create(config, context, metaClient);

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
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    records.add(
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
            rowChange1));
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    records.add(
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()),
            rowChange2));
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    records.add(
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()),
            rowChange3));

    // Insert new records
    final HoodieSparkTable cowTable = table;
    List<WriteStatus> insertStatuses = writeClient
        .insert(jsc.parallelize(records, 1), firstCommitTime).collect();

    // Verify the incremental file group satisfies bucket index requirements
    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(getRecordBucketIds(records), getFileBucketIds(allFiles));

    // Verify write stats
    assertEquals(allFiles.length, insertStatuses.size());
    assertEquals(3,
        insertStatuses.get(0).getStat().getNumInserts() + insertStatuses.get(1).getStat()
            .getNumInserts());
    assertEquals(0,
        insertStatuses.get(0).getStat().getNumDeletes() + insertStatuses.get(1).getStat()
            .getNumDeletes());
    assertEquals(3, insertStatuses.get(0).getStat().getNumWrites() + insertStatuses.get(1).getStat()
        .getNumWrites());

    // Read out the bloom filter and make sure filter can answer record exist or not
    BloomFilter filter1 = BaseFileUtils.getInstance(metaClient)
        .readBloomFilterFromMetadata(hadoopConf, allFiles[0].getPath());
    BloomFilter filter2 = BaseFileUtils.getInstance(metaClient)
        .readBloomFilterFromMetadata(hadoopConf, allFiles[1].getPath());
    for (HoodieRecord record : records) {
      assertTrue(filter1.mightContain(record.getRecordKey()) ^ filter2
          .mightContain(record.getRecordKey()));
    }

    // We update the 1st record & add two new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    RawTripTestPayload updateRowChanges1 = new RawTripTestPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()),
        updateRowChanges1);

    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieRecord insertedRecord1 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()),
            rowChange4);
    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeClient.upsert(jsc.parallelize(updatedRecords), newCommitTime)
        .collect();

    // Verify the incremental file group satisfies bucket index requirements
    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(getRecordBucketIds(updatedRecords), getFileBucketIds(allFiles));

    // Verify write stats
    assertEquals(allFiles.length, statuses.size());
    assertEquals(1,
        statuses.get(0).getStat().getNumInserts() + statuses.get(1).getStat().getNumInserts());
    assertEquals(1, statuses.get(0).getStat().getNumUpdateWrites() + statuses.get(1).getStat()
        .getNumUpdateWrites());
    assertTrue(
        statuses.get(0).getStat().getNumWrites() + statuses.get(1).getStat().getNumWrites() >= 2);

    // Check whether the record has been updated
    List<FileStatus> updateFiles = Stream.of(allFiles)
        .filter(fileStatus -> getFileBucketId(fileStatus) == getRecordBucketId(updatedRecord1))
        .collect(Collectors.toList());
    assertEquals(1, updateFiles.size());
    Path updatedParquetFilePath = updateFiles.get(0).getPath();
    BloomFilter updatedFilter = BaseFileUtils.getInstance(metaClient)
        .readBloomFilterFromMetadata(hadoopConf, updatedParquetFilePath);
    assertTrue(updatedFilter.mightContain(updatedRecord1.getRecordKey()));

    // Check the updated value
    GenericRecord newRecord;
    ParquetReader updatedReader = ParquetReader
        .builder(new AvroReadSupport<>(), updatedParquetFilePath).build();
    int count = 0;
    while ((newRecord = (GenericRecord) updatedReader.read()) != null) {
      if (newRecord.get("_row_key").toString().equals(updatedRecord1.getRecordKey())) {
        count++;
        assertEquals("15", newRecord.get("number").toString());
      }
    }
    assertEquals(1, count);
  }

  private Set<Integer> getRecordBucketIds(List<HoodieRecord> records) {
    return records.stream().map(hoodieRecord -> getRecordBucketId(hoodieRecord))
        .collect(Collectors.toSet());
  }

  private Integer getRecordBucketId(HoodieRecord record) {
    return BucketUtils.bucketId(record.getKey().getIndexKey(), NUM_BUCKET);
  }

  private Set<Integer> getFileBucketIds(FileStatus[] files) {
    return Stream.of(files).map(fileStatus -> getFileBucketId(fileStatus))
        .collect(Collectors.toSet());
  }

  private Integer getFileBucketId(FileStatus fileStatus) {
    return BucketUtils.bucketIdFromFileId(fileStatus.getPath().getName());
  }

  private FileStatus[] getIncrementalFiles(String partitionPath, String startCommitTime,
      int numCommitsToPull)
      throws Exception {
    // initialize parquet input format
    HoodieParquetInputFormat hoodieInputFormat = new HoodieParquetInputFormat();
    JobConf jobConf = new JobConf(hadoopConf);
    hoodieInputFormat.setConf(jobConf);
    HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    setupIncremental(jobConf, startCommitTime, numCommitsToPull);
    FileInputFormat.setInputPaths(jobConf, Paths.get(basePath, partitionPath).toString());
    return hoodieInputFormat.listStatus(jobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN,
            HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN,
            HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String
            .format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  private HoodieWriteConfig makeHoodieClientConfig(HoodieIndexConfig hoodieIndexConfig) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString())
        .withIndexConfig(hoodieIndexConfig).build();
  }

}
