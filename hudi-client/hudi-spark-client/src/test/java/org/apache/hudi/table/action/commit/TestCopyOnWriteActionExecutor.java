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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
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
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.execution.bulkinsert.TestBulkInsertInternalPartitioner.generateExpectedPartitionNumRecords;
import static org.apache.hudi.execution.bulkinsert.TestBulkInsertInternalPartitioner.generateTestRecordsForBulkInsert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCopyOnWriteActionExecutor extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestCopyOnWriteActionExecutor.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestCopyOnWriteActionExecutor.class, "/exampleSchema.avsc");
  private static final Stream<Arguments> indexType() {
    HoodieIndex.IndexType[] data = new HoodieIndex.IndexType[] {
        HoodieIndex.IndexType.BLOOM,
        HoodieIndex.IndexType.BUCKET
    };
    return Stream.of(data).map(Arguments::of);
  }

  @Test
  public void testMakeNewPath() {
    String fileName = UUID.randomUUID().toString();
    String partitionPath = "2016/05/04";

    String instantTime = makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
      HoodieRecord record = mock(HoodieRecord.class);
      when(record.getPartitionPath()).thenReturn(partitionPath);
      String writeToken = FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
          TaskContext.get().taskAttemptId());
      HoodieCreateHandle io = new HoodieCreateHandle(config, instantTime, table, partitionPath, fileName, supplier);
      return Pair.of(io.makeNewPath(record.getPartitionPath()), writeToken);
    }).collect().get(0);

    assertEquals(newPathWithWriteToken.getKey().toString(), Paths.get(this.basePath, partitionPath,
        FSUtils.makeDataFileName(instantTime, newPathWithWriteToken.getRight(), fileName)).toString());
  }

  private HoodieWriteConfig makeHoodieClientConfig() {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build());
  }

  private Properties makeIndexConfig(HoodieIndex.IndexType indexType) {
    Properties props = new Properties();
    HoodieIndexConfig.Builder indexConfig = HoodieIndexConfig.newBuilder()
        .withIndexType(indexType);
    props.putAll(indexConfig.build().getProps());
    if (indexType.equals(HoodieIndex.IndexType.BUCKET)) {
      props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
      indexConfig.fromProperties(props)
          .withIndexKeyField("_row_key")
          .withBucketNum("1")
          .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE);
      props.putAll(indexConfig.build().getProps());
      props.putAll(HoodieLayoutConfig.newBuilder().fromProperties(props)
          .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
          .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build().getProps());
    }
    return props;
  }

  // TODO (weiy): Add testcases for crossing file writing.
  @ParameterizedTest
  @MethodSource("indexType")
  public void testUpdateRecords(HoodieIndex.IndexType indexType) throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withProps(makeIndexConfig(indexType)).build();
    String firstCommitTime = makeNewCommitTime();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(firstCommitTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    String partitionPath = "2016/01/31";
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

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
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    final HoodieSparkCopyOnWriteTable cowTable = table;
    writeClient.insert(jsc.parallelize(records, 1), firstCommitTime);

    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path filePath = allFiles[0].getPath();
    BloomFilter filter = BaseFileUtils.getInstance(table.getBaseFileFormat()).readBloomFilterFromMetadata(hadoopConf, filePath);
    for (HoodieRecord record : records) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    // Read the base file, check the record content
    List<GenericRecord> fileRecords = BaseFileUtils.getInstance(table.getBaseFileFormat()).readAvroRecords(hadoopConf, filePath);
    GenericRecord newRecord;
    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertEquals(records.get(index).getRecordKey(), record.get("_row_key").toString());
      index++;
    }

    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    RawTripTestPayload updateRowChanges1 = new RawTripTestPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieAvroRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);

    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieRecord insertedRecord1 =
        new HoodieAvroRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeClient.upsert(jsc.parallelize(updatedRecords), newCommitTime).collect();

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(filePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    // Check whether the record has been updated
    Path updatedFilePath = allFiles[0].getPath();
    BloomFilter updatedFilter =
        BaseFileUtils.getInstance(metaClient).readBloomFilterFromMetadata(hadoopConf, updatedFilePath);
    for (HoodieRecord record : records) {
      // No change to the _row_key
      assertTrue(updatedFilter.mightContain(record.getRecordKey()));
    }

    assertTrue(updatedFilter.mightContain(insertedRecord1.getRecordKey()));
    records.add(insertedRecord1);// add this so it can further check below

    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), updatedFilePath).build();
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
    assertEquals(1, statuses.size(), "Should be only one file generated");
    assertEquals(4, writeStatus.getStat().getNumWrites());// 3 rewritten records + 1 new record
  }

  private FileStatus[] getIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
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
            String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
            String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
            String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  private List<HoodieRecord> newHoodieRecords(int n, String time) throws Exception {
    List<HoodieRecord> records = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String recordStr =
          String.format("{\"_row_key\":\"%s\",\"time\":\"%s\",\"number\":%d}", UUID.randomUUID().toString(), time, i);
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      records.add(new HoodieAvroRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }
    return records;
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config =
        makeHoodieClientConfigBuilder().withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    String firstCommitTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";

    List<HoodieRecord> records = new ArrayList<>();
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    BaseSparkCommitActionExecutor actionExecutor = new SparkInsertCommitActionExecutor(context, config, table,
        firstCommitTime, context.parallelize(records));
    List<WriteStatus> writeStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), records.iterator());
    }).flatMap(Transformations::flattenAsIterator).collect();

    Map<String, String> allWriteStatusMergedMetadataMap =
        MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(writeStatuses);
    assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
    // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
    // should be 2 * 3
    assertEquals("6", allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
  }

  private void verifyStatusResult(List<WriteStatus> statuses, Map<String, Long> expectedPartitionNumRecords) {
    Map<String, Long> actualPartitionNumRecords = new HashMap<>();

    for (int i = 0; i < statuses.size(); i++) {
      WriteStatus writeStatus = statuses.get(i);
      String partitionPath = writeStatus.getPartitionPath();
      actualPartitionNumRecords.put(
          partitionPath,
          actualPartitionNumRecords.getOrDefault(partitionPath, 0L) + writeStatus.getTotalRecords());
      assertEquals(0, writeStatus.getFailedRecords().size());
    }

    assertEquals(expectedPartitionNumRecords, actualPartitionNumRecords);
  }

  @Test
  public void testInsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfig();
    String instantTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    // Case 1:
    // 10 records for partition 1, 1 record for partition 2.
    List<HoodieRecord> records = newHoodieRecords(10, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(1, "2016-02-01T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs2 = records;
    BaseSparkCommitActionExecutor actionExecutor = new SparkInsertPreppedCommitActionExecutor(context, config, table,
        instantTime, context.parallelize(recs2));
    List<WriteStatus> returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), recs2.iterator());
    }).flatMap(Transformations::flattenAsIterator).collect();

    // TODO: check the actual files and make sure 11 records, total were written.
    assertEquals(2, returnedStatuses.size());
    Map<String, Long> expectedPartitionNumRecords = new HashMap<>();
    expectedPartitionNumRecords.put("2016/01/31", 10L);
    expectedPartitionNumRecords.put("2016/02/01", 1L);
    verifyStatusResult(returnedStatuses, expectedPartitionNumRecords);

    // Case 2:
    // 1 record for partition 1, 5 record for partition 2, 1 records for partition 3.
    records = newHoodieRecords(1, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(5, "2016-02-01T03:16:41.415Z"));
    records.addAll(newHoodieRecords(1, "2016-02-02T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs3 = records;
    BaseSparkCommitActionExecutor newActionExecutor = new SparkUpsertPreppedCommitActionExecutor(context, config, table,
        instantTime, context.parallelize(recs3));
    returnedStatuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return newActionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), recs3.iterator());
    }).flatMap(Transformations::flattenAsIterator).collect();

    assertEquals(3, returnedStatuses.size());
    expectedPartitionNumRecords.clear();
    expectedPartitionNumRecords.put("2016/01/31", 1L);
    expectedPartitionNumRecords.put("2016/02/01", 5L);
    expectedPartitionNumRecords.put("2016/02/02", 1L);
    verifyStatusResult(returnedStatuses, expectedPartitionNumRecords);
  }

  @Test
  public void testFileSizeUpsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder().withStorageConfig(HoodieStorageConfig.newBuilder()
        .parquetMaxFileSize(64 * 1024).hfileMaxFileSize(64 * 1024)
        .parquetBlockSize(64 * 1024).parquetPageSize(64 * 1024).build()).build();
    String instantTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    List<HoodieRecord> records = new ArrayList<>();
    // Approx 1150 records are written for block size of 64KB
    for (int i = 0; i < 2000; i++) {
      String recordStr = "{\"_row_key\":\"" + UUID.randomUUID().toString()
          + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":" + i + "}";
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      records.add(new HoodieAvroRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }

    // Insert new records
    BaseSparkCommitActionExecutor actionExecutor = new SparkUpsertCommitActionExecutor(context, config, table,
        instantTime, context.parallelize(records));
    jsc.parallelize(Arrays.asList(1))
        .map(i -> actionExecutor.handleInsert(FSUtils.createNewFileIdPfx(), records.iterator()))
        .map(Transformations::flatten).collect();

    // Check the updated file
    int counts = 0;
    for (File file : Paths.get(basePath, "2016/01/31").toFile().listFiles()) {
      if (file.getName().endsWith(table.getBaseFileExtension()) && FSUtils.getCommitTime(file.getName()).equals(instantTime)) {
        LOG.info(file.getName() + "-" + file.length());
        counts++;
      }
    }
    assertEquals(5, counts, "If the number of records are more than 1150, then there should be a new file");
  }

  @Test
  public void testInsertUpsertWithHoodieAvroPayload() throws Exception {
    Schema schema = getSchemaFromResource(TestCopyOnWriteActionExecutor.class, "/testDataGeneratorSchema.txt");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schema.toString())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(1000 * 1024).hfileMaxFileSize(1000 * 1024).build()).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    String instantTime = "000";
    // Perform inserts of 100 records to test CreateHandle and BufferedExecutor
    final List<HoodieRecord> inserts = dataGen.generateInsertsWithHoodieAvroPayload(instantTime, 100);
    BaseSparkCommitActionExecutor actionExecutor = new SparkInsertCommitActionExecutor(context, config, table,
        instantTime, context.parallelize(inserts));
    final List<List<WriteStatus>> ws = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return actionExecutor.handleInsert(UUID.randomUUID().toString(), inserts.iterator());
    }).map(Transformations::flatten).collect();

    WriteStatus writeStatus = ws.get(0).get(0);
    String fileId = writeStatus.getFileId();
    metaClient.getFs().create(new Path(Paths.get(basePath, ".hoodie", "000.commit").toString())).close();
    final List<HoodieRecord> updates = dataGen.generateUpdatesWithHoodieAvroPayload(instantTime, inserts);

    String partitionPath = writeStatus.getPartitionPath();
    long numRecordsInPartition = updates.stream().filter(u -> u.getPartitionPath().equals(partitionPath)).count();
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
    BaseSparkCommitActionExecutor newActionExecutor = new SparkUpsertCommitActionExecutor(context, config, table,
        instantTime, context.parallelize(updates));
    final List<List<WriteStatus>> updateStatus = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return newActionExecutor.handleUpdate(partitionPath, fileId, updates.iterator());
    }).map(Transformations::flatten).collect();
    assertEquals(updates.size() - numRecordsInPartition, updateStatus.get(0).get(0).getTotalErrorRecords());
  }

  public void testBulkInsertRecords(String bulkInsertMode) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withBulkInsertParallelism(2).withBulkInsertSortMode(bulkInsertMode).build();
    String instantTime = makeNewCommitTime();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(instantTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    // Insert new records
    final JavaRDD<HoodieRecord> inputRecords = generateTestRecordsForBulkInsert(jsc);
    SparkBulkInsertCommitActionExecutor bulkInsertExecutor = new SparkBulkInsertCommitActionExecutor(
        context, config, table, instantTime, HoodieJavaRDD.of(inputRecords), Option.empty());
    List<WriteStatus> returnedStatuses = ((HoodieData<WriteStatus>) bulkInsertExecutor.execute().getWriteStatuses()).collectAsList();
    verifyStatusResult(returnedStatuses, generateExpectedPartitionNumRecords(inputRecords));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @ValueSource(strings = {"global_sort", "partition_sort", "none"})
  public void testBulkInsertRecordsWithGlobalSort(String bulkInsertMode) throws Exception {
    testBulkInsertRecords(bulkInsertMode);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionMetafileFormat(boolean partitionMetafileUseBaseFormat) throws Exception {
    // By default there is no format specified for partition metafile
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    assertFalse(table.getPartitionMetafileFormat().isPresent());

    if (partitionMetafileUseBaseFormat) {
      // Add the setting to use datafile format
      Properties properties = new Properties();
      properties.setProperty(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(), "true");
      initMetaClient(HoodieTableType.COPY_ON_WRITE, properties);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().getPartitionMetafileFormat().isPresent());
      table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
      assertTrue(table.getPartitionMetafileFormat().isPresent());
    }

    String instantTime = makeNewCommitTime();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(instantTime);

    // Insert new records
    final JavaRDD<HoodieRecord> inputRecords = generateTestRecordsForBulkInsert(jsc, 10);
    writeClient.bulkInsert(inputRecords, instantTime);

    // Partition metafile should be created
    Path partitionPath = new Path(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, partitionPath));
    Option<Path> metafilePath = HoodiePartitionMetadata.getPartitionMetafilePath(fs, partitionPath);
    if (partitionMetafileUseBaseFormat) {
      // Extension should be the same as the data file format of the table
      assertTrue(metafilePath.get().toString().endsWith(table.getBaseFileFormat().getFileExtension()));
    } else {
      // No extension as it is in properties file format
      assertTrue(metafilePath.get().toString().endsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
    }

    // Validate contents of the partition metafile
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, partitionPath);
    partitionMetadata.readFromFS();
    assertTrue(partitionMetadata.getPartitionDepth() == 3);
    assertTrue(partitionMetadata.readPartitionCreatedCommitTime().get().equals(instantTime));
  }

}
