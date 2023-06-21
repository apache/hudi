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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.execution.bulkinsert.TestBulkInsertInternalPartitioner.generateExpectedPartitionNumRecords;
import static org.apache.hudi.execution.bulkinsert.TestBulkInsertInternalPartitioner.generateTestRecordsForBulkInsert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCopyOnWriteActionExecutor extends HoodieClientTestBase implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TestCopyOnWriteActionExecutor.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestCopyOnWriteActionExecutor.class, "/exampleSchema.avsc");
  private static final Stream<Arguments> indexType() {
    HoodieIndex.IndexType[] data = new HoodieIndex.IndexType[] {
        HoodieIndex.IndexType.BLOOM,
        HoodieIndex.IndexType.BUCKET
    };
    return Stream.of(data).map(Arguments::of);
  }

  public class TestHoodieSparkCopyOnWriteTable<T> extends HoodieSparkCopyOnWriteTable<T> {
    public TestHoodieSparkCopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
      super(config, context, metaClient);
    }

    public HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
                                             Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
      return super.getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, dataFileToBeMerged);
    }
  }

  @Test
  public void testMakeNewPath() {
    String fileName = UUID.randomUUID().toString();
    String partitionPath = "2016/05/04";

    String instantTime = makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), table, instantTime).createMarkerDir();

    Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
      HoodieRecord record = mock(HoodieRecord.class);
      when(record.getPartitionPath()).thenReturn(partitionPath);
      String writeToken = FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
          TaskContext.get().taskAttemptId());
      HoodieCreateHandle io = new HoodieCreateHandle(config, instantTime, table, partitionPath, fileName, supplier);
      return Pair.of(io.makeNewPath(record.getPartitionPath()), writeToken);
    }).collect().get(0);

    assertEquals(newPathWithWriteToken.getKey().toString(), Paths.get(this.basePath, partitionPath,
        FSUtils.makeBaseFileName(instantTime, newPathWithWriteToken.getRight(), fileName)).toString());
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
    if (indexType.equals(HoodieIndex.IndexType.BUCKET)) {
      props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
      indexConfig.fromProperties(props)
          .withIndexKeyField("_row_key")
          .withBucketNum("1")
          .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE);
      props.putAll(HoodieLayoutConfig.newBuilder().fromProperties(props)
          .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
          .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build().getProps());
    }
    props.putAll(indexConfig.build().getProps());
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

    List<HoodieAvroRecord> records = generateTestRecords();

    // Insert new records
    final HoodieSparkCopyOnWriteTable cowTable = table;
    writeClient.insert(jsc.parallelize(records, 1), firstCommitTime);

    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);
    String fileId = FSUtils.getFileId(allFiles[0].getPath().getName());

    File parquetFile = getParquetFileIfExists(partitionPath, fileId, firstCommitTime);
    assertNotNull(parquetFile);

    assertTrue(ensureRecordsArePresent(table, parquetFile, records));

    // Read the parquet file, check the record content
    assertTrue(checkRecordContentsAgainstParquetFile(table, parquetFile, records));

    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"8eb5b87d-1fek-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":51}";
    RawTripTestPayload updateRowChanges1 = new RawTripTestPayload(updateRecordStr1);
    HoodieAvroRecord updatedRecord1 = new HoodieAvroRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);

    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieAvroRecord insertedRecord1 =
        new HoodieAvroRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieAvroRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeClient.upsert(jsc.parallelize(updatedRecords), newCommitTime).collect();

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);

    File updatedParquetFile = getParquetFileIfExists(partitionPath,
        FSUtils.getFileId(allFiles[0].getPath().getName()), newCommitTime);
    assertNotNull(updatedParquetFile);

    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(parquetFile.getName()), FSUtils.getFileId(updatedParquetFile.getName()));

    // Check whether the record has been updated
    records.add(insertedRecord1);
    assertTrue(ensureRecordsArePresent(table, updatedParquetFile, records));

    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), allFiles[0].getPath()).build();
    int index = 0;
    GenericRecord newRecord;
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
    // mimic startCommit()
    WriteMarkersFactory.get(config.getMarkersType(), table, firstCommitTime).createMarkerDir();

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
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), table, instantTime).createMarkerDir();

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
    WriteMarkersFactory.get(config.getMarkersType(), table, instantTime).createMarkerDir();

    List<HoodieRecord> records = new ArrayList<>();
    // Approx 1150 records are written for block size of 64KB
    for (int i = 0; i < 2050; i++) {
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
    // we check canWrite only once every 1000 records. and so 2 files with 1000 records and 3rd file with 50 records.
    assertEquals(3, counts, "If the number of records are more than 1150, then there should be a new file");
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
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), table, instantTime).createMarkerDir();
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

  private void testBulkInsertRecords(String bulkInsertMode) {
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

  @Test
  public void testCompletedMarkerPreventsDuplicateFileCreation() throws Exception {
    String fileId = UUID.randomUUID().toString();
    String partitionPath = "2016/01/31";

    String commitTime = makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .withEnforceCompletionMarkerCheck(true)
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), table, commitTime).createMarkerDir();
    insertRecordsToFile(config, table, partitionPath, fileId, commitTime, generateTestRecords());

    try {
      Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
        String writeToken = FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
            TaskContext.get().taskAttemptId());
        HoodieCreateHandle<?,?,?,?> io = new HoodieCreateHandle(config, commitTime, table, partitionPath, fileId, table.getTaskContextSupplier());
        validateRecoveredWriteStatus(1, partitionPath, config, commitTime, table, fileId, false);
        try {
          io.createCompletedMarkerFileIfRequired(partitionPath, commitTime, Collections.emptyList());
        } catch (Exception e) {
          assertEquals(true, e.getMessage().contains("Cleaned up the data file"));
        }

        // mimic driver reconciling the markers
        HoodieLocalEngineContext context = new HoodieLocalEngineContext(new Configuration());
        WriteMarkersFactory.get(config.getMarkersType(), table, commitTime)
            .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());

        // mimic writer closing the handle after reconciliation
        try {
          io.close();
        } catch (Exception e) {
          assertEquals(true, e.getMessage().contains("Cleaned up the data file"));
        }
        Path filePath = io.makeNewPath(partitionPath);
        return Pair.of(filePath, filePath.toString());
      }).collect().get(0);
      assertTrue(!new File(newPathWithWriteToken.getRight()).exists());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCompletedMarkerPreventsDuplicateFileUpdate() throws Exception {
    String fileId = UUID.randomUUID().toString();
    String partitionPath = "2016/01/31";

    String firstCommitTime = makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    List<HoodieAvroRecord> records = generateTestRecords();
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), table, firstCommitTime).createMarkerDir();
    insertRecordsToFile(config, table, partitionPath, fileId, firstCommitTime, records);
    File parquetFile = getParquetFileIfExists(partitionPath, fileId, firstCommitTime);
    assertNotNull(parquetFile);

    String updateCommitTime = makeNewCommitTime();
    TestHoodieSparkCopyOnWriteTable updateTable = new TestHoodieSparkCopyOnWriteTable(config, context, metaClient);
    // mimic startCommit
    WriteMarkersFactory.get(config.getMarkersType(), updateTable, updateCommitTime).createMarkerDir();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    try {
      Pair<Path, String> newPathWithWriteToken = jsc.parallelize(Arrays.asList(1)).map(x -> {
        ExternalSpillableMap<String, HoodieRecord> externalSpillableMap =
            new ExternalSpillableMap<String, HoodieRecord>((long)1024 * 1024,
                config.getSpillableMapBasePath(), new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(SCHEMA));
        HoodieMergeHandle io = updateTable.getUpdateHandle(updateCommitTime, partitionPath, fileId,
            externalSpillableMap, new HoodieBaseFile(parquetFile.toString()));
        io.createCompletedMarkerFileIfRequired(partitionPath, updateCommitTime, Collections.emptyList());

        // mimic driver reconciling the markers
        HoodieLocalEngineContext context = new HoodieLocalEngineContext(new Configuration());
        WriteMarkersFactory.get(config.getMarkersType(), table, updateCommitTime)
            .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());

        // mimic writer closing the handle after reconciliation
        io.close();
        Path filePath = io.makeNewPath(partitionPath);
        return Pair.of(filePath, filePath.toString());
      }).collect().get(0);
      // To be deleted: assertTrue(!new File(newPathWithWriteToken.getRight()).exists());
      validateRecoveredWriteStatus(2, partitionPath, config, updateCommitTime, table, fileId, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static List<Arguments> enforceCompletionMarkerCheckArgs() {
    return asList(
        Arguments.of(false),
        Arguments.of(true)
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFinalizeWriteCheck(boolean enforceFinalizeWriteCheck) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withFailRetriesAfterFinalizeWrite(enforceFinalizeWriteCheck)
        .withAutoCommit(false)
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Insert data without committing.
    String commitTime = makeNewCommitTime();
    JavaRDD<WriteStatus> result = insertBatch(config, client, commitTime, HoodieTimeline.INIT_INSTANT_TS, 100,
        SparkRDDWriteClient::insert, false, false, 100, 100,
        0, Option.empty());

    // Call finalize write method to reconcile markers without completing the commit.
    List<HoodieWriteStat> hoodieWriteStats = result.collect().stream().map(WriteStatus::getStat).collect(Collectors.toList());
    getHoodieTable(this.metaClient, config).finalizeWrite(context, commitTime, hoodieWriteStats);

    // Now to rewrite the parquet files delete inflight files.
    HoodieInstant inflightInstant = new HoodieInstant(true, HoodieActiveTimeline.COMMIT_ACTION, commitTime);
    metaClient.getFs().delete(new Path(metaClient.getMetaPath(), inflightInstant.getFileName()));

    // It is hard to drop the cached rdds and recompute. So, reinserting the data with same commitTimestamp.
    List<HoodieRecord> newRecords = dataGen.generateInserts(commitTime, 100);
    JavaRDD<HoodieRecord> newRecordsRdd = jsc.parallelize(newRecords, 1);

    try {
      // When enforceFinalizeWriteCheck flag is enabled. It should fail with HoodieDataCorruptionException otherwise it will succeed.
      JavaRDD<WriteStatus> secondBatchResult = client.insert(newRecordsRdd, commitTime);
      client.commit(commitTime, secondBatchResult);
    } catch (Exception e) {
      Throwable exception = e;
      assertTrue(enforceFinalizeWriteCheck, "Finalize write check must be enabled");
      // SparkException is thrown during spark stage failures. To get the actual exception,
      // iterate over the stack trace to find the actual exception.
      boolean hoodieCorruptedDataExceptionSeen = false;
      while (exception != null) {
        if (exception instanceof HoodieCorruptedDataException) {
          if (exception.getMessage().equals("Reconciliation for instant " + commitTime
              + " is completed, job is trying to re-write the data files.")) {
            hoodieCorruptedDataExceptionSeen = true;
            break;
          }
        }
        exception = exception.getCause();
      }
      assertTrue(hoodieCorruptedDataExceptionSeen);
    }
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("enforceCompletionMarkerCheckArgs")
  public void testMarkerBasedWrites(boolean enforceCompletionMarkerCheck) throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withMarkersType(MarkerType.DIRECT.name())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.GLOBAL_BLOOM)
            .build())
        .withEnforceCompletionMarkerCheck(enforceCompletionMarkerCheck)
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String firstCommitTime = makeNewCommitTime();
    String partitionPath = "2016/01/31";
    String fileId = FSUtils.createNewFileIdPfx();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    List<HoodieAvroRecord> records = generateTestRecords();

    // A. Marker root folder gets created at startCommit.
    //    Ensure inserts fail when marker folder is not present.
    //    Mimics - driver has reconciled and cleaned up markers scenario
    assertThrows(SparkException.class,
        () -> insertRecordsToFile(config, table, partitionPath, fileId, firstCommitTime, records));

    // mimic start commit for inserts
    WriteMarkersFactory.get(config.getMarkersType(), table, firstCommitTime).createMarkerDir();

    // Insert new records
    insertRecordsToFile(config, table, partitionPath, fileId, firstCommitTime, records);

    // We should have a parquet file generated
    File parquetFile = getParquetFileIfExists(partitionPath, fileId, firstCommitTime);
    assertNotNull(parquetFile);

    // Check for marker files
    assertTrue(ensureMarkerFilesExist(table, partitionPath, fileId, firstCommitTime, IOType.CREATE));

    assertTrue(ensureRecordsArePresent(table, parquetFile, records));

    // B. Mimic second executor trying to insert a newer version of the existing file.
    if (enforceCompletionMarkerCheck) {
      insertRecordsToFile(config, table, partitionPath, fileId, firstCommitTime, records);
      validateRecoveredWriteStatus(1, partitionPath, config, firstCommitTime, table, fileId, false);
    } else {
      insertRecordsToFile(config, table, partitionPath, fileId, firstCommitTime, records);
    }

    // Create a commit file - to mimic commit
    new File(this.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + FSUtils.getCommitTime(parquetFile.getName()) + ".commit").createNewFile();

    // Read the parquet file and check the record content
    assertTrue(checkRecordContentsAgainstParquetFile(table, parquetFile, records));

    Thread.sleep(1000);
    HoodieSparkCopyOnWriteTable newTable = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    String updateCommitTime = makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // We update the 1st record & add a new record
    List<HoodieAvroRecord> updateRecords = generateUpateRecords(parquetFile);
    System.out.println("UpdateRecords: " + updateRecords + " " + parquetFile);


    // C. mimic driver has reconciled and cleaned up markers scenario for updates.
    try {
      updateRecordsToFile(config, newTable, updateCommitTime, partitionPath, fileId, updateRecords, parquetFile.getName());
      assertTrue(false);  // should throw an exception
    } catch (Exception e) {
      // expected to fail with an exception - file already present.
      e.printStackTrace();
      removeLeftoverFile(partitionPath, fileId, updateCommitTime);
    }

    // mimic start commit for updates
    WriteMarkersFactory.get(config.getMarkersType(), newTable, updateCommitTime).createMarkerDir();

    // add update records to the parquet file (record 0 is an update, with location/fileId set)
    List<WriteStatus> statuses = updateRecordsToFile(config, newTable, updateCommitTime, partitionPath,
        fileId, updateRecords, parquetFile.getName());

    // Check the updated file
    File updatedParquetFile = getParquetFileIfExists(partitionPath, fileId, updateCommitTime);
    assertNotNull(updatedParquetFile);

    // Check for marker files
    assertTrue(ensureMarkerFilesExist(newTable, partitionPath, fileId, updateCommitTime, IOType.MERGE));

    HoodieAvroRecord insertedRecord = updateRecords.get(1);
    records.add(insertedRecord);
    assertTrue(ensureRecordsArePresent(newTable, updatedParquetFile, records));

    // Check whether the record has been updated
    Path updatedParquetFilePath = new Path(updatedParquetFile.getAbsolutePath());
    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), updatedParquetFilePath).build();
    int index = 0;
    GenericRecord newRecord;
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
    assertEquals(1, statuses.size()); // should be only one file generated
    assertEquals(4, writeStatus.getStat().getNumWrites());// 3 rewritten records + 1 new record

    // D. mimics - second executor performs the upsert, after the upsert has completed on the first executor.

    if (enforceCompletionMarkerCheck) {
      updateRecordsToFile(config, newTable, updateCommitTime, partitionPath, fileId, updateRecords, parquetFile.getName());
      validateRecoveredWriteStatus(2, partitionPath, config, updateCommitTime, newTable, fileId, true);
    } else {
      updateRecordsToFile(config, newTable, updateCommitTime, partitionPath, fileId, updateRecords, parquetFile.getName());
    }
  }

  private void validateRecoveredWriteStatus(int numFilesExpected, String partitionPath, HoodieWriteConfig config,
                                            String commitTime, HoodieTable table, String fileId, boolean update) {
    List<File> files = Arrays.stream(new File(this.basePath + "/" + partitionPath).listFiles())
        .filter(f -> f.getName().endsWith("parquet")).collect(Collectors.toList());
    assertEquals(Long.valueOf(numFilesExpected), files.size());
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      if (update) {
        String parquetFile = files.get(0).getName();
        HoodieSparkCopyOnWriteTable updateTable = (HoodieSparkCopyOnWriteTable) table;
        ExternalSpillableMap<String, HoodieRecord> externalSpillableMap =
            new ExternalSpillableMap<String, HoodieRecord>((long)1024 * 1024,
                config.getSpillableMapBasePath(), new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(SCHEMA));
        HoodieMergeHandle io = updateTable.getUpdateHandle(commitTime, partitionPath, fileId,
            externalSpillableMap, new HoodieBaseFile(parquetFile));
        assertEquals(true, io.hasRecoveredWriteStatuses());
        assertEquals(1, io.getRecoveredWriteStatuses().size());
        Path filePath = io.makeNewPath(partitionPath);
        return Pair.of(filePath, filePath.toString());
      } else {
        HoodieCreateHandle<?, ?, ?, ?> io = new HoodieCreateHandle(config, commitTime, table, partitionPath, fileId,
            table.getTaskContextSupplier());
        assertEquals(true, io.hasRecoveredWriteStatuses());
        assertEquals(1, io.getRecoveredWriteStatuses().size());
        Path filePath = io.makeNewPath(partitionPath);
        return Pair.of(filePath, filePath.toString());
      }
    }).collect().get(0);
  }

  private List<HoodieAvroRecord> generateTestRecords() throws IOException {
    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";

    List<HoodieAvroRecord> records = new ArrayList<>();
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));
    return records;
  }

  private List<HoodieAvroRecord> generateUpateRecords(File parquetFile) throws IOException {
    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    RawTripTestPayload updateRowChanges1 = new RawTripTestPayload(updateRecordStr1);
    HoodieAvroRecord updatedRecord1 = new HoodieAvroRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);
    updatedRecord1.unseal();
    updatedRecord1.setCurrentLocation(new HoodieRecordLocation(FSUtils.getCommitTime(parquetFile.getName()),
        FSUtils.getFileId(parquetFile.getName())));
    updatedRecord1.seal();

    String recordStr4 = "{\"_row_key\":\"8eb5b87d-1fek-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":51}";
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieAvroRecord insertedRecord1 =
        new HoodieAvroRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    return Arrays.asList(updatedRecord1, insertedRecord1);
  }

  private Map<String, HoodieAvroRecord<?>> getRecordMap(HoodieWriteConfig config, List<HoodieAvroRecord> records) throws IOException {
    ExternalSpillableMap<String, HoodieAvroRecord<?>> recordMap =
        new ExternalSpillableMap<String, HoodieAvroRecord<?>>((long)1024 * 1024,
            config.getSpillableMapBasePath(), new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(SCHEMA));
    records.stream().map(r -> recordMap.put(r.getRecordKey(), r)).count();
    System.out.println("Record map has  " + recordMap.size() + " and records has " + records.size());
    return recordMap;
  }

  private void insertRecordsToFile(HoodieWriteConfig config, HoodieSparkCopyOnWriteTable cowTable, String partitionPath,
                                   String fileId, String commitTime, List<HoodieAvroRecord> records) {
    // Insert new records into the fileId, as part of the commit with commitTime
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      return cowTable.handleInsert(commitTime, partitionPath, fileId, getRecordMap(config, records));
    }).collect();
  }

  private List<WriteStatus> updateRecordsToFile(HoodieWriteConfig config, HoodieSparkCopyOnWriteTable table,
                                                String updateCommitTime, String partitionPath, String fileId,
                                                List<HoodieAvroRecord> updateRecords, String baseFileName) {
    List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      return table.handleUpdate(updateCommitTime, partitionPath, fileId,
          getRecordMap(config, updateRecords), new HoodieBaseFile(baseFileName));
    }).flatMap(Transformations::flattenAsIterator).collect();
    return statuses;
  }

  private File getParquetFileIfExists(String partitionPath, String fileId, String commitTime) {
    // We should have a parquet file generated
    File parquetFile = null;
    LOG.info("listing : " + this.basePath + "/" + partitionPath);
    for (File file : new File(this.basePath + "/" + partitionPath).listFiles()) {
      if (file.getName().endsWith(".parquet") && FSUtils.getFileId(file.getName()).contains(fileId)
          && FSUtils.getCommitTime(file.getName()).equals(commitTime)) {
        parquetFile = file;
        break;
      }
    }
    return parquetFile;
  }

  boolean ensureMarkerFilesExist(HoodieSparkCopyOnWriteTable table, String partitionPath, String fileId,
                                 String commitTime, IOType type) throws IOException {
    String inProgressMarker = null;
    String completionMarker = null;
    Path markerPath = new Path(metaClient.getMarkerFolderPath(commitTime) + "/" + partitionPath);
    WriteMarkers marker = WriteMarkersFactory.get(table.getConfig().getMarkersType(), table, commitTime);
    String completedMarkerPath = marker.getCompletionMarkerPath(partitionPath, fileId, commitTime, type).toString();
    for (String file : marker.allMarkerFilePaths()) {
      if (file.isEmpty()) {
        continue;
      }
      if (file.contains(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN) && file.endsWith(type.name())) {
        inProgressMarker = file;
      }
      if (getConfig().optimizeTaskRetriesWithMarkers() && completedMarkerPath.endsWith(file)) {
        completionMarker = file;
      }
    }

    return inProgressMarker != null && (!getConfig().optimizeTaskRetriesWithMarkers() || completionMarker != null);
  }

  boolean ensureRecordsArePresent(HoodieSparkCopyOnWriteTable table, File parquetFile, List<HoodieAvroRecord> records) {
    // Read out the bloom filter and make sure filter can answer record exist or not
    Path parquetFilePath = new Path(parquetFile.getAbsolutePath());
    Set<String> fileRecords = BaseFileUtils.getInstance(table.getBaseFileFormat())
        .readRowKeys(hadoopConf, parquetFilePath);
    for (HoodieRecord record : records) {
      assertTrue(fileRecords.contains(record.getRecordKey()));
    }
    return true;
  }

  boolean checkRecordContentsAgainstParquetFile(HoodieSparkCopyOnWriteTable table, File parquetFile,
                                                List<HoodieAvroRecord> records) {
    // Read the parquet file, check the record content
    Path parquetFilePath = new Path(parquetFile.getAbsolutePath());
    List<GenericRecord> fileRecords = BaseFileUtils.getInstance(table.getBaseFileFormat())
        .readAvroRecords(hadoopConf, parquetFilePath);
    int index = 0;
    for (GenericRecord fileRecord : fileRecords) {
      assertTrue(fileRecord.get("_row_key").toString().equals(records.get(index).getRecordKey()));
      index++;
    }
    return true;
  }

  private void removeLeftoverFile(String partitionPath, String fileId, String instantTime)  throws IOException {
    File leftoverFile = getParquetFileIfExists(partitionPath, fileId, instantTime);
    if (leftoverFile != null) {
      Path leftOverPath = new Path(metaClient.getBasePathV2(), partitionPath + SEPARATOR + leftoverFile.getName());
      System.out.println(leftOverPath);
      metaClient.getFs().delete(leftOverPath);
    }
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
    final JavaRDD<HoodieRecord> inputRecords = generateTestRecordsForBulkInsert(jsc, 50);
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
