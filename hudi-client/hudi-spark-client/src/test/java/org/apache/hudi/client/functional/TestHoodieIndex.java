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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("functional")
public class TestHoodieIndex extends HoodieClientTestHarness {

  private static Stream<Arguments> indexTypeParams() {
    Object[][] data = new Object[][] {
        {IndexType.BLOOM, true},
        {IndexType.GLOBAL_BLOOM, true},
        {IndexType.SIMPLE, true},
        {IndexType.GLOBAL_SIMPLE, true},
        {IndexType.RECORD_INDEX, true},
        {IndexType.SIMPLE, false},
        {IndexType.GLOBAL_SIMPLE, false},
        {IndexType.RECORD_INDEX, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private static final Schema SCHEMA = getSchemaFromResource(TestHoodieIndex.class, "/exampleSchema.avsc", true);
  private final Random random = new Random();
  private IndexType indexType;
  private HoodieIndex index;
  private HoodieWriteConfig config;

  private void setUp(IndexType indexType, boolean populateMetaFields) throws Exception {
    setUp(indexType, populateMetaFields, true, true);
  }

  private void setUp(IndexType indexType, boolean populateMetaFields, boolean enableMetadata, boolean rollbackUsingMarkers) throws Exception {
    this.indexType = indexType;
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, populateMetaFields ? new Properties()
        : getPropertiesForKeyGen());
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder().enable(enableMetadata);
    if (indexType == IndexType.RECORD_INDEX) {
      metadataConfigBuilder.createRecordIndex(true);
    }
    config = getConfigBuilder()
        .withProperties(populateMetaFields ? new Properties() : getPropertiesForKeyGen())
        .withRollbackUsingMarkers(rollbackUsingMarkers)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .build()).withAutoCommit(false).withMetadataConfig(metadataConfigBuilder.build()).build();
    writeClient = getHoodieWriteClient(config);
    this.index = writeClient.getIndex();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @MethodSource("indexTypeParams")
  public void testSimpleTagLocationAndUpdate(IndexType indexType, boolean populateMetaFields) throws Exception {
    setUp(indexType, populateMetaFields);
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    int totalRecords = 10 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Test tagLocation without any entries in index
    JavaRDD<HoodieRecord> javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

    // Insert totalRecords records
    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, index should not tag them since it was a failed
    // commit
    javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);
    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, index should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    javaRDD = tagLocation(index, writeRecords, hoodieTable);
    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(hoodieKeyJavaRDD, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));
  }

  @ParameterizedTest
  @MethodSource("indexTypeParams")
  public void testTagLocationAndDuplicateUpdate(IndexType indexType, boolean populateMetaFields) throws Exception {
    setUp(indexType, populateMetaFields);
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    int totalRecords = 10 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords, hoodieTable);

    // Duplicate upsert and ensure correctness is maintained
    // We are trying to approximately imitate the case when the RDD is recomputed. For RDD creating, driver code is not
    // recomputed. This includes the state transitions. We need to delete the inflight instance so that subsequent
    // upsert will not run into conflicts.
    metaClient.getFs().delete(new Path(metaClient.getMetaPath(), newCommitTime + ".inflight"));

    writeClient.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(writeStatues.collect());

    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, hbaseIndex should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD = tagLocation(index, writeRecords, hoodieTable);

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(hoodieKeyJavaRDD, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));
  }

  @ParameterizedTest
  @MethodSource("indexTypeParams")
  public void testSimpleTagLocationAndUpdateWithRollback(IndexType indexType, boolean populateMetaFields) throws Exception {
    setUp(indexType, populateMetaFields, true, false);
    String newCommitTime = writeClient.startCommit();
    int totalRecords = 20 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Now tagLocation for these records, hbaseIndex should tag them
    JavaRDD<HoodieRecord> javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == totalRecords);

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(WriteStatus::getFileId).collect();
    assert (javaRDD.filter(record -> record.getCurrentLocation().getFileId() == null).collect().size() == 0);
    List<String> taggedFileIds = javaRDD.map(record -> record.getCurrentLocation().getFileId()).distinct().collect();

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(hoodieKeyJavaRDD, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assert (javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 0);
    assert (javaRDD.filter(record -> record.getCurrentLocation() != null).collect().size() == 0);
  }

  private static Stream<Arguments> regularIndexTypeParams() {
    Object[][] data = new Object[][] {
        {IndexType.BLOOM, true},
        {IndexType.SIMPLE, true}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("regularIndexTypeParams")
  public void testTagLocationAndFetchRecordLocations(IndexType indexType, boolean populateMetaFields) throws Exception {
    setUp(indexType, populateMetaFields);
    String p1 = "2016/01/31";
    String p2 = "2015/01/31";
    String rowKey1 = UUID.randomUUID().toString();
    String rowKey2 = UUID.randomUUID().toString();
    String rowKey3 = UUID.randomUUID().toString();
    String recordStr1 = "{\"_row_key\":\"" + rowKey1 + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"" + rowKey2 + "\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"" + rowKey3 + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    // place same row key under a different partition.
    String recordStr4 = "{\"_row_key\":\"" + rowKey1 + "\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(hoodieTable, SCHEMA);
    String fileId1 = testTable.addCommit("001").getFileIdWithInserts(p1, record1);
    String fileId2 = testTable.addCommit("002").getFileIdWithInserts(p1, record2);
    String fileId3 = testTable.addCommit("003").getFileIdWithInserts(p2, record4);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals(p2)) {
          assertEquals(record.getCurrentLocation().getFileId(), fileId3);
        } else {
          assertEquals(record.getCurrentLocation().getFileId(), fileId1);
        }
      } else if (record.getRecordKey().equals(rowKey2)) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId2);
      } else if (record.getRecordKey().equals(rowKey3)) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }

    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(recordRDD.map(HoodieRecord::getKey), hoodieTable);
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> entry : recordLocations.collect()) {
      if (entry._1.getRecordKey().equals(rowKey1)) {
        assertTrue(entry._2.isPresent(), "Row1 should have been present ");
        if (entry._1.getPartitionPath().equals(p2)) {
          assertTrue(entry._2.isPresent(), "Row1 should have been present ");
          assertEquals(entry._2.get().getRight(), fileId3);
        } else {
          assertEquals(entry._2.get().getRight(), fileId1);
        }
      } else if (entry._1.getRecordKey().equals(rowKey2)) {
        assertTrue(entry._2.isPresent(), "Row2 should have been present ");
        assertEquals(entry._2.get().getRight(), fileId2);
      } else if (entry._1.getRecordKey().equals(rowKey3)) {
        assertFalse(entry._2.isPresent(), "Row3 should have been absent ");
      }
    }
  }

  @Test
  public void testSimpleGlobalIndexTagLocationWhenShouldUpdatePartitionPath() throws Exception {
    setUp(IndexType.GLOBAL_SIMPLE, true);
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .withGlobalSimpleIndexUpdatePartitionPath(true)
            .withBloomIndexUpdatePartitionPath(true)
            .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    writeClient = getHoodieWriteClient(config);
    index = writeClient.getIndex();

    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
        writeClient.getEngineContext().getHadoopConf().get(), config, writeClient.getEngineContext());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(hoodieTable.getMetaClient(),
        SCHEMA, metadataWriter);

    final String p1 = "2016/01/31";
    final String p2 = "2016/02/28";

    // Create the original partition, and put a record, along with the meta file
    // "2016/01/31": 1 file (1_0_20160131101010.parquet)
    // this record will be saved in table and will be tagged to an empty record
    RawTripTestPayload originalPayload =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord originalRecord =
        new HoodieRecord(new HoodieKey(originalPayload.getRowKey(), originalPayload.getPartitionPath()),
            originalPayload);

    /*
    This record has the same record key as originalRecord but different time so different partition
    Because GLOBAL_BLOOM_INDEX_SHOULD_UPDATE_PARTITION_PATH = true,
    globalBloomIndex should
    - tag the original partition of the originalRecord to an empty record for deletion, and
    - tag the new partition of the incomingRecord
    */
    RawTripTestPayload incomingPayload =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-02-28T03:16:41.415Z\",\"number\":12}");
    HoodieRecord incomingRecord =
        new HoodieRecord(new HoodieKey(incomingPayload.getRowKey(), incomingPayload.getPartitionPath()),
            incomingPayload);
    /*
    This record has the same record key as originalRecord and the same partition
    Though GLOBAL_BLOOM_INDEX_SHOULD_UPDATE_PARTITION_PATH = true,
    globalBloomIndex should just tag the original partition
    */
    RawTripTestPayload incomingPayloadSamePartition =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T04:16:41.415Z\",\"number\":15}");
    HoodieRecord incomingRecordSamePartition =
        new HoodieRecord(
            new HoodieKey(incomingPayloadSamePartition.getRowKey(), incomingPayloadSamePartition.getPartitionPath()),
            incomingPayloadSamePartition);

    final String file1P1C0 = UUID.randomUUID().toString();
    Map<String, List<Pair<String, Integer>>> c1PartitionToFilesNameLengthMap = new HashMap<>();
    c1PartitionToFilesNameLengthMap.put(p1, Collections.singletonList(Pair.of(file1P1C0, 100)));
    testTable.doWriteOperation("1000", WriteOperationType.INSERT, Arrays.asList(p1),
        c1PartitionToFilesNameLengthMap, false, false);

    // We have some records to be tagged (two different partitions)
    testTable.withInserts(p1, file1P1C0, originalRecord);

    // test against incoming record with a different partition
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Collections.singletonList(incomingRecord));
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    assertEquals(2, taggedRecordRDD.count());
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      switch (record.getPartitionPath()) {
        case p1:
          assertEquals("000", record.getRecordKey());
          assertTrue(record.getData() instanceof EmptyHoodieRecordPayload);
          break;
        case p2:
          assertEquals("000", record.getRecordKey());
          assertEquals(incomingPayload.getJsonData(), ((RawTripTestPayload) record.getData()).getJsonData());
          break;
        default:
          fail(String.format("Should not get partition path: %s", record.getPartitionPath()));
      }
    }

    // test against incoming record with the same partition
    JavaRDD<HoodieRecord> recordRDDSamePartition = jsc
        .parallelize(Collections.singletonList(incomingRecordSamePartition));
    JavaRDD<HoodieRecord> taggedRecordRDDSamePartition = tagLocation(index, recordRDDSamePartition, hoodieTable);

    assertEquals(1, taggedRecordRDDSamePartition.count());
    HoodieRecord record = taggedRecordRDDSamePartition.first();
    assertEquals("000", record.getRecordKey());
    assertEquals(p1, record.getPartitionPath());
    assertEquals(incomingPayloadSamePartition.getJsonData(), ((RawTripTestPayload) record.getData()).getJsonData());
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  private JavaPairRDD<HoodieKey, Option<Pair<String, String>>> getRecordLocations(JavaRDD<HoodieKey> keyRDD, HoodieTable hoodieTable) {
    JavaRDD<HoodieRecord> recordRDD = tagLocation(
        index, keyRDD.map(k -> new HoodieRecord(k, new EmptyHoodieRecordPayload())), hoodieTable);
    return recordRDD.mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
        ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
        : Option.empty())
    );
  }
}
