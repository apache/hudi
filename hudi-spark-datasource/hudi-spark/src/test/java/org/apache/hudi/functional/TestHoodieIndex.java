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

package org.apache.hudi.functional;

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.functional.TestHoodieMetadataBase;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.RawTripTestPayloadKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BaseSparkBucketIndexBucketInfoGetter;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.SparkBucketIndexBucketInfoGetter;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.action.commit.SparkPartitionBucketIndexBucketInfoGetter;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.SIMPLE_RECORD_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataPartition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieIndex extends TestHoodieMetadataBase {

  private static Stream<Arguments> indexTypeParams() {
    // IndexType, populateMetaFields, enableMetadataIndex
    Object[][] data = new Object[][] {
        {IndexType.BLOOM, true, true},
        {IndexType.BLOOM, true, false},
        {IndexType.GLOBAL_BLOOM, true, true},
        {IndexType.GLOBAL_BLOOM, true, false},
        {IndexType.SIMPLE, true, true},
        {IndexType.SIMPLE, true, false},
        {IndexType.GLOBAL_SIMPLE, true, true},
        {IndexType.GLOBAL_SIMPLE, true, false},
        {IndexType.BUCKET, true, false},
        {IndexType.BUCKET, false, false},
        {IndexType.RECORD_INDEX, true, true}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private HoodieIndex index;
  private HoodieWriteConfig config;

  private void setUp(IndexType indexType, boolean populateMetaFields, boolean enableMetadataIndex) throws Exception {
    setUp(indexType, populateMetaFields, enableMetadataIndex, true);
  }

  private void setUp(IndexType indexType, boolean populateMetaFields, boolean enableMetadataIndex, boolean rollbackUsingMarkers) throws Exception {
    initPath();
    initSparkContexts();
    initHoodieStorage();

    Properties keyGenProps = getPropsForKeyGen(indexType, populateMetaFields);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, keyGenProps);
    HoodieIndexConfig.Builder indexBuilder = HoodieIndexConfig.newBuilder().withIndexType(indexType)
        .fromProperties(keyGenProps)
        .withIndexType(indexType);

    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder()
        .withMetadataIndexBloomFilter(enableMetadataIndex)
        .withMetadataIndexColumnStats(enableMetadataIndex);
    if (indexType == IndexType.RECORD_INDEX) {
      metadataConfigBuilder.withEnableRecordIndex(true);
    }

    config = getConfigBuilder()
        .withProperties(keyGenProps)
        .withSchema(SIMPLE_RECORD_SCHEMA.toString())
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .withPayloadClass(RawTripTestPayload.class.getName())
            .withPayloadOrderingFields("number").build())
        .withRollbackUsingMarkers(rollbackUsingMarkers)
        .withIndexConfig(indexBuilder.build())
        .withMetadataConfig(metadataConfigBuilder.build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder().fromProperties(indexBuilder.build().getProps())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .build();
    KeyGeneratorType keyGeneratorType = HoodieSparkKeyGeneratorFactory.inferKeyGeneratorTypeFromWriteConfig(config.getProps());
    config.setValue(HoodieWriteConfig.KEYGENERATOR_TYPE, keyGeneratorType.name());
    writeClient = getHoodieWriteClient(config);
    this.index = writeClient.getIndex();
  }

  /**
   * For {@link KeyGenerator}'s use based on {@link HoodieTableConfig#POPULATE_META_FIELDS}.
   */
  private Properties getPropsForKeyGen(IndexType indexType, boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
    if (indexType == IndexType.BUCKET) {
      properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
      properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    }
    if (!populateMetaFields) {
      properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
      properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
      properties.put("hoodie.datasource.write.keygenerator.class", RawTripTestPayloadKeyGenerator.class.getName());
      properties.put("hoodie.datasource.write.partitionpath.field", "time");
      properties.put(HoodieTableConfig.ORDERING_FIELDS.key(), "number");
      properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "time");
      properties.put(HoodieTableConfig.ORDERING_FIELDS.key(), "number");
    }
    return properties;
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  private static List<HoodieRecord> getInserts() {
    return Arrays.asList(
        createSimpleRecord("001", "2016-01-31T00:00:01.000Z", 1),
        createSimpleRecord("002", "2016-01-31T00:00:02.000Z", 2),
        createSimpleRecord("003", "2016-01-31T00:00:03.000Z", 3),
        createSimpleRecord("004", "2016-01-31T00:00:04.000Z", 4));
  }

  @ParameterizedTest
  @MethodSource("indexTypeParams")
  public void testSimpleTagLocationAndUpdateWithRollback(IndexType indexType, boolean populateMetaFields, boolean enableMetadataIndex) throws Exception {
    setUp(indexType, populateMetaFields, enableMetadataIndex);
    final int totalRecords = 4;
    List<HoodieRecord> records = getInserts();
    JavaRDD<HoodieRecord> writtenRecords = jsc.parallelize(records, 1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Test tagLocation without any entries in index
    JavaRDD<HoodieRecord> javaRDD = tagLocation(index, writtenRecords, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().isEmpty());

    // Insert totalRecords records
    String newCommitTime = writeClient.startCommit();
    JavaRDD<WriteStatus> writeStatusRdd = writeClient.upsert(writtenRecords, newCommitTime);
    List<WriteStatus> writeStatuses = writeStatusRdd.collect();
    assertNoWriteErrors(writeStatuses);
    String[] fileIdsFromWriteStatuses = writeStatuses.stream().map(WriteStatus::getFileId)
        .sorted().toArray(String[]::new);

    // Now tagLocation for these records, index should not tag them since it was a failed
    // commit
    javaRDD = tagLocation(index, writtenRecords, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().isEmpty());
    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatusRdd);
    // Now tagLocation for these records, index should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    javaRDD = tagLocation(index, writtenRecords, hoodieTable);
    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writtenRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));
    String[] taggedFileIds = javaRDD.map(record -> record.getCurrentLocation().getFileId()).distinct().collect()
        .stream().sorted().toArray(String[]::new);
    assertArrayEquals(taggedFileIds, fileIdsFromWriteStatuses);

    assertEquals(totalRecords, javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> keysRdd = writtenRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(keysRdd, hoodieTable);
    List<HoodieKey> keys = keysRdd.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(keys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));

    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieSparkTable.create(config, context);
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    javaRDD = tagLocation(index, writtenRecords, hoodieTable);
    assertTrue(javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().isEmpty());
    assertTrue(javaRDD.filter(record -> record.getCurrentLocation() != null).collect().isEmpty());
  }

  @Test
  public void testLookupIndexWithAndWithoutColumnStats() throws Exception {
    setUp(IndexType.BLOOM, true, true);
    String newCommitTime = "001";
    final int totalRecords = 4;
    List<HoodieRecord> records = getInserts();
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // Test tagLocation without any entries in index
    JavaRDD<HoodieRecord> javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().isEmpty());

    // Insert totalRecords records
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records
    javaRDD = tagLocation(index, writeRecords, hoodieTable);
    assertTrue(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().isEmpty());
    // Now commit this & update location of records inserted
    writeClient.commit(newCommitTime, writeStatues);

    // check column_stats partition exists
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metadataPartitionExists(metaClient.getBasePath(), context, COLUMN_STATS.getPartitionPath()));
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));

    // delete the column_stats partition
    deleteMetadataPartition(metaClient.getBasePath(), context, COLUMN_STATS.getPartitionPath());

    // Now tagLocation for these records, they should be tagged correctly despite column_stats being enabled but not present
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
  public void testTagLocationAndDuplicateUpdate(IndexType indexType, boolean populateMetaFields, boolean enableMetadataIndex) throws Exception {
    setUp(indexType, populateMetaFields, enableMetadataIndex);
    final int totalRecords = 4;
    List<HoodieRecord> records = getInserts();
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    String newCommitTime = writeClient.startCommit();
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    JavaRDD<HoodieRecord> javaRDD1 = tagLocation(index, writeRecords, hoodieTable);

    // Duplicate upsert and ensure correctness is maintained
    // We are trying to approximately imitate the case when the RDD is recomputed. For RDD creating, driver code is not
    // recomputed. This includes the state transitions. We need to delete the inflight instance so that subsequent
    // upsert will not run into conflicts.
    metaClient.getStorage().deleteDirectory(
        new StoragePath(metaClient.getTimelinePath(), newCommitTime + ".inflight"));

    writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

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

  private static Stream<Arguments> regularIndexTypeParams() {
    // IndexType, populateMetaFields, enableMetadataIndex
    Object[][] data = new Object[][] {
        // TODO (codope): Enabling metadata index is flaky. Both bloom_filter and col_stats get generated but loading column ranges from the index is failing.
        // {IndexType.BLOOM, true, true},
        {IndexType.BLOOM, true, false},
        {IndexType.SIMPLE, true, true},
        {IndexType.SIMPLE, true, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @Disabled("HUDI-7353")
  @ParameterizedTest
  @MethodSource("regularIndexTypeParams")
  public void testTagLocationAndFetchRecordLocations(IndexType indexType, boolean populateMetaFields, boolean enableMetadataIndex) throws Exception {
    setUp(indexType, populateMetaFields, enableMetadataIndex);
    String p1 = "2016/01/31";
    String p2 = "2015/01/31";
    String rowKey1 = UUID.randomUUID().toString();
    String rowKey2 = UUID.randomUUID().toString();
    String rowKey3 = UUID.randomUUID().toString();
    HoodieRecord record1 = createSimpleRecord(rowKey1, "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord(rowKey2, "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord record3 = createSimpleRecord(rowKey3, "2016-01-31T03:16:41.415Z", 15);
    // place same row key under a different partition.
    HoodieRecord record4 = createSimpleRecord(rowKey1, "2015-01-31T03:16:41.415Z", 32);
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));
    String newCommitTime = writeClient.startCommit();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.upsert(recordRDD, newCommitTime);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet files, each having one record (two different partitions)
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, addMetadataFields(SIMPLE_RECORD_SCHEMA), metadataWriter);
    final String fileId1 = "fileID1";
    final String fileId2 = "fileID2";
    final String fileId3 = "fileID3";

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();
    StoragePath baseFilePath =
        testTable.forCommit("0000001").withInserts(p1, fileId1, Collections.singletonList(record1));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(p1, k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation("0000001", WriteOperationType.UPSERT, Arrays.asList(p1, p2),
        partitionToFilesNameLengthMap, false, false);

    partitionToFilesNameLengthMap.clear();
    baseFilePath =
        testTable.forCommit("0000002").withInserts(p1, fileId2, Collections.singletonList(record2));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(p1, k -> new ArrayList<>())
        .add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation("0000002", WriteOperationType.UPSERT, Arrays.asList(p1, p2),
        partitionToFilesNameLengthMap, false, false);

    partitionToFilesNameLengthMap.clear();
    baseFilePath =
        testTable.forCommit("0000003").withInserts(p2, fileId3, Collections.singletonList(record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(p2, k -> new ArrayList<>())
        .add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation("0000003", WriteOperationType.UPSERT, Arrays.asList(p1, p2),
        partitionToFilesNameLengthMap, false, false);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);
    List<HoodieRecord> records = taggedRecordRDD.collect();

    // Check results
    for (HoodieRecord record : records) {
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
  public void testCheckIfValidCommit() throws Exception {
    setUp(IndexType.BLOOM, true, false);

    // When timeline is empty, all commits are invalid
    HoodieTimeline timeline = TIMELINE_FACTORY.createDefaultTimeline(Collections.EMPTY_LIST.stream(), metaClient.getActiveTimeline());
    assertTrue(timeline.empty());
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, "001"));
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, WriteClientTestUtils.createNewInstantTime()));
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // Valid when timeline contains the timestamp or the timestamp is before timeline start
    final HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "010");
    String instantTimestamp = WriteClientTestUtils.createNewInstantTime();
    final HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, WriteClientTestUtils.createNewInstantTime());
    timeline = TIMELINE_FACTORY.createDefaultTimeline(Stream.of(instant1, instant2), metaClient.getActiveTimeline());
    assertFalse(timeline.empty());
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, instant1.requestedTime()));
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, instant2.requestedTime()));
    // no instant on timeline
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestamp));
    // future timestamp
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, WriteClientTestUtils.createNewInstantTime()));
    // timestamp before timeline starts
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, "001"));
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // Check for older timestamp which have sec granularity and an extension of DEFAULT_MILLIS_EXT may have been added via Timeline operations
    instantTimestamp = WriteClientTestUtils.createNewInstantTime();
    String instantTimestampSec = instantTimestamp.substring(0, instantTimestamp.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
    final HoodieInstant instant3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTimestampSec);
    timeline = TIMELINE_FACTORY.createDefaultTimeline(Stream.of(instant1, instant3), metaClient.getActiveTimeline());
    assertFalse(timeline.empty());
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestamp));
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestampSec));

    // With a sec format instant time lesser than first entry in the active timeline, checkifContainsOrBefore() should return true
    instantTimestamp = WriteClientTestUtils.createNewInstantTime();
    final HoodieInstant instant4 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTimestamp);
    timeline = TIMELINE_FACTORY.createDefaultTimeline(Stream.of(instant4), metaClient.getActiveTimeline());
    instantTimestampSec = instantTimestamp.substring(0, instantTimestamp.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
    assertFalse(timeline.empty());
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestamp));
    assertTrue(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestampSec));

    // With a sec format instant time checkIfValid should return false assuming its > first entry in the active timeline
    Thread.sleep(2010); // sleep required so that new timestamp differs in the seconds rather than msec
    instantTimestamp = WriteClientTestUtils.createNewInstantTime();
    instantTimestampSec = instantTimestamp.substring(0, instantTimestamp.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
    assertFalse(timeline.empty());
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestamp));
    assertFalse(HoodieIndexUtils.checkIfValidCommit(timeline, instantTimestampSec));

    // Check the completed delta commit instant which is end with DEFAULT_MILLIS_EXT timestamp
    // Timestamp not contain in inflight timeline, checkContainsInstant() should return false
    // Timestamp contain in inflight timeline, checkContainsInstant() should return true
    String checkInstantTimestampSec = instantTimestamp.substring(0, instantTimestamp.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
    String checkInstantTimestamp = checkInstantTimestampSec + HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT;
    Thread.sleep(2010); // sleep required so that new timestamp differs in the seconds rather than msec
    String newTimestamp = WriteClientTestUtils.createNewInstantTime();
    String newTimestampSec = newTimestamp.substring(0, newTimestamp.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
    final HoodieInstant instant5 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newTimestamp);
    timeline = TIMELINE_FACTORY.createDefaultTimeline(Stream.of(instant5), metaClient.getActiveTimeline());
    assertFalse(timeline.empty());
    assertFalse(timeline.containsInstant(checkInstantTimestamp));
    assertFalse(timeline.containsInstant(checkInstantTimestampSec));

    final HoodieInstant instant6 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT,
        HoodieTimeline.DELTA_COMMIT_ACTION, newTimestampSec + HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT);
    timeline = TIMELINE_FACTORY.createDefaultTimeline(Stream.of(instant6), metaClient.getActiveTimeline());
    assertFalse(timeline.empty());
    assertFalse(timeline.containsInstant(newTimestamp));
    assertFalse(timeline.containsInstant(checkInstantTimestamp));
    assertTrue(timeline.containsInstant(instant6.requestedTime()));
  }

  @Test
  public void testDelete() throws Exception {
    setUp(IndexType.INMEMORY, true, false);

    // Insert records
    String newCommitTime = writeClient.startCommit();
    List<HoodieRecord> records = getInserts();
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());
    writeClient.commit(newCommitTime, writeStatues);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Now tagLocation for these records, index should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord> javaRDD = tagLocation(hoodieTable.getIndex(), writeRecords, hoodieTable);
    Map<String, String> recordKeyToPartitionPathMap = new HashMap<>();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(records.size(), javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(records.size(), javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(records.size(), javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(HoodieRecord::getKey);
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = getRecordLocations(hoodieKeyJavaRDD, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(records.size(), recordLocations.collect().size());
    assertEquals(records.size(), recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));

    // Delete some of the keys
    final int numDeletes = records.size() / 2;
    List<HoodieKey> keysToDelete = records.stream().limit(numDeletes).map(r -> new HoodieKey(r.getRecordKey(), r.getPartitionPath())).collect(Collectors.toList());
    String deleteCommitTime = writeClient.startCommit();
    writeStatues = writeClient.delete(jsc.parallelize(keysToDelete, 1), deleteCommitTime);
    assertNoWriteErrors(writeStatues.collect());
    writeClient.commit(deleteCommitTime, writeStatues);

    // Deleted records should not be found in the index
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    javaRDD = tagLocation(hoodieTable.getIndex(), jsc.parallelize(records.subList(0, numDeletes)), hoodieTable);
    assertEquals(0, javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(numDeletes, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());

    // Other records should be found
    javaRDD = tagLocation(hoodieTable.getIndex(), jsc.parallelize(records.subList(numDeletes, records.size())), hoodieTable);
    assertEquals(records.size() - numDeletes, javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(records.size() - numDeletes, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withEmbeddedTimelineServerEnabled(true);
  }

  private JavaPairRDD<HoodieKey, Option<Pair<String, String>>> getRecordLocations(JavaRDD<HoodieKey> keyRDD, HoodieTable hoodieTable) {
    JavaRDD<HoodieRecord> recordRDD = tagLocation(
        index, keyRDD.map(k -> new HoodieAvroRecord(k, new EmptyHoodieRecordPayload())), hoodieTable);
    return recordRDD.mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
        ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
        : Option.empty())
    );
  }

  @Test
  public void testSparkBucketIndexBucketInfoGetterOverwrite() {
    int numBuckets = 2;
    List<String> partitions = Arrays.asList("part1", "part2", "part3");
    Integer[] partitionNumberToLocalBucketId = {0, 1, 0, 1, 0, 1};
    String[] partitionNumberToPath = {"part1", "part1", "part2", "part2", "part3", "part3"};

    SparkBucketIndexBucketInfoGetter overwriteGetter = new SparkBucketIndexBucketInfoGetter(
        numBuckets,
        partitions,
        Collections.emptyMap(),
        true,
        false
    );
    SparkPartitionBucketIndexBucketInfoGetter partitionOverwriteGetter = new SparkPartitionBucketIndexBucketInfoGetter(
        partitionNumberToLocalBucketId,
        partitionNumberToPath,
        Collections.emptyMap(),
        true,
        false
    );

    // bucketId -> expectedPartition, expectedPrefix
    Object[][] testCases = new Object[][]{
        {1, "part1", "00000001-"},
        {2, "part2", "00000000-"},
        {5, "part3", "00000001-"}
    };

    for (Object[] testCase : testCases) {
      int bucketId = (int) testCase[0];
      String expectedPartition = (String) testCase[1];
      String expectedPrefix = (String) testCase[2];

      for (BaseSparkBucketIndexBucketInfoGetter getter : Arrays.asList(overwriteGetter, partitionOverwriteGetter)) {
        BucketInfo info = getter.getBucketInfo(bucketId);
        assertEquals(BucketType.INSERT, info.getBucketType());
        assertEquals(expectedPartition, info.getPartitionPath());
        assertTrue(info.getFileIdPrefix().startsWith(expectedPrefix));
      }
    }
  }

  @Test
  public void testSparkBucketIndexBucketInfoGetter() {
    int numBuckets = 2;
    List<String> partitions = Arrays.asList("part1", "part2", "part3");
    Map<String, Set<String>> updateMap = new HashMap<>();
    updateMap.put("part1", new HashSet<>(Arrays.asList("00000000-fileA", "00000001-fileB")));
    updateMap.put("part2", new HashSet<>(Arrays.asList("00000000-fileC", "00000001-fileD")));
    Integer[] partitionNumberToLocalBucketId = {0, 1, 0, 1, 0, 1};
    String[] partitionNumberToPath = {"part1", "part1", "part2", "part2", "part3", "part3"};

    SparkBucketIndexBucketInfoGetter updateGetter = new SparkBucketIndexBucketInfoGetter(
        numBuckets,
        partitions,
        updateMap,
        false,
        false
    );

    SparkPartitionBucketIndexBucketInfoGetter partitionOverwriteGetter = new SparkPartitionBucketIndexBucketInfoGetter(
        partitionNumberToLocalBucketId,
        partitionNumberToPath,
        updateMap,
        false,
        false
    );

    // Bucket ID 0
    BucketInfo updateInfo = updateGetter.getBucketInfo(0);
    assertEquals(BucketType.UPDATE, updateInfo.getBucketType());
    assertEquals("part1", updateInfo.getPartitionPath());
    assertEquals("00000000-fileA", updateInfo.getFileIdPrefix());

    BucketInfo partitionInfo = partitionOverwriteGetter.getBucketInfo(0);
    assertEquals(BucketType.UPDATE, partitionInfo.getBucketType());
    assertEquals("part1", partitionInfo.getPartitionPath());
    assertEquals("00000000-fileA", partitionInfo.getFileIdPrefix());

    // Bucket ID 3
    updateInfo = updateGetter.getBucketInfo(3);
    assertEquals(BucketType.UPDATE, updateInfo.getBucketType());
    assertEquals("part2", updateInfo.getPartitionPath());
    assertEquals("00000001-fileD", updateInfo.getFileIdPrefix());

    partitionInfo = partitionOverwriteGetter.getBucketInfo(3);
    assertEquals(BucketType.UPDATE, partitionInfo.getBucketType());
    assertEquals("part2", partitionInfo.getPartitionPath());
    assertEquals("00000001-fileD", partitionInfo.getFileIdPrefix());

    // Bucket ID 4
    updateInfo = updateGetter.getBucketInfo(4);
    assertEquals(BucketType.INSERT, updateInfo.getBucketType());
    assertEquals("part3", updateInfo.getPartitionPath());
    assertTrue(updateInfo.getFileIdPrefix().startsWith("00000000-"));

    partitionInfo = partitionOverwriteGetter.getBucketInfo(4);
    assertEquals(BucketType.INSERT, partitionInfo.getBucketType());
    assertEquals("part3", partitionInfo.getPartitionPath());
    assertTrue(partitionInfo.getFileIdPrefix().startsWith("00000000-"));
  }

  @Test
  public void testSparkBucketIndexBucketInfoGetterWithNBCC() {
    int numBuckets = 2;
    List<String> partitions = Arrays.asList("part1", "part2", "part3");
    Map<String, Set<String>> updateMap = new HashMap<>();
    updateMap.put("part1", new HashSet<>(Arrays.asList("00000000-fileA", "00000001-fileB")));
    updateMap.put("part2", new HashSet<>(Arrays.asList("00000000-fileC", "00000001-fileD")));
    Integer[] partitionNumberToLocalBucketId = {0, 1, 0, 1, 0, 1};
    String[] partitionNumberToPath = {"part1", "part1", "part2", "part2", "part3", "part3"};

    SparkBucketIndexBucketInfoGetter updateGetterNBCC = new SparkBucketIndexBucketInfoGetter(
        numBuckets,
        partitions,
        updateMap,
        false,
        true
    );
    SparkPartitionBucketIndexBucketInfoGetter partitionOverwriteGetter = new SparkPartitionBucketIndexBucketInfoGetter(
        partitionNumberToLocalBucketId,
        partitionNumberToPath,
        updateMap,
        false,
        true
    );

    int[] testBucketIds = {0, 3, 4};
    String[] expectedPartitions = {"part1", "part2", "part3"};
    String[] expectedFileIds = {
        "00000000-fileA", "00000001-fileD", "00000000-0000-0000-0000-000000000000-0"
    };

    BaseSparkBucketIndexBucketInfoGetter[] getters = {updateGetterNBCC, partitionOverwriteGetter};

    for (BaseSparkBucketIndexBucketInfoGetter getter : getters) {
      for (int i = 0; i < testBucketIds.length; i++) {
        BucketInfo info = getter.getBucketInfo(testBucketIds[i]);
        assertEquals(BucketType.UPDATE, info.getBucketType());
        assertEquals(expectedPartitions[i], info.getPartitionPath());
        assertEquals(expectedFileIds[i], info.getFileIdPrefix());
      }
    }
  }
}
