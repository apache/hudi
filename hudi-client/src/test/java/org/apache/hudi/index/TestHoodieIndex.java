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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndex extends HoodieClientTestHarness {

  private static final Schema SCHEMA = getSchemaFromResource(TestHoodieIndex.class, "/exampleSchema.txt", true);
  private final Random random = new Random();
  private IndexType indexType;
  private HoodieIndex index;
  private HoodieWriteConfig config;

  private void setUp(IndexType indexType) throws Exception {
    this.indexType = indexType;
    initResources();
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .build()).withAutoCommit(false).build();
    writeClient = getHoodieWriteClient(config);
    this.index = writeClient.getIndex();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE"})
  public void testSimpleTagLocationAndUpdate(IndexType indexType) throws Exception {
    setUp(indexType);
    String newCommitTime = "001";
    int totalRecords = 10 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Test tagLocation without any entries in index
    JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

    // Insert totalRecords records
    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, index should not tag them since it was a failed
    // commit
    javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);
    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, index should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());
    javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = index.fetchRecordLocation(hoodieKeyJavaRDD, jsc, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE"})
  public void testTagLocationAndDuplicateUpdate(IndexType indexType) throws Exception {
    setUp(indexType);
    String newCommitTime = "001";
    int totalRecords = 10 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    JavaRDD<HoodieRecord> javaRDD1 = index.tagLocation(writeRecords, jsc, hoodieTable);

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
    hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());
    JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = index.fetchRecordLocation(hoodieKeyJavaRDD, jsc, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE"})
  public void testSimpleTagLocationAndUpdateWithRollback(IndexType indexType) throws Exception {
    setUp(indexType);
    String newCommitTime = writeClient.startCommit();
    int totalRecords = 20 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert 200 records
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // commit this upsert
    writeClient.commit(newCommitTime, writeStatues);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Now tagLocation for these records, hbaseIndex should tag them
    JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == totalRecords);

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(WriteStatus::getFileId).collect();
    assert (javaRDD.filter(record -> record.getCurrentLocation().getFileId() == null).collect().size() == 0);
    List<String> taggedFileIds = javaRDD.map(record -> record.getCurrentLocation().getFileId()).distinct().collect();

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = index.fetchRecordLocation(hoodieKeyJavaRDD, jsc, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    javaRDD = index.tagLocation(writeRecords, jsc, hoodieTable);
    assert (javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == 0);
    assert (javaRDD.filter(record -> record.getCurrentLocation() != null).collect().size() == 0);
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "SIMPLE",})
  public void testTagLocationAndFetchRecordLocations(IndexType indexType) throws Exception {
    setUp(indexType);
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

    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    JavaRDD<HoodieRecord> taggedRecordRDD = index.tagLocation(recordRDD, jsc, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Collections.singletonList(record1), SCHEMA, null, true);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Collections.singletonList(record2), SCHEMA, null, true);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/01/31", Collections.singletonList(record4), SCHEMA, null, true);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    taggedRecordRDD = index.tagLocation(recordRDD, jsc, hoodieTable);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals("2015/01/31")) {
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename3));
        } else {
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename1));
        }
      } else if (record.getRecordKey().equals(rowKey2)) {
        assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename2));
      } else if (record.getRecordKey().equals(rowKey3)) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }

    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations = index.fetchRecordLocation(recordRDD.map(entry -> entry.getKey()), jsc, hoodieTable);

    for (Tuple2<HoodieKey, Option<Pair<String, String>>> entry : recordLocations.collect()) {
      if (entry._1.getRecordKey().equals(rowKey1)) {
        assertTrue(entry._2.isPresent(), "Row1 should have been present ");
        if (entry._1.getPartitionPath().equals("2015/01/31")) {
          assertTrue(entry._2.isPresent(), "Row1 should have been present ");
          assertEquals(entry._2.get().getRight(), FSUtils.getFileId(filename3));
        } else {
          assertEquals(entry._2.get().getRight(), FSUtils.getFileId(filename1));
        }
      } else if (entry._1.getRecordKey().equals(rowKey2)) {
        assertTrue(entry._2.isPresent(), "Row2 should have been present ");
        assertEquals(entry._2.get().getRight(), FSUtils.getFileId(filename2));
      } else if (entry._1.getRecordKey().equals(rowKey3)) {
        assertFalse(entry._2.isPresent(), "Row3 should have been absent ");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"GLOBAL_SIMPLE"})
  public void testSimpleGlobalIndexTagLocationWhenShouldUpdatePartitionPath(IndexType indexType) throws Exception {
    setUp(indexType);
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .withGlobalSimpleIndexUpdatePartitionPath(true)
            .withBloomIndexUpdatePartitionPath(true)
            .build()).build();
    writeClient = getHoodieWriteClient(config);
    index = writeClient.getIndex();

    // Create the original partition, and put a record, along with the meta file
    // "2016/01/31": 1 file (1_0_20160131101010.parquet)
    new File(basePath + "/2016/01/31").mkdirs();
    new File(basePath + "/2016/01/31/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();

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
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-02-31T03:16:41.415Z\",\"number\":12}");
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

    // We have some records to be tagged (two different partitions)
    HoodieClientTestUtils
        .writeParquetFile(basePath, "2016/01/31", Collections.singletonList(originalRecord), SCHEMA, null, false);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Add some commits
    new File(basePath + "/.hoodie").mkdirs();

    // test against incoming record with a different partition
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Collections.singletonList(incomingRecord));
    JavaRDD<HoodieRecord> taggedRecordRDD = index.tagLocation(recordRDD, jsc, table);

    assertEquals(2, taggedRecordRDD.count());
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      switch (record.getPartitionPath()) {
        case "2016/01/31":
          assertEquals("000", record.getRecordKey());
          assertTrue(record.getData() instanceof EmptyHoodieRecordPayload);
          break;
        case "2016/02/31":
          assertEquals("000", record.getRecordKey());
          assertEquals(incomingPayload.getJsonData(), ((RawTripTestPayload) record.getData()).getJsonData());
          break;
        default:
          assertFalse(true, String.format("Should not get partition path: %s", record.getPartitionPath()));
      }
    }

    // test against incoming record with the same partition
    JavaRDD<HoodieRecord> recordRDDSamePartition = jsc
        .parallelize(Collections.singletonList(incomingRecordSamePartition));
    JavaRDD<HoodieRecord> taggedRecordRDDSamePartition = index.tagLocation(recordRDDSamePartition, jsc, table);

    assertEquals(1, taggedRecordRDDSamePartition.count());
    HoodieRecord record = taggedRecordRDDSamePartition.first();
    assertEquals("000", record.getRecordKey());
    assertEquals("2016/01/31", record.getPartitionPath());
    assertEquals(incomingPayloadSamePartition.getJsonData(), ((RawTripTestPayload) record.getData()).getJsonData());
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return getConfigBuilder(schemaStr, indexType);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  private HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, IndexType indexType) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

}
