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

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.bloom.HoodieSparkBloomIndex;
import org.apache.hudi.index.bloom.HoodieSparkGlobalBloomIndex;
import org.apache.hudi.index.hbase.SparkHBaseIndex;
import org.apache.hudi.index.simple.HoodieSparkSimpleIndex;
import org.apache.hudi.table.BaseHoodieTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.hudi.testutils.TestRawTripPayload;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndex extends HoodieClientTestHarness {

  private final Random random = new Random();
  private IndexType indexType;
  private HoodieIndex index;
  private HoodieWriteConfig config;
  private String schemaStr;
  private Schema schema;

  private void setUp(IndexType indexType) throws Exception {
    setUp(indexType, true);
  }

  private void setUp(IndexType indexType, boolean initializeIndex) throws Exception {
    this.indexType = indexType;
    initResources();
    // We have some records to be tagged (two different partitions)
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    if (initializeIndex) {
      instantiateIndex();
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE", "HBASE"})
  public void testCreateIndex(IndexType indexType) throws Exception {
    setUp(indexType, false);
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    switch (indexType) {
      case INMEMORY:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
        assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof SparkInMemoryHashIndex);
        break;
      case BLOOM:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
        assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof HoodieSparkBloomIndex);
        break;
      case GLOBAL_BLOOM:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.GLOBAL_BLOOM).build()).build();
        assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof HoodieSparkGlobalBloomIndex);
        break;
      case SIMPLE:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.SIMPLE).build()).build();
        assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof HoodieSparkSimpleIndex);
        break;
      case HBASE:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.HBASE)
                .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder().build()).build())
            .build();
        assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof SparkHBaseIndex);
        break;
      default:
        // no -op. just for checkstyle errors
    }
  }

  @Test
  public void testCreateDummyIndex() throws Exception {
    setUp(IndexType.BLOOM, false);
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(DummyHoodieIndex.class.getName()).build()).build();
    assertTrue(HoodieSparkIndexFactory.createIndex(config) instanceof DummyHoodieIndex);
  }

  @Test
  public void testCreateIndex_withException() throws Exception {
    setUp(IndexType.BLOOM, false);
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    final HoodieWriteConfig config1 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithConstructor.class.getName()).build()).build();
    final Throwable thrown1 = assertThrows(HoodieException.class, () -> {
      HoodieSparkIndexFactory.createIndex(config1);
    }, "exception is expected");
    assertTrue(thrown1.getMessage().contains("is not a subclass of HoodieIndex"));

    final HoodieWriteConfig config2 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithoutConstructor.class.getName()).build()).build();
    final Throwable thrown2 = assertThrows(HoodieException.class, () -> {
      HoodieSparkIndexFactory.createIndex(config2);
    }, "exception is expected");
    assertTrue(thrown2.getMessage().contains("Unable to instantiate class"));
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
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Test tagLocation without any entries in index
    JavaRDD<HoodieRecord> javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

    // Insert totalRecords records
    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    assertNoWriteErrors(writeStatues.collect());

    // Now tagLocation for these records, index should not tag them since it was a failed
    // commit
    javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);
    assert (javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);
    // Now commit this & update location of records inserted and validate no errors
    writeClient.commit(newCommitTime, writeStatues);
    // Now tagLocation for these records, index should tag them correctly
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());
    javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);
    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations =
        (JavaPairRDD<HoodieKey, Option<Pair<String, String>>>) index.fetchRecordLocation(hoodieKeyJavaRDD, context, hoodieTable);
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

    HoodieSparkTable hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    writeClient.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
    JavaRDD<HoodieRecord> javaRDD1 = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);

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
    hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());
    JavaRDD<HoodieRecord> javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    assertEquals(totalRecords, javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size());
    assertEquals(totalRecords, javaRDD.map(record -> record.getKey().getRecordKey()).distinct().count());
    assertEquals(totalRecords, javaRDD.filter(record -> (record.getCurrentLocation() != null
        && record.getCurrentLocation().getInstantTime().equals(newCommitTime))).distinct().count());
    javaRDD.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry.getRecordKey()), entry.getPartitionPath(), "PartitionPath mismatch"));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations =
        (JavaPairRDD<HoodieKey, Option<Pair<String, String>>>) index.fetchRecordLocation(hoodieKeyJavaRDD, context, hoodieTable);
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
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Now tagLocation for these records, hbaseIndex should tag them
    JavaRDD<HoodieRecord> javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);
    assert (javaRDD.filter(HoodieRecord::isCurrentLocationKnown).collect().size() == totalRecords);

    // check tagged records are tagged with correct fileIds
    List<String> fileIds = writeStatues.map(WriteStatus::getFileId).collect();
    assert (javaRDD.filter(record -> record.getCurrentLocation().getFileId() == null).collect().size() == 0);
    List<String> taggedFileIds = javaRDD.map(record -> record.getCurrentLocation().getFileId()).distinct().collect();

    Map<String, String> recordKeyToPartitionPathMap = new HashMap();
    List<HoodieRecord> hoodieRecords = writeRecords.collect();
    hoodieRecords.forEach(entry -> recordKeyToPartitionPathMap.put(entry.getRecordKey(), entry.getPartitionPath()));

    JavaRDD<HoodieKey> hoodieKeyJavaRDD = writeRecords.map(entry -> entry.getKey());
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations =
        (JavaPairRDD<HoodieKey, Option<Pair<String, String>>>) index.fetchRecordLocation(hoodieKeyJavaRDD, context, hoodieTable);
    List<HoodieKey> hoodieKeys = hoodieKeyJavaRDD.collect();
    assertEquals(totalRecords, recordLocations.collect().size());
    assertEquals(totalRecords, recordLocations.map(record -> record._1).distinct().count());
    recordLocations.foreach(entry -> assertTrue(hoodieKeys.contains(entry._1), "Missing HoodieKey"));
    recordLocations.foreach(entry -> assertEquals(recordKeyToPartitionPathMap.get(entry._1.getRecordKey()), entry._1.getPartitionPath(), "PartitionPath mismatch"));

    // both lists should match
    assertTrue(taggedFileIds.containsAll(fileIds) && fileIds.containsAll(taggedFileIds));
    // Rollback the last commit
    writeClient.rollback(newCommitTime);

    hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());
    // Now tagLocation for these records, hbaseIndex should not tag them since it was a rolled
    // back commit
    javaRDD = (JavaRDD<HoodieRecord>) index.tagLocation(writeRecords, context, hoodieTable);
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
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

    HoodieSparkTable hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    JavaRDD<HoodieRecord> taggedRecordRDD = (JavaRDD<HoodieRecord>) index.tagLocation(recordRDD, context, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Collections.singletonList(record1), schema, null, true);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Collections.singletonList(record2), schema, null, true);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/01/31", Collections.singletonList(record4), schema, null, true);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    taggedRecordRDD = (JavaRDD<HoodieRecord>) index.tagLocation(recordRDD, context, hoodieTable);

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

    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocations =
        (JavaPairRDD<HoodieKey, Option<Pair<String, String>>>)
            index.fetchRecordLocation(recordRDD.map(entry -> entry.getKey()), context, hoodieTable);

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
    TestRawTripPayload originalPayload =
        new TestRawTripPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
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
    TestRawTripPayload incomingPayload =
        new TestRawTripPayload("{\"_row_key\":\"000\",\"time\":\"2016-02-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord incomingRecord =
        new HoodieRecord(new HoodieKey(incomingPayload.getRowKey(), incomingPayload.getPartitionPath()),
            incomingPayload);
    /*
    This record has the same record key as originalRecord and the same partition
    Though GLOBAL_BLOOM_INDEX_SHOULD_UPDATE_PARTITION_PATH = true,
    globalBloomIndex should just tag the original partition
    */
    TestRawTripPayload incomingPayloadSamePartition =
        new TestRawTripPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T04:16:41.415Z\",\"number\":15}");
    HoodieRecord incomingRecordSamePartition =
        new HoodieRecord(
            new HoodieKey(incomingPayloadSamePartition.getRowKey(), incomingPayloadSamePartition.getPartitionPath()),
            incomingPayloadSamePartition);

    // We have some records to be tagged (two different partitions)
    String schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));

    HoodieClientTestUtils
        .writeParquetFile(basePath, "2016/01/31", Collections.singletonList(originalRecord), schema, null, false);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkTable table = HoodieSparkTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Add some commits
    new File(basePath + "/.hoodie").mkdirs();

    // test against incoming record with a different partition
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Collections.singletonList(incomingRecord));
    JavaRDD<HoodieRecord> taggedRecordRDD = (JavaRDD<HoodieRecord>) index.tagLocation(recordRDD, context, table);

    assertEquals(2, taggedRecordRDD.count());
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      switch (record.getPartitionPath()) {
        case "2016/01/31":
          assertEquals("000", record.getRecordKey());
          assertTrue(record.getData() instanceof EmptyHoodieRecordPayload);
          break;
        case "2016/02/31":
          assertEquals("000", record.getRecordKey());
          assertEquals(incomingPayload.getJsonData(), ((TestRawTripPayload) record.getData()).getJsonData());
          break;
        default:
          assertFalse(true, String.format("Should not get partition path: %s", record.getPartitionPath()));
      }
    }

    // test against incoming record with the same partition
    JavaRDD<HoodieRecord> recordRDDSamePartition = jsc
        .parallelize(Collections.singletonList(incomingRecordSamePartition));
    JavaRDD<HoodieRecord> taggedRecordRDDSamePartition =
        (JavaRDD<HoodieRecord>) index.tagLocation(recordRDDSamePartition, context, table);

    assertEquals(1, taggedRecordRDDSamePartition.count());
    HoodieRecord record = taggedRecordRDDSamePartition.first();
    assertEquals("000", record.getRecordKey());
    assertEquals("2016/01/31", record.getPartitionPath());
    assertEquals(incomingPayloadSamePartition.getJsonData(), ((TestRawTripPayload) record.getData()).getJsonData());
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
        .withWriteStatusClass(TestRawTripPayload.MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  private void instantiateIndex() {
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .build()).withAutoCommit(false).build();
    writeClient = getHoodieWriteClient(config);
    this.index = writeClient.getIndex();
  }

  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors());
    }
  }

  public static class DummyHoodieIndex<T extends HoodieRecordPayload, I, K, O, P> extends HoodieIndex<T, I, K, O, P> {

    public DummyHoodieIndex(HoodieWriteConfig config) {
      super(config);
    }

    @Override
    public P fetchRecordLocation(K hoodieKeys, HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) {
      return null;
    }

    @Override
    public I tagLocation(I recordRDD, HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) throws HoodieIndexException {
      return null;
    }

    @Override
    public O updateLocation(O writeStatusRDD, HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) throws HoodieIndexException {
      return null;
    }

    @Override
    public boolean rollbackCommit(String instantTime) {
      return false;
    }

    @Override
    public boolean isGlobal() {
      return false;
    }

    @Override
    public boolean canIndexLogFiles() {
      return false;
    }

    @Override
    public boolean isImplicitWithStorage() {
      return false;
    }

  }

  public static class IndexWithConstructor {
    public IndexWithConstructor(HoodieWriteConfig config) {
    }
  }

  public static class IndexWithoutConstructor {

  }

}
