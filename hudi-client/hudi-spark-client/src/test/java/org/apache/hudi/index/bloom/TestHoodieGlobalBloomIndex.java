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

package org.apache.hudi.index.bloom;

import org.apache.hudi.client.functional.TestHoodieMetadataBase;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieGlobalBloomIndex extends TestHoodieMetadataBase {

  private static final Schema SCHEMA = getSchemaFromResource(TestHoodieGlobalBloomIndex.class, "/exampleSchema.avsc", true);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initFileSystem();
    initMetaClient();
    HoodieIndexConfig.Builder indexBuilder = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.GLOBAL_BLOOM);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(indexBuilder.build())
        .build();
    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testLoadInvolvedFiles() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieGlobalBloomIndex index =
        new HoodieGlobalBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, metadataWriter);

    // Create some partitions, and put some files, along with the meta file
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
    final String p1 = "2016/01/21";
    final String p2 = "2016/04/01";
    final String p3 = "2015/03/12";

    RawTripTestPayload rowChange1 =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record1 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 =
        new RawTripTestPayload("{\"_row_key\":\"001\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record2 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 =
        new RawTripTestPayload("{\"_row_key\":\"002\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record3 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 =
        new RawTripTestPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    List<String> partitions = Arrays.asList(p1, p2);
    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    List<Pair<String, BloomIndexFileInfo>> filesList = index.loadColumnRangesFromFiles(partitions, context, hoodieTable);
    // Still 0, as no valid commit
    assertEquals(0, filesList.size());

    final String fileId1 = "1";
    final String fileId2 = "2";
    final String fileId3 = "3";
    final String fileId4 = "4";
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    final String c1 = "20160401010101";
    Path baseFilePath = testTable.forCommit(c1).withInserts(p2, fileId2, Collections.emptyList());
    long baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.computeIfAbsent(p2,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(c1, WriteOperationType.UPSERT, Collections.singletonList(p2),
        partitionToFilesNameLengthMap, false, false);

    final String c2 = "20150312101010";
    testTable.forCommit(c2);
    baseFilePath = testTable.withInserts(p3, fileId1, Collections.emptyList());
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(p3,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));

    baseFilePath = testTable.withInserts(p3, fileId3, Collections.singletonList(record1));
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.computeIfAbsent(p3,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));

    baseFilePath = testTable.withInserts(p3, fileId4, Arrays.asList(record2, record3, record4));
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.computeIfAbsent(p3,
        k -> new ArrayList<>()).add(Pair.of(fileId4, Integer.valueOf((int) baseFileLength)));

    testTable.doWriteOperation(c2, WriteOperationType.UPSERT, Collections.singletonList(p3),
        partitionToFilesNameLengthMap, false, false);

    filesList = index.loadColumnRangesFromFiles(partitions, context, hoodieTable);
    assertEquals(4, filesList.size());

    Map<String, BloomIndexFileInfo> filesMap = toFileMap(filesList);
    // key ranges checks
    assertNull(filesMap.get("2016/04/01/2").getMaxRecordKey());
    assertNull(filesMap.get("2016/04/01/2").getMinRecordKey());
    assertFalse(filesMap.get("2015/03/12/1").hasKeyRanges());
    assertNotNull(filesMap.get("2015/03/12/3").getMaxRecordKey());
    assertNotNull(filesMap.get("2015/03/12/3").getMinRecordKey());
    assertTrue(filesMap.get("2015/03/12/3").hasKeyRanges());

    Map<String, BloomIndexFileInfo> expected = new HashMap<>();
    expected.put("2016/04/01/2", new BloomIndexFileInfo("2"));
    expected.put("2015/03/12/1", new BloomIndexFileInfo("1"));
    expected.put("2015/03/12/3", new BloomIndexFileInfo("3", "000", "000"));
    expected.put("2015/03/12/4", new BloomIndexFileInfo("4", "001", "003"));

    assertEquals(expected, filesMap);
  }

  @Test
  public void testExplodeRecordRDDWithFileComparisons() {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieGlobalBloomIndex index =
        new HoodieGlobalBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());

    final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22", Arrays.asList(new BloomIndexFileInfo("f1"),
        new BloomIndexFileInfo("f2", "000", "000"), new BloomIndexFileInfo("f3", "001", "003")));

    partitionToFileIndexInfo.put("2017/10/23",
        Arrays.asList(new BloomIndexFileInfo("f4", "002", "007"), new BloomIndexFileInfo("f5", "009", "010")));

    // the partition of the key of the incoming records will be ignored
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        jsc.parallelize(Arrays.asList(new Tuple2<>("2017/10/21", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/23", "004"))).mapToPair(t -> t);

    List<Pair<String, HoodieKey>> comparisonKeyList = HoodieJavaRDD.getJavaRDD(
        index.explodeRecordsWithFileComparisons(partitionToFileIndexInfo,
            HoodieJavaPairRDD.of(partitionRecordKeyPairRDD))).collect();

    /*
     * expecting: f4, HoodieKey { recordKey=003 partitionPath=2017/10/23} f1, HoodieKey { recordKey=003
     * partitionPath=2017/10/22} f3, HoodieKey { recordKey=003 partitionPath=2017/10/22} f4, HoodieKey { recordKey=002
     * partitionPath=2017/10/23} f1, HoodieKey { recordKey=002 partitionPath=2017/10/22} f3, HoodieKey { recordKey=002
     * partitionPath=2017/10/22} f4, HoodieKey { recordKey=005 partitionPath=2017/10/23} f1, HoodieKey { recordKey=005
     * partitionPath=2017/10/22} f4, HoodieKey { recordKey=004 partitionPath=2017/10/23} f1, HoodieKey { recordKey=004
     * partitionPath=2017/10/22}
     */
    assertEquals(10, comparisonKeyList.size());

    Map<String, List<String>> recordKeyToFileComps = comparisonKeyList.stream()
        .collect(Collectors.groupingBy(t -> t.getRight().getRecordKey(), Collectors.mapping(Pair::getKey, Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("005")));
  }

  @Test
  public void testTagLocation() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.GLOBAL_BLOOM)
            .withBloomIndexUpdatePartitionPath(false)
            .build())
        .build();
    HoodieGlobalBloomIndex index = new HoodieGlobalBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, metadataWriter);

    // Create some partitions, and put some files, along with the meta file
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
    final String partition2 = "2016/04/01";
    final String partition3 = "2015/03/12";

    RawTripTestPayload rowChange1 =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record1 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 =
        new RawTripTestPayload("{\"_row_key\":\"001\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record2 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 =
        new RawTripTestPayload("{\"_row_key\":\"002\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record3 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);

    // this record will be saved in table and will be tagged to the incoming record5
    RawTripTestPayload rowChange4 =
        new RawTripTestPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    // this has the same record key as record4 but different time so different partition, but globalbloomIndex should
    // tag the original partition of the saved record4
    RawTripTestPayload rowChange5 =
        new RawTripTestPayload("{\"_row_key\":\"003\",\"time\":\"2016-02-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record5 =
        new HoodieLegacyAvroRecord(new HoodieKey(rowChange5.getRowKey(), rowChange5.getPartitionPath()), rowChange5);

    final String fileId1 = UUID.randomUUID().toString();
    final String fileId2 = UUID.randomUUID().toString();
    final String fileId3 = UUID.randomUUID().toString();
    final String fileId4 = UUID.randomUUID().toString();
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    String commitTime = "0000001";
    Path baseFilePath = testTable.forCommit(commitTime).withInserts(partition2, fileId1, Collections.singletonList(record1));
    long baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.computeIfAbsent(partition2,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition2),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000002";
    baseFilePath = testTable.forCommit(commitTime).withInserts(partition3, fileId2, Collections.emptyList());
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000003";
    baseFilePath = testTable.forCommit(commitTime).withInserts(partition3, fileId3, Collections.singletonList(record2));
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000004";
    baseFilePath = testTable.forCommit(commitTime).withInserts(partition3, fileId4, Collections.singletonList(record4));
    baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId4, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record5));

    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    for (HoodieRecord record : taggedRecordRDD.collect()) {
      switch (record.getRecordKey()) {
        case "000":
          assertEquals(record.getCurrentLocation().getFileId(), fileId1);
          assertEquals(((RawTripTestPayload) record.getData()).getJsonData(), rowChange1.getJsonData());
          break;
        case "001":
          assertEquals(record.getCurrentLocation().getFileId(), fileId3);
          assertEquals(((RawTripTestPayload) record.getData()).getJsonData(), rowChange2.getJsonData());
          break;
        case "002":
          assertFalse(record.isCurrentLocationKnown());
          assertEquals(((RawTripTestPayload) record.getData()).getJsonData(), rowChange3.getJsonData());
          break;
        case "003":
          assertEquals(record.getCurrentLocation().getFileId(), fileId4);
          assertEquals(((RawTripTestPayload) record.getData()).getJsonData(), rowChange5.getJsonData());
          break;
        case "004":
          assertEquals(record.getCurrentLocation().getFileId(), fileId4);
          assertEquals(((RawTripTestPayload) record.getData()).getJsonData(), rowChange4.getJsonData());
          break;
        default:
          throw new IllegalArgumentException("Unknown Key: " + record.getRecordKey());
      }
    }
  }

  @Test
  public void testTagLocationWhenShouldUpdatePartitionPath() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.GLOBAL_BLOOM)
            .withBloomIndexUpdatePartitionPath(true)
            .build())
        .build();
    HoodieGlobalBloomIndex index =
        new HoodieGlobalBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, metadataWriter);
    final String p1 = "2016/01/31";
    final String p2 = "2016/02/28";

    // Create the original partition, and put a record, along with the meta file
    // "2016/01/31": 1 file (1_0_20160131101010.parquet)
    // this record will be saved in table and will be tagged to an empty record
    RawTripTestPayload originalPayload =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord originalRecord =
        new HoodieLegacyAvroRecord(new HoodieKey(originalPayload.getRowKey(), originalPayload.getPartitionPath()),
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
        new HoodieLegacyAvroRecord(new HoodieKey(incomingPayload.getRowKey(), incomingPayload.getPartitionPath()),
            incomingPayload);

    /*
    This record has the same record key as originalRecord and the same partition
    Though GLOBAL_BLOOM_INDEX_SHOULD_UPDATE_PARTITION_PATH = true,
    globalBloomIndex should just tag the original partition
    */
    RawTripTestPayload incomingPayloadSamePartition =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T04:16:41.415Z\",\"number\":15}");
    HoodieRecord incomingRecordSamePartition =
        new HoodieLegacyAvroRecord(
            new HoodieKey(incomingPayloadSamePartition.getRowKey(), incomingPayloadSamePartition.getPartitionPath()),
            incomingPayloadSamePartition);

    final String fileId1 = UUID.randomUUID().toString();
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    final String commitTime = "0000001";
    Path baseFilePath = testTable.forCommit(commitTime).withInserts(p1, fileId1, Collections.singletonList(originalRecord));
    long baseFileLength = fs.getFileStatus(baseFilePath).getLen();
    partitionToFilesNameLengthMap.computeIfAbsent(p1,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Arrays.asList(p1),
        partitionToFilesNameLengthMap, false, false);

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

  // convert list to map to avoid sorting order dependencies
  private static Map<String, BloomIndexFileInfo> toFileMap(List<Pair<String, BloomIndexFileInfo>> filesList) {
    Map<String, BloomIndexFileInfo> filesMap = new HashMap<>();
    for (Pair<String, BloomIndexFileInfo> t : filesList) {
      filesMap.put(t.getKey() + "/" + t.getValue().getFileId(), t.getValue());
    }
    return filesMap;
  }

}
