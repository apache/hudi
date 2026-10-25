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
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.generic.IndexedRecord;
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

import static org.apache.hudi.common.testutils.HoodieTestUtils.SIMPLE_RECORD_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieGlobalBloomIndex extends TestHoodieMetadataBase {

  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieGlobalBloomIndex.class, "/exampleSchema.avsc", true);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
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

    HoodieRecord record1 = createSimpleRecord("000", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord("001", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record3 = createSimpleRecord("002", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record4 = createSimpleRecord("003", "2016-01-31T03:16:41.415Z", 12);

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
    StoragePath baseFilePath = testTable.forCommit(c1)
        .withInserts(p2, fileId2, Collections.emptyList());
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(p2,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(c1, WriteOperationType.UPSERT, Collections.singletonList(p2),
        partitionToFilesNameLengthMap, false, false);

    final String c2 = "20150312101010";
    testTable.forCommit(c2);
    baseFilePath = testTable.withInserts(p3, fileId1, Collections.emptyList());
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(p3,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));

    baseFilePath = testTable.withInserts(p3, fileId3, Collections.singletonList(record1));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(p3,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));

    baseFilePath = testTable.withInserts(p3, fileId4, Arrays.asList(record2, record3, record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
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

    List<Pair<HoodieFileGroupId, String>> comparisonKeyList =
        index.explodeRecordsWithFileComparisons(partitionToFileIndexInfo, HoodieJavaPairRDD.of(partitionRecordKeyPairRDD))
            .collectAsList();

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
        .collect(Collectors.groupingBy(t -> t.getRight(), Collectors.mapping(t -> t.getLeft().getFileId(), Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("005")));
  }

  @Test
  public void testTagLocation() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(SCHEMA.toString())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.GLOBAL_BLOOM)
            .withGlobalBloomIndexUpdatePartitionPath(false)
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

    HoodieAvroIndexedRecord record1 = createSimpleRecord("000", "2016-01-31T03:16:41.415Z", 12);
    HoodieAvroIndexedRecord record2 = createSimpleRecord("001", "2016-01-31T03:16:41.415Z", 12);
    HoodieAvroIndexedRecord record3 = createSimpleRecord("002", "2016-01-31T03:16:41.415Z", 12);
    // this record will be saved in table and will be tagged to the incoming record5
    HoodieAvroIndexedRecord record4 = createSimpleRecord("003", "2016-01-31T03:16:41.415Z", 12);

    // this has the same record key as record4 but different time so different partition, but globalbloomIndex should
    // tag the original partition of the saved record4
    HoodieAvroIndexedRecord record5 = createSimpleRecord("003", "2016-02-31T03:16:41.415Z", 12);

    final String fileId1 = UUID.randomUUID().toString();
    final String fileId2 = UUID.randomUUID().toString();
    final String fileId3 = UUID.randomUUID().toString();
    final String fileId4 = UUID.randomUUID().toString();
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    String commitTime = "0000001";
    StoragePath baseFilePath = testTable.forCommit(commitTime)
        .withInserts(partition2, fileId1, Collections.singletonList(record1));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition2,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition2),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000002";
    baseFilePath =
        testTable.forCommit(commitTime).withInserts(partition3, fileId2, Collections.emptyList());
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000003";
    baseFilePath = testTable.forCommit(commitTime)
        .withInserts(partition3, fileId3, Collections.singletonList(record2));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "0000004";
    baseFilePath = testTable.forCommit(commitTime)
        .withInserts(partition3, fileId4, Collections.singletonList(record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition3,
        k -> new ArrayList<>()).add(Pair.of(fileId4, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition3),
        partitionToFilesNameLengthMap, false, false);

    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record5));

    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(index, recordRDD, hoodieTable);

    for (HoodieRecord record : taggedRecordRDD.collect()) {
      IndexedRecord data = record.toIndexedRecord(SIMPLE_RECORD_SCHEMA, CollectionUtils.emptyProps()).get().getData();
      switch (record.getRecordKey()) {
        case "000":
          assertEquals(record.getCurrentLocation().getFileId(), fileId1);
          assertEquals(data, record1.getData());
          break;
        case "001":
          assertEquals(record.getCurrentLocation().getFileId(), fileId3);
          assertEquals(data, record2.getData());
          break;
        case "002":
          assertFalse(record.isCurrentLocationKnown());
          assertEquals(data, record3.getData());
          break;
        case "003":
          assertEquals(record.getCurrentLocation().getFileId(), fileId4);
          assertEquals(data, record5.getData());
          break;
        case "004":
          assertEquals(record.getCurrentLocation().getFileId(), fileId4);
          assertEquals(data, record4.getData());
          break;
        default:
          throw new IllegalArgumentException("Unknown Key: " + record.getRecordKey());
      }
    }
  }

  /**
   * convert list to map to avoid sorting order dependencies
   */
  private static Map<String, BloomIndexFileInfo> toFileMap(List<Pair<String, BloomIndexFileInfo>> filesList) {
    Map<String, BloomIndexFileInfo> filesMap = new HashMap<>();
    for (Pair<String, BloomIndexFileInfo> t : filesList) {
      filesMap.put(t.getKey() + "/" + t.getValue().getFileId(), t.getValue());
    }
    return filesMap;
  }

}
