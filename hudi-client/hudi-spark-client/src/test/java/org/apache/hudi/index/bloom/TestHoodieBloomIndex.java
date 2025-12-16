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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.genPseudoRandomUUID;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBloomIndex extends TestHoodieMetadataBase {

  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieBloomIndex.class, "/exampleSchema.avsc", true);
  private static final String TEST_NAME_WITH_PARAMS =
      "[{index}] Test with rangePruning={0}, treeFiltering={1}, bucketizedChecking={2}, "
          + "useMetadataTable={3}, enableFileGroupIdKeySorting={4}";
  private static final Random RANDOM = new Random(0xDEED);

  public static Stream<Arguments> configParams() {
    // rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting
    Object[][] data = new Object[][] {
        {true, true, true, false, false},
        {false, true, true, false, false},
        {true, true, false, false, false},
        {true, false, true, false, false},
        {true, true, true, true, false},
        {false, true, true, true, false},
        {true, true, false, true, false},
        {true, false, true, true, false},
        {true, true, false, false, true},
        {true, false, false, false, true},
        {false, false, false, false, true},
        {false, true, false, false, true}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    // We have some records to be tagged (two different partitions)
    initMetaClient();
    HoodieIndexConfig.Builder indexBuilder = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(indexBuilder.build())
        .build();
    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieWriteConfig makeConfig(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) {
    // For the bloom index to use column stats and bloom filters from metadata table,
    // the following configs must be set to true:
    // "hoodie.bloom.index.use.metadata"
    // "hoodie.metadata.enable" (by default is true)
    // "hoodie.metadata.index.column.stats.enable"
    // "hoodie.metadata.index.bloom.filter.enable"
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .bloomIndexPruneByRanges(rangePruning)
            .bloomIndexTreebasedFilter(treeFiltering)
            .bloomIndexBucketizedChecking(bucketizedChecking)
            .bloomIndexKeysPerBucket(2)
            .bloomIndexUseMetadata(useMetadataTable)
            .enableBloomIndexFileGroupIdKeySorting(enableFileGroupIdKeySorting)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMetadataIndexBloomFilter(useMetadataTable)
            .withMetadataIndexColumnStats(useMetadataTable)
            .build())
        .build();
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testLoadInvolvedFiles(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) throws Exception {
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    HoodieBloomIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), metadataWriter, Option.of(context));

    // Create some partitions, and put some files
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
    testTable.withPartitionMetaFiles("2016/01/21", "2016/04/01", "2015/03/12");

    HoodieRecord record1 = createSimpleRecord("000", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord("001", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record3 = createSimpleRecord("002", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record4 = createSimpleRecord("003", "2016-01-31T03:16:41.415Z", 12);

    List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01", "2015/03/12");
    List<Pair<String, BloomIndexFileInfo>> filesList = index.loadColumnRangesFromFiles(partitions, context, hoodieTable);
    // Still 0, as no valid commit
    assertEquals(0, filesList.size());

    final String fileId1 = "1";
    final String fileId2 = "2";
    final String fileId3 = "3";
    final String fileId4 = "4";
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    String commitTime = "20160401010101";
    StoragePath baseFilePath = testTable.forCommit(commitTime)
        .withInserts(partitions.get(1), fileId2, Collections.emptyList());
    long baseFileLength =
        storage.getPathInfo(new StoragePath(baseFilePath.toUri())).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partitions.get(1),
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT,
        Arrays.asList(partitions.get(1)),
        partitionToFilesNameLengthMap, false, false);

    commitTime = "20150312101010";
    partitionToFilesNameLengthMap.clear();
    testTable.forCommit(commitTime);
    baseFilePath = testTable.withInserts(partitions.get(2), fileId1, Collections.emptyList());
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partitions.get(2),
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));

    baseFilePath =
        testTable.withInserts(partitions.get(2), fileId3, Collections.singletonList(record1));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partitions.get(2),
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));

    baseFilePath =
        testTable.withInserts(partitions.get(2), fileId4, Arrays.asList(record2, record3, record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partitions.get(2),
        k -> new ArrayList<>()).add(Pair.of(fileId4, Integer.valueOf((int) baseFileLength)));

    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Arrays.asList(partitions.get(2)),
        partitionToFilesNameLengthMap, false, false);

    filesList = index.loadColumnRangesFromFiles(partitions, context, hoodieTable);
    assertEquals(4, filesList.size());

    if (rangePruning) {
      // these files will not have the key ranges
      assertNull(filesList.get(0).getRight().getMaxRecordKey());
      assertNull(filesList.get(0).getRight().getMinRecordKey());
      assertFalse(filesList.get(1).getRight().hasKeyRanges());
      assertNotNull(filesList.get(2).getRight().getMaxRecordKey());
      assertNotNull(filesList.get(2).getRight().getMinRecordKey());
      assertTrue(filesList.get(3).getRight().hasKeyRanges());

      // no longer sorted, but should have same files.

      List<ImmutablePair<String, BloomIndexFileInfo>> expected =
          Arrays.asList(new ImmutablePair<>("2016/04/01", new BloomIndexFileInfo("2")),
              new ImmutablePair<>("2015/03/12", new BloomIndexFileInfo("1")),
              new ImmutablePair<>("2015/03/12", new BloomIndexFileInfo("3", "000", "000")),
              new ImmutablePair<>("2015/03/12", new BloomIndexFileInfo("4", "001", "003")));
      assertEquals(expected, filesList);
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testRangePruning(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) {
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    HoodieBloomIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());

    final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22",
        Arrays.asList(new BloomIndexFileInfo("f1"), new BloomIndexFileInfo("f2", "000", "000"),
            new BloomIndexFileInfo("f3", "001", "003"), new BloomIndexFileInfo("f4", "002", "007"),
            new BloomIndexFileInfo("f5", "009", "010")));

    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        jsc.parallelize(Arrays.asList(new Tuple2<>("2017/10/22", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    List<Pair<HoodieFileGroupId, String>> comparisonKeyList =
        index.explodeRecordsWithFileComparisons(partitionToFileIndexInfo, HoodieJavaPairRDD.of(partitionRecordKeyPairRDD)).collectAsList();

    assertEquals(10, comparisonKeyList.size());
    Map<String, List<String>> recordKeyToFileComps = comparisonKeyList.stream()
        .collect(
            Collectors.groupingBy(t -> t.getRight(),
                Collectors.mapping(t -> t.getLeft().getFileId(), Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new HashSet<>(Arrays.asList("f1", "f3", "f4")), new HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f3", "f4")), new HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f4")), new HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f4")), new HashSet<>(recordKeyToFileComps.get("005")));
  }

  @Test
  public void testCheckUUIDsAgainstOneFile() throws Exception {
    final String partition = "2016/01/31";
    // Create some records to use
    HoodieRecord record1 = createSimpleRecord("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0", "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord record3 = createSimpleRecord("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 15);
    HoodieRecord record4 = createSimpleRecord("4eb5b87c-1fej-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 32);

    // We write record1, record2 to a parquet file, but the bloom filter contains (record1,
    // record2, record3).
    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    filter.add(record3.getRecordKey());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), filter, metadataWriter, Option.of(context));

    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();
    final String commitTime = "0000001";
    final String fileId = genRandomUUID();

    StoragePath baseFilePath = testTable.forCommit(commitTime)
        .withInserts(partition, fileId, Arrays.asList(record1, record2));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition,
        k -> new ArrayList<>()).add(Pair.of(fileId, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commitTime, WriteOperationType.UPSERT, Collections.singletonList(partition),
        partitionToFilesNameLengthMap, false, false);
    final String filename = testTable.getBaseFileNameById(fileId);

    // The bloom filter contains 3 records
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));
    assertTrue(filter.mightContain(record3.getRecordKey()));
    assertFalse(filter.mightContain(record4.getRecordKey()));

    // Compare with file
    List<String> uuids =
        Arrays.asList(record1.getRecordKey(), record2.getRecordKey(), record3.getRecordKey(), record4.getRecordKey());

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    List<Pair<String, Long>> results = HoodieIndexUtils.filterKeysFromFile(
        new StoragePath(Paths.get(basePath, partition, filename).toString()), uuids, storage);

    assertEquals(results.size(), 2);
    assertTrue(results.get(0).getLeft().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")
        || results.get(1).getLeft().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
    assertTrue(results.get(0).getLeft().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")
        || results.get(1).getLeft().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
    // TODO(vc): Need more coverage on actual filenames
    // assertTrue(results.get(0)._2().equals(filename));
    // assertTrue(results.get(1)._2().equals(filename));
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testTagLocationWithEmptyRDD(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) {
    // We have some records to be tagged (two different partitions)
    JavaRDD<HoodieRecord> recordRDD = jsc.emptyRDD();
    // Also create the metadata and config
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());

    assertDoesNotThrow(() -> {
      tagLocation(bloomIndex, recordRDD, table);
    }, "EmptyRDD should not result in IllegalArgumentException: Positive number of slices required");
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testTagLocationOnPartitionedTable(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) throws Exception {
    // We have some records to be tagged (two different partitions)
    String rowKey1 = genRandomUUID();
    String rowKey2 = genRandomUUID();
    String rowKey3 = genRandomUUID();
    HoodieRecord record1 = createSimpleRecord(rowKey1, "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord(rowKey2, "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord record3 = createSimpleRecord(rowKey3, "2016-01-31T03:16:41.415Z", 15);
    // place same row key under a different partition.
    HoodieRecord record4 = createSimpleRecord(rowKey1, "2015-01-31T03:16:41.415Z", 32);
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

    // Also create the metadata and config
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), metadataWriter, Option.of(context));

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(bloomIndex, recordRDD, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();
    final String partition1 = "2016/01/31";
    final String partition2 = "2015/01/31";

    // We create three parquet file, each having one record. (two different partitions)
    final String fileId1 = genRandomUUID();
    final String commit1 = "0000001";
    StoragePath baseFilePath = testTable.forCommit(commit1)
        .withInserts(partition1, fileId1, Collections.singletonList(record1));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition1,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit1, WriteOperationType.UPSERT, Collections.singletonList(partition1),
        partitionToFilesNameLengthMap, false, false);

    final String fileId2 = genRandomUUID();
    final String commit2 = "0000002";
    baseFilePath = testTable.forCommit(commit2)
        .withInserts(partition1, fileId2, Collections.singletonList(record2));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition1,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit2, WriteOperationType.UPSERT, Collections.singletonList(partition1),
        partitionToFilesNameLengthMap, false, false);

    final String fileId3 = genRandomUUID();
    final String commit3 = "0000003";
    baseFilePath = testTable.forCommit(commit3)
        .withInserts(partition2, fileId3, Collections.singletonList(record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(partition2,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit3, WriteOperationType.UPSERT, Collections.singletonList(partition2),
        partitionToFilesNameLengthMap, false, false);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    taggedRecordRDD = tagLocation(bloomIndex, recordRDD, HoodieSparkTable.create(config, context, metaClient));

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals(partition2)) {
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
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testTagLocationOnNonpartitionedTable(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) throws Exception {
    // We have some records to be tagged (two different partitions)
    String rowKey1 = genRandomUUID();
    String rowKey2 = genRandomUUID();
    String rowKey3 = genRandomUUID();

    String emptyPartitionPath = "";
    HoodieRecord record1 = createSimpleRecord(rowKey1, "2016-01-31T03:16:41.415Z", 12, Option.of(""));
    HoodieRecord record2 = createSimpleRecord(rowKey2, "2016-01-31T03:20:41.415Z", 100, Option.of(""));
    HoodieRecord record3 = createSimpleRecord(rowKey3, "2016-01-31T03:16:41.415Z", 15, Option.of(""));

    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3));

    // Also create the metadata and config
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), metadataWriter, Option.of(context));

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(bloomIndex, recordRDD, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();

    // We create three parquet file, each having one record
    final String fileId1 = genRandomUUID();
    final String commit1 = "0000001";
    StoragePath baseFilePath = testTable.forCommit(commit1)
        .withInserts(emptyPartitionPath, fileId1, Collections.singletonList(record1));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(emptyPartitionPath,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit1, WriteOperationType.UPSERT, Collections.singletonList(emptyPartitionPath),
        partitionToFilesNameLengthMap, false, false);

    final String fileId2 = genRandomUUID();
    final String commit2 = "0000002";
    baseFilePath = testTable.forCommit(commit2)
        .withInserts(emptyPartitionPath, fileId2, Collections.singletonList(record2));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(emptyPartitionPath,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit2, WriteOperationType.UPSERT, Collections.singletonList(emptyPartitionPath),
        partitionToFilesNameLengthMap, false, false);

    final String fileId3 = UUID.randomUUID().toString();
    final String commit3 = "0000003";
    baseFilePath = testTable.forCommit(commit3)
        .withInserts(emptyPartitionPath, fileId3, Collections.singletonList(record3));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.clear();
    partitionToFilesNameLengthMap.computeIfAbsent(emptyPartitionPath,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit3, WriteOperationType.UPSERT, Collections.singletonList(emptyPartitionPath),
        partitionToFilesNameLengthMap, false, false);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    taggedRecordRDD = tagLocation(bloomIndex, recordRDD, HoodieSparkTable.create(config, context, metaClient));

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId1);
      } else if (record.getRecordKey().equals(rowKey2)) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId2);
      } else if (record.getRecordKey().equals(rowKey3)) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId3);
      }
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testCheckExists(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) throws Exception {
    // We have some records to be tagged (two different partitions)
    HoodieRecord record1 = createSimpleRecord("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0", "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord record3 = createSimpleRecord("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 15);
    // record key same as recordStr2
    HoodieRecord record4 = createSimpleRecord("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0", "2015-01-31T03:16:41.415Z", 32);
    JavaRDD<HoodieKey> keysRDD = jsc.parallelize(Arrays.asList(record1.getKey(), record2.getKey(), record3.getKey(), record4.getKey()));

    // Also create the metadata and config
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), metadataWriter, Option.of(context));

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecords = tagLocation(
        bloomIndex, keysRDD.map(k -> new HoodieEmptyRecord<>(k, HoodieRecord.HoodieRecordType.AVRO)), hoodieTable);
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocationsRDD = taggedRecords
        .mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
            ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
            : Option.empty())
        );

    // Should not find any files
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : recordLocationsRDD.collect()) {
      assertTrue(!record._2.isPresent());
    }

    final String partition1 = "2016/01/31";
    final String partition2 = "2015/01/31";
    final String fileId1 = genRandomUUID();
    final String fileId2 = genRandomUUID();
    final String fileId3 = genRandomUUID();
    final Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();
    // We create three parquet file, each having one record. (two different partitions)
    final String commit1 = "0000001";
    StoragePath baseFilePath = testTable.forCommit(commit1)
        .withInserts(partition1, fileId1, Collections.singletonList(record1));
    long baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition1,
        k -> new ArrayList<>()).add(Pair.of(fileId1, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit1, WriteOperationType.UPSERT, Collections.singletonList(partition1),
        partitionToFilesNameLengthMap, false, false);

    final String commit2 = "0000002";
    partitionToFilesNameLengthMap.clear();
    baseFilePath = testTable.forCommit(commit2)
        .withInserts(partition1, fileId2, Collections.singletonList(record2));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition1,
        k -> new ArrayList<>()).add(Pair.of(fileId2, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit2, WriteOperationType.UPSERT, Collections.singletonList(partition1),
        partitionToFilesNameLengthMap, false, false);

    final String commit3 = "0000003";
    partitionToFilesNameLengthMap.clear();
    baseFilePath = testTable.forCommit(commit3)
        .withInserts(partition2, fileId3, Collections.singletonList(record4));
    baseFileLength = storage.getPathInfo(baseFilePath).getLength();
    partitionToFilesNameLengthMap.computeIfAbsent(partition2,
        k -> new ArrayList<>()).add(Pair.of(fileId3, Integer.valueOf((int) baseFileLength)));
    testTable.doWriteOperation(commit3, WriteOperationType.UPSERT, Collections.singletonList(partition2),
        partitionToFilesNameLengthMap, false, false);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    taggedRecords = tagLocation(bloomIndex, keysRDD.map(k -> new HoodieAvroRecord(k, null)), hoodieTable);
    recordLocationsRDD = taggedRecords
        .mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
            ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
            : Option.empty())
        );

    // Check results
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : recordLocationsRDD.collect()) {
      if (record._1.getRecordKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record._2.isPresent());
        assertEquals(fileId1, record._2.get().getRight());
      } else if (record._1.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record._2.isPresent());
        if (record._1.getPartitionPath().equals(partition2)) {
          assertEquals(fileId3, record._2.get().getRight());
        } else {
          assertEquals(fileId2, record._2.get().getRight());
        }
      } else if (record._1.getRecordKey().equals("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record._2.isPresent());
      }
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testBloomFilterFalseError(
      boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking,
      boolean useMetadataTable, boolean enableFileGroupIdKeySorting) throws Exception {
    // We have two hoodie records
    // We write record1 to a parquet file, using a bloom filter having both records
    HoodieRecord record1 = createSimpleRecord("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord record2 = createSimpleRecord("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0", "2016-01-31T03:20:41.415Z", 100);

    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    filter.add(record2.getRecordKey());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA.toAvroSchema(), filter, Option.of(context));
    String fileId = testTable.addCommit("000").getFileIdWithInserts("2016/01/31", record1);
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));

    // We do the tag
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2));
    HoodieWriteConfig config =
        makeConfig(rangePruning, treeFiltering, bucketizedChecking, useMetadataTable, enableFileGroupIdKeySorting);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(bloomIndex, recordRDD, table);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId);
      } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }
  }

  private static String genRandomUUID() {
    return genPseudoRandomUUID(RANDOM).toString();
  }
}
