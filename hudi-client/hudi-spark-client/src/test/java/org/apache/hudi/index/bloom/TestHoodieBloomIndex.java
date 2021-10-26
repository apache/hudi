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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBloomIndex extends HoodieClientTestHarness {

  private static final Schema SCHEMA = getSchemaFromResource(TestHoodieBloomIndex.class, "/exampleSchema.avsc", true);
  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with rangePruning={0}, treeFiltering={1}, bucketizedChecking={2}";

  public static Stream<Arguments> configParams() {
    Object[][] data =
        new Object[][] {{true, true, true}, {false, true, true}, {true, true, false}, {true, false, true}};
    return Stream.of(data).map(Arguments::of);
  }

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieWriteConfig makeConfig(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().bloomIndexPruneByRanges(rangePruning)
            .bloomIndexTreebasedFilter(treeFiltering).bloomIndexBucketizedChecking(bucketizedChecking)
            .bloomIndexKeysPerBucket(2).build())
        .build();
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testLoadInvolvedFiles(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    HoodieBloomIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Create some partitions, and put some files
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
    testTable.withPartitionMetaFiles("2016/01/21", "2016/04/01", "2015/03/12");

    RawTripTestPayload rowChange1 =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 =
        new RawTripTestPayload("{\"_row_key\":\"001\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 =
        new RawTripTestPayload("{\"_row_key\":\"002\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 =
        new RawTripTestPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01", "2015/03/12");
    List<ImmutablePair<String, BloomIndexFileInfo>> filesList = index.loadInvolvedFiles(partitions, context, hoodieTable);
    // Still 0, as no valid commit
    assertEquals(0, filesList.size());

    testTable.addCommit("20160401010101").withInserts("2016/04/01", "2");
    testTable.addCommit("20150312101010").withInserts("2015/03/12", "1")
        .withInserts("2015/03/12", "3", record1)
        .withInserts("2015/03/12", "4", record2, record3, record4);

    filesList = index.loadInvolvedFiles(partitions, context, hoodieTable);
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
  public void testRangePruning(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    HoodieBloomIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());

    final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22",
        Arrays.asList(new BloomIndexFileInfo("f1"), new BloomIndexFileInfo("f2", "000", "000"),
            new BloomIndexFileInfo("f3", "001", "003"), new BloomIndexFileInfo("f4", "002", "007"),
            new BloomIndexFileInfo("f5", "009", "010")));

    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        jsc.parallelize(Arrays.asList(new Tuple2<>("2017/10/22", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    List<Pair<String, HoodieKey>> comparisonKeyList = HoodieJavaRDD.getJavaRDD(
        index.explodeRecordsWithFileComparisons(partitionToFileIndexInfo, HoodieJavaPairRDD.of(partitionRecordKeyPairRDD))).collect();

    assertEquals(10, comparisonKeyList.size());
    Map<String, List<String>> recordKeyToFileComps = comparisonKeyList.stream()
        .collect(Collectors.groupingBy(t -> t.getRight().getRecordKey(), Collectors.mapping(Pair::getLeft, Collectors.toList())));

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
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
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

    // We write record1, record2 to a parquet file, but the bloom filter contains (record1,
    // record2, record3).
    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    filter.add(record3.getRecordKey());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, filter);
    String fileId = testTable.addCommit("000").getFileIdWithInserts(partition, record1, record2);
    String filename = testTable.getBaseFileNameById(fileId);

    // The bloom filter contains 3 records
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));
    assertTrue(filter.mightContain(record3.getRecordKey()));
    assertFalse(filter.mightContain(record4.getRecordKey()));

    // Compare with file
    List<String> uuids =
        Arrays.asList(record1.getRecordKey(), record2.getRecordKey(), record3.getRecordKey(), record4.getRecordKey());

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);
    HoodieKeyLookupHandle keyHandle = new HoodieKeyLookupHandle<>(config, table, Pair.of(partition, fileId));
    List<String> results = keyHandle.checkCandidatesAgainstFile(hadoopConf, uuids,
        new Path(Paths.get(basePath, partition, filename).toString()));
    assertEquals(results.size(), 2);
    assertTrue(results.get(0).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
    assertTrue(results.get(0).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
    // TODO(vc): Need more coverage on actual filenames
    // assertTrue(results.get(0)._2().equals(filename));
    // assertTrue(results.get(1)._2().equals(filename));
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testTagLocationWithEmptyRDD(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    // We have some records to be tagged (two different partitions)
    JavaRDD<HoodieRecord> recordRDD = jsc.emptyRDD();
    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
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
  public void testTagLocation(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have some records to be tagged (two different partitions)
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

    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(bloomIndex, recordRDD, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String fileId1 = testTable.addCommit("001").getFileIdWithInserts("2016/01/31", record1);
    String fileId2 = testTable.addCommit("002").getFileIdWithInserts("2016/01/31", record2);
    String fileId3 = testTable.addCommit("003").getFileIdWithInserts("2015/01/31", record4);

    // We do the tag again
    taggedRecordRDD = tagLocation(bloomIndex, recordRDD, HoodieSparkTable.create(config, context, metaClient));

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals("2015/01/31")) {
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
  public void testCheckExists(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have some records to be tagged (two different partitions)

    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    // record key same as recordStr2
    String recordStr4 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieKey key1 = new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath());
    HoodieRecord record1 = new HoodieRecord(key1, rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieKey key2 = new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath());
    HoodieRecord record2 = new HoodieRecord(key2, rowChange2);
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    HoodieKey key3 = new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath());
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieKey key4 = new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath());
    HoodieRecord record4 = new HoodieRecord(key4, rowChange4);
    JavaRDD<HoodieKey> keysRDD = jsc.parallelize(Arrays.asList(key1, key2, key3, key4));

    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecords = tagLocation(
        bloomIndex, keysRDD.map(k -> new HoodieRecord(k, null)), hoodieTable);
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> recordLocationsRDD = taggedRecords
        .mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
            ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
            : Option.empty())
        );

    // Should not find any files
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : recordLocationsRDD.collect()) {
      assertTrue(!record._2.isPresent());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String fileId1 = testTable.addCommit("001").getFileIdWithInserts("2016/01/31", record1);
    String fileId2 = testTable.addCommit("002").getFileIdWithInserts("2016/01/31", record2);
    String fileId3 = testTable.addCommit("003").getFileIdWithInserts("2015/01/31", record4);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    taggedRecords = tagLocation(bloomIndex, keysRDD.map(k -> new HoodieRecord(k, null)), hoodieTable);
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
        if (record._1.getPartitionPath().equals("2015/01/31")) {
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
  public void testBloomFilterFalseError(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have two hoodie records
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";

    // We write record1 to a parquet file, using a bloom filter having both records
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);

    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    filter.add(record2.getRecordKey());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, filter);
    String fileId = testTable.addCommit("000").getFileIdWithInserts("2016/01/31", record1);
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));

    // We do the tag
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2));
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
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
}
