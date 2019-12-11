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

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.common.BloomFilter;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.table.HoodieTable;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestHoodieBloomIndex extends HoodieClientTestHarness {

  private String schemaStr;
  private Schema schema;

  private boolean rangePruning;
  private boolean treeFiltering;
  private boolean bucketizedChecking;

  @Parameterized.Parameters(name = "{index}: Test with rangePruning={0}, treeFiltering ={1}, bucketizedChecking is:{2}")
  public static Collection<Object[]> data() {
    Object[][] data =
        new Object[][] {{true, true, true}, {false, true, true}, {true, true, false}, {true, false, true}};
    return Arrays.asList(data);
  }

  public TestHoodieBloomIndex(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    this.rangePruning = rangePruning;
    this.treeFiltering = treeFiltering;
    this.bucketizedChecking = bucketizedChecking;
  }

  @Before
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieBloomIndex");
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    initMetaClient();
  }

  @After
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupMetaClient();
  }

  private HoodieWriteConfig makeConfig() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().bloomIndexPruneByRanges(rangePruning)
            .bloomIndexTreebasedFilter(treeFiltering).bloomIndexBucketizedChecking(bucketizedChecking)
            .bloomIndexKeysPerBucket(2).build())
        .build();
    return config;
  }

  @Test
  public void testLoadInvolvedFiles() throws IOException {
    HoodieWriteConfig config = makeConfig();
    HoodieBloomIndex index = new HoodieBloomIndex(config);

    // Create some partitions, and put some files
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet,
    // 4_0_20150312101010.parquet)
    new File(basePath + "/2016/01/21").mkdirs();
    new File(basePath + "/2016/04/01").mkdirs();
    new File(basePath + "/2015/03/12").mkdirs();

    TestRawTripPayload rowChange1 =
        new TestRawTripPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    TestRawTripPayload rowChange2 =
        new TestRawTripPayload("{\"_row_key\":\"001\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    TestRawTripPayload rowChange3 =
        new TestRawTripPayload("{\"_row_key\":\"002\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    TestRawTripPayload rowChange4 =
        new TestRawTripPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    HoodieClientTestUtils.writeParquetFile(basePath, "2016/04/01", "2_0_20160401010101.parquet", Lists.newArrayList(),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "1_0_20150312101010.parquet", Lists.newArrayList(),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "3_0_20150312101010.parquet", Arrays.asList(record1),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "4_0_20150312101010.parquet",
        Arrays.asList(record2, record3, record4), schema, null, false);

    List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01", "2015/03/12");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    List<Tuple2<String, BloomIndexFileInfo>> filesList = index.loadInvolvedFiles(partitions, jsc, table);
    // Still 0, as no valid commit
    assertEquals(filesList.size(), 0);

    // Add some commits
    new File(basePath + "/.hoodie").mkdirs();
    new File(basePath + "/.hoodie/20160401010101.commit").createNewFile();
    new File(basePath + "/.hoodie/20150312101010.commit").createNewFile();

    table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    filesList = index.loadInvolvedFiles(partitions, jsc, table);
    assertEquals(filesList.size(), 4);

    if (rangePruning) {
      // these files will not have the key ranges
      assertNull(filesList.get(0)._2().getMaxRecordKey());
      assertNull(filesList.get(0)._2().getMinRecordKey());
      assertFalse(filesList.get(1)._2().hasKeyRanges());
      assertNotNull(filesList.get(2)._2().getMaxRecordKey());
      assertNotNull(filesList.get(2)._2().getMinRecordKey());
      assertTrue(filesList.get(3)._2().hasKeyRanges());

      // no longer sorted, but should have same files.

      List<Tuple2<String, BloomIndexFileInfo>> expected =
          Arrays.asList(new Tuple2<>("2016/04/01", new BloomIndexFileInfo("2")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("1")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("3", "000", "000")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("4", "001", "003")));
      assertEquals(expected, filesList);
    }
  }

  @Test
  public void testRangePruning() {
    HoodieWriteConfig config = makeConfig();
    HoodieBloomIndex index = new HoodieBloomIndex(config);

    final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22",
        Arrays.asList(new BloomIndexFileInfo("f1"), new BloomIndexFileInfo("f2", "000", "000"),
            new BloomIndexFileInfo("f3", "001", "003"), new BloomIndexFileInfo("f4", "002", "007"),
            new BloomIndexFileInfo("f5", "009", "010")));

    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        jsc.parallelize(Arrays.asList(new Tuple2<>("2017/10/22", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    List<Tuple2<String, HoodieKey>> comparisonKeyList =
        index.explodeRecordRDDWithFileComparisons(partitionToFileIndexInfo, partitionRecordKeyPairRDD).collect();

    assertEquals(10, comparisonKeyList.size());
    Map<String, List<String>> recordKeyToFileComps = comparisonKeyList.stream()
        .collect(Collectors.groupingBy(t -> t._2.getRecordKey(), Collectors.mapping(t -> t._1, Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new HashSet<>(Arrays.asList("f1", "f3", "f4")), new HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f3", "f4")), new HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f4")), new HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new HashSet<>(Arrays.asList("f1", "f4")), new HashSet<>(recordKeyToFileComps.get("005")));
  }

  @Test
  public void testCheckUUIDsAgainstOneFile() throws IOException, InterruptedException {

    // Create some records to use
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
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

    // We write record1, record2 to a parquet file, but the bloom filter contains (record1,
    // record2, record3).
    BloomFilter filter = new BloomFilter(10000, 0.0000001);
    filter.add(record3.getRecordKey());
    String filename = HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record1, record2),
        schema, filter, true);

    // The bloom filter contains 3 records
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));
    assertTrue(filter.mightContain(record3.getRecordKey()));
    assertFalse(filter.mightContain(record4.getRecordKey()));

    // Compare with file
    List<String> uuids =
        Arrays.asList(record1.getRecordKey(), record2.getRecordKey(), record3.getRecordKey(), record4.getRecordKey());

    List<String> results = HoodieKeyLookupHandle.checkCandidatesAgainstFile(jsc.hadoopConfiguration(), uuids,
        new Path(basePath + "/2016/01/31/" + filename));
    assertEquals(results.size(), 2);
    assertTrue(results.get(0).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
    assertTrue(results.get(0).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
    // TODO(vc): Need more coverage on actual filenames
    // assertTrue(results.get(0)._2().equals(filename));
    // assertTrue(results.get(1)._2().equals(filename));
  }

  @Test
  public void testTagLocationWithEmptyRDD() throws Exception {
    // We have some records to be tagged (two different partitions)
    JavaRDD<HoodieRecord> recordRDD = jsc.emptyRDD();
    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config);

    try {
      bloomIndex.tagLocation(recordRDD, jsc, table);
    } catch (IllegalArgumentException e) {
      fail("EmptyRDD should not result in IllegalArgumentException: Positive number of slices " + "required");
    }
  }

  @Test
  public void testTagLocation() throws Exception {
    // We have some records to be tagged (two different partitions)
    String rowKey1 = UUID.randomUUID().toString();
    String rowKey2 = UUID.randomUUID().toString();
    String rowKey3 = UUID.randomUUID().toString();
    String recordStr1 = "{\"_row_key\":\"" + rowKey1 + "\"," + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"" + rowKey2 + "\"," + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"" + rowKey3 + "\"," + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    // place same row key under a different partition.
    String recordStr4 = "{\"_row_key\":\"" + rowKey1 + "\"," + "\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
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

    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config);
    JavaRDD<HoodieRecord> taggedRecordRDD = bloomIndex.tagLocation(recordRDD, jsc, table);

    // Should not find any files
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record1), schema, null, true);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record2), schema, null, true);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/01/31", Arrays.asList(record4), schema, null, true);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    taggedRecordRDD = bloomIndex.tagLocation(recordRDD, jsc, table);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals("2015/01/31")) {
          assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename3)));
        } else {
          assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename1)));
        }
      } else if (record.getRecordKey().equals(rowKey2)) {
        assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename2)));
      } else if (record.getRecordKey().equals(rowKey3)) {
        assertTrue(!record.isCurrentLocationKnown());
      }
    }
  }

  @Test
  public void testCheckExists() throws Exception {
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
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    HoodieKey key1 = new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath());
    HoodieRecord record1 = new HoodieRecord(key1, rowChange1);
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    HoodieKey key2 = new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath());
    HoodieRecord record2 = new HoodieRecord(key2, rowChange2);
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    HoodieKey key3 = new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath());
    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieKey key4 = new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath());
    HoodieRecord record4 = new HoodieRecord(key4, rowChange4);
    JavaRDD<HoodieKey> keysRDD = jsc.parallelize(Arrays.asList(key1, key2, key3, key4));

    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // Let's tag
    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config);
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> taggedRecordRDD =
        bloomIndex.fetchRecordLocation(keysRDD, jsc, table);

    // Should not find any files
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : taggedRecordRDD.collect()) {
      assertTrue(!record._2.isPresent());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record1), schema, null, true);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record2), schema, null, true);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/01/31", Arrays.asList(record4), schema, null, true);

    // We do the tag again
    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    taggedRecordRDD = bloomIndex.fetchRecordLocation(keysRDD, jsc, table);

    // Check results
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : taggedRecordRDD.collect()) {
      if (record._1.getRecordKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record._2.isPresent());
        assertEquals(FSUtils.getFileId(filename1), record._2.get().getRight());
      } else if (record._1.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record._2.isPresent());
        if (record._1.getPartitionPath().equals("2015/01/31")) {
          assertEquals(FSUtils.getFileId(filename3), record._2.get().getRight());
        } else {
          assertEquals(FSUtils.getFileId(filename2), record._2.get().getRight());
        }
      } else if (record._1.getRecordKey().equals("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
        assertTrue(!record._2.isPresent());
      }
    }
  }

  @Test
  public void testBloomFilterFalseError() throws IOException, InterruptedException {
    // We have two hoodie records
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";

    // We write record1 to a parquet file, using a bloom filter having both records
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);

    BloomFilter filter = new BloomFilter(10000, 0.0000001);
    filter.add(record2.getRecordKey());
    String filename =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Arrays.asList(record1), schema, filter, true);
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));

    // We do the tag
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2));
    HoodieWriteConfig config = makeConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config);
    JavaRDD<HoodieRecord> taggedRecordRDD = bloomIndex.tagLocation(recordRDD, jsc, table);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename)));
      } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }
  }

}
