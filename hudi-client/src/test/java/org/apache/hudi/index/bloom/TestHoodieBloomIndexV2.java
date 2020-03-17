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

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieBloomIndexV2 extends HoodieClientTestHarness {

  private String schemaStr;
  private Schema schema;

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with rangePruning={0}, treeFiltering={1}, bucketizedChecking={2}";

  public static Stream<Arguments> configParams() {
    Object[][] data =
            new Object[][] {{true, true, true}, {false, true, true}, {true, true, false}, {true, false, true}};
    return Stream.of(data).map(Arguments::of);
  }

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieBloomIndexV2");
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupMetaClient();
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
  public void testTagLocationWithEmptyRDD(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    // We have some records to be tagged (two different partitions)
    JavaRDD<HoodieRecord> recordRDD = jsc.emptyRDD();
    // Also create the metadata and config
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Let's tag
    HoodieBloomIndexV2 bloomIndex = new HoodieBloomIndexV2(config);

    try {
      bloomIndex.tagLocation(recordRDD, jsc, table);
    } catch (IllegalArgumentException e) {
      fail("EmptyRDD should not result in IllegalArgumentException: Positive number of slices required");
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
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // Let's tag
    HoodieBloomIndexV2 bloomIndex = new HoodieBloomIndexV2(config);
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> taggedRecordRDD =
            bloomIndex.fetchRecordLocation(keysRDD, jsc, table);

    // Should not find any files
    for (Tuple2<HoodieKey, Option<Pair<String, String>>> record : taggedRecordRDD.collect()) {
      assertTrue(!record._2.isPresent());
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
    table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());
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
        assertFalse(record._2.isPresent());
      }
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testBloomFilterFalseError(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws IOException, InterruptedException {
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

    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    filter.add(record2.getRecordKey());
    String filename =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/01/31", Collections.singletonList(record1), schema, filter, true);
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));

    // We do the tag
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2));
    HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    HoodieBloomIndexV2 bloomIndex = new HoodieBloomIndexV2(config);
    JavaRDD<HoodieRecord> taggedRecordRDD = bloomIndex.tagLocation(recordRDD, jsc, table);

    // Check results
    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename));
      } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }
  }

}
