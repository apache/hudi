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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.TestRawTripPayload;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieGlobalBloomIndex extends HoodieClientTestHarness {

  private Schema schema;

  public TestHoodieGlobalBloomIndex() {
  }

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieGlobalBloomIndex");
    initPath();
    // We have some records to be tagged (two different partitions)
    String schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    initMetaClient();
  }

  @AfterEach
  public void tearDown() {
    cleanupSparkContexts();
    cleanupClients();
  }

  @Test
  public void testLoadInvolvedFiles() throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieGlobalBloomIndex index = new HoodieGlobalBloomIndex(config);

    // Create some partitions, and put some files, along with the meta file
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet,
    // 4_0_20150312101010.parquet)
    Path dir1 = Files.createDirectories(Paths.get(basePath, "2016", "01", "21"));
    Files.createFile(dir1.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    Path dir2 = Files.createDirectories(Paths.get(basePath, "2016", "04", "01"));
    Files.createFile(dir2.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    Path dir3 = Files.createDirectories(Paths.get(basePath, "2015", "03", "12"));
    Files.createFile(dir3.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));

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

    HoodieClientTestUtils.writeParquetFile(basePath, "2016/04/01", "2_0_20160401010101.parquet", new ArrayList<>(),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "1_0_20150312101010.parquet", new ArrayList<>(),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "3_0_20150312101010.parquet", Collections.singletonList(record1),
        schema, null, false);
    HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", "4_0_20150312101010.parquet",
        Arrays.asList(record2, record3, record4), schema, null, false);

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf);
    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    List<Tuple2<String, BloomIndexFileInfo>> filesList = index.loadInvolvedFiles(partitions, jsc, table);
    // Still 0, as no valid commit
    assertEquals(0, filesList.size());

    // Add some commits
    Path hoodieDir = Files.createDirectories(Paths.get(basePath, ".hoodie"));
    Files.createFile(hoodieDir.resolve("20160401010101.commit"));
    Files.createFile(hoodieDir.resolve("20150312101010.commit"));

    table = HoodieTable.create(metaClient, config, hadoopConf);
    filesList = index.loadInvolvedFiles(partitions, jsc, table);
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
    HoodieGlobalBloomIndex index = new HoodieGlobalBloomIndex(config);

    final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22", Arrays.asList(new BloomIndexFileInfo("f1"),
        new BloomIndexFileInfo("f2", "000", "000"), new BloomIndexFileInfo("f3", "001", "003")));

    partitionToFileIndexInfo.put("2017/10/23",
        Arrays.asList(new BloomIndexFileInfo("f4", "002", "007"), new BloomIndexFileInfo("f5", "009", "010")));

    // the partition of the key of the incoming records will be ignored
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        jsc.parallelize(Arrays.asList(new Tuple2<>("2017/10/21", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/23", "004"))).mapToPair(t -> t);

    List<Tuple2<String, HoodieKey>> comparisonKeyList =
        index.explodeRecordRDDWithFileComparisons(partitionToFileIndexInfo, partitionRecordKeyPairRDD).collect();

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
        .collect(Collectors.groupingBy(t -> t._2.getRecordKey(), Collectors.mapping(Tuple2::_1, Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1", "f3")), new HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new HashSet<>(Arrays.asList("f4", "f1")), new HashSet<>(recordKeyToFileComps.get("005")));
  }

  @Test
  public void testTagLocation() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieGlobalBloomIndex index = new HoodieGlobalBloomIndex(config);

    // Create some partitions, and put some files, along with the meta file
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet,
    // 4_0_20150312101010.parquet)
    Path dir1 = Files.createDirectories(Paths.get(basePath, "2016", "01", "21"));
    Files.createFile(dir1.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    Path dir2 = Files.createDirectories(Paths.get(basePath, "2016", "04", "01"));
    Files.createFile(dir2.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    Path dir3 = Files.createDirectories(Paths.get(basePath, "2015", "03", "12"));
    Files.createFile(dir3.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));

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

    // this record will be saved in table and will be tagged to the incoming record5
    TestRawTripPayload rowChange4 =
        new TestRawTripPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    // this has the same record key as record4 but different time so different partition, but globalbloomIndex should
    // tag the original partition of the saved record4
    TestRawTripPayload rowChange5 =
        new TestRawTripPayload("{\"_row_key\":\"003\",\"time\":\"2016-02-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record5 =
        new HoodieRecord(new HoodieKey(rowChange5.getRowKey(), rowChange5.getPartitionPath()), rowChange5);

    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record5));

    String filename0 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/04/01", Collections.singletonList(record1), schema, null, false);
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", new ArrayList<>(), schema, null, false);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", Collections.singletonList(record2), schema, null, false);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", Collections.singletonList(record4), schema, null, false);

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf);

    // Add some commits
    Files.createDirectories(Paths.get(basePath, ".hoodie"));

    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    JavaRDD<HoodieRecord> taggedRecordRDD = index.tagLocation(recordRDD, jsc, table);

    for (HoodieRecord record : taggedRecordRDD.collect()) {
      switch (record.getRecordKey()) {
        case "000":
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename0));
          assertEquals(((TestRawTripPayload) record.getData()).getJsonData(), rowChange1.getJsonData());
          break;
        case "001":
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename2));
          assertEquals(((TestRawTripPayload) record.getData()).getJsonData(), rowChange2.getJsonData());
          break;
        case "002":
          assertFalse(record.isCurrentLocationKnown());
          assertEquals(((TestRawTripPayload) record.getData()).getJsonData(), rowChange3.getJsonData());
          break;
        case "003":
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename3));
          assertEquals(((TestRawTripPayload) record.getData()).getJsonData(), rowChange5.getJsonData());
          break;
        case "004":
          assertEquals(record.getCurrentLocation().getFileId(), FSUtils.getFileId(filename3));
          assertEquals(((TestRawTripPayload) record.getData()).getJsonData(), rowChange4.getJsonData());
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
        .withIndexConfig(HoodieIndexConfig.newBuilder().withBloomIndexUpdatePartitionPath(true).build())
        .build();
    HoodieGlobalBloomIndex index = new HoodieGlobalBloomIndex(config);

    // Create the original partition, and put a record, along with the meta file
    // "2016/01/31": 1 file (1_0_20160131101010.parquet)
    Path dir = Files.createDirectories(Paths.get(basePath, "2016", "01", "31"));
    Files.createFile(dir.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));

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

    HoodieClientTestUtils
        .writeParquetFile(basePath, "2016/01/31", Collections.singletonList(originalRecord), schema, null, false);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf);

    // Add some commits
    Files.createDirectories(Paths.get(basePath, ".hoodie"));

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
          assertEquals(incomingPayload.getJsonData(), ((TestRawTripPayload) record.getData()).getJsonData());
          break;
        default:
          fail(String.format("Should not get partition path: %s", record.getPartitionPath()));
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
    assertEquals(incomingPayloadSamePartition.getJsonData(), ((TestRawTripPayload) record.getData()).getJsonData());
  }

  // convert list to map to avoid sorting order dependencies
  private Map<String, BloomIndexFileInfo> toFileMap(List<Tuple2<String, BloomIndexFileInfo>> filesList) {
    Map<String, BloomIndexFileInfo> filesMap = new HashMap<>();
    for (Tuple2<String, BloomIndexFileInfo> t : filesList) {
      filesMap.put(t._1() + "/" + t._2().getFileId(), t._2());
    }
    return filesMap;
  }

}
