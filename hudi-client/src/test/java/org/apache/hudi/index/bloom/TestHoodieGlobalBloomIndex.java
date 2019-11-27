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
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestHoodieGlobalBloomIndex extends HoodieClientTestHarness {

  private String schemaStr;
  private Schema schema;

  public TestHoodieGlobalBloomIndex() throws Exception {}

  @Before
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieGlobalBloomIndex");
    initPath();
    // We have some records to be tagged (two different partitions)
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    initMetaClient();
  }

  @After
  public void tearDown() {
    cleanupSparkContexts();
    cleanupMetaClient();
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
    new File(basePath + "/2016/01/21").mkdirs();
    new File(basePath + "/2016/01/21/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();
    new File(basePath + "/2016/04/01").mkdirs();
    new File(basePath + "/2016/04/01/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();
    new File(basePath + "/2015/03/12").mkdirs();
    new File(basePath + "/2015/03/12/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();

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

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    // partitions will NOT be respected by this loadInvolvedFiles(...) call
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

    // the partition partition of the key of the incoming records will be ignored
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
    new File(basePath + "/2016/01/21").mkdirs();
    new File(basePath + "/2016/01/21/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();
    new File(basePath + "/2016/04/01").mkdirs();
    new File(basePath + "/2016/04/01/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();
    new File(basePath + "/2015/03/12").mkdirs();
    new File(basePath + "/2015/03/12/" + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE).createNewFile();

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
        new HoodieRecord(new HoodieKey(rowChange5.getRowKey(), rowChange5.getPartitionPath()), rowChange4);

    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record5));

    String filename0 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2016/04/01", Arrays.asList(record1), schema, null, false);
    String filename1 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", Lists.newArrayList(), schema, null, false);
    String filename2 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", Arrays.asList(record2), schema, null, false);
    String filename3 =
        HoodieClientTestUtils.writeParquetFile(basePath, "2015/03/12", Arrays.asList(record4), schema, null, false);

    // intentionally missed the partition "2015/03/12" to see if the GlobalBloomIndex can pick it up
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);


    // Add some commits
    new File(basePath + "/.hoodie").mkdirs();

    // partitions will NOT be respected by this loadInvolvedFiles(...) call
    JavaRDD<HoodieRecord> taggedRecordRDD = index.tagLocation(recordRDD, jsc, table);

    for (HoodieRecord record : taggedRecordRDD.collect()) {
      if (record.getRecordKey().equals("000")) {
        assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename0)));
      } else if (record.getRecordKey().equals("001")) {
        assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename2)));
      } else if (record.getRecordKey().equals("002")) {
        assertTrue(!record.isCurrentLocationKnown());
      } else if (record.getRecordKey().equals("004")) {
        assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename3)));
      }
    }
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
