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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRDDSimpleBucketBulkInsertPartitioner extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts("TestRDDSimpleBucketPartitioner");
    initHoodieStorage();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testSimpleBucketPartitioner(String tableType, boolean partitionSort) throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableType.valueOf(tableType));
    int bucketNum = 10;
    HoodieWriteConfig config = HoodieWriteConfig
        .newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .build();
    config.setValue(HoodieIndexConfig.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    config.setValue(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE, HoodieIndex.BucketIndexEngineType.SIMPLE.name());
    config.setValue(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD, "_row_key");
    config.setValue(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS, "" + bucketNum);
    if (partitionSort) {
      config.setValue(HoodieWriteConfig.BULK_INSERT_SORT_MODE, BulkInsertSortMode.PARTITION_SORT.name());
    }

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGenerator.generateInserts("0", 1000);
    HoodieJavaRDD<HoodieRecord> javaRDD = HoodieJavaRDD.of(records, context, 1);

    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    BulkInsertPartitioner partitioner = BulkInsertInternalPartitionerFactory.get(table, config);
    JavaRDD<HoodieRecord> repartitionRecords =
        (JavaRDD<HoodieRecord>) partitioner.repartitionRecords(HoodieJavaRDD.getJavaRDD(javaRDD), 1);

    assertEquals(bucketNum * javaRDD.map(HoodieRecord::getPartitionPath).distinct().count(),
        repartitionRecords.getNumPartitions());

    if (partitionSort) {
      repartitionRecords.mapPartitionsWithIndex((num, partition) -> {
        List<HoodieRecord> partitionRecords = new ArrayList<>();
        partition.forEachRemaining(partitionRecords::add);
        ArrayList<HoodieRecord> sortedRecordList = new ArrayList<>(partitionRecords);
        sortedRecordList.sort(Comparator.comparing(HoodieRecord::getRecordKey));
        assertEquals(sortedRecordList, partitionRecords);
        return partitionRecords.iterator();
      }, false).collect();
    }

    // first writes, it will create new bucket files based on the records
    getHoodieWriteClient(config).startCommitWithTime("0");
    List<WriteStatus> writeStatues = getHoodieWriteClient(config).bulkInsert(HoodieJavaRDD.getJavaRDD(javaRDD), "0").collect();
    Map<String, WriteStatus> writeStatuesMap = new HashMap<>();
    writeStatues.forEach(ws -> writeStatuesMap.put(ws.getFileId(), ws));

    // second writes the same records, all records should be mapped to the same bucket files
    getHoodieWriteClient(config).startCommitWithTime("1");
    List<WriteStatus> writeStatues2 = getHoodieWriteClient(config).bulkInsert(HoodieJavaRDD.getJavaRDD(javaRDD), "1").collect();
    writeStatues2.forEach(ws -> assertEquals(ws.getTotalRecords(), writeStatuesMap.get(ws.getFileId()).getTotalRecords()));
  }

  private static final List<Object> TABLE_TYPES = Arrays.asList(
      "COPY_ON_WRITE",
      "MERGE_ON_READ"
  );

  private static Iterable<Object> tableTypes() {
    return TABLE_TYPES;
  }

  // table type, partitionSort
  private static Iterable<Object[]> configParams() {
    List<Object[]> opts = new ArrayList<>();
    for (Object tableType : TABLE_TYPES) {
      opts.add(new Object[] {tableType, "true"});
      opts.add(new Object[] {tableType, "false"});
    }
    return opts;
  }

}
