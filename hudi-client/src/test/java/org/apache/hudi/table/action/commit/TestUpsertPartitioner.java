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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUpsertPartitioner extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestUpsertPartitioner.class);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestUpsertPartitioner");
    initPath();
    initMetaClient();
    initTestDataGenerator();
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupMetaClient();
    cleanupFileSystem();
    cleanupTestDataGenerator();
  }

  private UpsertPartitioner getUpsertPartitioner(int smallFileSize, int numInserts, int numUpdates, int fileSize,
      String testPartitionPath, boolean autoSplitInserts) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize)
            .insertSplitSize(100).autoTuneInsertSplits(autoSplitInserts).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();

    HoodieClientTestUtils.fakeCommitFile(basePath, "001");
    HoodieClientTestUtils.fakeBaseFile(basePath, testPartitionPath, "001", "file1", fileSize);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, jsc);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("001", numInserts);
    List<HoodieRecord> updateRecords = dataGenerator.generateUpdates("001", numUpdates);
    for (HoodieRecord updateRec : updateRecords) {
      updateRec.unseal();
      updateRec.setCurrentLocation(new HoodieRecordLocation("001", "file1"));
      updateRec.seal();
    }
    List<HoodieRecord> records = new ArrayList<>();
    records.addAll(insertRecords);
    records.addAll(updateRecords);
    WorkloadProfile profile = new WorkloadProfile(jsc.parallelize(records));
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, jsc, table, config);
    assertEquals(0, partitioner.getPartition(
        new Tuple2<>(updateRecords.get(0).getKey(), Option.ofNullable(updateRecords.get(0).getCurrentLocation()))),
        "Update record should have gone to the 1 update partition");
    return partitioner;
  }

  @Test
  public void testUpsertPartitioner() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates... Check all updates go together & inserts subsplit
    UpsertPartitioner partitioner = getUpsertPartitioner(0, 200, 100, 1024, testPartitionPath, false);
    List<InsertBucket> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);
    assertEquals(2, insertBuckets.size(), "Total of 2 insert buckets");
  }

  @Test
  public void testUpsertPartitionerWithSmallInsertHandling() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates .. Check updates go together & inserts subsplit, after expanding
    // smallest file
    UpsertPartitioner partitioner = getUpsertPartitioner(1000 * 1024, 400, 100, 800 * 1024, testPartitionPath, false);
    List<InsertBucket> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals(3, partitioner.numPartitions(), "Should have 3 partitions");
    assertEquals(BucketType.UPDATE, partitioner.getBucketInfo(0).bucketType,
        "Bucket 0 is UPDATE");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(1).bucketType,
        "Bucket 1 is INSERT");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(2).bucketType,
        "Bucket 2 is INSERT");
    assertEquals(3, insertBuckets.size(), "Total of 3 insert buckets");
    assertEquals(0, insertBuckets.get(0).bucketNumber, "First insert bucket must be same as update bucket");
    assertEquals(0.5, insertBuckets.get(0).weight, 0.01, "First insert bucket should have weight 0.5");

    // Now with insert split size auto tuned
    partitioner = getUpsertPartitioner(1000 * 1024, 2400, 100, 800 * 1024, testPartitionPath, true);
    insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals(4, partitioner.numPartitions(), "Should have 4 partitions");
    assertEquals(BucketType.UPDATE, partitioner.getBucketInfo(0).bucketType,
        "Bucket 0 is UPDATE");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(1).bucketType,
        "Bucket 1 is INSERT");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(2).bucketType,
        "Bucket 2 is INSERT");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(3).bucketType,
        "Bucket 3 is INSERT");
    assertEquals(4, insertBuckets.size(), "Total of 4 insert buckets");
    assertEquals(0, insertBuckets.get(0).bucketNumber, "First insert bucket must be same as update bucket");
    assertEquals(200.0 / 2400, insertBuckets.get(0).weight, 0.01, "First insert bucket should have weight 0.5");
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    // Prepare the AvroParquetIO
    String schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr);
  }
}
