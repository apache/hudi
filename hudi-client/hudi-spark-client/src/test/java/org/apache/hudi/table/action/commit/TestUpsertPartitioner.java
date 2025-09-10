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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.cluster.ClusteringTestUtils;
import org.apache.hudi.table.action.deltacommit.SparkUpsertDeltaCommitPartitioner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUpsertPartitioner extends HoodieClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpsertPartitioner.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestUpsertPartitioner.class, "/exampleSchema.avsc");

  private UpsertPartitioner getUpsertPartitioner(int smallFileSize, int numInserts, int numUpdates, int fileSize,
      String testPartitionPath, boolean autoSplitInserts) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize)
            .insertSplitSize(100).autoTuneInsertSplits(autoSplitInserts).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1000 * 1024).parquetMaxFileSize(1000 * 1024).orcMaxFileSize(1000 * 1024).build())
        .build();

    FileCreateUtilsLegacy.createCommit(basePath, "001");
    FileCreateUtilsLegacy.createBaseFile(basePath, testPartitionPath, "001", "file1", fileSize);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.createForReads(config, context, metaClient);

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
    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(records)));
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, context, table, config, WriteOperationType.UPSERT);
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
    List<InsertBucketCumulativeWeightPair> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);
    assertEquals(2, insertBuckets.size(), "Total of 2 insert buckets");
  }

  @Test
  public void testUpsertPartitionerWithRecordsPerBucket() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates... Check all updates go together & inserts subsplit
    UpsertPartitioner partitioner = getUpsertPartitioner(0, 250, 100, 1024, testPartitionPath, false);
    List<InsertBucketCumulativeWeightPair> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);
    int insertSplitSize = partitioner.config.getCopyOnWriteInsertSplitSize();
    int remainedInsertSize = 250 - 2 * insertSplitSize;
    // will assigned 3 insertBuckets. 100, 100, 50 each
    assertEquals(3, insertBuckets.size(), "Total of 3 insert buckets");
    assertEquals(0.4, insertBuckets.get(0).getLeft().weight, "insert " + insertSplitSize + " records");
    assertEquals(0.4, insertBuckets.get(1).getLeft().weight, "insert " + insertSplitSize + " records");
    assertEquals(0.2, insertBuckets.get(2).getLeft().weight, "insert " + remainedInsertSize + " records");
  }

  @Test
  public void testPartitionWeight() throws Exception {
    final String testPartitionPath = "2016/09/26";
    int totalInsertNum = 2000;

    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0)
            .insertSplitSize(totalInsertNum / 2).autoTuneInsertSplits(false).build()).build();

    FileCreateUtilsLegacy.createCommit(basePath, "001");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.createForReads(config, context, metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("001", totalInsertNum);

    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(insertRecords)));
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, context, table, config, WriteOperationType.UPSERT);
    List<InsertBucketCumulativeWeightPair> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    float bucket0Weight = 0.2f;
    InsertBucketCumulativeWeightPair pair = insertBuckets.remove(0);
    pair.getKey().weight = bucket0Weight;
    pair.setValue(new Double(bucket0Weight));
    insertBuckets.add(0, pair);

    InsertBucketCumulativeWeightPair pair1 = insertBuckets.remove(1);
    pair1.getKey().weight = 1 - bucket0Weight;
    pair1.setValue(new Double(1));
    insertBuckets.add(1, pair1);

    Map<Integer, Integer> partition2numRecords = new HashMap<Integer, Integer>();
    for (HoodieRecord hoodieRecord: insertRecords) {
      int partition = partitioner.getPartition(new Tuple2<>(
              hoodieRecord.getKey(), Option.ofNullable(hoodieRecord.getCurrentLocation())));
      if (!partition2numRecords.containsKey(partition)) {
        partition2numRecords.put(partition, 0);
      }
      partition2numRecords.put(partition, partition2numRecords.get(partition) + 1);
    }

    assertTrue(partition2numRecords.get(0) < partition2numRecords.get(1),
            "The insert num of bucket1 should more than bucket0");
    assertTrue(partition2numRecords.get(0) + partition2numRecords.get(1) == totalInsertNum,
            "The total insert records should be " + totalInsertNum);
    assertEquals(String.valueOf(bucket0Weight),
            String.format("%.1f", (partition2numRecords.get(0) * 1.0f / totalInsertNum)),
            "The weight of bucket0 should be " + bucket0Weight);
    assertEquals(String.valueOf(1 - bucket0Weight),
            String.format("%.1f", (partition2numRecords.get(1) * 1.0f / totalInsertNum)),
            "The weight of bucket1 should be " + (1 - bucket0Weight));
  }

  private void assertInsertBuckets(Double[] weights,
                                   Double[] cumulativeWeights,
                                   List<InsertBucketCumulativeWeightPair> insertBuckets) {
    for (int i = 0; i < weights.length; i++) {
      assertEquals(i, insertBuckets.get(i).getKey().bucketNumber,
          String.format("BucketNumber of insert bucket %d must be same as %d", i, i));
      assertEquals(weights[i], insertBuckets.get(i).getKey().weight, 0.01,
          String.format("Insert bucket %d should have weight %.1f", i, weights[i]));
      assertEquals(cumulativeWeights[i], insertBuckets.get(i).getValue(), 0.01,
          String.format("Insert bucket %d should have cumulativeWeight %.1f", i, cumulativeWeights[i]));
    }
  }

  @Test
  public void testUpsertPartitionerWithSmallInsertHandling() throws Exception {
    final String testPartitionPath = "2016/09/26";
    // Inserts + Updates .. Check updates go together & inserts subsplit, after expanding
    // smallest file
    UpsertPartitioner partitioner = getUpsertPartitioner(1000 * 1024, 400, 100, 800 * 1024, testPartitionPath, false);
    List<InsertBucketCumulativeWeightPair> insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals(3, partitioner.numPartitions(), "Should have 3 partitions");
    SparkBucketInfoGetter bucketInfoGetter = partitioner.getSparkBucketInfoGetter();
    assertEquals(BucketType.UPDATE, bucketInfoGetter.getBucketInfo(0).bucketType,
        "Bucket 0 is UPDATE");
    assertEquals(BucketType.INSERT, bucketInfoGetter.getBucketInfo(1).bucketType,
        "Bucket 1 is INSERT");
    assertEquals(BucketType.INSERT, bucketInfoGetter.getBucketInfo(2).bucketType,
        "Bucket 2 is INSERT");
    assertEquals(3, insertBuckets.size(), "Total of 3 insert buckets");

    Double[] weights = { 0.5, 0.25, 0.25};
    Double[] cumulativeWeights = { 0.5, 0.75, 1.0};
    assertInsertBuckets(weights, cumulativeWeights, insertBuckets);

    // Now with insert split size auto tuned
    partitioner = getUpsertPartitioner(1000 * 1024, 2400, 100, 800 * 1024, testPartitionPath, true);
    insertBuckets = partitioner.getInsertBuckets(testPartitionPath);

    assertEquals(4, partitioner.numPartitions(), "Should have 4 partitions");
    bucketInfoGetter = partitioner.getSparkBucketInfoGetter();
    assertEquals(BucketType.UPDATE, bucketInfoGetter.getBucketInfo(0).bucketType,
        "Bucket 0 is UPDATE");
    assertEquals(BucketType.INSERT, bucketInfoGetter.getBucketInfo(1).bucketType,
        "Bucket 1 is INSERT");
    assertEquals(BucketType.INSERT, bucketInfoGetter.getBucketInfo(2).bucketType,
        "Bucket 2 is INSERT");
    assertEquals(BucketType.INSERT, bucketInfoGetter.getBucketInfo(3).bucketType,
        "Bucket 3 is INSERT");
    assertEquals(4, insertBuckets.size(), "Total of 4 insert buckets");

    weights = new Double[] { 0.08, 0.42, 0.42, 0.08};
    cumulativeWeights = new Double[] { 0.08, 0.5, 0.92, 1.0};
    assertInsertBuckets(weights, cumulativeWeights, insertBuckets);
  }

  @Test
  public void testUpsertPartitionerWithSmallFileHandlingWithInflightCompactionWithCanIndexLogFiles() throws Exception {
    // Note this is used because it is same partition path used in CompactionTestUtils.createCompactionPlan()
    final String testPartitionPath = DEFAULT_PARTITION_PATHS[0];

    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024).build())
            .withIndexConfig(HoodieIndexConfig.newBuilder()
                    .withIndexType(HoodieIndex.IndexType.GLOBAL_SIMPLE)
                    .build())
            .build();

    // This will generate initial commits and create a compaction plan which includes file groups created as part of this
    HoodieCompactionPlan plan = CompactionTestUtils.createCompactionPlan(metaClient, "001", "002", 1, true, false);
    FileCreateUtilsLegacy.createRequestedCompactionCommit(basePath, "002", plan);
    // Simulate one more commit so that inflight compaction is considered when building file groups in file system view
    FileCreateUtilsLegacy.createBaseFile(basePath, testPartitionPath, "003", "2", 1);
    FileCreateUtilsLegacy.createCommit(basePath, "003");

    // Partitioner will attempt to assign inserts to file groups including base file created by inflight compaction
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("004", 100);
    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(insertRecords)));

    HoodieSparkTable table = HoodieSparkTable.createForReads(config, context, metaClient);
    SparkUpsertDeltaCommitPartitioner partitioner = new SparkUpsertDeltaCommitPartitioner(profile, context, table, config, WriteOperationType.UPSERT);

    assertEquals(1, partitioner.numPartitions(), "Should have 1 partitions");
    SparkBucketInfoGetter bucketInfoGetter = partitioner.getSparkBucketInfoGetter();
    assertEquals(BucketType.UPDATE, bucketInfoGetter.getBucketInfo(0).bucketType,
            "Bucket 0 is UPDATE");
    assertEquals("2", bucketInfoGetter.getBucketInfo(0).fileIdPrefix,
            "Should be assigned to only file id not pending compaction which is 2");
  }

  @Test
  public void testUpsertPartitionerWithSmallFileHandlingAndClusteringPlan() throws Exception {
    final String testPartitionPath = DEFAULT_PARTITION_PATHS[0];

    // create HoodieWriteConfig and set inline and async clustering disable here.
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().build())
            .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(false).withAsyncClustering(false).build())
            .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1000 * 1024).parquetMaxFileSize(1000 * 1024).build())
            .build();

    // create file slice with instantTime 001 and build clustering plan including this created 001 file slice.
    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.createClusteringPlan(metaClient, "001", "1");
    // create requested replace commit
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
            .setClusteringPlan(clusteringPlan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    FileCreateUtilsLegacy.createRequestedClusterCommit(basePath,"002", requestedReplaceMetadata);

    // create file slice 003
    FileCreateUtilsLegacy.createBaseFile(basePath, testPartitionPath, "003", "3", 1);
    FileCreateUtilsLegacy.createCommit(basePath, "003");

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // generate new data to be ingested
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("004", 100);
    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(insertRecords)));

    HoodieSparkTable table = HoodieSparkTable.createForReads(config, context, metaClient);
    // create UpsertPartitioner
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, context, table, config, WriteOperationType.UPSERT);

    // for now we have file slice1 and file slice3 and file slice1 is contained in pending clustering plan
    // So that only file slice3 can be used for ingestion.
    assertEquals(1, partitioner.smallFiles.size(), "Should have 1 small file to be ingested.");
  }

  @Test
  public void testUpsertPartitionerWithSmallFileHandlingWithCanIndexLogFiles() throws Exception {
    // Note this is used because it is same partition path used in CompactionTestUtils.createCompactionPlan()
    final String testPartitionPath = DEFAULT_PARTITION_PATHS[0];

    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024).build())
            .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(1024).build())
            .withIndexConfig(HoodieIndexConfig.newBuilder()
                    .withIndexType(HoodieIndex.IndexType.INMEMORY)
                    .build())
            .build();

    // Create file group with only one log file
    FileCreateUtilsLegacy.createLogFile(basePath, testPartitionPath, "001", "fg1", 1);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "001");
    // Create another file group size set to max parquet file size so should not be considered during small file sizing
    FileCreateUtilsLegacy.createBaseFile(basePath, testPartitionPath, "002", "fg2", 1024);
    FileCreateUtilsLegacy.createCommit(basePath, "002");
    FileCreateUtilsLegacy.createLogFile(basePath, testPartitionPath, "003", "fg2", 1);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "003");

    // Partitioner will attempt to assign inserts to file groups including base file created by inflight compaction
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    // Default estimated record size will be 1024 based on last file group created. Only 1 record can be added to small file
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("004", 1);
    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(insertRecords)));

    HoodieSparkTable table = HoodieSparkTable.createForReads(config, context, metaClient);
    SparkUpsertDeltaCommitPartitioner partitioner = new SparkUpsertDeltaCommitPartitioner(profile, context, table, config, WriteOperationType.UPSERT);

    assertEquals(1, partitioner.numPartitions(), "Should have 1 partitions");
    SparkBucketInfoGetter bucketInfoGetter = partitioner.getSparkBucketInfoGetter();
    assertEquals(BucketType.UPDATE, bucketInfoGetter.getBucketInfo(0).bucketType,
            "Bucket 0 should be UPDATE");
    assertEquals("fg1", bucketInfoGetter.getBucketInfo(0).fileIdPrefix,
            "Insert should be assigned to fg1");
  }

  @Test
  public void testUpsertPartitionerWithSmallFileHandlingPickingMultipleCandidates() throws Exception {
    final String partitionPath = DEFAULT_PARTITION_PATHS[0];

    HoodieWriteConfig config =
        makeHoodieClientConfigBuilder()
            .withMergeSmallFileGroupCandidatesLimit(3)
            .withStorageConfig(
                HoodieStorageConfig.newBuilder()
                    .parquetMaxFileSize(2048)
                    .build()
            )
            .build();

    // Bootstrap base files ("small-file targets")
    FileCreateUtilsLegacy.createBaseFile(basePath, partitionPath, "002", "fg-1", 1024);
    FileCreateUtilsLegacy.createBaseFile(basePath, partitionPath, "002", "fg-2", 1024);
    FileCreateUtilsLegacy.createBaseFile(basePath, partitionPath, "002", "fg-3", 1024);

    FileCreateUtilsLegacy.createCommit(basePath, "002");

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    // Default estimated record size will be 1024 based on last file group created.
    // Only 1 record can be added to small file
    WorkloadProfile profile =
        new WorkloadProfile(buildProfile(jsc.parallelize(dataGenerator.generateInserts("003", 3))));

    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(this.metaClient);

    HoodieSparkTable<?> table = HoodieSparkTable.createForReads(config, context, reloadedMetaClient);

    SparkUpsertDeltaCommitPartitioner<?> partitioner = new SparkUpsertDeltaCommitPartitioner<>(profile, context, table, config, WriteOperationType.UPSERT);

    assertEquals(3, partitioner.numPartitions());
    assertEquals(
        Arrays.asList(
            new BucketInfo(BucketType.UPDATE, "fg-3", partitionPath),
            new BucketInfo(BucketType.UPDATE, "fg-2", partitionPath),
            new BucketInfo(BucketType.UPDATE, "fg-1", partitionPath)
        ),
        partitioner.getBucketInfos());
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString());
  }

  @Test
  void testMapAndListBasedSparkBucketInfoGetter() {
    List<BucketInfo> bucketInfos = Arrays.asList(new BucketInfo(BucketType.UPDATE, "bucket1", "partition1"),
        new BucketInfo(BucketType.UPDATE, "bucket2", "partition2"));
    Map<Integer, BucketInfo> bucketInfoMap = new HashMap<>();
    bucketInfoMap.put(0, bucketInfos.get(0));
    bucketInfoMap.put(1, bucketInfos.get(1));
    MapBasedSparkBucketInfoGetter getter = new MapBasedSparkBucketInfoGetter(bucketInfoMap);
    ListBasedSparkBucketInfoGetter listGetter = new ListBasedSparkBucketInfoGetter(bucketInfos);
    assertEquals(bucketInfos.get(0), getter.getBucketInfo(0));
    assertEquals(bucketInfos.get(0), listGetter.getBucketInfo(0));
    assertEquals(bucketInfos.get(1), getter.getBucketInfo(1));
    assertEquals(bucketInfos.get(1), listGetter.getBucketInfo(1));
  }

  @Test
  void testInsertOverwriteBucketInfoGetter() {
    BucketInfo insertInfo = new BucketInfo(BucketType.INSERT, "bucket1", "partition1");
    BucketInfo updateInfo = new BucketInfo(BucketType.UPDATE, "bucket2", "partition2");
    Map<Integer, BucketInfo> map = new HashMap<>();
    map.put(0, insertInfo);
    map.put(1, updateInfo);

    InsertOverwriteBucketInfoGetter getter = new InsertOverwriteBucketInfoGetter(map);
    BucketInfo result = getter.getBucketInfo(0);
    assertEquals(insertInfo, result);
    result = getter.getBucketInfo(1);
    assertEquals(BucketType.INSERT, result.getBucketType());
    assertEquals(updateInfo.getPartitionPath(), result.getPartitionPath());
    assertNotEquals(updateInfo.getFileIdPrefix(), result.getFileIdPrefix());
  }
}
