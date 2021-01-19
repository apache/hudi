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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestUtils.generateFakeHoodieWriteStat;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.table.action.commit.UpsertPartitioner.averageBytesPerRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUpsertPartitioner extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestUpsertPartitioner.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestUpsertPartitioner.class, "/exampleSchema.avsc");

  private UpsertPartitioner getUpsertPartitioner(int smallFileSize, int numInserts, int numUpdates, int fileSize,
      String testPartitionPath, boolean autoSplitInserts) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize)
            .insertSplitSize(100).autoTuneInsertSplits(autoSplitInserts).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1000 * 1024).parquetMaxFileSize(1000 * 1024).build())
        .build();

    FileCreateUtils.createCommit(basePath, "001");
    FileCreateUtils.createBaseFile(basePath, testPartitionPath, "001", "file1", fileSize);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

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
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, context, table, config);
    assertEquals(0, partitioner.getPartition(
        new Tuple2<>(updateRecords.get(0).getKey(), Option.ofNullable(updateRecords.get(0).getCurrentLocation()))),
        "Update record should have gone to the 1 update partition");
    return partitioner;
  }

  private static List<HoodieInstant> setupHoodieInstants() {
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts1"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts2"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts3"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts4"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts5"));
    Collections.reverse(instants);
    return instants;
  }

  private static List<HoodieWriteStat> generateCommitStatWith(int totalRecordsWritten, int totalBytesWritten) {
    List<HoodieWriteStat> writeStatsList = generateFakeHoodieWriteStat(5);
    // clear all record and byte stats except for last entry.
    for (int i = 0; i < writeStatsList.size() - 1; i++) {
      HoodieWriteStat writeStat = writeStatsList.get(i);
      writeStat.setNumWrites(0);
      writeStat.setTotalWriteBytes(0);
    }
    HoodieWriteStat lastWriteStat = writeStatsList.get(writeStatsList.size() - 1);
    lastWriteStat.setTotalWriteBytes(totalBytesWritten);
    lastWriteStat.setNumWrites(totalRecordsWritten);
    return writeStatsList;
  }

  private static HoodieCommitMetadata generateCommitMetadataWith(int totalRecordsWritten, int totalBytesWritten) {
    List<HoodieWriteStat> fakeHoodieWriteStats = generateCommitStatWith(totalRecordsWritten, totalBytesWritten);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    return commitMetadata;
  }

  /*
   * This needs to be a stack so we test all cases when either/both recordsWritten ,bytesWritten is zero before a non
   * zero averageRecordSize can be computed.
   */
  private static LinkedList<Option<byte[]>> generateCommitMetadataList() throws IOException {
    LinkedList<Option<byte[]>> commits = new LinkedList<>();
    // First commit with non zero records and bytes
    commits.push(Option.of(generateCommitMetadataWith(2000, 10000).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Second commit with non zero records and bytes
    commits.push(Option.of(generateCommitMetadataWith(1500, 7500).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Third commit with a small file
    commits.push(Option.of(generateCommitMetadataWith(100, 500).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Fourth commit with both zero records and zero bytes
    commits.push(Option.of(generateCommitMetadataWith(0, 0).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Fifth commit with zero records
    commits.push(Option.of(generateCommitMetadataWith(0, 1500).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Sixth commit with zero bytes
    commits.push(Option.of(generateCommitMetadataWith(2500, 0).toJsonString().getBytes(StandardCharsets.UTF_8)));
    return commits;
  }

  @Test
  public void testAverageBytesPerRecordForNonEmptyCommitTimeLine() throws Exception {
    HoodieTimeline commitTimeLine = mock(HoodieTimeline.class);
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1000).build())
        .build();
    when(commitTimeLine.empty()).thenReturn(false);
    when(commitTimeLine.getReverseOrderedInstants()).thenReturn(setupHoodieInstants().stream());
    LinkedList<Option<byte[]>> commits = generateCommitMetadataList();
    when(commitTimeLine.getInstantDetails(any(HoodieInstant.class))).thenAnswer(invocationOnMock -> commits.pop());
    long expectAvgSize = (long) Math.ceil((1.0 * 7500) / 1500);
    long actualAvgSize = averageBytesPerRecord(commitTimeLine, config);
    assertEquals(expectAvgSize, actualAvgSize);
  }

  @Test
  public void testAverageBytesPerRecordForEmptyCommitTimeLine() throws Exception {
    HoodieTimeline commitTimeLine = mock(HoodieTimeline.class);
    HoodieWriteConfig config = makeHoodieClientConfigBuilder().build();
    when(commitTimeLine.empty()).thenReturn(true);
    long expectAvgSize = config.getCopyOnWriteRecordSizeEstimate();
    long actualAvgSize = averageBytesPerRecord(commitTimeLine, config);
    assertEquals(expectAvgSize, actualAvgSize);
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

    FileCreateUtils.createCommit(basePath, "001");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    List<HoodieRecord> insertRecords = dataGenerator.generateInserts("001", totalInsertNum);

    WorkloadProfile profile = new WorkloadProfile(buildProfile(jsc.parallelize(insertRecords)));
    UpsertPartitioner partitioner = new UpsertPartitioner(profile, context, table, config);
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
    assertEquals(BucketType.UPDATE, partitioner.getBucketInfo(0).bucketType,
        "Bucket 0 is UPDATE");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(1).bucketType,
        "Bucket 1 is INSERT");
    assertEquals(BucketType.INSERT, partitioner.getBucketInfo(2).bucketType,
        "Bucket 2 is INSERT");
    assertEquals(3, insertBuckets.size(), "Total of 3 insert buckets");

    Double[] weights = { 0.5, 0.25, 0.25};
    Double[] cumulativeWeights = { 0.5, 0.75, 1.0};
    assertInsertBuckets(weights, cumulativeWeights, insertBuckets);

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

    weights = new Double[] { 0.08, 0.42, 0.42, 0.08};
    cumulativeWeights = new Double[] { 0.08, 0.5, 0.92, 1.0};
    assertInsertBuckets(weights, cumulativeWeights, insertBuckets);
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString());
  }
}
