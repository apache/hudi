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
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieTestDataGenerator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestUtils.generateFakeHoodieWriteStats;
import static org.apache.hudi.table.action.commit.UpsertPartitioner.averageBytesPerRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUpsertPartitioner extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestUpsertPartitioner.class);

  private UpsertPartitioner getUpsertPartitioner(int smallFileSize, int numInserts, int numUpdates, int fileSize,
      String testPartitionPath, boolean autoSplitInserts) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize)
            .insertSplitSize(100).autoTuneInsertSplits(autoSplitInserts).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();

    HoodieClientTestUtils.fakeCommitFile(basePath, "001");
    HoodieClientTestUtils.fakeDataFile(basePath, testPartitionPath, "001", "file1", fileSize);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = (HoodieCopyOnWriteTable) HoodieTable.create(metaClient, config, hadoopConf);

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
    List<HoodieWriteStat> writeStatsList = generateFakeHoodieWriteStats(5);
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
    final String testPartitionPath = "1/09/26";
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
