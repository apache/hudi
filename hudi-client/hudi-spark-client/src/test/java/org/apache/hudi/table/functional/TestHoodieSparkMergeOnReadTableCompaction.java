/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableCompaction extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> writeLogTest() {
    // enable metadata table, enable embedded time line server
    Object[][] data = new Object[][] {
        {true, true},
        {true, false},
        {false, true},
        {false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private static Stream<Arguments> writePayloadTest() {
    // Payload class
    return Stream.of(new Object[] {DefaultHoodieRecordPayload.class.getName(), PartialUpdateAvroPayload.class.getName()}).map(Arguments::of);
  }

  private HoodieTestDataGenerator dataGen;
  private SparkRDDWriteClient client;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setup() {
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void teardown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @ParameterizedTest
  @MethodSource("writePayloadTest")
  public void testWriteDuringCompaction(String payloadClass) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(false)
        .withWritePayLoad(payloadClass)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(1024).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    // write data and commit
    writeData(client.createNewInstantTime(), 100, true);
    // write data again, and in the case of bucket index, all records will go into log files (we use a small max_file_size)
    writeData(client.createNewInstantTime(), 100, true);
    Assertions.assertEquals(200, readTableTotalRecordsNum());
    // schedule compaction
    String compactionTime = (String) client.scheduleCompaction(Option.empty()).get();
    // write data, and do not commit. those records should not visible to reader
    String insertTime = client.createNewInstantTime();
    List<WriteStatus> writeStatuses = writeData(insertTime, 100, false);
    Assertions.assertEquals(200, readTableTotalRecordsNum());
    // commit the write. The records should be visible now even though the compaction does not complete.
    client.commitStats(insertTime, context().parallelize(writeStatuses, 1), writeStatuses.stream().map(WriteStatus::getStat)
        .collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
    Assertions.assertEquals(300, readTableTotalRecordsNum());
    // after the compaction, total records should remain the same
    config.setValue(AUTO_COMMIT_ENABLE, "true");
    client.compact(compactionTime);
    Assertions.assertEquals(300, readTableTotalRecordsNum());
  }

  @Test
  public void testOutOfOrderCompactionSchedules() throws IOException, ExecutionException, InterruptedException {
    HoodieWriteConfig config = getWriteConfigWithMockCompactionStrategy();
    HoodieWriteConfig config2 = getWriteConfig();

    TimeGenerator timeGenerator = TimeGenerators
        .getTimeGenerator(HoodieTimeGeneratorConfig.defaultConfig(basePath()),
            HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()));

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());
    client = getHoodieWriteClient(config);

    // write data and commit
    writeData(client.createNewInstantTime(), 100, true);

    // 2nd batch
    String commit2 = client.createNewInstantTime();
    JavaRDD records = jsc().parallelize(dataGen.generateUniqueUpdates(commit2, 50), 2);
    client.startCommitWithTime(commit2);
    List<WriteStatus> writeStatuses = client.upsert(records, commit2).collect();
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    client.commitStats(commit2, context().parallelize(writeStatuses, 1), writeStats, Option.empty(), metaClient.getCommitActionType());

    // schedule compaction
    String compactionInstant1 = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);
    Thread.sleep(10);
    String compactionInstant2 = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);

    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);
    final ExecutorService executors = Executors.newFixedThreadPool(2);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(config2);
    Future future1 = executors.submit(() -> {
      try {
        client.scheduleCompactionAtInstant(compactionInstant1, Option.empty());
        // since compactionInstant1 is earlier than compactionInstant2, and compaction strategy sleeps for 10s, this is expected to throw.
        fail("Should not have reached here");
      } catch (Exception e) {
        writer1Completed.set(true);
      }
    });

    Future future2 = executors.submit(() -> {
      try {
        Thread.sleep(10);
        assertTrue(client2.scheduleCompactionAtInstant(compactionInstant2, Option.empty()));
        writer2Completed.set(true);
      } catch (Exception e) {
        throw new HoodieException("Should not have reached here");
      }
    });

    future1.get();
    future2.get();

    // both should have been completed successfully. I mean, we already assert for conflict for writer2 at L155.
    assertTrue(writer1Completed.get() && writer2Completed.get());
    client.close();
    client2.close();
    executors.shutdownNow();
  }

  private HoodieWriteConfig getWriteConfigWithMockCompactionStrategy() {
    MockCompactionStrategy compactionStrategy = new MockCompactionStrategy();
    return getWriteConfig(compactionStrategy);
  }

  private HoodieWriteConfig getWriteConfig() {
    return getWriteConfig(new LogFileSizeBasedCompactionStrategy());
  }

  private HoodieWriteConfig getWriteConfig(CompactionStrategy compactionStrategy) {
    return HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(compactionStrategy)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  @ParameterizedTest
  @MethodSource("writeLogTest")
  public void testWriteLogDuringCompaction(boolean enableMetadataTable, boolean enableTimelineServer) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(true)
        .withEmbeddedTimelineServerEnabled(enableTimelineServer)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    final List<HoodieRecord> records = dataGen.generateInserts("001", 100);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);

    // initialize 100 records
    client.upsert(writeRecords, client.startCommit());
    // update 100 records
    client.upsert(writeRecords, client.startCommit());
    // schedule compaction
    client.scheduleCompaction(Option.empty());
    // delete 50 records
    List<HoodieKey> toBeDeleted = records.stream().map(HoodieRecord::getKey).limit(50).collect(Collectors.toList());
    JavaRDD<HoodieKey> deleteRecords = jsc().parallelize(toBeDeleted, 2);
    client.delete(deleteRecords, client.startCommit());
    // insert the same 100 records again
    client.upsert(writeRecords, client.startCommit());
    Assertions.assertEquals(100, readTableTotalRecordsNum());
  }

  private long readTableTotalRecordsNum() {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf(),
        Arrays.stream(dataGen.getPartitionPaths()).map(p -> Paths.get(basePath(), p).toString()).collect(Collectors.toList()), basePath()).size();
  }

  private List<WriteStatus> writeData(String instant, int numRecords, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    JavaRDD records = jsc().parallelize(dataGen.generateInserts(instant, numRecords), 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    client.startCommitWithTime(instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, context().parallelize(writeStatuses, 1), writeStats, Option.empty(), metaClient.getCommitActionType());
      assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }
}
