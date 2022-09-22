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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;

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

  @ParameterizedTest
  @MethodSource("writePayloadTest")
  public void testWriteDuringCompaction(String payloadClass) throws IOException {
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
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();

    Properties props = getPropertiesForKeyGen(true);
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    // write data and commit
    writeData(HoodieActiveTimeline.createNewInstantTime(), 100, true);
    // write data again, and in the case of bucket index, all records will go into log files (we use a small max_file_size)
    writeData(HoodieActiveTimeline.createNewInstantTime(), 100, true);
    Assertions.assertEquals(200, readTableTotalRecordsNum());
    // schedule compaction
    String compactionTime = (String) client.scheduleCompaction(Option.empty()).get();
    // write data, and do not commit. those records should not visible to reader
    String insertTime = HoodieActiveTimeline.createNewInstantTime();
    List<WriteStatus> writeStatuses = writeData(insertTime, 100, false);
    Assertions.assertEquals(200, readTableTotalRecordsNum());
    // commit the write. The records should be visible now even though the compaction does not complete.
    client.commitStats(insertTime, writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
    Assertions.assertEquals(300, readTableTotalRecordsNum());
    // after the compaction, total records should remain the same
    config.setValue(AUTO_COMMIT_ENABLE, "true");
    client.compact(compactionTime);
    Assertions.assertEquals(300, readTableTotalRecordsNum());
  }

  @ParameterizedTest
  @MethodSource("writeLogTest")
  public void testWriteLogDuringCompaction(boolean enableMetadataTable, boolean enableTimelineServer) throws IOException {
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
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();

    Properties props = getPropertiesForKeyGen(true);
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
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(),
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
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }
}