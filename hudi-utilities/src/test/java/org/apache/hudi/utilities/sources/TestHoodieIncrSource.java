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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIncrSource extends SparkClientFunctionalTestHarness {

  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;
  private HoodieTableType tableType = COPY_ON_WRITE;

  @BeforeEach
  public void setUp() throws IOException {
    dataGen = new HoodieTestDataGenerator();
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath, Properties props) throws IOException {
    props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(tableType)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, props);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSource(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(true).withMaxNumDeltaCommitsBeforeCompaction(3).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(false).build())
        .build();

    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);
    Pair<String, List<HoodieRecord>> inserts = writeRecords(writeClient, INSERT, null, "100");
    Pair<String, List<HoodieRecord>> inserts2 = writeRecords(writeClient, INSERT, null, "200");
    Pair<String, List<HoodieRecord>> inserts3 = writeRecords(writeClient, INSERT, null, "300");
    Pair<String, List<HoodieRecord>> inserts4 = writeRecords(writeClient, INSERT, null, "400");
    Pair<String, List<HoodieRecord>> inserts5 = writeRecords(writeClient, INSERT, null, "500");

    // read everything upto latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.empty(), 500, inserts5.getKey());

    // even if the begin timestamp is archived (100), full table scan should kick in, but should filter for records having commit time > 100
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.of("100"), 400, inserts5.getKey());

    // even if the read upto latest is set, if begin timestamp is in active timeline, only incremental should kick in.
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.of("400"), 100, inserts5.getKey());

    // read just the latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.empty(), 100, inserts5.getKey());

    // ensure checkpoint does not move
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(inserts5.getKey()), 0, inserts5.getKey());

    Pair<String, List<HoodieRecord>> inserts6 = writeRecords(writeClient, INSERT, null, "600");

    // insert new batch and ensure the checkpoint moves
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(inserts5.getKey()), 100, inserts6.getKey());
    writeClient.close();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSourceInflightCommitBeforeCompletedCommit(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(2).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withInlineCompaction(true)
                .withMaxNumDeltaCommitsBeforeCompaction(3)
                .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);
    List<Pair<String, List<HoodieRecord>>> inserts = new ArrayList<>();

    for (int i = 0; i < 6; i++) {
      inserts.add(writeRecords(writeClient, INSERT, null, HoodieActiveTimeline.createNewInstantTime()));
    }

    // Emulates a scenario where an inflight commit is before a completed commit
    // The checkpoint should not go past this commit
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant4 = activeTimeline
        .filter(instant -> instant.getTimestamp().equals(inserts.get(4).getKey())).firstInstant().get();
    Option<byte[]> instant4CommitData = activeTimeline.getInstantDetails(instant4);
    activeTimeline.revertToInflight(instant4);
    metaClient.reloadActiveTimeline();

    // Reads everything up to latest
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
        Option.empty(),
        400,
        inserts.get(3).getKey());

    // Even if the beginning timestamp is archived, full table scan should kick in, but should filter for records having commit time > first instant time
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
        Option.of(inserts.get(0).getKey()),
        300,
        inserts.get(3).getKey());

    // Even if the read upto latest is set, if begin timestamp is in active timeline, only incremental should kick in.
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
        Option.of(inserts.get(2).getKey()),
        100,
        inserts.get(3).getKey());

    // Reads just the latest
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
        Option.empty(),
        100,
        inserts.get(3).getKey());

    // Ensures checkpoint does not move
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
        Option.of(inserts.get(3).getKey()),
        0,
        inserts.get(3).getKey());

    activeTimeline.reload().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, instant4.getAction(), inserts.get(4).getKey()),
        instant4CommitData);

    // After the inflight commit completes, the checkpoint should move on after incremental pull
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
        Option.of(inserts.get(3).getKey()),
        200,
        inserts.get(5).getKey());

    writeClient.close();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSourceWithPendingTableServices(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(10, 12).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(9).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withScheduleInlineCompaction(true)
                .withMaxNumDeltaCommitsBeforeCompaction(1)
                .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);
    List<Pair<String, List<HoodieRecord>>> dataBatches = new ArrayList<>();

    // For COW:
    //   0: bulk_insert of 100 records
    //   1: bulk_insert of 100 records
    //   2: bulk_insert of 100 records
    //      schedule clustering
    //   3: bulk_insert of 100 records
    //   4: upsert of 100 records (updates only based on round 3)
    //   5: upsert of 100 records (updates only based on round 3)
    //   6: bulk_insert of 100 records
    // For MOR:
    //   0: bulk_insert of 100 records
    //   1: bulk_insert of 100 records
    //   2: bulk_insert of 100 records
    //   3: bulk_insert of 100 records
    //   4: upsert of 100 records (updates only based on round 3)
    //      schedule compaction
    //   5: upsert of 100 records (updates only based on round 3)
    //      schedule clustering
    //   6: bulk_insert of 100 records
    for (int i = 0; i < 6; i++) {
      WriteOperationType opType = i < 4 ? BULK_INSERT : UPSERT;
      List<HoodieRecord> recordsForUpdate = i < 4 ? null : dataBatches.get(3).getRight();
      dataBatches.add(writeRecords(writeClient, opType, recordsForUpdate, HoodieActiveTimeline.createNewInstantTime()));
      if (tableType == COPY_ON_WRITE) {
        if (i == 2) {
          writeClient.scheduleClustering(Option.empty());
        }
      } else if (tableType == MERGE_ON_READ) {
        if (i == 4) {
          writeClient.scheduleCompaction(Option.empty());
        }
        if (i == 5) {
          writeClient.scheduleClustering(Option.empty());
        }
      }
    }
    dataBatches.add(writeRecords(writeClient, BULK_INSERT, null, HoodieActiveTimeline.createNewInstantTime()));

    String latestCommitTimestamp = dataBatches.get(dataBatches.size() - 1).getKey();
    // Pending clustering exists
    Option<HoodieInstant> clusteringInstant =
        metaClient.getActiveTimeline().filterPendingReplaceTimeline()
            .filter(instant -> ClusteringUtils.getClusteringPlan(metaClient, instant).isPresent())
            .firstInstant();
    assertTrue(clusteringInstant.isPresent());
    assertTrue(clusteringInstant.get().getTimestamp().compareTo(latestCommitTimestamp) < 0);

    if (tableType == MERGE_ON_READ) {
      // Pending compaction exists
      Option<HoodieInstant> compactionInstant =
          metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
      assertTrue(compactionInstant.isPresent());
      assertTrue(compactionInstant.get().getTimestamp().compareTo(latestCommitTimestamp) < 0);
    }

    // The pending tables services should not block the incremental pulls
    // Reads everything up to latest
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
        Option.empty(),
        500,
        dataBatches.get(6).getKey());

    // Even if the read upto latest is set, if begin timestamp is in active timeline, only incremental should kick in.
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
        Option.of(dataBatches.get(2).getKey()),
        200,
        dataBatches.get(6).getKey());

    // Reads just the latest
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
        Option.empty(),
        100,
        dataBatches.get(6).getKey());

    // Ensures checkpoint does not move
    readAndAssert(
        IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
        Option.of(dataBatches.get(6).getKey()),
        0,
        dataBatches.get(6).getKey());

    writeClient.close();
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy, Option<String> checkpointToPull, int expectedCount, String expectedCheckpoint) {

    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy", missingCheckpointStrategy.name());
    TypedProperties typedProperties = new TypedProperties(properties);
    HoodieIncrSource incrSource = new HoodieIncrSource(typedProperties, jsc(), spark(), new DummySchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA));

    // read everything until latest
    Pair<Option<Dataset<Row>>, String> batchCheckPoint = incrSource.fetchNextBatch(checkpointToPull, 500);
    Assertions.assertNotNull(batchCheckPoint.getValue());
    if (expectedCount == 0) {
      assertFalse(batchCheckPoint.getKey().isPresent());
    } else {
      assertEquals(expectedCount, batchCheckPoint.getKey().get().count());
    }
    Assertions.assertEquals(expectedCheckpoint, batchCheckPoint.getRight());
  }

  private Pair<String, List<HoodieRecord>> writeRecords(SparkRDDWriteClient writeClient,
                                                        WriteOperationType writeOperationType,
                                                        List<HoodieRecord> insertRecords,
                                                        String commit) throws IOException {
    writeClient.startCommitWithTime(commit);
    // Only supports INSERT, UPSERT, and BULK_INSERT
    List<HoodieRecord> records = writeOperationType == WriteOperationType.UPSERT
        ? dataGen.generateUpdates(commit, insertRecords) : dataGen.generateInserts(commit, 100);
    JavaRDD<WriteStatus> result = writeOperationType == WriteOperationType.BULK_INSERT
        ? writeClient.bulkInsert(jsc().parallelize(records, 1), commit)
        : writeClient.upsert(jsc().parallelize(records, 1), commit);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    return Pair.of(commit, records);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  private static class DummySchemaProvider extends SchemaProvider {

    private final Schema schema;

    public DummySchemaProvider(Schema schema) {
      super(new TypedProperties());
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }
}
