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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.TestSnapshotQuerySplitterImpl;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceProfile;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieIncrSource extends SparkClientFunctionalTestHarness {

  @Override
  public SparkConf conf() {
    return conf(SparkClientFunctionalTestHarness.getSparkSqlConf());
  }

  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;
  private HoodieTableType tableType = COPY_ON_WRITE;
  private final Option<SourceProfileSupplier> sourceProfile = Option.of(mock(SourceProfileSupplier.class));
  private final HoodieIngestionMetrics metrics = mock(HoodieIngestionMetrics.class);

  @BeforeEach
  public void setUp() throws IOException {
    dataGen = new HoodieTestDataGenerator();
    String basePath = basePath();
    metaClient = getHoodieMetaClient(storageConf(), basePath);
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props) throws IOException {
    return HoodieTableMetaClient.newTableBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(tableType)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .initTable(storageConf.newInstance(), basePath);
  }

  @Test
  public void testCreateSource() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy", READ_UPTO_LATEST_COMMIT.name());
    // Validate constructor with metrics.
    HoodieIncrSource incrSource = new HoodieIncrSource(properties, jsc(), spark(), metrics, new DefaultStreamContext(new DummySchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA), sourceProfile));
    assertEquals(Source.SourceType.ROW, incrSource.getSourceType());
    // Validate constructor without metrics.
    incrSource = new HoodieIncrSource(properties, jsc(), spark(), new DefaultStreamContext(new DummySchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA), sourceProfile));
    assertEquals(Source.SourceType.ROW, incrSource.getSourceType());
  }

  @ParameterizedTest
  @MethodSource("getArgumentsForHoodieIncrSource")
  public void testHoodieIncrSource(HoodieTableType tableType, boolean useSourceProfile) throws IOException {
    this.tableType = tableType;
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(true).withMaxNumDeltaCommitsBeforeCompaction(3).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(false).build())
        .build();

    if (useSourceProfile) {
      when(sourceProfile.get().getSourceProfile()).thenReturn(new TestSourceProfile(Long.MAX_VALUE, 4, 500));
    } else {
      when(sourceProfile.get().getSourceProfile()).thenReturn(null);
    }

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      // WriteResult is a Pair<HoodieInstant, Records>
      WriteResult insert1 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 98);
      WriteResult insert2 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 106);
      WriteResult insert3 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 114);
      WriteResult insert4 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 122);
      WriteResult insert5 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 130);

      // read everything upto latest
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.empty(), 570,
          insert5.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.EIGHT,
          Option.empty(), 570, new StreamerCheckpointV2(insert5.getCompletionTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.SIX,
          Option.empty(), 570, new StreamerCheckpointV1(insert5.getInstantTime()));

      // even if the start completion timestamp is archived (100), full table scan should kick in, but should filter for records having commit time > 100
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(insert1.getCompletionTime()), 472, insert5.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.EIGHT,
          Option.of(new StreamerCheckpointV1(insert1.getInstant().requestedTime())),
          472, new StreamerCheckpointV2(insert5.getCompletionTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV2(insert1.getInstant().getCompletionTime())),
          472, new StreamerCheckpointV1(insert5.getInstantTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV1(insert1.getInstant().requestedTime())),
          472, new StreamerCheckpointV1(insert5.getInstantTime()));

      // even if the read upto latest is set, if start completion timestamp is in active timeline, only incremental should kick in.
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.of(insert4.getCompletionTime()),
          130,
          insert5.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.EIGHT,
          Option.of(new StreamerCheckpointV1(insert4.getInstant().requestedTime())),
          130, new StreamerCheckpointV2(insert5.getCompletionTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV2(insert4.getInstant().getCompletionTime())),
          130, new StreamerCheckpointV1(insert5.getInstantTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV1(insert4.getInstant().requestedTime())),
          130, new StreamerCheckpointV1(insert5.getInstantTime()));

      // read just the latest
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.empty(), 130, insert5.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.EIGHT,
          Option.empty(), 130, new StreamerCheckpointV2(insert5.getCompletionTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.SIX,
          Option.empty(), 130, new StreamerCheckpointV1(insert5.getInstantTime()));

      // ensure checkpoint does not move
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(insert5.getCompletionTime()), 0,
          insert5.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.EIGHT,
          Option.of(new StreamerCheckpointV1(insert5.getInstantTime())), 0, new StreamerCheckpointV1(insert5.getInstantTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV2(insert5.getCompletionTime())), 0, new StreamerCheckpointV1(insert5.getInstantTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV1(insert5.getInstantTime())), 0, new StreamerCheckpointV1(insert5.getInstantTime()));

      WriteResult insert6 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime(), 168);

      // insert new batch and ensure the checkpoint moves
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(insert5.getCompletionTime()), 168,
          insert6.getCompletionTime());
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.EIGHT,
          Option.of(new StreamerCheckpointV1(insert5.getInstantTime())), 168, new StreamerCheckpointV2(insert6.getCompletionTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV2(insert5.getCompletionTime())), 168, new StreamerCheckpointV1(insert6.getInstantTime()));
      readAndAssertCheckpointTranslation(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, HoodieTableVersion.SIX,
          Option.of(new StreamerCheckpointV1(insert5.getInstantTime())), 168, new StreamerCheckpointV1(insert6.getInstantTime()));

      if (useSourceProfile) {
        // TODO(yihua): fix this
        //verify(metrics, times(5)).updateStreamerSourceBytesToBeIngestedInSyncRound(Long.MAX_VALUE);
        //verify(metrics, times(5)).updateStreamerSourceParallelism(4);
      }
    }
  }

  private static Stream<Arguments> getArgumentsForHoodieIncrSource() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, true),
        Arguments.of(MERGE_ON_READ, true),
        Arguments.of(COPY_ON_WRITE, false),
        Arguments.of(MERGE_ON_READ, false)
    );
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSourceInflightCommitBeforeCompletedCommit(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(storageConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(2).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withInlineCompaction(true)
                .withMaxNumDeltaCommitsBeforeCompaction(3)
                .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      List<WriteResult> inserts = new ArrayList<>();

      for (int i = 0; i < 6; i++) {
        inserts.add(writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime()));
      }

      // Emulates a scenario where an inflight commit is before a completed commit
      // The checkpoint should not go past this commit
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      HoodieInstant instant4 = activeTimeline
          .filter(instant -> instant.requestedTime().equals(inserts.get(4).getInstantTime())).firstInstant().get();
      Option<byte[]> instant4CommitData = activeTimeline.getInstantDetails(instant4);
      activeTimeline.revertToInflight(instant4);
      metaClient.reloadActiveTimeline();

      // instant 0
      // instant 1
      // instant 2
      // instant 3
      // instant 4_inflight
      // instant 5
      // Reads everything up to latest
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          500,
          inserts.get(5).getCompletionTime());

      // Even if the start completion timestamp is archived, full table scan should kick in, but should filter for records having commit time > first instant
      // time
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(inserts.get(0).getCompletionTime()),
          400,
          inserts.get(5).getCompletionTime());

      // Even if the read upto latest is set, if start completion timestamp is in active timeline, only incremental should kick in.
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(inserts.get(2).getCompletionTime()),
          200,
          inserts.get(5).getCompletionTime());

      // Reads just the latest
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
          Option.empty(),
          100,
          inserts.get(5).getCompletionTime());

      // Ensures checkpoint does not move
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
          Option.of(inserts.get(5).getCompletionTime()),
          0,
          inserts.get(5).getCompletionTime());

      activeTimeline.reload().saveAsComplete(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instant4.getAction(), inserts.get(4).getInstantTime()),
          instant4CommitData);

      // find instant4's new completion time
      String instant4CompletionTime = activeTimeline.reload().getInstantsAsStream()
          .filter(instant -> instant.requestedTime().equals(instant4.requestedTime()))
          .findFirst().get().getCompletionTime();

      // After the inflight commit completes, the checkpoint should move on after incremental pull
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
          Option.of(inserts.get(3).getCompletionTime()),
          200,
          instant4CompletionTime);
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSourceWithPendingTableServices(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(storageConf(), basePath());
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

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      List<WriteResult> dataBatches = new ArrayList<>();

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
        List<HoodieRecord> recordsForUpdate = i < 4 ? null : dataBatches.get(3).getRecords();
        dataBatches.add(writeRecords(writeClient, opType, recordsForUpdate, writeClient.createNewInstantTime()));
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
      dataBatches.add(writeRecords(writeClient, BULK_INSERT, null, writeClient.createNewInstantTime()));

      String latestCommitTimestamp = dataBatches.get(dataBatches.size() - 1).getInstantTime();
      // Pending clustering exists
      Option<HoodieInstant> clusteringInstant =
          metaClient.getActiveTimeline().filterPendingClusteringTimeline()
              .filter(instant -> ClusteringUtils.getClusteringPlan(metaClient, instant).isPresent())
              .firstInstant();
      assertTrue(clusteringInstant.isPresent());
      assertTrue(clusteringInstant.get().requestedTime().compareTo(latestCommitTimestamp) < 0);

      if (tableType == MERGE_ON_READ) {
        // Pending compaction exists
        Option<HoodieInstant> compactionInstant =
            metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
        assertTrue(compactionInstant.isPresent());
        assertTrue(compactionInstant.get().requestedTime().compareTo(latestCommitTimestamp) < 0);
      }

      // test SnapshotLoadQuerySplitter to split snapshot query .
      // Reads only first commit
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          100,
          dataBatches.get(0).getCompletionTime(),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()), new TypedProperties());

      // The pending tables services should not block the incremental pulls
      // Reads everything up to latest
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          500,
          dataBatches.get(6).getCompletionTime());

      // Even if the read upto latest is set, if start completion timestamp is in active timeline, only incremental should kick in.
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(dataBatches.get(2).getCompletionTime()),
          200,
          dataBatches.get(6).getCompletionTime());

      // Reads just the latest
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
          Option.empty(),
          100,
          dataBatches.get(6).getCompletionTime());

      // Ensures checkpoint does not move
      readAndAssertWithLatestTableVersion(
          IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST,
          Option.of(dataBatches.get(6).getCompletionTime()),
          0,
          dataBatches.get(6).getCompletionTime());
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testHoodieIncrSourceWithDataSourceOptions(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    metaClient = getHoodieMetaClient(storageConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(10, 12).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(9).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withScheduleInlineCompaction(true)
                .withMaxNumDeltaCommitsBeforeCompaction(1)
                .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMetadataIndexColumnStats(true)
            .withColumnStatsIndexForColumns("_hoodie_commit_time")
            .build())
        .build();

    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty(HoodieIncrSourceConfig.HOODIE_INCREMENTAL_SPARK_DATASOURCE_OPTIONS.key(), "hoodie.metadata.enable=true,hoodie.enable.data.skipping=true");
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      WriteResult inserts = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime());
      WriteResult inserts2 = writeRecords(writeClient, INSERT, null, writeClient.createNewInstantTime());
      readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          100,
          inserts.getCompletionTime(),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()), extraProps);
    }
  }

  @Test
  public void testPartitionPruningInHoodieIncrSource()
      throws IOException {
    this.tableType = MERGE_ON_READ;
    metaClient = getHoodieMetaClient(storageConf(), basePath());
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(10, 12).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(9).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withScheduleInlineCompaction(true)
                .withMaxNumDeltaCommitsBeforeCompaction(1)
                .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(false).build())
        // if col stats is enabled, col stats based pruning kicks in and changes expected value in this test.
        .build();
    List<WriteResult> inserts = new ArrayList<>();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      for (int i = 0; i < 3; i++) {
        inserts.add(writeRecordsForPartition(writeClient, BULK_INSERT, writeClient.createNewInstantTime(), DEFAULT_PARTITION_PATHS[i]));
      }

      /*
          chkpt, maxRows, expectedChkpt, expected partitions
          Arguments.of(null, 1, "100", 100, 1),
          Arguments.of(null, 101, "200", 200, 3),
          Arguments.of(null, 10001, "300", 300, 3),
          Arguments.of("100", 101, "300", 200, 2),
          Arguments.of("200", 101, "300", 100, 1),
          Arguments.of("300", 101, "300", 0, 0)
      */
      TypedProperties extraProps = new TypedProperties();
      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(1));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          100,
          new StreamerCheckpointV2(inserts.get(0).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(1));

      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(101));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          200,
          new StreamerCheckpointV2(inserts.get(1).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(3));

      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(10001));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.empty(),
          300,
          new StreamerCheckpointV2(inserts.get(2).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(3));

      // even if TestSnapshotQuerySplitterImpl is configured, it shouldn't be used if it's not a snapshot query
      // Conditions to determine if HoodieIncrSource should run a snapshot query
      //   1. checkpoint exists or checkpoint is missing but the MissingCheckpointStrategy is set to READ_LATEST
      //   2. start completion time/checkpoint is archived
      // The tests below do not meet either of one of the condition, so they should run normal incremental queries
      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(101));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(new StreamerCheckpointV2(inserts.get(0).getCompletionTime())),
          200,
          new StreamerCheckpointV2(inserts.get(2).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(2));

      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(101));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(new StreamerCheckpointV2(inserts.get(1).getCompletionTime())),
          100,
          new StreamerCheckpointV2(inserts.get(2).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(1));

      extraProps.setProperty(TestSnapshotQuerySplitterImpl.MAX_ROWS_PER_BATCH, String.valueOf(101));
      readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT,
          Option.of(new StreamerCheckpointV2(inserts.get(2).getCompletionTime())),
          0,
          new StreamerCheckpointV2(inserts.get(2).getCompletionTime()),
          Option.of(TestSnapshotQuerySplitterImpl.class.getName()),
          extraProps,
          Option.ofNullable(0));
    }
  }

  private void readAndAssertCheckpointTranslation(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                                                  HoodieTableVersion targetTableVersion, Option<Checkpoint> checkpointToPull,
                                                  int expectedCount, Checkpoint expectedCheckpoint) {
    TypedProperties properties = new TypedProperties();
    properties.put(
        HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(targetTableVersion.versionCode()));
    readAndAssert(missingCheckpointStrategy, checkpointToPull, expectedCount, expectedCheckpoint,
        Option.empty(), properties, Option.empty());
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<Checkpoint> checkpointToPull, int expectedCount,
                             Checkpoint expectedCheckpoint, Option<String> snapshotCheckPointImplClassOpt,
                             TypedProperties extraProps, Option<Integer> expectedRDDPartitions) {

    Properties properties = new Properties();
    if (!ConfigUtils.containsConfigProperty(extraProps, HoodieWriteConfig.WRITE_TABLE_VERSION)) {
      properties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(),
          String.valueOf(HoodieTableVersion.current().versionCode()));
    }
    properties.setProperty("hoodie.streamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy", missingCheckpointStrategy.name());
    properties.putAll(extraProps);
    snapshotCheckPointImplClassOpt.map(className ->
        properties.setProperty(SnapshotLoadQuerySplitter.Config.SNAPSHOT_LOAD_QUERY_SPLITTER_CLASS_NAME, className));
    TypedProperties typedProperties = new TypedProperties(properties);
    HoodieIncrSource incrSource = new HoodieIncrSource(typedProperties, jsc(), spark(), metrics, new DefaultStreamContext(new DummySchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA), sourceProfile));

    // read everything until latest
    Pair<Option<Dataset<Row>>, Checkpoint> batchCheckPoint = incrSource.fetchNextBatch(checkpointToPull, 500);
    assertNotNull(batchCheckPoint.getValue());
    if (expectedCount == 0) {
      assertFalse(batchCheckPoint.getKey().isPresent());
    } else {
      assertEquals(expectedCount, batchCheckPoint.getKey().get().count());
      expectedRDDPartitions.ifPresent(rddPartitions -> assertEquals(rddPartitions, batchCheckPoint.getKey().get().rdd().getNumPartitions()));
    }
    assertEquals(expectedCheckpoint, batchCheckPoint.getRight());
  }

  private void readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                                                   Option<String> checkpointToPull, int expectedCount,
                                                   String expectedCheckpoint, Option<String> snapshotCheckPointImplClassOpt,
                                                   TypedProperties extraProps) {
    readAndAssert(missingCheckpointStrategy,
        checkpointToPull.isPresent() ? Option.of(new StreamerCheckpointV2(checkpointToPull.get())) : Option.empty(), expectedCount,
        new StreamerCheckpointV2(expectedCheckpoint), snapshotCheckPointImplClassOpt, extraProps, Option.empty());
  }

  private void readAndAssertWithLatestTableVersion(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                                                   Option<String> checkpointToPull,
                                                   int expectedCount, String expectedCheckpoint) {
    readAndAssertWithLatestTableVersion(missingCheckpointStrategy, checkpointToPull, expectedCount, expectedCheckpoint, Option.empty(),
        new TypedProperties());
  }

  private WriteResult writeRecords(SparkRDDWriteClient writeClient,
                                   WriteOperationType writeOperationType,
                                   List<HoodieRecord> insertRecords,
                                   String commit) throws IOException {
    return writeRecords(writeClient, writeOperationType, insertRecords, commit, 100);
  }

  private WriteResult writeRecords(SparkRDDWriteClient writeClient,
                                   WriteOperationType writeOperationType,
                                   List<HoodieRecord> insertRecords,
                                   String commit,
                                   int numRecords) throws IOException {
    writeClient.startCommitWithTime(commit);
    // Only supports INSERT, UPSERT, and BULK_INSERT
    List<HoodieRecord> records = writeOperationType == WriteOperationType.UPSERT
        ? dataGen.generateUpdates(commit, insertRecords) : dataGen.generateInserts(commit, numRecords);
    JavaRDD<WriteStatus> result = writeOperationType == WriteOperationType.BULK_INSERT
        ? writeClient.bulkInsert(jsc().parallelize(records, 1), commit)
        : writeClient.upsert(jsc().parallelize(records, 1), commit);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    metaClient.reloadActiveTimeline();
    return new WriteResult(
        metaClient
          .getCommitsAndCompactionTimeline()
          .filterCompletedInstants()
          .lastInstant().get(),
        records);
  }

  private WriteResult writeRecordsForPartition(SparkRDDWriteClient writeClient,
                                               WriteOperationType writeOperationType,
                                               String commit,
                                               String partitionPath) {
    writeClient.startCommitWithTime(commit);
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(commit, 100, partitionPath);
    JavaRDD<WriteStatus> result = writeOperationType == WriteOperationType.BULK_INSERT
        ? writeClient.bulkInsert(jsc().parallelize(records, 1), commit)
        : writeClient.upsert(jsc().parallelize(records, 1), commit);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    metaClient.reloadActiveTimeline();
    return new WriteResult(
        metaClient
          .getCommitsAndCompactionTimeline()
          .filterCompletedInstants()
          .lastInstant().get(),
        records);
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

  static class TestSourceProfile implements SourceProfile<Integer> {

    private final long maxSourceBytes;
    private final int sourcePartitions;
    private final int numInstantsPerFetch;

    public TestSourceProfile(long maxSourceBytes, int sourcePartitions, int numInstantsPerFetch) {
      this.maxSourceBytes = maxSourceBytes;
      this.sourcePartitions = sourcePartitions;
      this.numInstantsPerFetch = numInstantsPerFetch;
    }

    @Override
    public long getMaxSourceBytes() {
      return maxSourceBytes;
    }

    @Override
    public int getSourcePartitions() {
      return sourcePartitions;
    }

    @Override
    public Integer getSourceSpecificContext() {
      return numInstantsPerFetch;
    }
  }

  static class WriteResult {
    private HoodieInstant instant;
    private List<HoodieRecord> records;

    WriteResult(HoodieInstant instant, List<HoodieRecord> records) {
      this.instant = instant;
      this.records = records;
    }

    public HoodieInstant getInstant() {
      return instant;
    }

    public String getInstantTime() {
      return instant.requestedTime();
    }

    public String getCompletionTime() {
      return instant.getCompletionTime();
    }

    public List<HoodieRecord> getRecords() {
      return records;
    }
  }
}
