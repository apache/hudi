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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.config.SourceConfigs;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.apache.hudi.common.testutils.FixtureUtils.prepareFixtureTable;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.BULK_INSERT_SORT_MODE;
import static org.apache.hudi.config.HoodieWriteConfig.FINALIZE_WRITE_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.UPSERT_PARALLELISM_VALUE;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.functional.HoodieDeltaStreamerTestBase.PROPS_FILENAME_TEST_MULTI_WRITER;
import static org.apache.hudi.utilities.functional.HoodieDeltaStreamerTestBase.addCommitToTimeline;
import static org.apache.hudi.utilities.functional.HoodieDeltaStreamerTestBase.defaultSchemaProviderClassName;
import static org.apache.hudi.utilities.functional.HoodieDeltaStreamerTestBase.prepareInitialConfigs;
import static org.apache.hudi.utilities.functional.TestHoodieDeltaStreamer.deltaStreamerTestRunner;
import static org.apache.hudi.utilities.testutils.sources.AbstractBaseTestSource.DEFAULT_PARTITION_NUM;
import static org.apache.hudi.utilities.testutils.sources.AbstractBaseTestSource.dataGeneratorMap;
import static org.apache.hudi.utilities.testutils.sources.AbstractBaseTestSource.initDataGen;

@Tag("functional")
public class TestHoodieDeltaStreamerWithMultiWriter extends SparkClientFunctionalTestHarness {

  private static final String COW_TEST_TABLE_NAME = "testtable_COPY_ON_WRITE";

  String basePath;
  String propsFilePath;
  String tableBasePath;
  int totalRecords;

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testUpsertsContinuousModeWithMultipleWriters(HoodieTableType tableType) throws Exception {
    // NOTE : Overriding the LockProvider to FileSystemBasedLockProviderTestClass since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    setUpTestTable(tableType);
    prepareInitialConfigs(fs(), basePath, "foo");
    // enable carrying forward latest checkpoint
    TypedProperties props = prepareMultiWriterProps(fs(), basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "3");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "5000");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs(), propsFilePath);

    HoodieDeltaStreamer.Config cfgIngestionJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));

    // create a backfill job
    HoodieDeltaStreamer.Config cfgBackfillJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgBackfillJob.continuousMode = false;
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(hadoopConf()).setBasePath(tableBasePath).build();
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);
    cfgBackfillJob.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc());

    // re-init ingestion job to start sync service
    HoodieDeltaStreamer ingestionJob2 = new HoodieDeltaStreamer(cfgIngestionJob, jsc());

    // run ingestion & backfill in parallel, create conflict and fail one
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob2,
        cfgIngestionJob, backfillJob, cfgBackfillJob, true);

    // create new ingestion & backfill job config to generate only INSERTS to avoid conflict
    props = prepareMultiWriterProps(fs(), basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    props.setProperty("hoodie.test.source.generate.inserts", "true");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs(), basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);
    cfgBackfillJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.INSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TestIdentityTransformer.class.getName()));
    cfgBackfillJob.continuousMode = false;
    meta = HoodieTableMetaClient.builder().setConf(hadoopConf()).setBasePath(tableBasePath).build();
    timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    commitMetadata = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);
    cfgBackfillJob.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));

    cfgIngestionJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TestIdentityTransformer.class.getName()));
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));
    // re-init ingestion job
    HoodieDeltaStreamer ingestionJob3 = new HoodieDeltaStreamer(cfgIngestionJob, jsc());
    // re-init backfill job
    HoodieDeltaStreamer backfillJob2 = new HoodieDeltaStreamer(cfgBackfillJob, jsc());

    // run ingestion & backfill in parallel, avoid conflict and succeed both
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob3,
        cfgIngestionJob, backfillJob2, cfgBackfillJob, false);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE"})
  public void testLatestCheckpointCarryOverWithMultipleWriters(HoodieTableType tableType) throws Exception {
    testCheckpointCarryOver(tableType);
  }

  private void testCheckpointCarryOver(HoodieTableType tableType) throws Exception {
    // NOTE : Overriding the LockProvider to FileSystemBasedLockProviderTestClass since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    setUpTestTable(tableType);
    prepareInitialConfigs(fs(), basePath, "foo");
    TypedProperties props = prepareMultiWriterProps(fs(), basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs(), propsFilePath);

    HoodieDeltaStreamer.Config cfgIngestionJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));

    // create a backfill job with checkpoint from the first instant
    HoodieDeltaStreamer.Config cfgBackfillJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgBackfillJob.continuousMode = false;
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(hadoopConf()).setBasePath(tableBasePath).build();

    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadataForFirstInstant = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);

    // run the backfill job
    props = prepareMultiWriterProps(fs(), basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs(), propsFilePath);

    // get current checkpoint after preparing base dataset with some commits
    HoodieCommitMetadata commitMetadataForLastInstant = getLatestMetadata(meta);

    // Set checkpoint to the last successful position
    cfgBackfillJob.checkpoint = commitMetadataForLastInstant.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc());
    backfillJob.sync();

    meta.reloadActiveTimeline();
    int totalCommits = meta.getCommitsTimeline().filterCompletedInstants().countInstants();

    // add a new commit to timeline which may not have the checkpoint in extra metadata
    addCommitToTimeline(meta);
    meta.reloadActiveTimeline();
    verifyCommitMetadataCheckpoint(meta, null);

    cfgBackfillJob.checkpoint = null;
    new HoodieDeltaStreamer(cfgBackfillJob, jsc()).sync(); // if deltastreamer checkpoint fetch does not walk back to older commits, this sync will fail
    meta.reloadActiveTimeline();
    Assertions.assertEquals(totalCommits + 2, meta.getCommitsTimeline().filterCompletedInstants().countInstants());
    verifyCommitMetadataCheckpoint(meta, "00008");
  }

  private void verifyCommitMetadataCheckpoint(HoodieTableMetaClient metaClient, String expectedCheckpoint) throws IOException {
    HoodieCommitMetadata commitMeta = getLatestMetadata(metaClient);
    if (expectedCheckpoint == null) {
      Assertions.assertNull(commitMeta.getMetadata(CHECKPOINT_KEY));
    } else {
      Assertions.assertEquals(expectedCheckpoint, commitMeta.getMetadata(CHECKPOINT_KEY));
    }
  }

  private static HoodieCommitMetadata getLatestMetadata(HoodieTableMetaClient meta) throws IOException {
    HoodieTimeline timeline = meta.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    return HoodieCommitMetadata
            .fromBytes(timeline.getInstantDetails(timeline.lastInstant().get()).get(), HoodieCommitMetadata.class);
  }

  private static TypedProperties prepareMultiWriterProps(FileSystem fs, String basePath, String propsFilePath) throws IOException {
    TypedProperties props = new TypedProperties();
    HoodieDeltaStreamerTestBase.populateCommonProps(props, basePath);
    HoodieDeltaStreamerTestBase.populateCommonHiveProps(props);

    props.setProperty("include", "sql-transformer.properties");
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", basePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", basePath + "/target.avsc");

    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty("hoodie.cleaner.policy.failed.writes", "LAZY");
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider");
    props.setProperty("hoodie.write.lock.hivemetastore.database", "testdb1");
    props.setProperty("hoodie.write.lock.hivemetastore.table", "table1");
    props.setProperty("hoodie.write.lock.zookeeper.url", "127.0.0.1");
    props.setProperty("hoodie.write.lock.zookeeper.port", "2828");
    props.setProperty("hoodie.write.lock.wait_time_ms", "1200000");
    props.setProperty("hoodie.write.lock.num_retries", "10");
    props.setProperty("hoodie.write.lock.zookeeper.lock_key", "test_table");
    props.setProperty("hoodie.write.lock.zookeeper.base_path", "/test");
    props.setProperty(INSERT_PARALLELISM_VALUE.key(), "4");
    props.setProperty(UPSERT_PARALLELISM_VALUE.key(), "4");
    props.setProperty(BULKINSERT_PARALLELISM_VALUE.key(), "4");
    props.setProperty(FINALIZE_WRITE_PARALLELISM_VALUE.key(), "4");
    props.setProperty(BULK_INSERT_SORT_MODE.key(), BulkInsertSortMode.NONE.name());

    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs, propsFilePath);
    return props;
  }

  private static HoodieDeltaStreamer.Config getDeltaStreamerConfig(String basePath,
      String tableType, WriteOperationType op, String propsFilePath, List<String> transformerClassNames) {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    cfg.targetBasePath = basePath;
    cfg.targetTableName = "hoodie_trips";
    cfg.tableType = tableType;
    cfg.sourceClassName = TestDataSource.class.getName();
    cfg.transformerClassNames = transformerClassNames;
    cfg.operation = op;
    cfg.enableHiveSync = false;
    cfg.sourceOrderingField = "timestamp";
    cfg.propsFilePath = propsFilePath;
    cfg.sourceLimit = 1000;
    cfg.schemaProviderClassName = defaultSchemaProviderClassName;
    return cfg;
  }

  /**
   * Specifically used for {@link TestHoodieDeltaStreamerWithMultiWriter}.
   *
   * The fixture test tables have random records generated by
   * {@link org.apache.hudi.common.testutils.HoodieTestDataGenerator} using
   * {@link org.apache.hudi.common.testutils.HoodieTestDataGenerator#TRIP_EXAMPLE_SCHEMA}.
   *
   * The COW fixture test table has 3000 unique records in 7 commits.
   * The MOR fixture test table has 3000 unique records in 9 deltacommits and 1 compaction commit.
   */
  private void setUpTestTable(HoodieTableType tableType) throws IOException {
    basePath = Paths.get(URI.create(basePath().replaceAll("/$", ""))).toString();
    propsFilePath = basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER;
    String fixtureName = String.format("fixtures/testUpsertsContinuousModeWithMultipleWriters.%s.zip", tableType.name());
    tableBasePath = prepareFixtureTable(Objects.requireNonNull(getClass()
        .getClassLoader().getResource(fixtureName)), Paths.get(basePath)).toString();
    initDataGen(sqlContext(), tableBasePath + "/*/*.parquet", DEFAULT_PARTITION_NUM);
    totalRecords = dataGeneratorMap.get(DEFAULT_PARTITION_NUM).getNumExistingKeys(TRIP_EXAMPLE_SCHEMA);
  }

  private void runJobsInParallel(String tableBasePath, HoodieTableType tableType, int totalRecords,
      HoodieDeltaStreamer ingestionJob, HoodieDeltaStreamer.Config cfgIngestionJob, HoodieDeltaStreamer backfillJob,
      HoodieDeltaStreamer.Config cfgBackfillJob, boolean expectConflict) throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(hadoopConf()).setBasePath(tableBasePath).build();
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String lastSuccessfulCommit = timeline.lastInstant().get().getTimestamp();
    // Condition for parallel ingestion job
    Function<Boolean, Boolean> conditionForRegularIngestion = (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHoodieDeltaStreamer.TestHelpers.assertAtleastNDeltaCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath, fs());
      } else {
        TestHoodieDeltaStreamer.TestHelpers.assertAtleastNCompactionCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath, fs());
      }
      TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext());
      TestHoodieDeltaStreamer.TestHelpers.assertDistanceCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext());
      return true;
    };

    try {
      Future regularIngestionJobFuture = service.submit(() -> {
        try {
          deltaStreamerTestRunner(ingestionJob, cfgIngestionJob, conditionForRegularIngestion);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
      Future backfillJobFuture = service.submit(() -> {
        try {
          backfillJob.sync();
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
      backfillJobFuture.get();
      regularIngestionJobFuture.get();
      if (expectConflict) {
        Assertions.fail("Failed to handle concurrent writes");
      }
    } catch (Exception e) {
      /*
       * Need to perform getMessage().contains since the exception coming
       * from {@link org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.DeltaSyncService} gets wrapped many times into RuntimeExceptions.
       */
      if (expectConflict && e.getCause().getMessage().contains(ConcurrentModificationException.class.getName())) {
        // expected ConcurrentModificationException since ingestion & backfill will have overlapping writes
      } else {
        throw e;
      }
    }
  }

}
