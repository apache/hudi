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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.BULK_INSERT_SORT_MODE;
import static org.apache.hudi.config.HoodieWriteConfig.FINALIZE_WRITE_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.UPSERT_PARALLELISM_VALUE;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.deltaStreamerTestRunner;

public class TestHoodieDeltaStreamerWithMultiWriter extends HoodieDeltaStreamerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieDeltaStreamerWithMultiWriter.class);

  String basePath;
  String propsFilePath;
  String tableBasePath;

  @BeforeEach
  public void setup() throws Exception {
    basePath = UtilitiesTestBase.basePath;
    super.setupTest();
  }

  @AfterEach
  public void teardown() throws Exception {
    TestDataSource.resetDataGen();
    FileIOUtils.deleteDirectory(new File(basePath));
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testUpsertsContinuousModeWithMultipleWritersForConflicts(HoodieTableType tableType) throws Exception {
    // NOTE : Overriding the LockProvider to InProcessLockProvider since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    basePath = Paths.get(URI.create(basePath.replaceAll("/$", ""))).toString();
    propsFilePath = basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER;
    tableBasePath =
        basePath + "/testUpsertsContinuousModeWithMultipleWritersForConflicts_" + tableType;
    prepareInitialConfigs(storage, basePath, "foo");
    TypedProperties props = prepareMultiWriterProps(storage, basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider",
        "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    props.setProperty("hoodie.merge.use.record.positions", "false");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, propsFilePath);
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    HoodieDeltaStreamer.Config prepJobConfig =
        getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
            propsFilePath, Collections.singletonList(
                TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    prepJobConfig.continuousMode = true;
    prepJobConfig.configs.add(
        String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    prepJobConfig.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    // if we don't disable small file handling, log files may never get created and hence for MOR, compaction may not kick in.
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      prepJobConfig.configs.add(String.format("%s=3", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key()));
      prepJobConfig.configs.add(String.format("%s=0", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key()));
    }
    HoodieDeltaStreamer prepJob = new HoodieDeltaStreamer(prepJobConfig, jsc);

    // Prepare base dataset with some commits
    deltaStreamerTestRunner(prepJob, prepJobConfig, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath);
        TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(3, tableBasePath);
      }
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      assertDistanceCount(totalRecords, tableBasePath, sqlContext);
      return true;
    });

    HoodieDeltaStreamer.Config cfgIngestionJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));

    // create a backfill job
    HoodieDeltaStreamer.Config cfgBackfillJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgBackfillJob.continuousMode = false;
    HoodieTableMetaClient meta = createMetaClient(new HadoopStorageConfiguration(hadoopConf), tableBasePath);
    HoodieTimeline timeline = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(timeline.firstInstant().get());
    cfgBackfillJob.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc);

    // re-init ingestion job to start sync service
    HoodieDeltaStreamer ingestionJob2 = new HoodieDeltaStreamer(cfgIngestionJob, jsc);

    // run ingestion & backfill in parallel, create conflict and fail one
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob2,
        cfgIngestionJob, backfillJob, cfgBackfillJob, true, "batch1");
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testUpsertsContinuousModeWithMultipleWritersWithoutConflicts(HoodieTableType tableType) throws Exception {
    // NOTE : Overriding the LockProvider to InProcessLockProvider since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    basePath = Paths.get(URI.create(basePath.replaceAll("/$", ""))).toString();
    propsFilePath = basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER;
    tableBasePath =
        basePath + "/testUpsertsContinuousModeWithMultipleWritersWithoutConflicts_" + tableType;
    prepareInitialConfigs(storage, basePath, "foo");
    TypedProperties props = prepareMultiWriterProps(storage, basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider",
        "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, propsFilePath);
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    HoodieDeltaStreamer.Config prepJobConfig =
        getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
            propsFilePath, Collections.singletonList(
                TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    prepJobConfig.continuousMode = true;
    prepJobConfig.configs.add(
        String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    prepJobConfig.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer prepJob = new HoodieDeltaStreamer(prepJobConfig, jsc);

    // Prepare base dataset with some commits
    deltaStreamerTestRunner(prepJob, prepJobConfig, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath);
        TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(3, tableBasePath);
      }
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      assertDistanceCount(totalRecords, tableBasePath, sqlContext);
      return true;
    });

    // create new ingestion & backfill job config to generate only INSERTS to avoid conflict
    props = prepareMultiWriterProps(storage, basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider",
        "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    props.setProperty("hoodie.test.source.generate.inserts", "true");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage,
        basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);
    HoodieDeltaStreamer.Config cfgBackfillJob2 = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.INSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TestIdentityTransformer.class.getName()));
    cfgBackfillJob2.continuousMode = false;
    HoodieTableMetaClient meta = createMetaClient(new HadoopStorageConfiguration(hadoopConf), tableBasePath);
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(timeline.firstInstant().get());
    cfgBackfillJob2.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob2.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfgBackfillJob2.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));

    HoodieDeltaStreamer.Config cfgIngestionJob2 = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TestIdentityTransformer.class.getName()));
    cfgIngestionJob2.continuousMode = true;
    cfgIngestionJob2.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfgIngestionJob2.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    // re-init ingestion job
    HoodieDeltaStreamer ingestionJob3 = new HoodieDeltaStreamer(cfgIngestionJob2, jsc);
    // re-init backfill job
    HoodieDeltaStreamer backfillJob2 = new HoodieDeltaStreamer(cfgBackfillJob2, jsc);

    // run ingestion & backfill in parallel, avoid conflict and succeed both
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob3,
        cfgIngestionJob2, backfillJob2, cfgBackfillJob2, false, "batch2");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE"})
  void testLatestCheckpointCarryOverWithMultipleWriters(HoodieTableType tableType) throws Exception {
    // NOTE : Overriding the LockProvider to InProcessLockProvider since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    basePath = Paths.get(URI.create(basePath.replaceAll("/$", ""))).toString();
    propsFilePath = basePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER;
    tableBasePath = basePath + "/testLatestCheckpointCarryOverWithMultipleWriters_" + tableType;
    prepareInitialConfigs(storage, basePath, "foo");
    TypedProperties props = prepareMultiWriterProps(storage, basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider",
        "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, propsFilePath);
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    HoodieDeltaStreamer.Config prepJobConfig =
        getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
            propsFilePath, Collections.singletonList(
                TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    prepJobConfig.continuousMode = true;
    prepJobConfig.configs.add(
        String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    prepJobConfig.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer prepJob = new HoodieDeltaStreamer(prepJobConfig, jsc);

    // Prepare base dataset with some commits
    deltaStreamerTestRunner(prepJob, prepJobConfig, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath);
        TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(3, tableBasePath);
      }
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      assertDistanceCount(totalRecords, tableBasePath, sqlContext);
      return true;
    });

    // create a backfill job with checkpoint from the first instant
    HoodieDeltaStreamer.Config cfgBackfillJob = getDeltaStreamerConfig(tableBasePath, tableType.name(), WriteOperationType.UPSERT,
        propsFilePath, Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()));
    cfgBackfillJob.continuousMode = false;

    // run the backfill job
    props = prepareMultiWriterProps(storage, basePath, propsFilePath);
    props.setProperty("hoodie.write.lock.provider",
        "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    props.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, propsFilePath);
    HoodieTableMetaClient meta = createMetaClient(new HadoopStorageConfiguration(hadoopConf), tableBasePath);

    // get current checkpoint after preparing base dataset with some commits
    HoodieCommitMetadata commitMetadataForLastInstant = getLatestMetadata(meta);

    // Set checkpoint to the last successful position
    cfgBackfillJob.checkpoint = commitMetadataForLastInstant.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc);
    backfillJob.sync();

    meta.reloadActiveTimeline();
    int totalCommits = meta.getCommitsTimeline().filterCompletedInstants().countInstants();

    // add a new commit to timeline which may not have the checkpoint in extra metadata
    addCommitToTimeline(meta);
    meta.reloadActiveTimeline();
    verifyCommitMetadataCheckpoint(meta, null);

    cfgBackfillJob.checkpoint = null;
    new HoodieDeltaStreamer(cfgBackfillJob, jsc).sync(); // if deltastreamer checkpoint fetch does not walk back to older commits, this sync will fail
    meta.reloadActiveTimeline();
    Assertions.assertEquals(totalCommits + 2, meta.getCommitsTimeline().filterCompletedInstants().countInstants());
    verifyCommitMetadataCheckpoint(meta, "00008");
  }

  private void verifyCommitMetadataCheckpoint(HoodieTableMetaClient metaClient, String expectedCheckpoint) throws IOException {
    HoodieCommitMetadata commitMeta = getLatestMetadata(metaClient);
    if (expectedCheckpoint == null) {
      Assertions.assertThrows(HoodieException.class, () -> CheckpointUtils.getCheckpoint(commitMeta));
    } else {
      Assertions.assertEquals(expectedCheckpoint, CheckpointUtils.getCheckpoint(commitMeta).getCheckpointKey());
    }
  }

  private static HoodieCommitMetadata getLatestMetadata(HoodieTableMetaClient meta)
      throws IOException {
    HoodieTimeline timeline =
        meta.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    return timeline.readCommitMetadata(timeline.lastInstant().get());
  }

  private static TypedProperties prepareMultiWriterProps(HoodieStorage storage, String basePath,
                                                         String propsFilePath) throws IOException {
    TypedProperties props = new TypedProperties();
    populateCommonProps(props, basePath);
    populateCommonHiveProps(props);

    props.setProperty("include", "sql-transformer.properties");
    props.setProperty("hoodie.datasource.write.keygenerator.class",
        TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/source.avsc");
    props.setProperty("hoodie.streamer.schemaprovider.target.schema.file", basePath + "/target.avsc");

    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty("hoodie.clean.failed.writes.policy", "LAZY");
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

    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, propsFilePath);
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
    cfg.sourceOrderingFields = "timestamp";
    cfg.propsFilePath = propsFilePath;
    cfg.sourceLimit = 1000;
    cfg.schemaProviderClassName = defaultSchemaProviderClassName;
    return cfg;
  }

  private void runJobsInParallel(String tableBasePath, HoodieTableType tableType, int totalRecords,
      HoodieDeltaStreamer ingestionJob, HoodieDeltaStreamer.Config cfgIngestionJob, HoodieDeltaStreamer backfillJob,
      HoodieDeltaStreamer.Config cfgBackfillJob, boolean expectConflict, String jobId) throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTableMetaClient meta = createMetaClient(new HadoopStorageConfiguration(hadoopConf), tableBasePath);
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String lastSuccessfulCommit = timeline.lastInstant().get().requestedTime();
    // Condition for parallel ingestion job
    Function<Boolean, Boolean> conditionForRegularIngestion = (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath);
      } else {
        TestHelpers.assertAtleastNCompactionCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath);
      }
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      assertDistanceCount(totalRecords, tableBasePath, sqlContext);
      return true;
    };

    AtomicBoolean continuousFailed = new AtomicBoolean(false);
    AtomicBoolean backfillFailed = new AtomicBoolean(false);
    try {
      Future regularIngestionJobFuture = service.submit(() -> {
        try {
          deltaStreamerTestRunner(ingestionJob, cfgIngestionJob, conditionForRegularIngestion, jobId);
        } catch (Throwable ex) {
          continuousFailed.set(true);
          LOG.error("Continuous job failed " + ex.getMessage());
          throw new RuntimeException(ex);
        }
      });
      Future backfillJobFuture = service.submit(() -> {
        try {
          // trigger backfill atleast after 1 requested entry is added to timeline from continuous job. If not, there is a chance that backfill will complete even before
          // continuous job starts.
          awaitCondition(new GetCommitsAfterInstant(tableBasePath, lastSuccessfulCommit));
          backfillJob.sync();
        } catch (Throwable ex) {
          LOG.error("Backfilling job failed " + ex.getMessage());
          backfillFailed.set(true);
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
      if (expectConflict && backfillFailed.get() && e.getCause().getMessage().contains(ConcurrentModificationException.class.getName())) {
        // expected ConcurrentModificationException since ingestion & backfill will have overlapping writes
        if (!continuousFailed.get()) {
          // if backfill job failed, shutdown the continuous job.
          LOG.warn("Calling shutdown on ingestion job since the backfill job has failed for " + jobId);
          ingestionJob.shutdownGracefully();
        } else {
          // both backfill and ingestion job cannot fail.
          throw new HoodieException("Both backfilling and ingestion job failed ", e);
        }
      } else if (expectConflict && continuousFailed.get() && e.getCause().getMessage().contains("Ingestion service was shut down with exception")) {
        // incase of regular ingestion job failing, ConcurrentModificationException is not throw all the way.
        if (!backfillFailed.get()) {
          LOG.warn("Calling shutdown on backfill job since the ingstion/continuous job has failed for " + jobId);
          backfillJob.shutdownGracefully();
        } else {
          // both backfill and ingestion job cannot fail.
          throw new HoodieException("Both backfilling and ingestion job failed ", e);
        }
      } else {
        LOG.error("Conflict happened, but not expected " + e.getCause().getMessage());
        throw e;
      }
    } finally {
      service.shutdown();
    }
  }

  class GetCommitsAfterInstant {

    String basePath;
    String lastSuccessfulCommit;
    HoodieTableMetaClient meta;
    GetCommitsAfterInstant(String basePath, String lastSuccessfulCommit) {
      this.basePath = basePath;
      this.lastSuccessfulCommit = lastSuccessfulCommit;
      meta = createMetaClient(storage, basePath);
    }

    long getCommitsAfterInstant() {
      HoodieTimeline timeline1 = meta.reloadActiveTimeline().getAllCommitsTimeline().findInstantsAfter(lastSuccessfulCommit);
      return timeline1.countInstants();
    }
  }

  private static void awaitCondition(GetCommitsAfterInstant callback) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long soFar = 0;
    while (soFar <= 5000) {
      if (callback.getCommitsAfterInstant() > 0) {
        break;
      } else {
        Thread.sleep(500);
        soFar += 500;
      }
    }
    LOG.warn("Awaiting completed in " + (System.currentTimeMillis() - startTime));
  }

}
