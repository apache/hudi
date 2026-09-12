/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataTableServiceRequest;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestHoodieMetadataTableServicesToolExecution extends UtilitiesTestBase {
  @TempDir
  Path tempDir;

  @BeforeAll
  static void startSpark() throws Exception {
    initTestServices(false, false, false);
  }

  private HoodieMetadataTableServicesTool.Config config(String mode, String services) {
    HoodieMetadataTableServicesTool.Config cfg = new HoodieMetadataTableServicesTool.Config();
    cfg.basePath = tempDir.resolve("table").toString();
    cfg.mode = mode;
    cfg.services = services;
    return cfg;
  }

  private TypedProperties properties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    props.setProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), FileSystemBasedLockProvider.class.getName());
    props.setProperty(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key(), tempDir.resolve("dt-lock").toString());
    props.setProperty(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS.key(), "10");
    props.setProperty(HoodieLockConfig.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS.key(), "10");
    props.setProperty(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.key(), "1000");
    props.setProperty(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS.key(), "10000");
    return props;
  }

  private HoodieTableMetaClient initTable(HoodieMetadataTableServicesTool.Config cfg, boolean metadataAvailable) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableName("metadata_services_test")
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setRecordKeyFields("_row_key")
        .setPartitionFields("partition_path")
        .setTableVersion(HoodieTableVersion.EIGHT)
        .initTable(new HadoopStorageConfiguration(jsc.hadoopConfiguration()), cfg.basePath);
    if (metadataAvailable) {
      // Orchestration tests substitute the writer; only the availability guard needs a persisted flag.
      metaClient.getTableConfig().setMetadataPartitionState(metaClient, "files", true);
    }
    return metaClient;
  }

  @Test
  void skipsUninitializedMetadataWithoutCreatingWriter() throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config("schedule-and-execute", "all");
    initTable(cfg, false);
    HoodieMetadataTableServicesTool tool = spy(new HoodieMetadataTableServicesTool(cfg, jsc, properties()));
    tool.run();
    verify(tool, never()).createWriter(any());
    verify(tool, never()).createTransactionManager(any());
  }

  @Test
  void buildsIsolatedWriterProfilesSharingTheDataTableLock() throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config("schedule", "compaction");
    initTable(cfg, true);
    TypedProperties props = properties();
    props.setProperty(HoodieMetadataConfig.FAIL_ON_TABLE_SERVICE_FAILURES.key(), "false");
    props.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    props.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "clean,archive");
    props.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_SCHEDULE_ACTIONS.key(), "compaction");
    HoodieMetadataTableServicesTool tool = new HoodieMetadataTableServicesTool(cfg, jsc, props);
    for (WriteConcurrencyMode mode : Arrays.asList(WriteConcurrencyMode.SINGLE_WRITER,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)) {
      HoodieWriteConfig config = tool.buildWriteConfig(mode);
      assertEquals(cfg.basePath, config.getBasePath());
      assertEquals(mode.name(), config.getMetadataConfig().getWriteConcurrencyMode());
      assertTrue(config.getMetadataConfig().shouldFailOnTableServiceFailures());
      assertFalse(config.getMetadataConfig().isTableServiceManagerEnabled());
      assertEquals("", config.getMetadataConfig().getTableServiceManagerActions());
      assertEquals("", config.getMetadataConfig().getTableServiceManagerScheduleActions());
      HoodieWriteConfig mdtConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
          config, HoodieFailedWritesCleaningPolicy.LAZY, HoodieTableVersion.EIGHT);
      assertEquals(mode, mdtConfig.getWriteConcurrencyMode());
      if (mode.supportsMultiWriter()) {
        assertEquals(config.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH), mdtConfig.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH));
      } else {
        assertFalse(mdtConfig.isLockRequired());
      }
    }
    assertEquals("false", props.getProperty(HoodieMetadataConfig.FAIL_ON_TABLE_SERVICE_FAILURES.key()));
    assertEquals("true", props.getProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_ENABLED.key()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"schedule", "execute", "schedule-and-execute"})
  void runsRequestedPhasesInOrder(String mode) throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config(mode, "all");
    initTable(cfg, true);
    HoodieMetadataTableServicesTool tool = spy(new HoodieMetadataTableServicesTool(cfg, jsc, properties()));
    HoodieTableMetadataWriter writer = mock(HoodieTableMetadataWriter.class);
    List<String> calls = new ArrayList<>();
    List<WriteConcurrencyMode> profiles = new ArrayList<>();
    doAnswer(invocation -> {
      HoodieWriteConfig writeConfig = invocation.getArgument(0);
      profiles.add(WriteConcurrencyMode.valueOf(writeConfig.getMetadataConfig().getWriteConcurrencyMode()));
      return writer;
    }).when(tool).createWriter(any());
    doAnswer(invocation -> {
      MetadataTableServiceRequest request = invocation.getArgument(0);
      assertTrue(request.shouldDisableTableServiceManagerDelegation());
      calls.add("schedule:" + request.getServices());
      return null;
    }).when(writer).scheduleTableServices(any());
    doAnswer(invocation -> {
      MetadataTableServiceRequest request = invocation.getArgument(0);
      assertTrue(request.shouldDisableTableServiceManagerDelegation());
      calls.add("execute:" + request.getServices());
      return null;
    }).when(writer).executeTableServices(any());
    tool.run();
    if (mode.equals("schedule")) {
      assertEquals(Arrays.asList("schedule:[COMPACT, LOG_COMPACT]"), calls);
    } else if (mode.equals("execute")) {
      assertEquals(Arrays.asList("execute:[COMPACT, LOG_COMPACT]", "execute:[CLEAN]", "execute:[ARCHIVE]"), calls);
    } else {
      assertEquals(Arrays.asList("execute:[COMPACT, LOG_COMPACT]", "execute:[CLEAN]", "schedule:[COMPACT]",
          "execute:[COMPACT]", "schedule:[LOG_COMPACT]", "execute:[LOG_COMPACT]", "execute:[ARCHIVE]"), calls);
    }
    for (int index = 0; index < calls.size(); index++) {
      assertEquals(calls.get(index).startsWith("schedule:") ? WriteConcurrencyMode.SINGLE_WRITER
          : WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, profiles.get(index));
    }
    verify(writer, times(calls.size())).close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"operation", "create", "close", "unlock-only"})
  void preservesPrimaryFailureWhenUnlockAlsoFails(String failurePhase) throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config("schedule", "compaction");
    initTable(cfg, true);
    HoodieMetadataTableServicesTool tool = spy(new HoodieMetadataTableServicesTool(cfg, jsc, properties()));
    HoodieTableMetadataWriter writer = mock(HoodieTableMetadataWriter.class);
    TransactionManager transactionManager = mock(TransactionManager.class);
    doReturn(transactionManager).when(tool).createTransactionManager(any());
    doReturn(writer).when(tool).createWriter(any());
    RuntimeException primary = new IllegalStateException("primary failure");
    RuntimeException unlock = new IllegalStateException("unlock failure");
    doThrow(unlock).when(transactionManager).endStateChange(Option.empty());
    if (failurePhase.equals("operation")) {
      doThrow(primary).when(writer).scheduleTableServices(any());
    } else if (failurePhase.equals("create")) {
      doThrow(primary).when(tool).createWriter(any());
    } else if (failurePhase.equals("close")) {
      doThrow(primary).when(writer).close();
    }
    HoodieException failure = assertThrows(HoodieException.class, tool::run);
    if (failurePhase.equals("unlock-only")) {
      assertSame(unlock, failure.getCause());
    } else {
      assertSame(primary, failure.getCause());
      assertEquals(1, primary.getSuppressed().length);
      assertSame(unlock, primary.getSuppressed()[0]);
    }
    verify(transactionManager).endStateChange(Option.empty());
    verify(transactionManager).close();
  }

  @Test
  void schedulingWaitsForDataTableLock() throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config("schedule", "compaction");
    HoodieTableMetaClient metaClient = initTable(cfg, true);
    HoodieMetadataTableServicesTool tool = spy(new HoodieMetadataTableServicesTool(cfg, jsc, properties()));
    HoodieTableMetadataWriter writer = mock(HoodieTableMetadataWriter.class);
    doReturn(writer).when(tool).createWriter(any());
    CountDownLatch attemptingLock = new CountDownLatch(1);
    doAnswer(invocation -> {
      TransactionManager transactionManager = spy((TransactionManager) invocation.callRealMethod());
      doAnswer(begin -> {
        attemptingLock.countDown();
        return begin.callRealMethod();
      }).when(transactionManager).beginStateChange(any(), any());
      return transactionManager;
    }).when(tool).createTransactionManager(any());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (TransactionManager dataTransaction = new TransactionManager(
        tool.buildWriteConfig(WriteConcurrencyMode.SINGLE_WRITER), metaClient.getStorage())) {
      dataTransaction.beginStateChange(Option.empty(), Option.empty());
      Future<?> run;
      try {
        run = executor.submit(tool::run);
        assertTrue(attemptingLock.await(10, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> run.get(200, TimeUnit.MILLISECONDS));
        verify(writer, never()).scheduleTableServices(any());
      } finally {
        dataTransaction.endStateChange(Option.empty());
      }
      run.get(30, TimeUnit.SECONDS);
      verify(writer).scheduleTableServices(any());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void compactsRealMetadataWithoutAddingDataTableCommits() throws Exception {
    HoodieMetadataTableServicesTool.Config cfg = config("schedule-and-execute", "compaction");
    HoodieTableMetaClient metaClient = initTable(cfg, false);
    TypedProperties props = properties();
    props.setProperty("hoodie.metadata.index.column.stats.enable", "false");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(cfg.basePath).forTable("metadata_services_test").withSchema(TRIP_EXAMPLE_SCHEMA).withPreCombineField("timestamp")
        .withWriteTableVersion(8).withParallelism(2, 2)
        .withProps(props)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withStreamingWriteEnabled(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .withTableServiceManagerEnabled(true)
            .withTableServiceManagerActions("compaction,logcompaction,clean,archive")
            .withTableServiceManagerScheduleActions("compaction,logcompaction").build())
        .build();
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
         SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {
      for (int index = 0; index < 2; index++) {
        String instant = client.startCommit();
        List<WriteStatus> statuses = client.insert(jsc.parallelize(dataGen.generateInserts(instant, 20), 2), instant).collect();
        assertTrue(statuses.stream().noneMatch(WriteStatus::hasErrors));
        assertTrue(client.commit(instant, jsc.parallelize(statuses, 2)));
      }
    }
    metaClient.reloadTableConfig();
    assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());
    List<HoodieInstant> dataTimelineBefore = new ArrayList<>(metaClient.reloadActiveTimeline().getInstants());
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(new HadoopStorageConfiguration(jsc.hadoopConfiguration()))
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(cfg.basePath)).build();
    int completedCompactionsBefore = metadataMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().countInstants();

    new HoodieMetadataTableServicesTool(cfg, jsc, TypedProperties.copy(writeConfig.getProps())).run();

    assertEquals(completedCompactionsBefore + 1,
        metadataMetaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants().countInstants());
    assertTrue(metadataMetaClient.getActiveTimeline().filterPendingCompactionTimeline().empty());
    assertEquals(dataTimelineBefore, metaClient.reloadActiveTimeline().getInstants());
  }
}
