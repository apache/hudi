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

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestBaseHoodieWriteClient extends HoodieCommonTestHarness {

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"REMOTE_FIRST", "REMOTE_ONLY"})
  void validateClientClose(FileSystemViewStorageType viewStorageType) throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = getHoodieWriteConfigForRemoteView(viewStorageType);
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    EmbeddedTimelineService service = mock(EmbeddedTimelineService.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.of(service), tableServiceClient);
    SyncableFileSystemView view = mock(SyncableFileSystemView.class);
    when(table.getHoodieView()).thenReturn(view);

    writeClient.close();
    verify(view).close();
    verify(tableServiceClient).close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"REMOTE_FIRST", "REMOTE_ONLY"})
  void validateClientCloseSkippedIfTimelineServiceNotProvided(FileSystemViewStorageType viewStorageType) throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = getHoodieWriteConfigForRemoteView(viewStorageType);
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient);

    writeClient.close();
    verify(table, never()).getHoodieView();
    verify(tableServiceClient).close();
  }

  private HoodieWriteConfig getHoodieWriteConfigForRemoteView(FileSystemViewStorageType viewStorageType) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .build();
    // update the view config to simulate how the configs are updated when adding the embedded timeline service url and port
    writeConfig.setViewStorageConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(viewStorageType).build());
    return writeConfig;
  }

  @Test
  void validateClientCloseSkippedIfViewIsNotRemote() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .build();
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient);

    writeClient.close();
    verify(table, never()).getHoodieView();
    verify(tableServiceClient).close();
  }

  @ParameterizedTest
  @MethodSource("generateParametersForValidateTimestampInternal")
  void testValidateTimestampInternal(
      boolean shouldEnableTimestampOrderingValidation,
      boolean supportsOcc,
      boolean shouldValidateForLatestTimestamp
  ) throws IOException {
    initMetaClient();
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .withEnableTimestampOrderingValidation(shouldEnableTimestampOrderingValidation);
    if (supportsOcc) {
      writeConfigBuilder.withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    }

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);
    if (shouldValidateForLatestTimestamp) {
      HoodieInstant lastEntry = metaClient.getActiveTimeline().getWriteTimeline().lastInstant().get();
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> writeClient.validateTimestampInternal(metaClient, "001"));
      assertEquals(String.format("Found later commit time %s, compared to the current instant 001, hence failing to create requested commit meta file", lastEntry), exception.getMessage());
    } else {
      assertDoesNotThrow(() -> writeClient.validateTimestampInternal(metaClient, "001"));
    }
  }

  private static Stream<Arguments> generateParametersForValidateTimestampInternal() {
    return Stream.of(
        Arguments.of(true, true, true),
        Arguments.of(true, false, false),
        Arguments.of(false, false, false)
    );
  }

  @Test
  void testStartCommit() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .withEnableTimestampOrderingValidation(true)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withLockWaitTimeInMillis(50L)
            .withNumRetries(2)
            .withRetryWaitTimeInMillis(10L)
            .withClientNumRetries(2)
            .withClientRetryWaitTimeInMillis(10L)
            .build())
        .build();

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient);

    writeClient.startCommitWithTime("001", "commit");

    HoodieDefaultTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
    assertTrue(writeTimeline.lastInstant().isPresent());
    assertEquals("commit", writeTimeline.lastInstant().get().getAction());
    assertEquals("001", writeTimeline.lastInstant().get().getTimestamp());
  }

  private static class TestWriteClient extends BaseHoodieWriteClient<String, String, String, String> {
    private final HoodieTable<String, String, String, String> table;

    public TestWriteClient(HoodieWriteConfig writeConfig, HoodieTable<String, String, String, String> table, Option<EmbeddedTimelineService> timelineService,
                           BaseHoodieTableServiceClient<String, String, String> tableServiceClient) {
      super(new HoodieLocalEngineContext(new Configuration(false)), writeConfig, timelineService, null);
      this.table = table;
      this.tableServiceClient = tableServiceClient;
    }

    @Override
    protected HoodieIndex<?, ?> createIndex(HoodieWriteConfig writeConfig) {
      return new HoodieSimpleIndex(config, Option.empty());
    }

    @Override
    public boolean commit(String instantTime, String writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                          Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
      return false;
    }

    @Override
    protected Pair<HoodieCommitMetadata, List<HoodieWriteStat>> reconcileCommitMetadataAndWriteStatsForAdditionalLogFiles(HoodieTable table, String commitActionType, String instantTime,
                                                                                                                          HoodieCommitMetadata originalMetadata) {
      return null;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
    }

    @Override
    protected void validateTimestamp(HoodieTableMetaClient metaClient, String instantTime) {
      // no op
    }

    @Override
    public String filterExists(String hoodieRecords) {
      return "";
    }

    @Override
    public String upsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String upsertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String insert(String records, String instantTime) {
      return "";
    }

    @Override
    public String insertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
      return "";
    }

    @Override
    public String bulkInsertPreppedRecords(String preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
      return "";
    }

    @Override
    public String delete(String keys, String instantTime) {
      return "";
    }

    @Override
    public String deletePrepped(String preppedRecords, String instantTime) {
      return "";
    }
  }
}