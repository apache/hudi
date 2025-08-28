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

import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.testutils.Assertions.assertComplexKeyGeneratorValidationThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestBaseHoodieWriteClient extends HoodieCommonTestHarness {

  @Test
  void startCommitWillRollbackFailedWritesInEagerMode() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient);

    // mock no inflight restore
    HoodieTimeline inflightRestoreTimeline = mock(HoodieTimeline.class);
    when(mockMetaClient.getActiveTimeline().getRestoreTimeline().filterInflightsAndRequested()).thenReturn(inflightRestoreTimeline);
    when(inflightRestoreTimeline.countInstants()).thenReturn(0);
    // mock no pending compaction
    when(mockMetaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant()).thenReturn(Option.empty());
    // mock table version
    when(mockMetaClient.getTableConfig().getTableVersion()).thenReturn(HoodieTableVersion.current());

    writeClient.startCommit(HoodieActiveTimeline.COMMIT_ACTION, mockMetaClient);
    verify(tableServiceClient).rollbackFailedWrites(mockMetaClient);
  }

  @Test
  void rollbackDelegatesToTableServiceClient() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient);

    writeClient.rollbackFailedWrites(mockMetaClient);
    verify(tableServiceClient).rollbackFailedWrites(mockMetaClient);
  }

  private static Stream<Arguments> testStartCommitWithComplexKeyGeneratorValidation() {
    List<Arguments> arguments = new ArrayList<>();

    List<Arguments> keyAndPartitionFieldOptions = Arrays.asList(
        Arguments.of("r1", "p1"),
        Arguments.of("r1", "p1,p2"),
        Arguments.of("r1", ""),
        Arguments.of("r1,r2", "p1")
    );

    List<Arguments> booleanOptions = Arrays.asList(
        Arguments.of(false, true),
        Arguments.of(true, true),
        Arguments.of(true, false)
    );

    List<Integer> tableVersionOptions = Arrays.asList(8, 9);

    arguments.addAll(Stream.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator",
            "org.apache.hudi.keygen.ComplexKeyGenerator")
        .flatMap(keyGenClass -> keyAndPartitionFieldOptions.stream()
            .flatMap(keyAndPartitionField -> booleanOptions.stream()
                .flatMap(booleans -> tableVersionOptions.stream()
                    .map(tableVersion -> Arguments.of(
                        keyGenClass,
                        keyAndPartitionField.get()[0],
                        keyAndPartitionField.get()[1],
                        booleans.get()[0],
                        booleans.get()[1],
                        tableVersion
                    ))
                )
            ))
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.SimpleAvroKeyGenerator",
            "org.apache.hudi.keygen.SimpleKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.CustomAvroKeyGenerator",
            "org.apache.hudi.keygen.CustomKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1:SIMPLE",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));

    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource
  void testStartCommitWithComplexKeyGeneratorValidation(String keyGeneratorClass,
                                                        String recordKeyFields,
                                                        String partitionPathFields,
                                                        boolean setComplexKeyGeneratorValidationConfig,
                                                        boolean enableComplexKeyGeneratorValidation,
                                                        int tableVersion) throws IOException {
    if (basePath == null) {
      initPath();
    }
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFields);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionPathFields);
    tableProperties.put(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion));
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFields);
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathFields);
    writeProperties.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion));
    if (setComplexKeyGeneratorValidationConfig) {
      writeProperties.put(
          HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), enableComplexKeyGeneratorValidation);
    }
    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    if (tableVersion <= 8 && enableComplexKeyGeneratorValidation
        && (ComplexAvroKeyGenerator.class.getCanonicalName().equals(keyGeneratorClass)
        || "org.apache.hudi.keygen.ComplexKeyGenerator".equals(keyGeneratorClass))
        && recordKeyFields.split(",").length == 1) {
      assertComplexKeyGeneratorValidationThrows(() -> writeClient.startCommit("commit"));
    } else {
      String requestedTime = writeClient.startCommit("commit");

      HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
      assertTrue(writeTimeline.lastInstant().isPresent());
      assertEquals("commit", writeTimeline.lastInstant().get().getAction());
      assertEquals(requestedTime, writeTimeline.lastInstant().get().requestedTime());
    }
  }

  @Test
  void testStartCommit() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
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

    HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.UTC);
    TransactionManager transactionManager = mock(TransactionManager.class);
    TimeGenerator timeGenerator = mock(TimeGenerator.class);

    Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
    when(timeGenerator.generateTime(true)).thenReturn(now.toEpochMilli());
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfig, table, Option.empty(), tableServiceClient, transactionManager, timeGenerator);

    String instantTime = writeClient.startCommit("commit");

    HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
    assertTrue(writeTimeline.lastInstant().isPresent());
    assertEquals("commit", writeTimeline.lastInstant().get().getAction());
    assertEquals(instantTime, writeTimeline.lastInstant().get().requestedTime());
    HoodieInstant expectedInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieActiveTimeline.COMMIT_ACTION, instantTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);

    InOrder inOrder = Mockito.inOrder(transactionManager, timeGenerator);
    inOrder.verify(transactionManager).beginStateChange(Option.empty(), Option.empty());
    inOrder.verify(timeGenerator).generateTime(true);
    inOrder.verify(transactionManager).endStateChange(Option.of(expectedInstant));
  }

  private static class TestWriteClient extends BaseHoodieWriteClient<String, String, String, String> {
    private final HoodieTable<String, String, String, String> table;

    public TestWriteClient(HoodieWriteConfig writeConfig, HoodieTable<String, String, String, String> table, Option<EmbeddedTimelineService> timelineService,
                           BaseHoodieTableServiceClient<String, String, String> tableServiceClient) {
      super(new HoodieLocalEngineContext(getDefaultStorageConf()), writeConfig, timelineService, null);
      this.table = table;
      this.tableServiceClient = tableServiceClient;
    }

    public TestWriteClient(HoodieWriteConfig writeConfig, HoodieTable<String, String, String, String> table, Option<EmbeddedTimelineService> timelineService,
                           BaseHoodieTableServiceClient<String, String, String> tableServiceClient, TransactionManager transactionManager, TimeGenerator timeGenerator) {
      super(new HoodieLocalEngineContext(getDefaultStorageConf()), writeConfig, timelineService, null, transactionManager, timeGenerator);
      this.table = table;
      this.tableServiceClient = tableServiceClient;
    }

    @Override
    protected HoodieIndex<?, ?> createIndex(HoodieWriteConfig writeConfig) {
      return new HoodieSimpleIndex(config, Option.empty());
    }

    @Override
    public boolean commit(String instantTime, String writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                          Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc, Option<WriteStatusValidator> writeStatusValidatorOpt) {
      return false;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
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

    @Override
    protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {

    }
  }
}