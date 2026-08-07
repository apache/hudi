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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
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
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieSimpleIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestBaseHoodieWriteClient extends HoodieCommonTestHarness {

  private static HoodieTableConfig tableConfigWithMode(MetaFieldsMode mode) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.META_FIELDS_MODE, mode.name());
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS,
        Boolean.toString(mode.toLegacyPopulateMetaFields()));
    tableConfig.setValue(HoodieTableConfig.VERSION,
        String.valueOf(HoodieTableVersion.current().versionCode()));
    return tableConfig;
  }

  private static BaseHoodieWriteClient<?, ?, ?, ?> validatorClient(HoodieWriteConfig writeConfig) {
    return new TestWriteClient(writeConfig, mock(HoodieTable.class), Option.empty(),
        mock(BaseHoodieTableServiceClient.class));
  }

  @Test
  void validateAgainstTablePropertiesRejectsMetaFieldsModeMismatch() throws IOException {
    initMetaClient();
    // A writer that explicitly asks for NONE against a COMMIT_TIME_ONLY table: both legacy booleans
    // are false, so a boolean-only check passes and the writer goes on to produce null commit times
    // while the table still advertises COMMIT_TIME_ONLY.
    HoodieWriteConfig noneWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .build();
    assertEquals(MetaFieldsMode.NONE, noneWriteConfig.getMetaFieldsMode());

    HoodieTableConfig commitTimeOnlyTable = tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_ONLY);
    assertFalse(commitTimeOnlyTable.populateMetaFields(),
        "precondition: both legacy booleans are false, so only the enum comparison can catch this");

    HoodieException ex = assertThrows(HoodieException.class, () ->
        validatorClient(noneWriteConfig).validateAgainstTableProperties(commitTimeOnlyTable, noneWriteConfig));
    assertTrue(ex.getMessage().contains(HoodieTableConfig.META_FIELDS_MODE.key()),
        "error must name the mode property: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("COMMIT_TIME_ONLY") && ex.getMessage().contains("NONE"),
        "error must name both modes: " + ex.getMessage());
  }

  @Test
  void validateAgainstTablePropertiesMakesAnUnstatedWriterInheritTheTableMode() throws IOException {
    initMetaClient();
    // hoodie.meta.fields.mode is a table property, changeable only through hudi-cli or an upgrade. A
    // writer that states neither meta-field property therefore inherits the table's mode. Many
    // callers build a write config that way — table services and a restarted StreamSync both do — so
    // they must inherit rather than be rejected.
    //
    // "Unstated" means neither property. An explicitly-passed legacy boolean is a stated intent and
    // is compared instead, see validateAgainstTablePropertiesRejectsAnExplicitLegacyBooleanThatDisagrees.
    HoodieWriteConfig unstated = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    assertEquals(MetaFieldsMode.ALL, unstated.getMetaFieldsMode(),
        "precondition: on its own an unstated writer resolves to the ALL default");

    validatorClient(unstated)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.ALL), unstated);

    assertEquals(MetaFieldsMode.ALL, unstated.getMetaFieldsMode());
    assertTrue(unstated.populateMetaFields(),
        "the derived legacy boolean must follow the inherited mode, or the ~55 call sites still "
            + "reading populateMetaFields() would disagree with the enum");
  }

  @Test
  void validateAgainstTablePropertiesMakesAnUnstatedWriterInheritASelectiveMode() throws IOException {
    initMetaClient();
    // The inheriting half of the StreamSync-restart case: a restarted writer that states nothing must
    // pick up COMMIT_TIME_ONLY and keep writing commit times, rather than resolving to the ALL default
    // (rejected as a widening) or to NONE (silently producing rows incremental queries then drop).
    HoodieWriteConfig unstated = HoodieWriteConfig.newBuilder().withPath(basePath).build();

    validatorClient(unstated)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_ONLY), unstated);

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, unstated.getMetaFieldsMode(),
        "the writer must have adopted the table's selective mode");
    assertFalse(unstated.populateMetaFields(),
        "COMMIT_TIME_ONLY does not populate the record key, so the derived boolean stays false");
  }

  @Test
  void validateAgainstTablePropertiesRejectsUnstatedWriterNarrowingASelectiveTable() throws IOException {
    initMetaClient();
    // The StreamSync-restart case. A writer that sets only populate.meta.fields=false resolves to
    // NONE without ever naming a mode, so neither the widening check (NONE is not wider than
    // COMMIT_TIME_ONLY) nor the stated-mode check catches it. It would then write base files with a
    // null _hoodie_commit_time while hoodie.properties still says COMMIT_TIME_ONLY, and incremental
    // queries — which the table is still deemed to support — would silently drop those rows.
    //
    // The writer must *state* NONE for this to be a conflict. An unstated writer inherits instead —
    // see validateAgainstTablePropertiesMakesAnUnstatedWriterInheritASelectiveMode.
    HoodieWriteConfig stated = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .build();
    assertEquals(MetaFieldsMode.NONE, stated.getMetaFieldsMode());

    HoodieException ex = assertThrows(HoodieException.class, () ->
        validatorClient(stated).validateAgainstTableProperties(
            tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_ONLY), stated));
    assertTrue(ex.getMessage().contains(HoodieTableConfig.META_FIELDS_MODE.key()), ex.getMessage());
    assertTrue(ex.getMessage().contains("COMMIT_TIME_ONLY") && ex.getMessage().contains("NONE"),
        "error must name both modes: " + ex.getMessage());
    // The message must point at the sanctioned mutation path rather than implying a write can do it.
    assertTrue(ex.getMessage().contains("hudi-cli"), ex.getMessage());
  }

  @Test
  void validateAgainstTablePropertiesRejectsStatedSelectiveToSelectiveNarrowing() throws IOException {
    initMetaClient();
    // Same shape one step in: a writer stating COMMIT_TIME_ONLY against COMMIT_TIME_AND_FILE_NAME.
    // Narrowing between two selective modes is still a conflict — only hudi-cli may do that.
    HoodieWriteConfig stated = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();

    assertThrows(HoodieException.class, () ->
        validatorClient(stated).validateAgainstTableProperties(
            tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME), stated));
  }

  @Test
  void validateAgainstTablePropertiesRejectsSiblingSelectiveModesBothWays() throws IOException {
    initMetaClient();
    // COMMIT_TIME_ONLY and FILE_NAME_ONLY are each wider than the other, so neither can stand in for
    // the other. Nothing tested this before, and it is the case that makes "narrowing is allowed"
    // subtle: there is no narrowing direction between them at all.
    HoodieWriteConfig commitTimeOnly = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY).build();
    HoodieWriteConfig fileNameOnly = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withMetaFieldsMode(MetaFieldsMode.FILE_NAME_ONLY).build();

    assertThrows(HoodieException.class, () -> validatorClient(commitTimeOnly)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.FILE_NAME_ONLY), commitTimeOnly));
    assertThrows(HoodieException.class, () -> validatorClient(fileNameOnly)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_ONLY), fileNameOnly));
  }

  @Test
  void validateAgainstTablePropertiesAllowsSelectiveWriterToNarrowAnAllTable() throws IOException {
    initMetaClient();
    // COMMIT_TIME_AND_FILE_NAME against ALL populates strictly fewer columns (no record key), so it
    // is a narrowing and stays allowed. Pins the one pair the isRecordKeyPopulated clause decides.
    HoodieWriteConfig selective = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME)
        .build();

    // Rejected in this direction only because the writer stated the mode explicitly.
    assertThrows(HoodieException.class, () -> validatorClient(selective)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.ALL), selective));
    // ...but never as a *widening*: the reverse direction is what isWiderThan must catch.
    HoodieWriteConfig allWriter = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withMetaFieldsMode(MetaFieldsMode.ALL).build();
    HoodieException ex = assertThrows(HoodieException.class, () -> validatorClient(allWriter)
        .validateAgainstTableProperties(
            tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME), allWriter));
    assertTrue(ex.getMessage().contains("cannot be widened"), ex.getMessage());
  }

  @Test
  void validateAgainstTablePropertiesMakesADefaultWriterInheritNoneRatherThanWiden() throws IOException {
    initMetaClient();
    // A default writer states neither meta-field property, so it inherits NONE rather than being
    // rejected for widening. That is the point of inheritance: the overwhelmingly common writer —
    // one that says nothing about meta fields — must keep working against any table, and it cannot
    // accidentally start populating columns the table never wrote.
    HoodieWriteConfig defaultWriteConfig = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    assertEquals(MetaFieldsMode.ALL, defaultWriteConfig.getMetaFieldsMode(),
        "precondition: on its own a default writer resolves to ALL");

    validatorClient(defaultWriteConfig)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.NONE), defaultWriteConfig);

    assertEquals(MetaFieldsMode.NONE, defaultWriteConfig.getMetaFieldsMode(),
        "the writer must have inherited NONE from the table");
    assertFalse(defaultWriteConfig.populateMetaFields());
  }

  @Test
  void validateAgainstTablePropertiesRejectsStatedWidening() throws IOException {
    initMetaClient();
    // Widening is rejected when the writer asks for it: ALL against a NONE table would claim meta
    // columns the table never wrote, and earlier commits cannot be distinguished from later ones.
    HoodieWriteConfig allWriter = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .build();

    HoodieException ex = assertThrows(HoodieException.class, () ->
        validatorClient(allWriter)
            .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.NONE), allWriter));
    assertTrue(ex.getMessage().contains("cannot be widened"), ex.getMessage());
  }

  @Test
  void validateAgainstTablePropertiesRejectsAnExplicitLegacyBooleanThatDisagrees() throws IOException {
    initMetaClient();
    // The one user-visible break in this rule. Passing populate.meta.fields=false against an ALL
    // table used to narrow it silently (HUDI-2161); it now throws, because the writer explicitly
    // asked for something the table cannot do. Telling the user beats overriding them — but existing
    // jobs that pass this flag against a default table will fail until they drop it.
    HoodieWriteConfig legacyFalse = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withPopulateMetaFields(false)
        .build();

    HoodieException ex = assertThrows(HoodieException.class, () ->
        validatorClient(legacyFalse)
            .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.ALL), legacyFalse));
    assertTrue(ex.getMessage().contains("hudi-cli"),
        "the message must point at the sanctioned mutation path: " + ex.getMessage());
  }

  @Test
  void validateAgainstTablePropertiesAcceptsMatchingMetaFieldsMode() throws IOException {
    initMetaClient();
    // Default writer and default table both resolve to ALL — the overwhelmingly common case.
    HoodieWriteConfig defaultWriteConfig = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    assertEquals(MetaFieldsMode.ALL, defaultWriteConfig.getMetaFieldsMode());
    validatorClient(defaultWriteConfig)
        .validateAgainstTableProperties(tableConfigWithMode(MetaFieldsMode.ALL), defaultWriteConfig);

    // And a selective writer against a table recorded with the same mode.
    HoodieWriteConfig selectiveWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();
    validatorClient(selectiveWriteConfig).validateAgainstTableProperties(
        tableConfigWithMode(MetaFieldsMode.COMMIT_TIME_ONLY), selectiveWriteConfig);
  }

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

  private static Stream<Arguments> testWithComplexKeyGeneratorValidation() {
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
  void testWithComplexKeyGeneratorValidation(String keyGeneratorClass,
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
        && KeyGenUtils.getRecordKeyFields(recordKeyFields).size() == 1) {
      assertComplexKeyGeneratorValidationThrows(() -> writeClient.initTable(WriteOperationType.INSERT, Option.empty()), "ingestion");
    } else {
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());
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
      // Ensure the returned table has the correct metaClient
      when(table.getMetaClient()).thenReturn(metaClient);
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
