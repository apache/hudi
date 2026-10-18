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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestUpgradeDowngradeOrchestration {

  @Test
  void testUpgradeRunsEveryVersionHopAndRollbackOnce() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.SIX);
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();

    try (MockedStatic<UpgradeDowngradeUtils> upgradeUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableConfig> tableConfigStatic = mockStatic(HoodieTableConfig.class)) {
      upgradeDowngrade.run(HoodieTableVersion.NINE, "100");

      assertEquals(Arrays.asList("6->7", "7->8", "8->9"), upgradeDowngrade.hops);
      verify(testContext.tableConfig).setTableVersion(HoodieTableVersion.NINE);
      upgradeUtils.verify(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          testContext.table, testContext.engineContext, testContext.config, testContext.helper, false, HoodieTableVersion.SIX));
      tableConfigStatic.verify(() -> HoodieTableConfig.updateAndDeleteProps(
          eq(testContext.storage), any(StoragePath.class), any(), eq(java.util.Collections.emptySet())));
    }
  }

  @Test
  void testDowngradeRunsEveryVersionHopAndRollbackOnce() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.NINE);
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();

    try (MockedStatic<UpgradeDowngradeUtils> upgradeUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableConfig> ignored = mockStatic(HoodieTableConfig.class)) {
      upgradeDowngrade.run(HoodieTableVersion.SIX, null);

      assertEquals(Arrays.asList("9->8", "8->7", "7->6"), upgradeDowngrade.hops);
      verify(testContext.tableConfig).setTableVersion(HoodieTableVersion.SIX);
      upgradeUtils.verify(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          testContext.table, testContext.engineContext, testContext.config, testContext.helper, false, HoodieTableVersion.NINE));
    }
  }

  @Test
  void testHandlerFailureDoesNotPublishTargetVersion() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.SEVEN);
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();
    upgradeDowngrade.failure = new IllegalStateException("handler failed");

    try (MockedStatic<UpgradeDowngradeUtils> upgradeUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableConfig> tableConfigStatic = mockStatic(HoodieTableConfig.class)) {
      assertThrows(IllegalStateException.class,
          () -> upgradeDowngrade.run(HoodieTableVersion.EIGHT, "100"));

      assertEquals(java.util.Collections.singletonList("7->8"), upgradeDowngrade.hops);
      verify(testContext.tableConfig, never()).setTableVersion(any(HoodieTableVersion.class));
      tableConfigStatic.verifyNoInteractions();
      upgradeUtils.verify(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          testContext.table, testContext.engineContext, testContext.config, testContext.helper, false, HoodieTableVersion.SIX));
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "SIX"})
  void testDowngradeBelowNineRefusesSeveralOrderingFieldsBeforeAnyStep(HoodieTableVersion toVersion) throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.TEN);
    when(testContext.tableConfig.getOrderingFieldsStr()).thenReturn(Option.of("ts,seq"));
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();

    try (MockedStatic<UpgradeDowngradeUtils> upgradeUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableConfig> tableConfigStatic = mockStatic(HoodieTableConfig.class)) {
      HoodieUpgradeDowngradeException exception = assertThrows(HoodieUpgradeDowngradeException.class,
          () -> upgradeDowngrade.run(toVersion, "100"));
      assertTrue(exception.getMessage().startsWith(String.format(
          "Cannot downgrade to table version %d: hoodie.table.ordering.fields=ts,seq orders records on several fields", toVersion.versionCode())),
          exception.getMessage());

      // no rollback or compaction, metadata table lookup, version hop or table config write ran before the refusal
      assertTrue(upgradeDowngrade.hops.isEmpty());
      upgradeUtils.verifyNoInteractions();
      verify(testContext.storage, never()).exists(any(StoragePath.class));
      verify(testContext.tableConfig, never()).setTableVersion(any(HoodieTableVersion.class));
      tableConfigStatic.verifyNoInteractions();
    }
  }

  static Stream<Arguments> downgradesPassingOrderingFieldsCheck() {
    return Stream.of(
        // version 9 keeps several ordering fields under hoodie.table.ordering.fields
        Arguments.of(HoodieTableVersion.NINE, null, "ts,seq", java.util.Collections.singletonList("10->9")),
        // the binlog file and position MySqlDebeziumAvroPayload orders on map back to its single sequence field
        Arguments.of(HoodieTableVersion.EIGHT, MySqlDebeziumAvroPayload.class.getName(), MySqlDebeziumAvroPayload.ORDERING_FIELDS,
            Arrays.asList("10->9", "9->8")));
  }

  @ParameterizedTest
  @MethodSource("downgradesPassingOrderingFieldsCheck")
  void testDowngradePassesOrderingFieldsCheck(HoodieTableVersion toVersion, String legacyPayloadClass, String orderingFields,
                                               List<String> expectedHops) throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.TEN);
    when(testContext.tableConfig.getLegacyPayloadClass()).thenReturn(legacyPayloadClass);
    when(testContext.tableConfig.getOrderingFieldsStr()).thenReturn(Option.of(orderingFields));
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();

    try (MockedStatic<UpgradeDowngradeUtils> upgradeUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableConfig> ignored = mockStatic(HoodieTableConfig.class)) {
      upgradeDowngrade.run(toVersion, null);

      assertEquals(expectedHops, upgradeDowngrade.hops);
      verify(testContext.tableConfig).setTableVersion(toVersion);
      upgradeUtils.verify(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          testContext.table, testContext.engineContext, testContext.config, testContext.helper, false, HoodieTableVersion.TEN));
    }
  }

  @Test
  void testVersionValidationAndNoOpPaths() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/table")
        .withAutoUpgradeVersion(false)
        .build();

    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> UpgradeDowngrade.needsDowngrade(HoodieTableVersion.SIX, HoodieTableVersion.FIVE));
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.FIVE);
    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> UpgradeDowngrade.needsUpgrade(metaClient, config, HoodieTableVersion.SIX));
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    assertFalse(UpgradeDowngrade.needsUpgrade(metaClient, config, HoodieTableVersion.NINE));
    assertEquals(HoodieTableVersion.SIX, config.getWriteVersion());

    TestContext noOpContext = new TestContext(HoodieTableVersion.SIX);
    noOpContext.trackingUpgradeDowngrade().run(HoodieTableVersion.SIX, null);
  }

  @Test
  void testUnsupportedDirectHopsVisitEveryHandlerBranch() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.SIX);
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();

    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> upgradeDowngrade.callRealUpgrade(HoodieTableVersion.SIX, HoodieTableVersion.NINE));
    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> upgradeDowngrade.callRealDowngrade(HoodieTableVersion.NINE, HoodieTableVersion.SIX));
  }

  @Test
  void testHandlerConfigChangesAreAppliedWithAlternativeKeys() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.SIX);
    TrackingUpgradeDowngrade upgradeDowngrade = testContext.trackingUpgradeDowngrade();
    ConfigProperty<String> propertyToAdd = ConfigProperty.key("test.property").noDefaultValue()
        .withAlternatives("test.property.legacy");
    ConfigProperty<String> propertyToRemove = ConfigProperty.key("test.remove").noDefaultValue();
    upgradeDowngrade.changeSet = new UpgradeDowngrade.TableConfigChangeSet(
        Map.of(propertyToAdd, "value"), Set.of(propertyToRemove));

    try (MockedStatic<HoodieTableConfig> ignored = mockStatic(HoodieTableConfig.class)) {
      upgradeDowngrade.run(HoodieTableVersion.SEVEN, null);
    }

    verify(testContext.tableConfig).clearValue(propertyToRemove);
    verify(testContext.tableConfig).setValue(propertyToAdd, "value");
    verify(testContext.tableConfig).setValue("test.property.legacy", "value");
  }

  @Test
  void testMetadataTableLookupFailureIsWrapped() throws Exception {
    TestContext testContext = new TestContext(HoodieTableVersion.SIX);
    when(testContext.storage.exists(any(StoragePath.class))).thenThrow(new java.io.IOException("lookup failed"));

    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> testContext.trackingUpgradeDowngrade().run(HoodieTableVersion.SEVEN, null));
  }

  private static class TestContext {
    private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    private final HoodieStorage storage = mock(HoodieStorage.class);
    private final HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    private final SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    private final HoodieTable table = mock(HoodieTable.class);
    private final HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/table")
        .withAutoUpgradeVersion(true)
        .build();

    TestContext(HoodieTableVersion version) throws Exception {
      when(metaClient.getTableConfig()).thenReturn(tableConfig);
      when(tableConfig.getTableVersion()).thenReturn(version);
      when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.empty());
      when(tableConfig.isMetadataTableAvailable()).thenReturn(false);
      when(metaClient.getStorage()).thenReturn(storage);
      when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
      when(metaClient.getMetaPath()).thenReturn(new StoragePath("/table/.hoodie"));
      when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
      when(storage.exists(any(StoragePath.class))).thenReturn(false);
      when(helper.getTable(config, engineContext)).thenReturn(table);
    }

    TrackingUpgradeDowngrade trackingUpgradeDowngrade() {
      return new TrackingUpgradeDowngrade(metaClient, config, engineContext, helper);
    }
  }

  private static class TrackingUpgradeDowngrade extends UpgradeDowngrade {
    private final List<String> hops = new ArrayList<>();
    private RuntimeException failure;
    private TableConfigChangeSet changeSet = new TableConfigChangeSet();

    TrackingUpgradeDowngrade(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                             HoodieEngineContext context, SupportsUpgradeDowngrade helper) {
      super(metaClient, config, context, helper);
    }

    @Override
    protected TableConfigChangeSet upgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
      return recordHop(fromVersion, toVersion);
    }

    @Override
    protected TableConfigChangeSet downgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
      return recordHop(fromVersion, toVersion);
    }

    private TableConfigChangeSet recordHop(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) {
      hops.add(fromVersion.versionCode() + "->" + toVersion.versionCode());
      if (failure != null) {
        throw failure;
      }
      return changeSet;
    }

    private TableConfigChangeSet callRealUpgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) {
      return super.upgrade(fromVersion, toVersion, null);
    }

    private TableConfigChangeSet callRealDowngrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) {
      return super.downgrade(fromVersion, toVersion, null);
    }
  }
}
