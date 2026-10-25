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

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.DirectWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestLegacyUpgradeDowngradeHandlers {

  @Test
  void testZeroToOneRecreatesMarkersAndSkipsCurrentInstant() {
    HoodieTable table = mockTableWithPendingInstants("001", "002");
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    doReturn(getDefaultStorageConf()).when(context).getStorageConf();
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(config, context)).thenReturn(table);
    when(config.getMarkersDeleteParallelism()).thenReturn(3);

    ZeroToOneUpgradeHandler handler = spy(new ZeroToOneUpgradeHandler());
    doNothing().when(handler).recreateMarkers(anyString(), eq(table), eq(context), anyInt());

    handler.upgrade(config, context, "002", helper);

    verify(handler).recreateMarkers("001", table, context, 3);
    verify(handler, never()).recreateMarkers("002", table, context, 3);
  }

  @Test
  void testZeroToOneCreatesBaseAndLogMarkers() {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001");
    HoodieTable table = mock(HoodieTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));
    when(table.getBaseFileExtension()).thenReturn(".parquet");
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));

    StoragePathInfo logFile = new StoragePathInfo(
        new StoragePath("partition/.file-id_001.log.1_1-0-1"), 1L, false, (short) 1, 1L, 1L);
    HoodieRollbackStat rollbackStat = HoodieRollbackStat.newBuilder()
        .withPartitionPath("partition")
        .withDeletedFileResult("/table/partition/file.parquet", true)
        .withRollbackBlockAppendResults(Collections.singletonMap(logFile, 1L))
        .build();
    ZeroToOneUpgradeHandler handler = new ZeroToOneUpgradeHandler() {
      @Override
      List<HoodieRollbackStat> getListBasedRollBackStats(
          HoodieTable<?, ?, ?, ?> ignoredTable, HoodieEngineContext ignoredContext, Option<HoodieInstant> ignoredInstant) {
        return Collections.singletonList(rollbackStat);
      }
    };

    WriteMarkers markers = mock(WriteMarkers.class);
    try (MockedStatic<WriteMarkersFactory> markerFactory = mockStatic(WriteMarkersFactory.class)) {
      markerFactory.when(() -> WriteMarkersFactory.get(MarkerType.DIRECT, table, "001")).thenReturn(markers);

      handler.recreateMarkers("001", table, mock(HoodieEngineContext.class), 2);

      verify(markers).quietDeleteMarkerDir(any(HoodieEngineContext.class), eq(2));
      verify(markers).create("partition", "file.parquet", IOType.MERGE);
      verify(markers).create("partition", "file-id_1-0-1_001.parquet", IOType.APPEND);
    }
  }

  @Test
  void testZeroToOneIgnoresMissingInstantAndWrapsFailures() {
    HoodieTable table = mock(HoodieTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitsTimeline()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenReturn(Stream.empty());
    assertDoesNotThrow(() -> new ZeroToOneUpgradeHandler().recreateMarkers("001", table, mock(HoodieEngineContext.class), 1));

    when(timeline.getInstantsAsStream()).thenThrow(new RuntimeException("timeline failure"));
    assertThrows(HoodieRollbackException.class,
        () -> new ZeroToOneUpgradeHandler().recreateMarkers("001", table, mock(HoodieEngineContext.class), 1));
  }

  @Test
  void testTwoToOneConvertsTimelineServerMarkersToDirectMarkers() throws Exception {
    HoodieTable table = mockTableWithPendingInstants("001");
    HoodieTableMetaClient metaClient = table.getMetaClient();
    when(metaClient.getMarkerFolderPath("001")).thenReturn("/table/.hoodie/.temp/001");
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    doReturn(getDefaultStorageConf()).when(context).getStorageConf();
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getMarkersDeleteParallelism()).thenReturn(2);
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(config, context)).thenReturn(table);
    HoodieStorage storage = mock(HoodieStorage.class);
    Map<String, Set<String>> markerMap = Collections.singletonMap("MARKERS0",
        Set.of("partition/file.parquet.marker.CREATE", "partition/file2.parquet.marker.MERGE"));

    try (MockedStatic<HoodieStorageUtils> storageUtils = mockStatic(HoodieStorageUtils.class);
         MockedStatic<MarkerUtils> markerUtils = mockStatic(MarkerUtils.class);
         MockedStatic<org.apache.hudi.common.fs.FSUtils> fsUtils = mockStatic(org.apache.hudi.common.fs.FSUtils.class);
         MockedConstruction<DirectWriteMarkers> directMarkers = mockConstruction(DirectWriteMarkers.class)) {
      storageUtils.when(() -> HoodieStorageUtils.getStorage(eq("/table/.hoodie/.temp/001"), any())).thenReturn(storage);
      markerUtils.when(() -> MarkerUtils.readMarkerType(storage, "/table/.hoodie/.temp/001"))
          .thenReturn(Option.of(MarkerType.TIMELINE_SERVER_BASED));
      markerUtils.when(() -> MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
          "/table/.hoodie/.temp/001", storage, context, 2)).thenReturn(markerMap);

      new TwoToOneDowngradeHandler().downgrade(config, context, null, helper);

      DirectWriteMarkers direct = directMarkers.constructed().get(0);
      verify(direct).create("partition/file.parquet.marker.CREATE");
      verify(direct).create("partition/file2.parquet.marker.MERGE");
      markerUtils.verify(() -> MarkerUtils.deleteMarkerTypeFile(storage, "/table/.hoodie/.temp/001"));
      fsUtils.verify(() -> org.apache.hudi.common.fs.FSUtils.parallelizeSubPathProcess(
          eq(context), eq(storage), eq(new StoragePath("/table/.hoodie/.temp/001")), eq(2), any(), any()));
    }
  }

  @Test
  void testTwoToOneCleansPartialMarkersAndRejectsUnsupportedMarkerType() throws Exception {
    HoodieTable table = mockTableWithPendingInstants("001");
    when(table.getMetaClient().getMarkerFolderPath("001")).thenReturn("/markers/001");
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    doReturn(getDefaultStorageConf()).when(context).getStorageConf();
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getMarkersDeleteParallelism()).thenReturn(1);
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(config, context)).thenReturn(table);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.exists(new StoragePath("/markers/001"))).thenReturn(true);

    try (MockedStatic<HoodieStorageUtils> storageUtils = mockStatic(HoodieStorageUtils.class);
         MockedStatic<MarkerUtils> markerUtils = mockStatic(MarkerUtils.class);
         MockedStatic<org.apache.hudi.common.fs.FSUtils> fsUtils = mockStatic(org.apache.hudi.common.fs.FSUtils.class)) {
      storageUtils.when(() -> HoodieStorageUtils.getStorage(eq("/markers/001"), any())).thenReturn(storage);
      markerUtils.when(() -> MarkerUtils.readMarkerType(storage, "/markers/001")).thenReturn(Option.empty());

      new TwoToOneDowngradeHandler().downgrade(config, context, null, helper);
      fsUtils.verify(() -> org.apache.hudi.common.fs.FSUtils.parallelizeSubPathProcess(
          eq(context), eq(storage), eq(new StoragePath("/markers/001")), eq(1), any(), any()));

      markerUtils.when(() -> MarkerUtils.readMarkerType(storage, "/markers/001")).thenReturn(Option.of(MarkerType.DIRECT));
      assertThrows(HoodieException.class,
          () -> new TwoToOneDowngradeHandler().downgrade(config, context, null, helper));
    }
  }

  @Test
  void testFourToFiveValidatesDefaultPartitionLayouts() throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getStorage()).thenReturn(storage);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(tableConfig.getHiveStylePartitioningEnable()).thenReturn("false");
    when(storage.exists(new StoragePath("/table/" + DEPRECATED_DEFAULT_PARTITION_PATH))).thenReturn(true);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getBasePath()).thenReturn("/table");
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    when(helper.getTable(config, context)).thenReturn(table);

    assertThrows(HoodieException.class,
        () -> new FourToFiveUpgradeHandler().upgrade(config, context, null, helper));

    when(tableConfig.getHiveStylePartitioningEnable()).thenReturn("true");
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[] {"dt", "hh"}));
    when(storage.exists(new StoragePath("/table/dt=" + DEPRECATED_DEFAULT_PARTITION_PATH))).thenReturn(false);
    assertDoesNotThrow(() -> new FourToFiveUpgradeHandler().upgrade(config, context, null, helper));

    when(config.doSkipDefaultPartitionValidation()).thenReturn(true);
    assertDoesNotThrow(() -> new FourToFiveUpgradeHandler().upgrade(config, context, null, helper));
  }

  @Test
  void testFourToFiveHandlesNonPartitionedTableAndStorageFailure() throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getStorage()).thenReturn(storage);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getBasePath()).thenReturn("/table");
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    when(helper.getTable(config, context)).thenReturn(table);

    when(tableConfig.isTablePartitioned()).thenReturn(false);
    assertDoesNotThrow(() -> new FourToFiveUpgradeHandler().upgrade(config, context, null, helper));

    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(tableConfig.getHiveStylePartitioningEnable()).thenReturn("false");
    when(storage.exists(any(StoragePath.class))).thenThrow(new IOException("storage failure"));
    assertThrows(HoodieException.class,
        () -> new FourToFiveUpgradeHandler().upgrade(config, context, null, helper));
  }

  @Test
  void testFiveToSixDeletesRequestedCompactionFromAuxiliaryFolder() throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline compactionTimeline = mock(HoodieTimeline.class);
    InstantFileNameGenerator fileNameGenerator = mock(InstantFileNameGenerator.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieInstant requested = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "001");
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterPendingCompactionTimeline()).thenReturn(compactionTimeline);
    when(compactionTimeline.filter(any())).thenReturn(compactionTimeline);
    when(compactionTimeline.getInstantsAsStream()).thenReturn(Stream.of(requested));
    when(metaClient.getInstantFileNameGenerator()).thenReturn(fileNameGenerator);
    when(fileNameGenerator.getFileName(requested)).thenReturn("001.compaction.requested");
    when(metaClient.getMetaAuxiliaryPath()).thenReturn("/table/.hoodie/.aux");
    when(metaClient.getStorage()).thenReturn(storage);
    StoragePath auxFile = new StoragePath("/table/.hoodie/.aux/001.compaction.requested");
    when(storage.exists(auxFile)).thenReturn(true);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(config, context)).thenReturn(table);

    new FiveToSixUpgradeHandler().upgrade(config, context, null, helper);
    verify(storage).deleteFile(auxFile);

    when(compactionTimeline.getInstantsAsStream()).thenReturn(Stream.empty());
    assertDoesNotThrow(() -> new FiveToSixUpgradeHandler().upgrade(config, context, null, helper));
  }

  @Test
  void testFiveToSixWrapsAuxiliaryFolderDeleteFailure() throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline compactionTimeline = mock(HoodieTimeline.class);
    InstantFileNameGenerator fileNameGenerator = mock(InstantFileNameGenerator.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieInstant requested = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "001");
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterPendingCompactionTimeline()).thenReturn(compactionTimeline);
    when(compactionTimeline.filter(any())).thenReturn(compactionTimeline);
    when(compactionTimeline.getInstantsAsStream()).thenReturn(Stream.of(requested));
    when(metaClient.getInstantFileNameGenerator()).thenReturn(fileNameGenerator);
    when(fileNameGenerator.getFileName(requested)).thenReturn("001.compaction.requested");
    when(metaClient.getMetaAuxiliaryPath()).thenReturn("/table/.hoodie/.aux");
    when(metaClient.getStorage()).thenReturn(storage);
    StoragePath auxFile = new StoragePath("/table/.hoodie/.aux/001.compaction.requested");
    when(storage.exists(auxFile)).thenThrow(new IOException("failure"));
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(config, context)).thenReturn(table);

    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> new FiveToSixUpgradeHandler().upgrade(config, context, null, helper));
  }

  @SuppressWarnings("unchecked")
  private HoodieTable mockTableWithPendingInstants(String... instantTimes) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline pendingTimeline = mock(HoodieTimeline.class);
    List<HoodieInstant> instants = Arrays.stream(instantTimes)
        .map(t -> INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, t))
        .collect(Collectors.toList());
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterPendingExcludingCompactionAndLogCompaction()).thenReturn(pendingTimeline);
    when(pendingTimeline.getReverseOrderedInstants()).thenAnswer(ignored -> instants.stream());
    return table;
  }
}
