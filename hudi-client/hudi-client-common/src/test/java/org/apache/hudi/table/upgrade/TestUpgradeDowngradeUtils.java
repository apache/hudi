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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestUpgradeDowngradeUtils {

  @Mock
  private HoodieTable table;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieEngineContext context;
  @Mock
  private HoodieWriteConfig config;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(table.getMetaClient()).thenReturn(metaClient);
  }

  @Test
  void testRunCompaction() {
    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    when(config.getProps()).thenReturn(new TypedProperties());
    when(config.isMetadataTableEnabled()).thenReturn(true);

    SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    when(upgradeDowngradeHelper.getWriteClient(any(), any())).thenReturn(writeClient);
    when(writeClient.scheduleCompaction(Option.empty())).thenReturn(Option.of("compactionInstant"));

    UpgradeDowngradeUtils.runCompaction(table, context, config, upgradeDowngradeHelper);

    verify(writeClient).compact("compactionInstant");
  }

  @Test
  void testSyncCompactionRequestedFileToAuxiliaryFolder() throws IOException {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, CLUSTERING_ACTION, "20211012123000");
    HoodieTimeline compactionTimeline = mock(HoodieTimeline.class);
    when(compactionTimeline.getInstantsAsStream()).thenReturn(Collections.singletonList(instant).stream());
    when(metaClient.getStorage().exists(any())).thenReturn(false);

    new UpgradeDowngradeUtils().syncCompactionRequestedFileToAuxiliaryFolder(table);

    ArgumentCaptor<StoragePath> srcPathCaptor = ArgumentCaptor.forClass(StoragePath.class);
    ArgumentCaptor<StoragePath> destPathCaptor = ArgumentCaptor.forClass(StoragePath.class);
    verify(metaClient.getStorage()).rename(srcPathCaptor.capture(), destPathCaptor.capture());
    assertEquals("20211012123000.cluster.requested", srcPathCaptor.getValue().getName());
    assertEquals("20211012123000.cluster.requested", destPathCaptor.getValue().getName());
  }

  @Test
  void testUpdateMetadataTableVersion() throws IOException {
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/base/path"));
    when(metaClient.getStorage().exists(any())).thenReturn(true);

    HoodieTableVersion newVersion = HoodieTableVersion.SIX;
    UpgradeDowngradeUtils.updateMetadataTableVersion(context, newVersion, metaClient);

    verify(metaClient.getTableConfig()).setTableVersion(newVersion);
  }

  @Test
  void testUpgradeToLSMTimeline() {
    HoodieArchivedTimeline archivedTimeline = mock(HoodieArchivedTimeline.class);
    when(metaClient.getArchivedTimeline()).thenReturn(archivedTimeline);

    LSMTimelineWriter lsmTimelineWriter = mock(LSMTimelineWriter.class);
    try (MockedStatic<LSMTimelineWriter> mockedLSMTimelineWriter = mockStatic(LSMTimelineWriter.class)) {
      mockedLSMTimelineWriter.when(() -> LSMTimelineWriter.getInstance(config, table)).thenReturn(lsmTimelineWriter);
      UpgradeDowngradeUtils.upgradeToLSMTimeline(table, context, config);
      verify(lsmTimelineWriter).write(any(), any(), any());
    }
  }

  @Test
  void testDowngradeFromLSMTimeline() {
    HoodieArchivedTimeline archivedTimeline = mock(HoodieArchivedTimeline.class);
    when(metaClient.getArchivedTimeline()).thenReturn(archivedTimeline);

    UpgradeDowngradeUtils.downgradeFromLSMTimeline(table, context, config);

    // Add further verifications once the TODO for downgrade logic is implemented
  }

  @Test
  void testDowngradeActiveTimelineInstant() throws IOException {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, CLUSTERING_ACTION, "20211012123000");
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    String fileName = instantFileNameGenerator.getFileName(instant);
    CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
    CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
    ActiveTimelineV1 activeTimelineV1 = new ActiveTimelineV1(metaClient);
    UpgradeDowngradeUtils.downgradeActiveTimelineInstant(instant, metaClient, fileName, commitMetadataSerDeV2, commitMetadataSerDeV1, activeTimelineV1);
    verify(metaClient.getStorage()).rename(any(), any());
  }

  @Test
  void testUpgradeActiveTimelineInstant() throws IOException {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "20211012123000");
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    String fileName = instantFileNameGenerator.getFileName(instant);
    CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
    CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
    ActiveTimelineV1 activeTimelineV1 = new ActiveTimelineV1(metaClient);
    UpgradeDowngradeUtils.upgradeActiveTimelineInstant(instant, metaClient, fileName, commitMetadataSerDeV2, commitMetadataSerDeV1, activeTimelineV1);
    verify(metaClient.getStorage()).rename(any(), any());
  }
}
