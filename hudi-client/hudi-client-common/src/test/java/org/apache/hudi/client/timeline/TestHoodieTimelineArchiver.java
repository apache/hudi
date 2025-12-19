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

package org.apache.hudi.client.timeline;

import org.apache.hudi.client.timeline.versioning.v1.TimelineArchiverV1;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieWriteableTestTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieTimelineArchiver extends HoodieCommonTestHarness {
  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieTimelineArchiver.class, "/exampleSchema.avsc", true);

  @BeforeEach
  void setUp() throws Exception {
    initPath();
    initMetaClient();
  }

  @Test
  void archiveIfRequired_instantsAreArchived() throws Exception {
    TypedProperties advanceProperties = new TypedProperties();
    advanceProperties.put(TimelineArchiverV1.ARCHIVE_LIMIT_INSTANTS, 1L);
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath(tempDir.toString())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 3)
            .build())
        .withMarkersType("DIRECT")
        .withProperties(advanceProperties)
        .build();
    HoodieEngineContext context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieStorage hoodieStorage = new HoodieHadoopStorage(basePath, metaClient.getStorageConf());
    HoodieWriteableTestTable testTable = new HoodieWriteableTestTable(basePath, hoodieStorage, metaClient, SCHEMA, null, null, Option.of(context));
    testTable.addCommit(InProcessTimeGenerator.createNewInstantTime());
    testTable.addCommit(InProcessTimeGenerator.createNewInstantTime());
    testTable.addCommit(InProcessTimeGenerator.createNewInstantTime());
    testTable.addCommit(InProcessTimeGenerator.createNewInstantTime());
    testTable.addCommit(InProcessTimeGenerator.createNewInstantTime());
    HoodieTable hoodieTable = setupMockHoodieTable(context, writeConfig);

    TimelineArchiverV1 archiver = new TimelineArchiverV1<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(4, metaClient.reloadActiveTimeline().countInstants());
    // Run archive again with reloaded metaClient results.
    hoodieTable = setupMockHoodieTable(context, writeConfig);
    archiver = new TimelineArchiverV1<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(3, metaClient.reloadActiveTimeline().countInstants());
    // Run archive again with reloaded metaClient results but it's no op as timeline instants is not above max limit of 3.
    hoodieTable = setupMockHoodieTable(context, writeConfig);
    archiver = new TimelineArchiverV1<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(3, metaClient.reloadActiveTimeline().countInstants());

    ActiveTimelineV1 rawActiveTimeline = new ActiveTimelineV1(metaClient, false);
    assertEquals(9, rawActiveTimeline.countInstants());
  }

  private HoodieTable setupMockHoodieTable(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    HoodieTable hoodieTable = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    when(hoodieTable.getContext()).thenReturn(context);
    when(hoodieTable.getConfig()).thenReturn(writeConfig);
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(hoodieTable.getActiveTimeline()).thenReturn(metaClient.getActiveTimeline());
    when(hoodieTable.getCompletedCommitsTimeline()).thenReturn(metaClient.getCommitsTimeline().filterCompletedInstants());
    when(hoodieTable.getCompletedCleanTimeline()).thenReturn(metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants());
    when(hoodieTable.getCompletedSavepointTimeline()).thenReturn(metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants());
    when(hoodieTable.getSavepointTimestamps()).thenReturn(Collections.emptySet());
    return hoodieTable;
  }
}
