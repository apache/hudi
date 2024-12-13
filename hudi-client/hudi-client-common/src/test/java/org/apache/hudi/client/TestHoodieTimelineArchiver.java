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

package org.apache.hudi.client;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieWriteableTestTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.hudi.common.config.HoodieMetaserverConfig.METASERVER_ENABLE;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieTimelineArchiver extends HoodieCommonTestHarness {
  private static final Schema SCHEMA = getSchemaFromResource(TestHoodieTimelineArchiver.class, "/exampleSchema.avsc", true);

  @BeforeEach
  void setUp() throws Exception {
    initPath();
    initMetaClient();
  }

  @Test
  void archiveIfRequired_noInstantsToArchive() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath(tempDir.toString())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(1, 2)
            .build())
        .withMarkersType("DIRECT")
        .build();

    Configuration conf = new Configuration(false);
    HoodieEngineContext context = new HoodieLocalEngineContext(conf);
    FileSystem fs = FSUtils.getFs(new org.apache.hadoop.fs.Path(basePath), conf);
    HoodieWriteableTestTable testTable = new HoodieWriteableTestTable(basePath, fs, metaClient, SCHEMA, null, null, Option.of(context));

    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());

    HoodieTable hoodieTable = setupMockHoodieTable(context, writeConfig);

    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(1, metaClient.reloadActiveTimeline().countInstants());

    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    assertEquals(3, rawActiveTimeline.countInstants());
  }

  @Test
  void archiveIfRequired_instantsAreArchived() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath(tempDir.toString())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 3)
            .build())
        .withMarkersType("DIRECT")
        .build();

    Configuration conf = new Configuration(false);
    HoodieEngineContext context = new HoodieLocalEngineContext(conf);
    FileSystem fs = FSUtils.getFs(new org.apache.hadoop.fs.Path(basePath), conf);
    HoodieWriteableTestTable testTable = new HoodieWriteableTestTable(basePath, fs, metaClient, SCHEMA, null, null, Option.of(context));

    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());

    HoodieTable hoodieTable = setupMockHoodieTable(context, writeConfig);

    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(2, metaClient.reloadActiveTimeline().countInstants());

    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    assertEquals(6, rawActiveTimeline.countInstants());
  }

  @Test
  void archiveIfRequired_metaServerEnabled() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath(tempDir.toString())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 3)
            .build())
        .withMarkersType("DIRECT")
        .withProps(Collections.singletonMap(METASERVER_ENABLE.key(), "true"))
        .build();

    Configuration conf = new Configuration(false);
    HoodieEngineContext context = new HoodieLocalEngineContext(conf);
    FileSystem fs = FSUtils.getFs(new org.apache.hadoop.fs.Path(basePath), conf);
    HoodieWriteableTestTable testTable = new HoodieWriteableTestTable(basePath, fs, metaClient, SCHEMA, null, null, Option.of(context));

    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());
    testTable.addCommit(HoodieActiveTimeline.createNewInstantTime());

    HoodieTable hoodieTable = setupMockHoodieTable(context, writeConfig);

    // Archival will be skipped with the metaserver enabled so timeline will not be modified
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver<>(writeConfig, hoodieTable);
    archiver.archiveIfRequired(context);
    assertEquals(5, metaClient.reloadActiveTimeline().countInstants());

    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    assertEquals(15, rawActiveTimeline.countInstants());
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
