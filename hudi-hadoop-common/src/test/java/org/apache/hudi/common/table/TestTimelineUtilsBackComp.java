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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.TimelineUtils.handleHollowCommitIfNeeded;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TestTimelineUtilsBackComp extends HoodieCommonTestHarness {
  @BeforeEach
  void setUp() throws Exception {
    initPath();
  }

  @ParameterizedTest
  @CsvSource({
      "FAIL, SIX",
      "FAIL, EIGHT",
      "BLOCK, SIX",
      "BLOCK, EIGHT",
      "USE_TRANSITION_TIME, SIX",
      "USE_TRANSITION_TIME, EIGHT"
  })
  void testHandleHollowCommit(HollowCommitHandling handlingMode,
                             HoodieTableVersion tableVersion) throws Exception {
    Integer timelineLayoutVersion = tableVersion == HoodieTableVersion.SIX
        ? TimelineLayoutVersion.VERSION_1 
        : TimelineLayoutVersion.VERSION_2;
    
    metaClient =
      HoodieTableMetaClient.newTableBuilder()
        .setDatabaseName("dataset")
        .setTableName("testTable")
        .setTimelineLayoutVersion(timelineLayoutVersion)
        .setTableVersion(tableVersion)
        .setTableType(HoodieTableType.MERGE_ON_READ).initTable(getDefaultStorageConf(), basePath);
    HoodieTableVersion actualTableVersion = metaClient.getTableConfig().getTableVersion();
    assertEquals(actualTableVersion.getTimelineLayoutVersion().getVersion(), timelineLayoutVersion);
    assertEquals(actualTableVersion.versionCode(), tableVersion.versionCode());
    HoodieTestTable.of(metaClient)
        .addCommit("001")
        .addInflightCommit("003")
        .addCommit("005");
    verifyHollowCommitHandling(handlingMode);
  }

  private void verifyHollowCommitHandling(HollowCommitHandling handlingMode) {
    Stream<String> completed = Stream.of("001", "005");
    HoodieTimeline completedTimeline = new MockHoodieTimeline(completed, Stream.empty());
    switch (handlingMode) {
      case FAIL:
        HoodieException e = assertThrows(HoodieException.class, () ->
            handleHollowCommitIfNeeded(completedTimeline, metaClient, handlingMode));
        assertTrue(e.getMessage().startsWith("Found hollow commit:"));
        break;
      case BLOCK: {
        HoodieTimeline filteredTimeline = handleHollowCommitIfNeeded(completedTimeline, metaClient, handlingMode);
        assertTrue(filteredTimeline.containsInstant("001"));
        assertFalse(filteredTimeline.containsInstant("003"));
        assertFalse(filteredTimeline.containsInstant("005"));
        break;
      }
      case USE_TRANSITION_TIME: {
        HoodieTimeline filteredTimeline = handleHollowCommitIfNeeded(completedTimeline, metaClient, handlingMode);
        assertTrue(filteredTimeline.containsInstant("001"));
        assertFalse(filteredTimeline.containsInstant("003"));
        assertTrue(filteredTimeline.containsInstant("005"));
        break;
      }
      default:
        fail("should cover all handling mode.");
    }
  }
}
