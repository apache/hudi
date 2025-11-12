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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.versioning.v1.TimelinePathProviderV1;
import org.apache.hudi.common.table.timeline.versioning.v2.TimelinePathProviderV2;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTimelinePathProvider extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testTimelinePathProviderV1() {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    StoragePath basePath = new StoragePath("file:/tmp");
    TimelinePathProviderV1 timelinePathProviderV1 = new TimelinePathProviderV1();
    StoragePath timelinePath = timelinePathProviderV1.getTimelinePath(tableConfig, basePath);
    assertEquals("file:/tmp/.hoodie", timelinePath.toString());
    StoragePath timelineHistoryPath = timelinePathProviderV1.getTimelineHistoryPath(tableConfig, basePath);
    assertEquals("file:/tmp/.hoodie/archived", timelineHistoryPath.toString());
  }

  @Test
  public void testTimelinePathProviderV2() {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    StoragePath basePath = new StoragePath("file:/tmp");
    TimelinePathProviderV2 timelinePathProviderV2 = new TimelinePathProviderV2();
    StoragePath timelinePath = timelinePathProviderV2.getTimelinePath(tableConfig, basePath);
    assertEquals("file:/tmp/.hoodie/timeline", timelinePath.toString());
    StoragePath timelineHistoryPath = timelinePathProviderV2.getTimelineHistoryPath(tableConfig, basePath);
    assertEquals("file:/tmp/.hoodie/timeline/history", timelineHistoryPath.toString());
  }
}
