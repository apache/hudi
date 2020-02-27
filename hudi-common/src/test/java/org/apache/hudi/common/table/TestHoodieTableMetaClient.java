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

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests hoodie table meta client {@link HoodieTableMetaClient}.
 */
public class TestHoodieTableMetaClient extends HoodieCommonTestHarness {

  @Before
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void checkMetadata() {
    assertEquals("Table name should be raw_trips", HoodieTestUtils.RAW_TRIPS_TEST_NAME,
        metaClient.getTableConfig().getTableName());
    assertEquals("Basepath should be the one assigned", basePath, metaClient.getBasePath());
    assertEquals("Metapath should be ${basepath}/.hoodie", basePath + "/.hoodie", metaClient.getMetaPath());
  }

  @Test
  public void checkSerDe() {
    // check if this object is serialized and de-serialized, we are able to read from the file system
    HoodieTableMetaClient deseralizedMetaClient =
        HoodieTestUtils.serializeDeserialize(metaClient, HoodieTableMetaClient.class);
    assertNotNull(deseralizedMetaClient);
    HoodieActiveTimeline commitTimeline = deseralizedMetaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    commitTimeline.createNewInstant(instant);
    commitTimeline.saveAsComplete(instant, Option.of("test-detail".getBytes()));
    commitTimeline = commitTimeline.reload();
    HoodieInstant completedInstant = HoodieTimeline.getCompletedInstant(instant);
    assertEquals("Commit should be 1 and completed", completedInstant, commitTimeline.getInstants().findFirst().get());
    assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
        commitTimeline.getInstantDetails(completedInstant).get());
  }

  @Test
  public void checkCommitTimeline() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue("Should be empty commit timeline", activeCommitTimeline.empty());

    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of("test-detail".getBytes()));

    // Commit timeline should not auto-reload every time getActiveCommitTimeline(), it should be cached
    activeTimeline = metaClient.getActiveTimeline();
    activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue("Should be empty commit timeline", activeCommitTimeline.empty());

    HoodieInstant completedInstant = HoodieTimeline.getCompletedInstant(instant);
    activeTimeline = activeTimeline.reload();
    activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertFalse("Should be the 1 commit we made", activeCommitTimeline.empty());
    assertEquals("Commit should be 1", completedInstant, activeCommitTimeline.getInstants().findFirst().get());
    assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
        activeCommitTimeline.getInstantDetails(completedInstant).get());
  }
}
