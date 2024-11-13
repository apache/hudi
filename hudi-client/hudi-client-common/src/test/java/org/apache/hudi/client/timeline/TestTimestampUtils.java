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

package org.apache.hudi.client.timeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.client.timeline.TimestampUtils.validateForLatestTimestamp;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTimestampUtils extends HoodieCommonTestHarness {
  @Test
  void testValidateForLatestTimestamp() throws IOException {
    initPath();
    initMetaClient();

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);

    HoodieInstant lastEntry = metaClient.getActiveTimeline().getWriteTimeline().lastInstant().get();
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validateForLatestTimestamp(metaClient, "001"));
    assertEquals(String.format("Found later commit time %s, compared to the current instant 001, hence failing to create requested commit meta file", lastEntry), exception.getMessage());

    assertDoesNotThrow(() -> validateForLatestTimestamp(metaClient, "003"));
  }

  @Test
  void testValidateForLatestTimestampMdt() throws IOException {
    java.nio.file.Path basePath = tempDir.resolve("test");
    java.nio.file.Files.createDirectories(basePath);
    HoodieTableMetaClient mdtMetaClient = HoodieTestUtils.init(basePath + "/.hoodie/metadata");

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(mdtMetaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);
    assertDoesNotThrow(() -> validateForLatestTimestamp(mdtMetaClient, "001"));
  }
}
