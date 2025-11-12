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

import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.Assertions.assertStreamEquals;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_PARSER;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieInstant extends HoodieCommonTestHarness {

  @Test
  public void testExtractTimestamp() {
    String fileName = "20230104152218702.inflight";
    assertEquals("20230104152218702", INSTANT_FILE_NAME_PARSER.extractTimestamp(fileName));

    fileName = "20230104152218702.commit.request";
    assertEquals("20230104152218702", INSTANT_FILE_NAME_PARSER.extractTimestamp(fileName));

    fileName = "20230104152218702_20230104152219346.commit";
    assertEquals("20230104152218702", INSTANT_FILE_NAME_PARSER.extractTimestamp(fileName));

    String illegalFileName = "hoodie.properties";
    assertThrows(IllegalArgumentException.class, () -> INSTANT_FILE_NAME_PARSER.extractTimestamp(illegalFileName));
  }

  @Test
  public void testGetTimelineFileExtension() {
    String fileName = "20230104152218702.inflight";
    assertEquals(".inflight", INSTANT_FILE_NAME_PARSER.getTimelineFileExtension(fileName));

    fileName = "20230104152218702.commit.request";
    assertEquals(".commit.request", INSTANT_FILE_NAME_PARSER.getTimelineFileExtension(fileName));

    fileName = "20230104152218702_20230104152219346.commit";
    assertEquals(".commit", INSTANT_FILE_NAME_PARSER.getTimelineFileExtension(fileName));

    fileName = "hoodie.properties";
    assertEquals("", INSTANT_FILE_NAME_PARSER.getTimelineFileExtension(fileName));
  }

  @Test
  public void testCreateHoodieInstantByFileStatus() throws IOException {
    try {
      initMetaClient();
      HoodieInstant instantRequested =
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "001");
      HoodieInstant instantCommitted =
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
      timeline.createNewInstant(instantRequested);
      timeline.transitionRequestedToInflight(instantRequested, Option.empty());
      timeline.saveAsComplete(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instantRequested.getAction(), instantRequested.requestedTime()),
          Option.empty());
      metaClient.reloadActiveTimeline();
      timeline = metaClient.getActiveTimeline();
      assertEquals(1, timeline.countInstants());

      assertStreamEquals(Stream.of(instantCommitted),
          timeline.getInstantsAsStream(), "Instants in timeline is not matched");

      // Make sure StateTransitionTime is set in the timeline
      assertEquals(0,
          timeline.getInstantsAsStream().filter(s -> s.getCompletionTime().isEmpty()).count());
    } finally {
      cleanMetaClient();
    }
  }

  @Test
  public void testHoodieTimelineTimeZone() {
    for (HoodieTimelineTimeZone timeZone : HoodieTimelineTimeZone.values()) {
      assertNotNull(timeZone.getZoneId());
    }
  }
}
