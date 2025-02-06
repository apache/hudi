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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.convertCompletionTimeToEpoch;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUpgradeDowngradeUtils {

  @Test
  void testConvertCompletionTimeToEpoch() {
    // Mock a HoodieInstant with a completion time
    String completionTime = "20241112153045678"; // yyyyMMddHHmmssSSS
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "20231112153045678", completionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
    //when(instant.getCompletionTime()).thenReturn(completionTime);

    // Expected epoch time
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LocalDateTime dateTime = LocalDateTime.parse(completionTime.substring(0, completionTime.length() - 3), formatter);
    long expectedEpoch = dateTime.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000
        + Long.parseLong(completionTime.substring(completionTime.length() - 3));

    assertEquals(expectedEpoch, convertCompletionTimeToEpoch(instant), "Epoch time does not match the expected value.");

    // HoodieInstant with an invalid completion time
    String invalidCompletionTime = "12345";
    HoodieInstant inValidInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, "dummy_action", "20231112153045678", invalidCompletionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);

    assertEquals(-1, convertCompletionTimeToEpoch(inValidInstant), "Epoch time for invalid input should be -1.");
  }
}
