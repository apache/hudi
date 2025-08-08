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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.MILLIS_INSTANT_TIME_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieInstantTimeGenerator {

  @Test
  void testCreateNewInstantTimes() {
    List<String> instantTimesSoFar = new ArrayList<>();
    // explicitly set timezone to UTC and generate timestamps
    HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.UTC);

    TimeGenerator timeGenerator = mock(TimeGenerator.class);
    when(timeGenerator.generateTime()).thenAnswer(invocation -> System.currentTimeMillis());
    // run for few iterations
    for (int j = 0; j < 5; j++) {
      instantTimesSoFar.clear();
      // Generate an instant time in UTC and validate that all instants generated are within few seconds apart.
      String newCommitTimeInUTC = getNewInstantTimeInUTC();

      // new instant that we generate below should be within few seconds apart compared to above time we generated. If not, the time zone is not honored
      for (int i = 0; i < 10; i++) {
        String newInstantTime = HoodieInstantTimeGenerator.createNewInstantTime(timeGenerator, 0);
        assertTrue(!instantTimesSoFar.contains(newInstantTime));
        instantTimesSoFar.add(newInstantTime);
        assertTrue((Long.parseLong(newInstantTime) - Long.parseLong(newCommitTimeInUTC)) < 60000L,
            String.format("Validation failed on new instant time created: %s, newCommitTimeInUTC=%s",
                newInstantTime, newCommitTimeInUTC));
      }
    }
    // reset timezone to local
    HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.LOCAL);
  }

  @Test
  void testFormatDate() throws Exception {
    // Multiple thread test
    final int numChecks = 100000;
    final int numThreads = 100;
    final long milliSecondsInYear = 365 * 24 * 3600 * 1000;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future> futures = new ArrayList<>(numThreads);
    for (int idx = 0; idx < numThreads; ++idx) {
      futures.add(executorService.submit(() -> {
        Date date = new Date(System.currentTimeMillis() + (int) (Math.random() * numThreads) * milliSecondsInYear);
        final String expectedFormat = TimelineUtils.formatDate(date);
        for (int tidx = 0; tidx < numChecks; ++tidx) {
          final String curFormat = TimelineUtils.formatDate(date);
          assertEquals(expectedFormat, curFormat);
        }
      }));
    }

    executorService.shutdown();
    assertTrue(executorService.awaitTermination(60, TimeUnit.SECONDS));
    // required to catch exceptions
    for (Future f : futures) {
      f.get();
    }
  }

  private String getNewInstantTimeInUTC() {
    Date d = new Date(System.currentTimeMillis());
    return d.toInstant().atZone(HoodieTimelineTimeZone.UTC.getZoneId())
        .toLocalDateTime().format(MILLIS_INSTANT_TIME_FORMATTER);
  }
}
