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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieFileGroup {

  @Test
  public void testCommittedFileSlices() {
    // "000" is archived
    Stream<String> completed = Arrays.asList("001").stream();
    Stream<String> inflight = Arrays.asList("002").stream();
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data",
        activeTimeline.getCommitsTimeline().filterCompletedInstants());
    for (int i = 0; i < 3; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_00" + i);
      fileGroup.addBaseFile(baseFile);
    }
    assertEquals(2, fileGroup.getAllFileSlices().count());
    assertTrue(!fileGroup.getAllFileSlices().anyMatch(s -> s.getBaseInstantTime().equals("002")));
    assertEquals(3, fileGroup.getAllFileSlicesIncludingInflight().count());
    assertTrue(fileGroup.getLatestFileSlice().get().getBaseInstantTime().equals("001"));
    assertTrue((new HoodieFileGroup(fileGroup)).getLatestFileSlice().get().getBaseInstantTime().equals("001"));
  }
}
