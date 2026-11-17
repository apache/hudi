/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.split;

import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link HoodieSourceSplitLagInfo}.
 */
public class TestHoodieSourceSplitLagInfo {

  @Test
  void testSplitWithExactMatchRangeReadsTheInstantsOfTheRange() {
    HoodieSourceSplit split = createSplit(Option.of(InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(new HashSet<>(Arrays.asList("20231201000000000", "20231202000000000")))
        .build()));

    HoodieSourceSplitLagInfo lagInfo = HoodieSourceSplitLagInfo.of(split);

    assertEquals(new HashSet<>(Arrays.asList("20231201000000000", "20231202000000000")), new HashSet<>(lagInfo.getReadCommits()));
  }

  @Test
  void testSplitWithoutExactMatchRangeReadsItsLatestCommit() {
    HoodieSourceSplit split = createSplit(Option.empty());

    HoodieSourceSplitLagInfo lagInfo = HoodieSourceSplitLagInfo.of(split);

    assertEquals(new HoodieSourceSplitLagInfo(Collections.singletonList("20231202000000000"), null, 0L, 0L), lagInfo);
  }

  private static HoodieSourceSplit createSplit(Option<InstantRange> instantRange) {
    return new HoodieSourceSplit(
        1, "base-file", Option.empty(), "/table", "par1", "read_optimized", "20231202000000000", "file-1", instantRange);
  }
}
