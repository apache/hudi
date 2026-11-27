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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * What a split reads, used by the enumerator to report the read lag and the pending work.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class HoodieSourceSplitLagInfo {

  /**
   * Instant times of the commits whose records the split emits.
   */
  private final Collection<String> readCommits;

  /**
   * Completion time of the oldest of the read commits, null if unknown.
   */
  @Nullable
  private final String oldestCommitCompletionTime;

  /**
   * Expected number of records the split emits, 0 if unknown.
   */
  private final long estimatedRecords;

  /**
   * Size in bytes of the files the split reads, 0 if unknown.
   */
  private final long estimatedBytes;

  /**
   * Returns the lag info known from the split alone: it reads the instants of its exact-match range,
   * otherwise its latest commit, and the completion time and estimates are unknown.
   */
  public static HoodieSourceSplitLagInfo of(HoodieSourceSplit split) {
    return new HoodieSourceSplitLagInfo(getRangeCommits(split), null, 0L, 0L);
  }

  /**
   * Returns the lag info known from the splits alone, see {@link #of(HoodieSourceSplit)}, keyed by split id.
   */
  public static Map<String, HoodieSourceSplitLagInfo> ofSplits(Collection<HoodieSourceSplit> splits) {
    return splits.stream().collect(Collectors.toMap(HoodieSourceSplit::splitId, split -> of(split), (first, second) -> first));
  }

  /**
   * Returns the instants of the exact-match range of the split, otherwise its latest commit.
   */
  static Collection<String> getRangeCommits(HoodieSourceSplit split) {
    if (split.getInstantRange().isPresent() && split.getInstantRange().get() instanceof InstantRange.ExactMatchRange) {
      return ((InstantRange.ExactMatchRange) split.getInstantRange().get()).getInstants();
    }
    return Collections.singletonList(split.getLatestCommit());
  }
}
