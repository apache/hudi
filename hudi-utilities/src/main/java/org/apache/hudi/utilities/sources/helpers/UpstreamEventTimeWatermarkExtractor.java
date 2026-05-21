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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates per-partition min/max event time across a set of upstream Hudi commits, using the
 * per-partition rollup exposed on {@link HoodieCommitMetadata#getMinAndMaxEventTimePerPartition()}.
 *
 * <p>Used by Hudi sources (e.g. {@code HoodieIncrSource}) to propagate upstream freshness into the
 * downstream commit's write stats when
 * {@code hoodie.write.track.event.time.propagate.from.upstream} is enabled.
 *
 * <p>Fold semantics per partition:
 * <ul>
 *   <li>min event time: {@code Math.min} across all upstream commits' per-partition mins</li>
 *   <li>max event time: {@code Math.max} across all upstream commits' per-partition maxes</li>
 * </ul>
 *
 * <p>Commits whose per-partition rollup is empty (upstream did not track watermark, or carried no
 * event-time-bearing write stats) contribute nothing. A partition with no signal in any commit is
 * omitted from the returned map.
 */
@Slf4j
public final class UpstreamEventTimeWatermarkExtractor {

  private UpstreamEventTimeWatermarkExtractor() {
  }

  /**
   * Folds per-partition min/max event time across the supplied commits.
   *
   * @param timeline       active commits timeline of the upstream table
   * @param commitInstants instants to fold (must be {@code COMMIT} or {@code DELTA_COMMIT} actions
   *                       that are present on {@code timeline}); pass an empty collection to get
   *                       back an empty map
   * @return partition path → (min, max) event time in millis. Partitions with no event-time signal
   *         across the supplied commits are omitted.
   */
  public static Map<String, Pair<Long, Long>> extractPerPartitionWatermarks(
      HoodieTimeline timeline,
      Collection<HoodieInstant> commitInstants) {
    if (commitInstants == null || commitInstants.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Pair<Long, Long>> result = new HashMap<>();
    for (HoodieInstant instant : commitInstants) {
      HoodieCommitMetadata commitMetadata;
      try {
        commitMetadata = timeline.readCommitMetadata(instant);
      } catch (Exception e) {
        // Don't fail the source if a single commit's metadata can't be read; propagation is
        // best-effort observability.
        log.warn("Skipping upstream commit {} while extracting per-partition event-time watermarks: {}",
            instant, e.getMessage());
        continue;
      }
      Map<String, Pair<Option<Long>, Option<Long>>> perPartition =
          commitMetadata.getMinAndMaxEventTimePerPartition();
      for (Map.Entry<String, Pair<Option<Long>, Option<Long>>> entry : perPartition.entrySet()) {
        Option<Long> upstreamMin = entry.getValue().getLeft();
        Option<Long> upstreamMax = entry.getValue().getRight();
        if (!upstreamMin.isPresent() && !upstreamMax.isPresent()) {
          continue;
        }
        result.compute(entry.getKey(), (partition, existing) -> fold(existing, upstreamMin, upstreamMax));
      }
    }
    return result;
  }

  private static Pair<Long, Long> fold(Pair<Long, Long> existing, Option<Long> upstreamMin, Option<Long> upstreamMax) {
    Long existingMin = existing == null ? null : existing.getLeft();
    Long existingMax = existing == null ? null : existing.getRight();
    Long newMin = foldMin(existingMin, upstreamMin.orElse(null));
    Long newMax = foldMax(existingMax, upstreamMax.orElse(null));
    return Pair.of(newMin, newMax);
  }

  private static Long foldMin(Long a, Long b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return Math.min(a, b);
  }

  private static Long foldMax(Long a, Long b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return Math.max(a, b);
  }
}
