/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.table.timeline.InstantComparison;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;

/** Computes the contiguous source-instant marker frontier for one vector generation. */
public final class VectorIndexFrontier {

  private VectorIndexFrontier() {
  }

  /**
   * Advances from the persisted frontier across the contiguous marked prefix of source instants.
   *
   * <p>The caller must provide the complete ordered source-write timeline after archival resolution.
   * Missing archived history must not be represented as an empty prefix because that would turn
   * absence of evidence into evidence of freshness. Instants at or before the persisted frontier are
   * ignored; the first unmarked later instant stops advancement.
   */
  public static String advance(
      String bootstrapInstant,
      String persistedFrontier,
      List<String> orderedSourceInstants,
      Set<String> markedInstants) {
    Objects.requireNonNull(bootstrapInstant, "bootstrapInstant");
    Objects.requireNonNull(orderedSourceInstants, "orderedSourceInstants");
    Objects.requireNonNull(markedInstants, "markedInstants");
    String frontier = persistedFrontier == null ? bootstrapInstant : persistedFrontier;
    validateOrdered(orderedSourceInstants);
    Set<String> markers = new HashSet<>(markedInstants);
    for (String instant : orderedSourceInstants) {
      if (!InstantComparison.compareTimestamps(instant, GREATER_THAN, frontier)) {
        continue;
      }
      if (!markers.contains(instant)) {
        break;
      }
      frontier = instant;
    }
    return frontier;
  }

  private static void validateOrdered(List<String> instants) {
    String previous = null;
    for (String instant : instants) {
      Objects.requireNonNull(instant, "source instant");
      if (previous != null && InstantComparison.compareTimestamps(previous, GREATER_THAN, instant)) {
        throw new IllegalArgumentException("Source instants must be ordered");
      }
      previous = instant;
    }
  }
}
