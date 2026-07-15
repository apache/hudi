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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.util.Objects;

/**
 * The RLI finalist arbiter: resolves whether a vector-index posting still faithfully represents
 * a live record, using the record-level index as the table's global version authority.
 *
 * <p>This is the pure classification core of RFC-104 "Upsert and Delete Support". It has no
 * Spark or metadata-table dependency: callers resolve the current RLI location for a finalist
 * key (a batched {@code readRecordIndexLocationsWithKeys} in the plan builder), then call
 * {@link #classify} with the posting locator and that current location. The action taken per
 * verdict is mode-specific and lives in the caller:
 *
 * <ul>
 *   <li>Approximate mode: {@code SERVE} -> keep, {@code STALE} -> exclude, {@code DELETED} -> exclude.</li>
 *   <li>Exact mode: {@code SERVE} -> positional fetch, {@code STALE} -> key-based fallback fetch,
 *       {@code DELETED} -> exclude.</li>
 * </ul>
 *
 * <p>The classification itself is identical across modes so both report the same semantics and
 * the same {@link ExclusionCounts} ({@code arbiterExclusions.stale} / {@code .deleted}).
 */
public final class VectorIndexArbiter {

  /**
   * Verdict for a single finalist posting.
   *
   * <ul>
   *   <li>{@code SERVE}: RLI hit and the current location matches the posting locator. The posting
   *       faithfully represents the live record; positional trust is preserved (subject to the
   *       positional validity gate for {@code rowPosition >= 0}).</li>
   *   <li>{@code STALE}: RLI hit but the current location differs. The record was rewritten or
   *       moved; this posting's locator (and, for updates, its code) is no longer authoritative.</li>
   *   <li>{@code DELETED}: RLI miss. The record no longer exists in the table.</li>
   * </ul>
   */
  public enum Decision {
    SERVE,
    STALE,
    DELETED
  }

  private VectorIndexArbiter() {
  }

  /**
   * Classify a single finalist posting against the record's current RLI location.
   *
   * @param postingPartitionPath  partition path stored in the posting locator (may be null)
   * @param postingFileGroupId    file group id stored in the posting locator (may be null)
   * @param postingBaseInstantTime base instant time stored in the posting locator (may be null)
   * @param currentLocation       current RLI location for the record key, or {@code null} for an
   *                              RLI miss (deleted)
   * @return the arbiter verdict
   */
  public static Decision classify(String postingPartitionPath,
                                  String postingFileGroupId,
                                  String postingBaseInstantTime,
                                  HoodieRecordGlobalLocation currentLocation) {
    if (currentLocation == null) {
      return Decision.DELETED;
    }
    boolean matches = Objects.equals(postingPartitionPath, currentLocation.getPartitionPath())
        && Objects.equals(postingFileGroupId, currentLocation.getFileId())
        && Objects.equals(postingBaseInstantTime, currentLocation.getInstantTime());
    return matches ? Decision.SERVE : Decision.STALE;
  }

  /**
   * Mutable tally of arbiter exclusions, mirrored into both query modes' log lines as the
   * {@code arbiterExclusions} freshness observability metric. Split into {@code stale} and
   * {@code deleted} so the two upsert/delete effects are separable in dashboards.
   */
  public static final class ExclusionCounts {
    private long stale;
    private long deleted;

    /** Record a verdict, incrementing the matching counter. {@code SERVE} is a no-op. */
    public void record(Decision decision) {
      switch (decision) {
        case STALE:
          stale++;
          break;
        case DELETED:
          deleted++;
          break;
        default:
          break;
      }
    }

    public long stale() {
      return stale;
    }

    public long deleted() {
      return deleted;
    }

    public long total() {
      return stale + deleted;
    }

    @Override
    public String toString() {
      return "arbiterExclusions{stale=" + stale + ", deleted=" + deleted + "}";
    }
  }
}
