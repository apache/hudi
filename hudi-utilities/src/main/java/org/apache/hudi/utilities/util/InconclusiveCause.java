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

package org.apache.hudi.utilities.util;

/**
 * Why a v2 run reached no verdict.
 *
 * <p>Not a finding and not a failure: each of these says the run could not be performed against an
 * account of the window it describes. Both modes answer that the same two ways — fail-fast sanctions
 * and reports success, exhaustive refuses — so the branch that decides is shared, and what a cause
 * supplies is only how the outcome is *named* to a caller.
 *
 * <p><b>The marker is an alerting contract.</b> Each cause has its own, because alerting may count
 * consecutive markers to tell a busy table from a broken one, and a run sanctioned for one reason
 * must never be counted against the other's threshold.
 */
public enum InconclusiveCause {

  /**
   * The archiver moved the account of the window mid-run; see {@link ArchivalProbe}. The message
   * names the FS-MDT comparison as the dimension that reached no verdict, because the
   * duplicate-base-file dimension had already concluded and its findings are still published.
   */
  ARCHIVAL_RAN(
      "MDT_VALIDATOR_ARCHIVAL_SKIPPED",
      "Archival ran while validating ",
      ". The timeline delta the FS-MDT comparison would adjudicate against is therefore missing "
          + "instants that left the active timeline, so that dimension reached no verdict and no "
          + "findings were written for it",
      Branch.MTV_ARCHIVAL_EXHAUSTIVE_FAILED,
      Branch.MTV_ARCHIVAL_FAIL_FAST_SKIPPED),

  /**
   * Phase 1's cached RDD was recomputed after the barrier had sealed it, so at least one record
   * phase 2 consumed describes storage as it was after the timeline the run pins to; see
   * {@link RecomputeProbe}.
   */
  PHASE_ONE_RECOMPUTED(
      "MDT_VALIDATOR_RECOMPUTE_SKIPPED",
      "Phase 1 was recomputed after the barrier while validating ",
      ". At least one record phase 2 consumed therefore describes storage as it was after the "
          + "timeline this run pins to, so the FS-MDT comparison reached no verdict and no findings "
          + "were written for it",
      Branch.MTV_RECOMPUTE_EXHAUSTIVE_FAILED,
      Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED);

  private final String marker;
  private final String messagePrefix;
  private final String messageTail;
  private final Branch exhaustiveBeacon;
  private final Branch failFastBeacon;

  InconclusiveCause(String marker, String messagePrefix, String messageTail,
                    Branch exhaustiveBeacon, Branch failFastBeacon) {
    this.marker = marker;
    this.messagePrefix = messagePrefix;
    this.messageTail = messageTail;
    this.exhaustiveBeacon = exhaustiveBeacon;
    this.failFastBeacon = failFastBeacon;
  }

  /** The token a fail-fast skip line carries, and the one alerting counts. Never changes spelling. */
  public String marker() {
    return marker;
  }

  /** How the exhaustive exception opens, before the base path. */
  public String messagePrefix() {
    return messagePrefix;
  }

  /**
   * How the exhaustive exception closes, after the reason — what this cause cost the run.
   *
   * <p>Carried per cause rather than shared, so moving the archival path onto this enum reproduces
   * its existing sentence exactly. An operator reading a v2 failure sees the words they always saw.
   */
  public String messageTail() {
    return messageTail;
  }

  /** The branch an exhaustive run takes when it refuses for this cause. */
  public Branch exhaustiveBeacon() {
    return exhaustiveBeacon;
  }

  /** The branch a fail-fast run takes when it sanctions for this cause. */
  public Branch failFastBeacon() {
    return failFastBeacon;
  }
}
