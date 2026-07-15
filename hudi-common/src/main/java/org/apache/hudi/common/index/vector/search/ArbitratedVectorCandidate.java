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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.io.Serializable;

/**
 * A candidate after RLI freshness arbitration (RFC-104 v3 §7): the original candidate, its
 * {@link VectorCandidateState} verdict, and the live location resolved from the RLI (present for
 * {@code SERVE} and {@code STALE}, null for {@code DELETED}).
 *
 * <ul>
 *   <li>{@code SERVE}: {@code liveLocation} matches the posting hint; positional read allowed.</li>
 *   <li>{@code STALE}: {@code liveLocation} is the record's current location; exact mode must
 *       key-fetch there rather than trust the posting's row position.</li>
 *   <li>{@code DELETED}: dropped; no live location.</li>
 * </ul>
 */
public final class ArbitratedVectorCandidate implements Serializable {

  private static final long serialVersionUID = 1L;

  private final VectorCandidate candidate;
  private final VectorCandidateState state;
  private final HoodieRecordGlobalLocation liveLocation;

  public ArbitratedVectorCandidate(VectorCandidate candidate,
                                   VectorCandidateState state,
                                   HoodieRecordGlobalLocation liveLocation) {
    this.candidate = candidate;
    this.state = state;
    this.liveLocation = liveLocation;
  }

  public VectorCandidate getCandidate() {
    return candidate;
  }

  public VectorCandidateState getState() {
    return state;
  }

  /** Live RLI location for SERVE/STALE, or null for DELETED. */
  public HoodieRecordGlobalLocation getLiveLocation() {
    return liveLocation;
  }

  public boolean isServable() {
    return state == VectorCandidateState.SERVE || state == VectorCandidateState.STALE;
  }
}
