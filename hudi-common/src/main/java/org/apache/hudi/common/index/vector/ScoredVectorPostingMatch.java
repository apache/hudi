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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

public final class ScoredVectorPostingMatch extends VectorPostingMatch {
  private final float approxDistance;
  private final HoodieRecordGlobalLocation location;
  private final VectorIndexArbiter.Decision arbiterDecision;

  public ScoredVectorPostingMatch(VectorPostingMatch match, float approxDistance, HoodieRecordGlobalLocation location) {
    this(match, approxDistance, location, null);
  }

  public ScoredVectorPostingMatch(VectorPostingMatch match,
                            float approxDistance,
                            HoodieRecordGlobalLocation location,
                            VectorIndexArbiter.Decision arbiterDecision) {
    super(
        match.getRecordKey(),
        match.getClusterId(),
        match.getShardId(),
        match.getFileGroupId(),
        match.getPartitionPath(),
        match.getBaseInstantTime(),
        match.getRowPosition(),
        match.getBinaryCode(),
        match.getExtendedCode(),
        match.getScalar(),
        match.getAdditiveFactor(),
        match.getRescaleFactor(),
        match.getVectorNorm(),
        match.isDelta(),
        match.isDeleted());
    this.approxDistance = approxDistance;
    this.location = location;
    this.arbiterDecision = arbiterDecision;
  }

  public float getApproxDistance() {
    return approxDistance;
  }

  public HoodieRecordGlobalLocation getLocation() {
    return location;
  }

  public ScoredVectorPostingMatch withLocation(HoodieRecordGlobalLocation newLocation) {
    return new ScoredVectorPostingMatch(this, approxDistance, newLocation, arbiterDecision);
  }

  /**
   * The RLI arbiter verdict for this finalist, or {@code null} if it has not been arbitrated.
   * See {@link VectorIndexArbiter} and {@code arbitrateFinalists}.
   */
  public VectorIndexArbiter.Decision getArbiterDecision() {
    return arbiterDecision;
  }

  public ScoredVectorPostingMatch withArbiterVerdict(VectorIndexArbiter.Decision decision,
                                               HoodieRecordGlobalLocation resolvedLocation) {
    return new ScoredVectorPostingMatch(this, approxDistance, resolvedLocation, decision);
  }
}
