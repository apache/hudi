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

import java.io.Serializable;

/**
 * A retained ANN candidate emitted by a {@link VectorCandidateSource} (RFC-109 v3 §4): the logical
 * record key, its cluster/shard, the approximate (squared L2) distance from RaBitQ scoring, and the
 * posting locator hint. Record keys and locators are decoded only for retained candidates, never
 * for rejected posting rows.
 */
public final class VectorCandidate implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String recordKey;
  private final int clusterId;
  private final int shardId;
  private final double approximateDistance;
  private final VectorPostingLocator postingLocator;

  public VectorCandidate(String recordKey,
                         int clusterId,
                         int shardId,
                         double approximateDistance,
                         VectorPostingLocator postingLocator) {
    this.recordKey = recordKey;
    this.clusterId = clusterId;
    this.shardId = shardId;
    this.approximateDistance = approximateDistance;
    this.postingLocator = postingLocator;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public int getClusterId() {
    return clusterId;
  }

  public int getShardId() {
    return shardId;
  }

  /** Approximate squared-L2 distance from RaBitQ scoring; kept squared internally through ranking. */
  public double getApproximateDistance() {
    return approximateDistance;
  }

  public VectorPostingLocator getPostingLocator() {
    return postingLocator;
  }
}
