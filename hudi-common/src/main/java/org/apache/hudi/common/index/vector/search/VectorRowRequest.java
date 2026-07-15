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
 * One row to fetch within a {@link VectorFetchTask} (RFC-104 v3 §8). For {@link VectorCandidateState#SERVE}
 * the {@link #rowPosition} is authoritative for a positional read; for {@link VectorCandidateState#STALE}
 * the position is ignored and the read handle falls back to a key-based lookup.
 */
public final class VectorRowRequest implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String recordKey;
  private final long rowPosition;
  private final VectorCandidateState state;
  private final double approximateDistance;

  public VectorRowRequest(String recordKey,
                          long rowPosition,
                          VectorCandidateState state,
                          double approximateDistance) {
    this.recordKey = recordKey;
    this.rowPosition = rowPosition;
    this.state = state;
    this.approximateDistance = approximateDistance;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public long getRowPosition() {
    return rowPosition;
  }

  public VectorCandidateState getState() {
    return state;
  }

  public double getApproximateDistance() {
    return approximateDistance;
  }

  public boolean isPositional() {
    return state == VectorCandidateState.SERVE && rowPosition >= 0;
  }
}
