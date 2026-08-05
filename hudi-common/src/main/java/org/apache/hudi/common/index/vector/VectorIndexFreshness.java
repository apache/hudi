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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.table.timeline.InstantComparison;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;

/** Determines whether a vector generation can serve a pinned table snapshot. */
public final class VectorIndexFreshness {

  private VectorIndexFreshness() {
  }

  /**
   * Returns whether marker coverage proves that the vector index includes the query snapshot.
   * A missing frontier is never eligible; callers must use the exact plan instead.
   */
  public static boolean isIndexEligible(String queryInstant, String verifiedFrontier) {
    if (queryInstant == null || queryInstant.isEmpty()) {
      throw new IllegalArgumentException("queryInstant must not be empty");
    }
    if (verifiedFrontier == null || verifiedFrontier.isEmpty()) {
      return false;
    }
    return !InstantComparison.compareTimestamps(queryInstant, GREATER_THAN, verifiedFrontier);
  }
}
