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

import org.apache.hudi.common.index.vector.VectorStalePolicy;
import org.apache.hudi.common.util.ValidationUtils;

/** Applies the configured policy when vector-index source coverage trails the pinned query instant. */
public final class VectorFreshnessGate {

  private VectorFreshnessGate() {
  }

  public static VectorFreshnessDecision decide(
      String lastContiguousSourceInstant,
      String queryInstant,
      VectorStalePolicy stalePolicy) {
    ValidationUtils.checkArgument(queryInstant != null && !queryInstant.isEmpty(),
        "Query instant must not be empty");
    ValidationUtils.checkArgument(stalePolicy != null, "Stale policy must not be null");

    if (lastContiguousSourceInstant != null
        && lastContiguousSourceInstant.compareTo(queryInstant) >= 0) {
      return VectorFreshnessDecision.USE_INDEX;
    }
    switch (stalePolicy) {
      case WARN:
        return VectorFreshnessDecision.USE_INDEX_WITH_WARNING;
      case FALLBACK:
        return VectorFreshnessDecision.EXACT_FALLBACK;
      case FAIL:
      default:
        return VectorFreshnessDecision.FAIL_QUERY;
    }
  }
}
