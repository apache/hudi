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

import org.apache.hudi.common.util.ValidationUtils;

import java.util.List;
import java.util.Set;

/**
 * Computes contiguous source-timeline coverage from persisted source-instant markers.
 */
public final class VectorSourceInstantCoverage {

  private VectorSourceInstantCoverage() {
  }

  /**
   * Advances coverage across the ordered source instants immediately following the current value.
   * Processing stops at the first missing marker; a later marker can never bridge that gap.
   */
  public static String advance(
      String lastContiguousSourceInstant,
      List<String> subsequentSourceInstants,
      Set<String> coveredSourceInstants) {
    ValidationUtils.checkArgument(
        lastContiguousSourceInstant != null && !lastContiguousSourceInstant.isEmpty(),
        "Last contiguous source instant must not be empty");
    ValidationUtils.checkArgument(subsequentSourceInstants != null, "Source instants must not be null");
    ValidationUtils.checkArgument(coveredSourceInstants != null, "Covered source instants must not be null");

    String lastCovered = lastContiguousSourceInstant;
    for (String sourceInstant : subsequentSourceInstants) {
      ValidationUtils.checkArgument(sourceInstant.compareTo(lastCovered) > 0,
          "Source instants must be strictly ordered after the last contiguous source instant");
      if (!coveredSourceInstants.contains(sourceInstant)) {
        break;
      }
      lastCovered = sourceInstant;
    }
    return lastCovered;
  }
}
