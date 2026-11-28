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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;

/**
 * Utilities for bucketing suspect files based on the archival cutoff of the active timeline.
 * Determines whether a suspect file's writing instant should be resolved against the active
 * timeline or the archive timeline.
 */
public final class InstantBuckets {

  private InstantBuckets() {
  }

  /**
   * Returns the earliest instant time on the given timeline, or null when the timeline is null
   * or holds none. Iterates through completed instants and finds the minimum using
   * {@link InstantComparison#minInstant(String, String)}.
   *
   * @param completedCommits the completed commits timeline
   * @return the earliest instant time, or null if the timeline is null or contains no instants
   */
  public static String earliestCompletedWriteCommit(HoodieTimeline completedCommits) {
    if (completedCommits == null) {
      return null;
    }

    String earliest = null;
    for (HoodieInstant instant : completedCommits.getInstants()) {
      String requestedTime = instant.requestedTime();
      if (requestedTime == null) {
        continue;
      }
      earliest = earliest == null
          ? requestedTime : InstantComparison.minInstant(earliest, requestedTime);
    }
    return earliest;
  }

  /**
   * Determines whether a suspect file's writing instant should be resolved against the archive
   * timeline.
   *
   * <p>Returns true when the writing instant lies before the earliest instant on the active
   * timeline (meaning it was archived), or when the active timeline is empty (earliest is null).
   * The boundary is exclusive: if writingInstant equals earliest, it is still on the active
   * timeline and this method returns false.
   *
   * @param writingInstant the instant when the suspect file was written; must not be null
   * @param earliest the earliest instant on the active timeline; may be null if empty
   * @return true if the instant should be resolved against the archive timeline
   * @throws IllegalArgumentException if writingInstant is null
   */
  public static boolean pokesArchive(String writingInstant, String earliest) {
    if (writingInstant == null) {
      throw new IllegalArgumentException("writingInstant must not be null");
    }
    if (earliest == null) {
      return true;
    }
    return writingInstant.compareTo(earliest) < 0;
  }
}
