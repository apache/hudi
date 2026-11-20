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

package org.apache.hudi.source.split;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Interface for continuously discover new splits from active timeline.
 */
@Internal
public interface HoodieContinuousSplitDiscover {

  /** Discover the files appended between {@code lastInstant} and the last commit of current table */
  HoodieContinuousSplitBatch discoverSplits(String lastInstant);

  /**
   * Returns the completion times of the completed commits in the active timeline after {@code offset},
   * and at or before the configured end commit if any, which the next discoveries are going to read, in
   * ascending order.
   *
   * @param offset         the consumption offset issued so far, nothing is returned if null
   * @param reloadTimeline whether to reload the timeline, otherwise the one of the last discovery is used
   */
  default List<String> getUndiscoveredCommits(@Nullable String offset, boolean reloadTimeline) {
    return Collections.emptyList();
  }

  /**
   * Returns the lag info of the splits, keyed by split id, using the timeline of the last discovery.
   */
  default Map<String, HoodieSourceSplitLagInfo> getLagInfo(Collection<HoodieSourceSplit> splits) {
    return HoodieSourceSplitLagInfo.ofSplits(splits);
  }

  /**
   * Returns the lag info of the splits known without reading the commit metadata, keyed by split id,
   * using the timeline of the last discovery: the splits read all the commits of their ranges, with
   * unknown estimates.
   */
  default Map<String, HoodieSourceSplitLagInfo> getRangeLagInfo(Collection<HoodieSourceSplit> splits) {
    return HoodieSourceSplitLagInfo.ofSplits(splits);
  }

  /**
   * Returns the time zone of the instant and completion times of the table.
   */
  default ZoneId getTimelineZone() {
    return ZoneId.systemDefault();
  }
}
