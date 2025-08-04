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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparator;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Comparator specifically for computing the instant order when computing the instants to rollback as part of a restore operation.
 * The order relies on the completion time of the instants, except for compaction instants on Merge-on-Read tables. These instants may be completed after a
 * delta-commit but should still be considered earlier since the log files from the next delta-commit become associated with the base files from this compaction.
 * For example if we have the following sequence of commits (DC=delta-commit, C=compaction):
 * ... DC-10 starts -> DC-10 ends -> compaction-1 starts -> delta-commit-11 starts -> delta-commit-11 ends -> compaction-1 ends ...
 * If we restore to delta-commit-11, we do not roll back the compaction-1 instant, even though it finished after delta-commit-11.
 */
public class RestoreInstantComparator implements Comparator<HoodieInstant> {
  private static final Set<String> COMPACTION_ACTIONS = Stream.of(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.COMMIT_ACTION).collect(Collectors.toSet());
  private final InstantComparator instantComparator;
  private final HoodieTableType tableType;

  public RestoreInstantComparator(HoodieTableMetaClient metaClient) {
    this.instantComparator = metaClient.getTimelineLayout().getInstantComparator();
    this.tableType = metaClient.getTableType();
  }

  @Override
  public int compare(HoodieInstant o1, HoodieInstant o2) {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      return instantComparator.completionTimeOrderedComparator().compare(o1, o2);
    } else {
      // Do to special handling of compaction instants, we need to use requested time based comparator for compaction instants but completion time based comparator for others
      if (COMPACTION_ACTIONS.contains(o1.getAction()) || COMPACTION_ACTIONS.contains(o2.getAction())) {
        return instantComparator.requestedTimeOrderedComparator().compare(o1, o2);
      } else {
        return instantComparator.completionTimeOrderedComparator().compare(o1, o2);
      }
    }
  }
}
