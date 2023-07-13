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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * ConflictResolutionStrategy base on StateTransitionTime, it uses StateTransitionTime (end_timestamp) to get completed
 * instants after current instant, and no need to record the pending instants before write.
 */
public class StateTransitionTimeBasedConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  public StateTransitionTimeBasedConflictResolutionStrategy(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  /**
   * Get completed instants after current instant starts based on StateTransitionTime (end_timestamp):
   *   1) pick end_timestamp > current start_timestamp && have completed instants
   *
   * @param metaClient meta client
   * @param currentInstant current instant
   * @param pendingInstantsBeforeWrite pending instant recorded before write (no use)
   * @return selected {@link HoodieInstant} stream
   */
  @Override
  public Stream<HoodieInstant> getCompletedInstantsAfterCurrent(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                                Option<Set<String>> pendingInstantsBeforeWrite) {
     return metaClient.getActiveTimeline()
              .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
              .filterCompletedInstants()
              .findInstantsModifiedAfterByStateTransitionTime(currentInstant.getTimestamp())
              .getInstantsAsStream();
  }

  @Override
  public boolean isPendingInstantsRequiredBeforeWrite() {
    return false;
  }
}
