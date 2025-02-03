/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.state.upgrade.source;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.state.upgrade.StateUpgrader;
import org.apache.hudi.state.upgrade.StateVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StreamReadMonitoringStateUpgrader implements StateUpgrader<String> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamReadMonitoringStateUpgrader.class);

  private final HoodieTableMetaClient metaClient;
  private final String issuedInstant;

  public StreamReadMonitoringStateUpgrader(HoodieTableMetaClient metaClient, String issuedInstant) {
    this.metaClient = metaClient;
    this.issuedInstant = issuedInstant;
  }

  @Override
  public List<String> upgrade(List<String> oldState, StateVersion fromVersion, StateVersion toVersion) {
    switch (fromVersion) {
      case V0:
        if (toVersion == StateVersion.V0) {
          return upgradeV0ToV1(oldState);
        }
        throw new IllegalStateException("Unsupported version upgrade path");
      case V1:
        // Do nothing
        return oldState;
      default:
        throw new IllegalStateException("Unsupported version upgrade path");
    }
  }

  @Override
  public boolean canUpgrade(StateVersion fromVersion, StateVersion toVersion) {
    return fromVersion.getValue() < toVersion.getValue();
  }

  private List<String> upgradeV0ToV1(List<String> oldState) {
    ValidationUtils.checkState(oldState.size() == 1, "Retrieved state must have a size of 1");

    // this is the case where we have both legacy and new state.
    // the two should be mutually exclusive for the operator, thus we throw the exception.
    ValidationUtils.checkState(this.issuedInstant != null,
        "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

    String issuedInstant = oldState.get(0);
    HoodieTimeline filteredTimeline;
    boolean isReadArchivedTimeline = false;
    if (metaClient.getActiveTimeline().isBeforeTimelineStarts(issuedInstant)) {
      isReadArchivedTimeline = true;
      // if issuedInstant (requestedTime) is in archive timeline, scan archive timeline to find its completion time
      LOG.warn("The reader's restored instant is in the archive timeline, will scan the archive timeline for issuedOffset (completionTime) using requestTime: {}", issuedInstant);
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline(StringUtils.EMPTY_STRING, false);
      filteredTimeline = archivedTimeline.findInstantsAfterOrEquals(issuedInstant, 3);
    } else {
      filteredTimeline = metaClient.getActiveTimeline().findInstantsAfterOrEquals(issuedInstant, 3);
    }

    if (filteredTimeline.firstInstant().isEmpty()) {
      LOG.error("Unable to find instant {} in [isArchived: {}] timeline: {}", issuedInstant, isReadArchivedTimeline, filteredTimeline);
      throw new HoodieException("Unable to find completionTime in timeline for instant: " + issuedInstant);
    }
    String issuedOffset = filteredTimeline.firstInstant().get().getCompletionTime();

    return Arrays.asList(issuedInstant, issuedOffset);
  }
}
