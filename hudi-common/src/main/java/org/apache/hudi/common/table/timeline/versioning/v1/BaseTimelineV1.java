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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.table.timeline.BaseHoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseTimelineV1 extends BaseHoodieTimeline {

  public BaseTimelineV1(Stream<HoodieInstant> instants, HoodieInstantReader instantReader) {
    this(instants, instantReader, TimelineLayout.fromVersion(TimelineLayoutVersion.LAYOUT_VERSION_1));
  }

  private BaseTimelineV1(Stream<HoodieInstant> instants, HoodieInstantReader instantReader, TimelineLayout layout) {
    super(instants, instantReader, layout.getTimelineFactory(), layout.getInstantComparator(), layout.getInstantGenerator());
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  @Deprecated
  public BaseTimelineV1() {
    super(TimelineLayout.fromVersion(TimelineLayoutVersion.LAYOUT_VERSION_1), null);
  }

  @Override
  public HoodieTimeline getWriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> validActions.contains(s.getAction())), getInstantReader());
  }

  @Override
  public HoodieTimeline filterPendingClusteringTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) && !s.isCompleted())
        .filter(i -> ClusteringUtils.isClusteringInstant(this, i, instantGenerator)), getInstantReader());
  }

  @Override
  public HoodieTimeline filterPendingReplaceOrClusteringTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> (s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
            && !s.isCompleted()), getInstantReader());
  }

  @Override
  public HoodieTimeline filterPendingReplaceClusteringAndCompactionTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> !s.isCompleted() && (s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)
            || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION))), getInstantReader());
  }

  @Override
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  @Override
  public HoodieTimeline getCommitAndReplaceTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  @Override
  public TimelineLayoutVersion getTimelineLayoutVersion() {
    return TimelineLayoutVersion.LAYOUT_VERSION_1;
  }

  @Override
  public Option<HoodieInstant> getLastClusteringInstant() {
    return Option.fromJavaOptional(getCommitsTimeline().filter(s -> s.getAction().equalsIgnoreCase(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .getReverseOrderedInstants()
        .filter(i -> ClusteringUtils.isClusteringInstant(this, i, instantGenerator))
        .findFirst());
  }

  @Override
  public Option<HoodieInstant> getFirstPendingClusterInstant() {
    return getLastOrFirstPendingClusterInstant(false);
  }

  @Override
  public Option<HoodieInstant> getLastPendingClusterInstant() {
    return getLastOrFirstPendingClusterInstant(true);
  }

  private Option<HoodieInstant> getLastOrFirstPendingClusterInstant(boolean isLast) {
    HoodieTimeline replaceTimeline = filterPendingReplaceTimeline();
    Stream<HoodieInstant> replaceStream;
    if (isLast) {
      replaceStream = replaceTimeline.getReverseOrderedInstants();
    } else {
      replaceStream = replaceTimeline.getInstantsAsStream();
    }
    return  Option.fromJavaOptional(replaceStream
        .filter(i -> ClusteringUtils.isClusteringInstant(this, i, instantGenerator)).findFirst());
  }

  @Override
  public boolean isPendingClusteringInstant(String instantTime) {
    return getOrCreatePendingClusteringInstantSet().contains(instantTime);
  }

  private Set<String> getOrCreatePendingClusteringInstantSet() {
    if (this.pendingClusteringInstants == null) {
      synchronized (this) {
        if (this.pendingClusteringInstants == null) {
          List<HoodieInstant> pendingReplaceInstants = getCommitsTimeline().filterPendingReplaceTimeline().getInstants();
          // Validate that there are no instants with same timestamp
          pendingReplaceInstants.stream().collect(Collectors.groupingBy(HoodieInstant::requestedTime)).forEach((timestamp, instants) -> {
            if (instants.size() > 1) {
              throw new IllegalStateException("Multiple instants with same timestamp: " + timestamp + " instants: " + instants);
            }
          });
          // Filter replace commits down to those that are due to clustering
          this.pendingClusteringInstants = pendingReplaceInstants.stream()
              .filter(instant -> ClusteringUtils.isClusteringInstant(this, instant, instantGenerator))
              .map(HoodieInstant::requestedTime).collect(Collectors.toSet());
        }
      }
    }
    return this.pendingClusteringInstants;
  }
}
