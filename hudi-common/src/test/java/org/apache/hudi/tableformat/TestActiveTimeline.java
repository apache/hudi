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

package org.apache.hudi.tableformat;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.collection.Pair;

import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An active timeline for test table format which merges timeline assuming test-format as the source of truth.
 */
@NoArgsConstructor
public class TestActiveTimeline extends ActiveTimelineV2 {

  public TestActiveTimeline(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    this.setInstants(getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters));
    this.metaClient = metaClient;
  }

  public TestActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), true);
  }

  public TestActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilters) {
    this(
        metaClient,
        Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE),
        applyLayoutFilters);
  }

  @Override
  protected List<HoodieInstant> getInstantsFromFileSystem(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    Map<Pair<String, String>, HoodieInstant> instantsInTestTableFormat = TestTableFormat.getRecordedInstants(metaClient.getBasePath().toString())
        .stream()
        .collect(Collectors.toMap(instant -> Pair.of(instant.requestedTime(), instant.getAction()), instant -> instant));
    List<HoodieInstant> instantsFromHoodieTimeline =
        super.getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters);
    List<HoodieInstant> inflightInstantsInTestTableFormat =
        instantsFromHoodieTimeline.stream()
            .filter(
                hoodieInstant -> !instantsInTestTableFormat.containsKey(Pair.of(hoodieInstant.requestedTime(), hoodieInstant.getAction())))
            .map(
                instant -> {
                  if (instant.isCompleted()) {
                    return new HoodieInstant(
                        HoodieInstant.State.INFLIGHT,
                        instant.getAction(),
                        instant.requestedTime(),
                        instant.getCompletionTime(),
                        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
                  }
                  return instant;
                })
            .collect(Collectors.toList());
    List<HoodieInstant> completedInstantsInTestTableFormat =
        instantsInTestTableFormat.values().stream()
            .filter(instantsFromHoodieTimeline::contains)
            .collect(Collectors.toList());
    return Stream.concat(completedInstantsInTestTableFormat.stream(), inflightInstantsInTestTableFormat.stream())
        .sorted(InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
        .collect(Collectors.toList());
  }
}
