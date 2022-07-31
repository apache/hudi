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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A mocked {@link HoodieActiveTimeline}.
 */
public class MockHoodieTimeline extends HoodieActiveTimeline {

  public MockHoodieTimeline(Stream<String> completed, Stream<String> inflights) {
    super();
    this.setInstants(Stream
        .concat(completed.map(s -> new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, s)),
            inflights.map(s -> new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, s)))
        .sorted(Comparator.comparing(HoodieInstant::getFileName)).collect(Collectors.toList()));
  }

  public MockHoodieTimeline(List<HoodieInstant> instants) {
    super();
    this.setInstants(instants);
  }
}
