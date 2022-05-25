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

package org.apache.hudi.sink.compact.strategy;

import java.util.Collections;
import java.util.List;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;

/**
 * Specify the compaction plan instant to compact
 */
public class InstantCompactionPlanSelectStrategy implements CompactionPlanSelectStrategy {
  @Override
  public List<HoodieInstant> select(HoodieTimeline pendingCompactionTimeline, FlinkCompactionConfig config) {
    HoodieInstant specifiedInstant = pendingCompactionTimeline.getInstants()
        .filter(instant -> config.compactionPlanInstant.equals(instant.getTimestamp()))
        .findFirst()
        .orElseThrow(() -> new HoodieException("The instant " + config.compactionPlanInstant + " is not found in timeline"));
    return Collections.singletonList(specifiedInstant);
  }
}
