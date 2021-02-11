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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.timeline.HoodieInstant.State;

/**
 * Generates final {@link HoodieInstant}s to write to the timeline.
 */
public class InstantTimeGenerator {

  public static HoodieInstant setActionEndTimeIfNeeded(HoodieInstant instant) {
    if (instant.getStateTransitionTime() != null) {
      return new HoodieInstant(instant.getState(), instant.getAction(), instant.getTimestamp(),
          HoodieActiveTimeline.createNewInstantTime());
    }
    return instant;
  }

  public static HoodieInstant createHoodieInstantWithActionEndTime(State state, String action, String actionStartTimestamp) {
    return new HoodieInstant(state, action, actionStartTimestamp, HoodieActiveTimeline.createNewInstantTime());
  }

  public static HoodieInstant createHoodieInstant(State state, String action, String actionStartTimestamp, String actionEndTimestamp) {
    return new HoodieInstant(state, action, actionStartTimestamp, actionEndTimestamp);
  }
}
