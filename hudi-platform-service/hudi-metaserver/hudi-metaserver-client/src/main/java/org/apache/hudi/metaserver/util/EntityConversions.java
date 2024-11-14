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

package org.apache.hudi.metaserver.util;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.metaserver.thrift.TAction;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;

import java.util.Locale;

/**
 * Conversion helpers to convert between hoodie entity and thrift entity.
 */
public class EntityConversions {

  public static THoodieInstant toTHoodieInstant(HoodieInstant instant) {
    return new THoodieInstant(instant.requestedTime(), toTAction(instant.getAction()), toTState(instant.getState()));
  }

  public static HoodieInstant fromTHoodieInstant(THoodieInstant instant, InstantGenerator instantGenerator) {
    return instantGenerator.createNewInstant(fromTState(instant.getState()), fromTAction(instant.getAction()), instant.getTimestamp());
  }

  public static TAction toTAction(String action) {
    switch (action) {
      case HoodieTimeline.COMMIT_ACTION:
        return TAction.COMMIT;
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        return TAction.DELTACOMMIT;
      case HoodieTimeline.CLEAN_ACTION:
        return TAction.CLEAN;
      case HoodieTimeline.ROLLBACK_ACTION:
        return TAction.ROLLBACK;
      case HoodieTimeline.SAVEPOINT_ACTION:
        return TAction.SAVEPOINT;
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        return TAction.REPLACECOMMIT;
      case HoodieTimeline.COMPACTION_ACTION:
        return TAction.COMPACTION;
      case HoodieTimeline.RESTORE_ACTION:
        return TAction.RESTORE;
      default:
        throw new IllegalArgumentException("Unknown action: " + action);
    }
  }

  public static TState toTState(HoodieInstant.State state) {
    switch (state) {
      case COMPLETED:
        return TState.COMPLETED;
      case INFLIGHT:
        return TState.INFLIGHT;
      case REQUESTED:
        return TState.REQUESTED;
      case NIL:
        return TState.NIL;
      default:
        throw new IllegalArgumentException("Unknown state: " + state.name());
    }
  }

  public static String fromTAction(TAction action) {
    switch (action) {
      case COMMIT:
      case DELTACOMMIT:
      case CLEAN:
      case ROLLBACK:
      case SAVEPOINT:
      case REPLACECOMMIT:
      case COMPACTION:
      case RESTORE:
        return action.name().toLowerCase(Locale.ROOT);
      default:
        throw new IllegalArgumentException("Unknown action: " + action);
    }
  }

  public static HoodieInstant.State fromTState(TState state) {
    switch (state) {
      case COMPLETED:
        return HoodieInstant.State.COMPLETED;
      case INFLIGHT:
        return HoodieInstant.State.INFLIGHT;
      case REQUESTED:
        return HoodieInstant.State.REQUESTED;
      case NIL:
        return HoodieInstant.State.NIL;
      default:
        throw new IllegalArgumentException("Unknown state: " + state.name());
    }
  }
}
