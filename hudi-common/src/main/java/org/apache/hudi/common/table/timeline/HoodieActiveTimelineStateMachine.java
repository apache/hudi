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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;

/**
 * In-Memory Timeline State Machine to hold all states for hoodie instants on the {@link HoodieActiveTimeline}.
 * Provides APIs to be able to perform in memory create, delete, revert and transition of Action States.
 * All writes to {@link HoodieActiveTimeline} go through this State Machine to ensure {@link HoodieInstant}s have
 * {@link HoodieInstant#stateTransitionTime}.
 */
public class HoodieActiveTimelineStateMachine {

  private Map<String, List<HoodieInstant>> actionStartTimeToInstantsMap = new ConcurrentHashMap<>();

  private static HoodieActiveTimelineStateMachine instance;

  /**
   * Initialize State Machine from an instant stream.
   * @param instantStream
   */
  public synchronized void initialize(Stream<HoodieInstant> instantStream) {
    if (actionStartTimeToInstantsMap.size() > 0) {
      throw new UnsupportedOperationException("call reset first");
    }
    instantStream.forEach(instant -> {
      if (actionStartTimeToInstantsMap.containsKey(instant.getTimestamp())) {
        appendToActionInstantList(instant);
      } else {
        addNewActionInstantInternal(instant);
      }
    });
  }

  /**
   * Reset the current state machine.
   */
  public void reset() {
    this.actionStartTimeToInstantsMap = new ConcurrentHashMap<>();
  }

  /**
   * Get the current instance.
   * @return
   */
  public static synchronized HoodieActiveTimelineStateMachine getInstance() {
    if (instance == null) {
      instance = new HoodieActiveTimelineStateMachine();
    }
    return instance;
  }

  /**
   * Transition a {@link HoodieInstant} for an action from one state to another.
   * @param fromInstant
   * @param toInstant
   * @return
   */
  public synchronized HoodieInstant transition(HoodieInstant fromInstant, HoodieInstant toInstant) {
    if (actionStartTimeToInstantsMap.containsKey(fromInstant.getTimestamp())) {
      HoodieInstant currentState = InstantTimeGenerator.setActionEndTimeIfNeeded(toInstant);
      appendToActionInstantList(currentState);
      return currentState;
    } else {
      HoodieInstant currentState = InstantTimeGenerator.setActionEndTimeIfNeeded(toInstant);
      addNewActionInstantInternal(currentState);
      return currentState;
    }
  }

  /**
   * Add a new {@link HoodieInstant} to the State Machine for a new action.
   * @param instant
   * @return
   */
  public synchronized HoodieInstant addNewActionInstant(HoodieInstant instant) {
    if (!actionStartTimeToInstantsMap.containsKey(instant.getTimestamp())) {
      HoodieInstant currentInstant = InstantTimeGenerator.setActionEndTimeIfNeeded(instant);
      return addNewActionInstantInternal(currentInstant);
    } else {
      throw new IllegalArgumentException("This instant " + instant + " is not a new state for action " + instant.getAction());
    }
  }

  /**
   * Delete instant from the State Machine.
   * @param instant
   * @return
   */
  public synchronized HoodieInstant removeInstant(HoodieInstant instant) {
    if (actionStartTimeToInstantsMap.containsKey(instant.getTimestamp())) {
      actionStartTimeToInstantsMap.get(instant.getTimestamp()).remove(instant);
      return instant;
    }
    throw new IllegalArgumentException(); // TODO: Not found exception
  }

  /**
   * Find instant in the State Machine.
   * @param instant
   * @return
   */
  public Option<HoodieInstant> findInstant(HoodieInstant instant) {
    if (actionStartTimeToInstantsMap.containsKey(instant.getTimestamp())) {
      return Option.fromJavaOptional(actionStartTimeToInstantsMap.get(instant.getTimestamp()).stream()
          .filter(instant1 -> instant1.getState().equals(instant.getState())
              && instant1.getAction().equals(instant.getAction())).findFirst());
    }
    return Option.empty();
  }

  /**
   * Get list of instants from State Machine for the given action start time.
   * @param actionStartTime
   * @return
   */
  public Option<List<HoodieInstant>> getInstants(String actionStartTime) {
    return Option.of(actionStartTimeToInstantsMap.get(actionStartTime));
  }

  /**
   * Get latest state instant from State Machine for the given action start time.
   * @param actionStartTime
   * @return
   */
  public Option<HoodieInstant> getLatestInstant(String actionStartTime) {
    List<HoodieInstant> instants = actionStartTimeToInstantsMap.get(actionStartTime);
    if (instants != null) {
      return Option.of(instants.get(instants.size() - 1));
    }
    return Option.empty();
  }

  public Option<HoodieInstant> getInstant(String action, String actionStartTime, State actionState) {
    List<HoodieInstant> instants = actionStartTimeToInstantsMap.get(actionStartTime);
    if (instants != null) {
      return Option.fromJavaOptional(instants.stream().filter(s -> s.getState().equals(actionState)).findFirst());
    }
    return Option.empty();
  }

  private HoodieInstant addNewActionInstantInternal(HoodieInstant instant) {
    List<HoodieInstant> instants = new LinkedList<>();
    instants.add(instant);
    actionStartTimeToInstantsMap.put(instant.getTimestamp(), instants);
    return instant;
  }

  private HoodieInstant appendToActionInstantList(HoodieInstant instant) {
    List<HoodieInstant> instants = actionStartTimeToInstantsMap.get(instant.getTimestamp());
    if (!instants.contains(instant)) {
      instants.add(instant);
    }
    return instant;
  }

}
