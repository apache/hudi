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

package org.apache.hudi.state.upgrade;

import org.apache.flink.api.common.state.ListState;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StateUpgradeHelper<T> {

  private final ListState<T> state;
  private final StateUpgrader<T> upgrader;
  private final StateVersion currentVersion;

  public StateUpgradeHelper(ListState<T> state, StateUpgrader<T> upgrader, StateVersion currentVersion) {
    this.state = state;
    this.upgrader = upgrader;
    this.currentVersion = currentVersion;
  }

  public void upgradeState() throws Exception {
    List<T> currentState = StreamSupport
        .stream(state.get().spliterator(), false)
        .collect(Collectors.toList());

    StateVersion detectedVersion = detectVersion(currentState);
    if (upgrader.canUpgrade(detectedVersion, currentVersion)) {
      List<T> upgradedState = upgrader.upgrade(currentState, detectedVersion, currentVersion);
      state.clear();
      state.addAll(upgradedState);
    }
  }

  private StateVersion detectVersion(List<T> state) {
    switch (state.size()) {
      case 1:
        return StateVersion.V0;
      case 2:
        return StateVersion.V1;
      default:
        throw new IllegalStateException("Unknown state size when detecting version, size: " + state.size());
    }
  }
}
