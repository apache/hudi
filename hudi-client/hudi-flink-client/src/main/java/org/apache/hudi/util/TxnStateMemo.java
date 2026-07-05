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

package org.apache.hudi.util;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Memorizes transaction states by instant for Flink streaming writes.
 */
public class TxnStateMemo {

  private final Map<String, TxnState> memo = new HashMap<>();

  public void memo(String instant,
                   Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata,
                   Set<String> conflictResolutionExclusionInstants,
                   Set<String> pendingInflightAndRequestedInstants) {
    memo.put(instant, new TxnState(
        lastCompletedTxnAndMetadata,
        new HashSet<>(conflictResolutionExclusionInstants),
        new HashSet<>(pendingInflightAndRequestedInstants)));
  }

  public Option<TxnState> get(String instant) {
    return Option.ofNullable(memo.get(instant));
  }

  public void slip(String instant) {
    memo.remove(instant);
  }

  public boolean contains(String currentInstant) {
    return memo.containsKey(currentInstant);
  }

  @Getter
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class TxnState {
    private final Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata;
    private final Set<String> conflictResolutionExclusionInstants;
    private final Set<String> pendingInflightAndRequestedInstants;
  }
}
