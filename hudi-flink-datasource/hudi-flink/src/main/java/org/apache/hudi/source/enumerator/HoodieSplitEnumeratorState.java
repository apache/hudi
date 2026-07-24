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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplitState;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.io.Serializable;
import java.util.Collection;

/**
 * State of Hoodie split enumerator. Mainly include the states of pending splits of split provider.
 */
@Value
@AllArgsConstructor
public class HoodieSplitEnumeratorState implements Serializable {

  Collection<HoodieSourceSplitState> pendingSplitStates;
  Option<String> lastEnumeratedInstant;
  Option<String> lastEnumeratedInstantOffset;
  /**
   * The {@code read.start-commit} / {@code read.end-commit} bounds configured when this checkpoint
   * was taken, recorded only for bounded reads. Both hold {@code Option.of("")} when the option was
   * not configured, so that "recorded but unset" stays distinguishable from "not recorded at all";
   * both are {@link Option#empty()} for streaming reads and for checkpoints written by serializer
   * VERSION 1, which predates this field.
   *
   * <p>A bounded read's split set is frozen at enumeration time and is NOT re-derived on restore, so
   * {@code HoodieSource} compares these against the configured bounds and fails fast when they
   * differ. See {@code HoodieSource#checkBoundedCommitRangeUnchanged}.
   */
  Option<String> readStartCommit;
  Option<String> readEndCommit;

  /**
   * Backward-compatible constructor for callers that do not record a commit range: the streaming
   * enumerator and pre-existing tests. Leaves both bounds {@link Option#empty()}.
   */
  public HoodieSplitEnumeratorState(
      Collection<HoodieSourceSplitState> pendingSplitStates,
      Option<String> lastEnumeratedInstant,
      Option<String> lastEnumeratedInstantOffset) {
    this(pendingSplitStates, lastEnumeratedInstant, lastEnumeratedInstantOffset,
        Option.empty(), Option.empty());
  }
}
