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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What one archived-timeline pass established about a batch of instants, and the content it
 * projected for the ones it found.
 *
 * <p><b>This is the materialized answer.</b> Once a resolver has produced one, no further archive
 * read happens for those instants: every consumer reads this set instead. That is the whole point
 * of the batch pass, and a consumer that goes back to the archive defeats it.
 *
 * <p><b>The three states are the safety property.</b> {@link InstantSearchState#NOT_FOUND} means
 * every byte that could have held the instant was read cleanly and it was not there;
 * {@link InstantSearchState#INDETERMINATE} means the search could not cover what it needed to.
 * Only the first is a claim about the table, because downstream an absence licenses treating a file
 * as unreferenced, and therefore a delete.
 *
 * <p>An instant that was never a target is a caller bug rather than an unknown: answering
 * {@code INDETERMINATE} for it would let a consumer silently ask the wrong question and get a
 * plausible refusal back.
 *
 * @param <T> whatever the caller's {@link ArchivedRecordProjector} produced per instant
 */
public final class ArchivedInstantWorkingSet<T> {

  private final Set<String> targets;
  private final Map<String, InstantSearchState> states;
  private final Map<String, T> content;

  private ArchivedInstantWorkingSet(Set<String> targets, Map<String, InstantSearchState> states,
                                    Map<String, T> content) {
    this.targets = targets;
    this.states = states;
    this.content = content;
  }

  /** Every target unproven, which refuses. The answer when a pass could not run or could not be trusted. */
  public static <T> ArchivedInstantWorkingSet<T> allIndeterminate(Set<String> targets) {
    return uniform(targets, InstantSearchState.INDETERMINATE);
  }

  /**
   * Every target cleanly absent. Only for a caller that has established the archive holds nothing
   * at all -- on layout 1 the archive directory does not exist until the first archival.
   */
  public static <T> ArchivedInstantWorkingSet<T> allNotFound(Set<String> targets) {
    return uniform(targets, InstantSearchState.NOT_FOUND);
  }

  private static <T> ArchivedInstantWorkingSet<T> uniform(Set<String> targets,
                                                          InstantSearchState state) {
    Objects.requireNonNull(targets, "targets");
    Set<String> copy = Collections.unmodifiableSet(new HashSet<>(targets));
    Map<String, InstantSearchState> states = new HashMap<>();
    for (String target : copy) {
      states.put(target, state);
    }
    return new ArchivedInstantWorkingSet<>(copy, states, Collections.<String, T>emptyMap());
  }

  /** Starts a builder that accumulates per-instant decisions for the given targets. */
  public static <T> Builder<T> builder(Set<String> targets) {
    return new Builder<>(targets);
  }

  /** The distinct instants this pass was asked about. */
  public Set<String> targets() {
    return targets;
  }

  /**
   * A target the pass returned without deciding answers {@link InstantSearchState#INDETERMINATE} --
   * never {@link InstantSearchState#NOT_FOUND}, and never a throw. That fallback is the safety
   * property this type exists to guarantee.
   *
   * @throws IllegalArgumentException when {@code instantTime} was never a target
   */
  public InstantSearchState stateOf(String instantTime) {
    checkTarget(instantTime);
    InstantSearchState state = states.get(instantTime);
    // A target the pass returned without deciding has not been proven absent, so it refuses.
    return state == null ? InstantSearchState.INDETERMINATE : state;
  }

  /**
   * What the projector produced for this instant, present only when {@link #stateOf} is
   * {@link InstantSearchState#FOUND}.
   *
   * @throws IllegalArgumentException when {@code instantTime} was never a target
   */
  public Option<T> contentOf(String instantTime) {
    checkTarget(instantTime);
    return Option.ofNullable(content.get(instantTime));
  }

  private void checkTarget(String instantTime) {
    if (!targets.contains(instantTime)) {
      throw new IllegalArgumentException("instant " + instantTime + " was never a target of this "
          + "archived pass; the pass covered " + targets.size() + " instant(s)");
    }
  }

  /** Accumulates one pass's decisions. Not thread-safe: resolvers decide after their pass returns. */
  public static final class Builder<T> {

    private final Set<String> targets;
    private final Map<String, InstantSearchState> states = new HashMap<>();
    private final Map<String, T> content = new HashMap<>();

    private Builder(Set<String> targets) {
      Objects.requireNonNull(targets, "targets");
      this.targets = Collections.unmodifiableSet(new HashSet<>(targets));
    }

    /**
     * Records {@code instantTime} as {@link InstantSearchState#FOUND} with its projected content.
     *
     * @throws NullPointerException when {@code projected} is null
     */
    public Builder<T> found(String instantTime, T projected) {
      Objects.requireNonNull(projected, "projected content of a FOUND instant");
      states.put(instantTime, InstantSearchState.FOUND);
      content.put(instantTime, projected);
      return this;
    }

    /** Records {@code instantTime} as {@link InstantSearchState#NOT_FOUND}. */
    public Builder<T> notFound(String instantTime) {
      states.put(instantTime, InstantSearchState.NOT_FOUND);
      content.remove(instantTime);
      return this;
    }

    /** Records {@code instantTime} as {@link InstantSearchState#INDETERMINATE}. */
    public Builder<T> indeterminate(String instantTime) {
      states.put(instantTime, InstantSearchState.INDETERMINATE);
      content.remove(instantTime);
      return this;
    }

    /** Builds the working set, copying rather than aliasing this builder's decisions so far. */
    public ArchivedInstantWorkingSet<T> build() {
      return new ArchivedInstantWorkingSet<>(targets, new HashMap<>(states), new HashMap<>(content));
    }
  }
}
