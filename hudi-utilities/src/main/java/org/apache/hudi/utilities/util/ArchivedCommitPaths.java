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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.HoodieFileGroupId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What one archived instant's records say about the wanted file groups, as
 * {@link CommitPathProjector} projects it.
 *
 * <p>Two kinds of fact, kept apart because only one of them may condemn a file:
 *
 * <ul>
 *   <li><b>Names by group</b>, unioned over every record of the instant (requested, inflight and
 *       completed). Extra names can only turn a would-be {@code DANGLING} into
 *       {@code COMMITTED}, so the union never over-condemns.
 *   <li><b>Whether the completed commit recorded any write stats</b>. Taken from completed records
 *       only, because a pending record's planned stats are not what the commit wrote. A completed
 *       commit that recorded write stats and names nothing for a group did not write that group at
 *       this instant (proof); one that recorded no write stats at all claimed nothing (a refusal).
 * </ul>
 *
 * <p>Groups are keyed as the caller supplied them; partition matching is normalised when
 * projecting.
 * Immutable.
 */
public final class ArchivedCommitPaths {

  private static final ArchivedCommitPaths EMPTY = new ArchivedCommitPaths(
      Collections.<HoodieFileGroupId, Set<String>>emptyMap(), false);

  private final Map<HoodieFileGroupId, Set<String>> namesByGroup;
  private final boolean completedWithWriteStats;

  private ArchivedCommitPaths(Map<HoodieFileGroupId, Set<String>> namesByGroup,
                              boolean completedWithWriteStats) {
    this.namesByGroup = namesByGroup;
    this.completedWithWriteStats = completedWithWriteStats;
  }

  /** Records nothing at all. */
  public static ArchivedCommitPaths empty() {
    return EMPTY;
  }

  /**
   * Names only, with no completed-commit facts: an absent group stays a refusal. The shape a record
   * that is not a completed commit projects to.
   */
  public static ArchivedCommitPaths namesOnly(Map<HoodieFileGroupId, Set<String>> namesByGroup) {
    return new ArchivedCommitPaths(copy(namesByGroup), false);
  }

  /**
   * A completed commit's projection.
   *
   * @param namesByGroup  bare names recorded for each wanted group
   * @param hasWriteStats whether the commit recorded any partition at all
   */
  public static ArchivedCommitPaths completed(Map<HoodieFileGroupId, Set<String>> namesByGroup,
                                              boolean hasWriteStats) {
    return new ArchivedCommitPaths(copy(namesByGroup), hasWriteStats);
  }

  /**
   * Unions two projections of the same instant into a new value: names per group, and whether
   * either completed record recorded write stats. Order-independent, and mutates neither argument.
   */
  public static ArchivedCommitPaths merge(ArchivedCommitPaths first, ArchivedCommitPaths second) {
    Map<HoodieFileGroupId, Set<String>> names = new LinkedHashMap<>();
    unionInto(names, first.namesByGroup);
    unionInto(names, second.namesByGroup);
    return new ArchivedCommitPaths(Collections.unmodifiableMap(names),
        first.completedWithWriteStats || second.completedWithWriteStats);
  }

  /** Every name recorded per wanted group; a group nothing names is absent. */
  public Map<HoodieFileGroupId, Set<String>> namesByGroup() {
    return namesByGroup;
  }

  /** The names recorded for {@code groupId}, or an empty set. */
  public Set<String> namesOf(HoodieFileGroupId groupId) {
    Set<String> names = namesByGroup.get(groupId);
    return names == null ? Collections.<String>emptySet() : names;
  }

  /** Whether a completed record of this instant recorded any write stats. */
  public boolean isCompletedWithWriteStats() {
    return completedWithWriteStats;
  }

  private static Map<HoodieFileGroupId, Set<String>> copy(
      Map<HoodieFileGroupId, Set<String>> source) {
    Map<HoodieFileGroupId, Set<String>> copied = new LinkedHashMap<>();
    unionInto(copied, Objects.requireNonNull(source, "namesByGroup"));
    return Collections.unmodifiableMap(copied);
  }

  private static void unionInto(Map<HoodieFileGroupId, Set<String>> target,
                                Map<HoodieFileGroupId, Set<String>> source) {
    for (Map.Entry<HoodieFileGroupId, Set<String>> entry : source.entrySet()) {
      Set<String> names = target.get(entry.getKey());
      Set<String> merged = names == null ? new LinkedHashSet<>() : new LinkedHashSet<>(names);
      merged.addAll(entry.getValue());
      target.put(entry.getKey(), Collections.unmodifiableSet(merged));
    }
  }

  @Override
  public String toString() {
    return "ArchivedCommitPaths{names=" + namesByGroup + ", completedWithWriteStats="
        + completedWithWriteStats + "}";
  }
}
