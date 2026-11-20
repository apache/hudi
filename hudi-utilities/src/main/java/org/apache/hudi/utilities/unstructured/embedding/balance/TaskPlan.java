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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

/**
 * How many prep tasks each group gets, from its {@link GroupStats} alone (driver math, no Spark). For group g with budget
 * {@code b}: {@code byBytes = soloFiles + ceil((bytes - soloBytes) / b)} (at least one task
 * beyond the solo ones when any file is under the budget, even if those files weigh 0), {@code floor = min(files, P)}, and
 * {@code tasks = min(files, max(byBytes, floor, 1))}, so the budget is a ceiling per task and a small batch still spreads over
 * P; {@link BalanceGroups#UNMAPPED} always gets exactly one task (preprocessing reads none of its bytes). Groups are laid out as
 * consecutive partition blocks in group-name order; an empty batch has one partition and no group.
 *
 * <p>Within a group, {@link #partitionOf} places the file of each rank (0 is the heaviest): the solo files take one partition
 * each, and the rest are dealt in snake order over the remaining partitions (rank q goes to {@code q mod k}, reversed on odd
 * rounds), which approximates largest-first packing and keeps task byte totals close. Serializable, so Spark tasks place rows
 * with it.
 */
public final class TaskPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, Integer> offsets;
  private final Map<String, Integer> tasks;
  private final Map<String, Integer> soloTasks;
  private final int numPartitions;

  private TaskPlan(Map<String, Integer> offsets, Map<String, Integer> tasks, Map<String, Integer> soloTasks, int numPartitions) {
    this.offsets = offsets;
    this.tasks = tasks;
    this.soloTasks = soloTasks;
    this.numPartitions = numPartitions;
  }

  /**
   * @param stats  one entry per non-empty group of the batch
   * @param budget the byte budget per task of a group (positive)
   * @param p      the parallelism each group's floor uses (positive)
   * @throws IllegalArgumentException if {@code p} or a group's budget is not positive
   * @throws IllegalStateException    if a group or the whole plan would need more than {@code Integer.MAX_VALUE} partitions
   */
  public static TaskPlan plan(List<GroupStats> stats, ToLongFunction<String> budget, int p) {
    if (p <= 0) {
      throw new IllegalArgumentException("parallelism must be positive, got " + p);
    }
    List<GroupStats> sorted = new ArrayList<>(stats);
    sorted.sort(Comparator.comparing(GroupStats::group));
    Map<String, Integer> offsets = new LinkedHashMap<>();
    Map<String, Integer> tasks = new LinkedHashMap<>();
    Map<String, Integer> soloTasks = new LinkedHashMap<>();
    long next = 0;
    for (GroupStats group : sorted) {
      if (group.files() <= 0) {
        continue;
      }
      boolean unmapped = BalanceGroups.UNMAPPED.equals(group.group());
      long groupTasks = unmapped ? 1 : mappedTasks(group, budget.applyAsLong(group.group()), p);
      long solo = unmapped ? 0 : Math.min(group.soloFiles(), groupTasks);
      if (groupTasks > Integer.MAX_VALUE || next + groupTasks > Integer.MAX_VALUE) {
        throw new IllegalStateException("group " + group.group() + " would need " + groupTasks + " prep tasks after "
            + next + " others, more than " + Integer.MAX_VALUE + " partitions; raise its budget");
      }
      offsets.put(group.group(), (int) next);
      tasks.put(group.group(), (int) groupTasks);
      soloTasks.put(group.group(), (int) solo);
      next += groupTasks;
    }
    return new TaskPlan(Collections.unmodifiableMap(offsets), Collections.unmodifiableMap(tasks),
        Collections.unmodifiableMap(soloTasks), (int) Math.max(1, next));
  }

  /** The tasks of a mapped group: its solo files plus enough dealt tasks for its other bytes, at least min(files, p). */
  private static long mappedTasks(GroupStats group, long groupBudget, int p) {
    if (groupBudget <= 0) {
      throw new IllegalArgumentException("budget of group " + group.group() + " must be positive, got " + groupBudget);
    }
    long rest = Math.max(0, group.bytes() - group.soloBytes());
    // the files under the budget need at least one dealt task even when they weigh 0 in all
    long dealt = Math.max(group.files() > group.soloFiles() ? 1 : 0, rest / groupBudget + (rest % groupBudget == 0 ? 0 : 1));
    long byBytes = group.soloFiles() + dealt;
    long floor = Math.min(group.files(), p);
    return Math.min(group.files(), Math.max(1, Math.max(byBytes, floor)));
  }

  public int numPartitions() {
    return numPartitions;
  }

  /** @return the groups of the plan, in partition-block order */
  public List<String> groups() {
    return new ArrayList<>(offsets.keySet());
  }

  /** @return the first partition of {@code group}'s block */
  public int offset(String group) {
    return lookup(offsets, group);
  }

  public int tasks(String group) {
    return lookup(tasks, group);
  }

  /** @return how many of {@code group}'s tasks each hold one file at or over the budget */
  public int soloTasks(String group) {
    return lookup(soloTasks, group);
  }

  /**
   * @param rank the file's 0-based rank within {@code group}, by weight descending then the source version key
   * @return the partition of that file: {@code offset + rank} for the first {@link #soloTasks} ranks, else the snake-dealt
   *     partition of {@code q = rank - soloTasks} over the group's other {@code k = max(tasks - soloTasks, 1)} partitions
   * @throws IllegalArgumentException if {@code group} is not in the plan or {@code rank} is negative
   */
  public int partitionOf(String group, long rank) {
    if (rank < 0) {
      throw new IllegalArgumentException("rank must not be negative, got " + rank);
    }
    int offset = offset(group);
    int solo = soloTasks(group);
    if (rank < solo) {
      return offset + (int) rank;
    }
    long dealt = Math.max(tasks(group) - solo, 1);
    long q = rank - solo;
    long position = q % dealt;
    long snake = (q / dealt) % 2 == 0 ? position : dealt - 1 - position;
    return offset + solo + (int) snake;
  }

  private static int lookup(Map<String, Integer> values, String group) {
    Integer value = values.get(group);
    if (value == null) {
      throw new IllegalArgumentException("group " + group + " is not in the plan " + values.keySet());
    }
    return value;
  }
}
