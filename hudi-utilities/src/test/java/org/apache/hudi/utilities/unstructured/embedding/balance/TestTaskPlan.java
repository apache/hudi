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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-group task counts {@link TaskPlan} derives from the per-group aggregates: budget as a ceiling, a floor of
 * {@code min(files, P)}, never more tasks than files, one task for unmapped rows, groups as blocks in name order; and where
 * {@link TaskPlan#partitionOf} places each rank of a group: solo files alone, the rest dealt in snake order.
 */
class TestTaskPlan {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;
  private static final long GIB = 1024L * MIB;

  private static ToLongFunction<String> budgets(long pdf, long video) {
    return group -> group.equals(BalanceGroups.VIDEO) ? video : group.equals(BalanceGroups.PDF) ? pdf : 512 * MIB;
  }

  @Test
  void smallBatchSpreadsToTheFloor() {
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.PDF, 50, 100 * MIB, 0, 0)),
        budgets(256 * MIB, GIB), 8);
    assertEquals(8, plan.tasks(BalanceGroups.PDF));
    assertEquals(8, plan.numPartitions());
  }

  @Test
  void groupsGetTheirOwnFloor() {
    TaskPlan plan = TaskPlan.plan(Arrays.asList(new GroupStats(BalanceGroups.VIDEO, 1, 2 * GIB, 1, 2 * GIB),
        new GroupStats(BalanceGroups.PDF, 1000, 1000 * 100 * KIB, 0, 0)), budgets(256 * MIB, GIB), 8);
    assertEquals(1, plan.tasks(BalanceGroups.VIDEO));
    assertEquals(1, plan.soloTasks(BalanceGroups.VIDEO));
    assertEquals(8, plan.tasks(BalanceGroups.PDF));
    assertEquals(9, plan.numPartitions());
    assertEquals(0, plan.offset(BalanceGroups.PDF));
    assertEquals(8, plan.offset(BalanceGroups.VIDEO));
    assertEquals(Arrays.asList(BalanceGroups.PDF, BalanceGroups.VIDEO), plan.groups());
  }

  @Test
  void neverMoreTasksThanFiles() {
    // three videos with P = 40: three tasks, no empty partition
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.VIDEO, 3, 300 * MIB, 0, 0)),
        budgets(256 * MIB, GIB), 40);
    assertEquals(3, plan.tasks(BalanceGroups.VIDEO));
    assertEquals(3, plan.numPartitions());
  }

  @Test
  void bytesAboveTheFloor() {
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.PDF, 5000, 10 * GIB, 0, 0)),
        budgets(256 * MIB, GIB), 8);
    assertEquals(40, plan.tasks(BalanceGroups.PDF));
  }

  @Test
  void soloFilesPlusTheRest() {
    // 2 solo videos of 3 GiB + 10 of 100 MiB (1000 MiB in total) under a 1 GiB budget, P = 1: 2 + ceil(1000 MiB / 1 GiB) = 3
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(
        new GroupStats(BalanceGroups.VIDEO, 12, 6 * GIB + 1000 * MIB, 2, 6 * GIB)), budgets(256 * MIB, GIB), 1);
    assertEquals(3, plan.tasks(BalanceGroups.VIDEO));
    assertEquals(2, plan.soloTasks(BalanceGroups.VIDEO));
  }

  @Test
  void zeroWeightFilesBesideASoloFileGetADealtTask() {
    // one solo file and two that weigh 0, P = 1: the two still need a task beside the solo one
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.PDF, 3, 3 * MIB, 1, 3 * MIB)),
        group -> 2 * MIB, 1);
    assertEquals(2, plan.tasks(BalanceGroups.PDF));
    assertEquals(1, plan.soloTasks(BalanceGroups.PDF));
    // every file solo: no dealt task
    plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.PDF, 2, 6 * MIB, 2, 6 * MIB)),
        group -> 2 * MIB, 1);
    assertEquals(2, plan.tasks(BalanceGroups.PDF));
    assertEquals(2, plan.soloTasks(BalanceGroups.PDF));
  }

  @Test
  void unmappedIsOneTask() {
    TaskPlan plan = TaskPlan.plan(Arrays.asList(new GroupStats(BalanceGroups.UNMAPPED, 500, 500 * GIB, 0, 0),
        new GroupStats(BalanceGroups.IMAGE, 2, 2 * MIB, 0, 0)), budgets(256 * MIB, GIB), 8);
    assertEquals(1, plan.tasks(BalanceGroups.UNMAPPED));
    assertEquals(0, plan.soloTasks(BalanceGroups.UNMAPPED));
    assertEquals(2, plan.tasks(BalanceGroups.IMAGE));
    assertEquals(2, plan.offset(BalanceGroups.UNMAPPED));
    assertEquals(3, plan.numPartitions());
  }

  @Test
  void emptyBatchIsOnePartition() {
    TaskPlan plan = TaskPlan.plan(Collections.emptyList(), budgets(256 * MIB, GIB), 8);
    assertEquals(1, plan.numPartitions());
    assertEquals(Collections.emptyList(), plan.groups());
  }

  @Test
  void zeroByteGroupUsesTheFloor() {
    TaskPlan plan = TaskPlan.plan(Collections.singletonList(new GroupStats(BalanceGroups.PDF, 5, 0, 0, 0)),
        budgets(256 * MIB, GIB), 8);
    assertEquals(5, plan.tasks(BalanceGroups.PDF));
  }

  @Test
  void tooManyTasksIsRefused() {
    List<GroupStats> stats = Collections.singletonList(new GroupStats(BalanceGroups.PDF, Long.MAX_VALUE, Long.MAX_VALUE, 0, 0));
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> TaskPlan.plan(stats, group -> 1L, 8));
    assertTrue(e.getMessage().contains(BalanceGroups.PDF), e.getMessage());
  }

  @Test
  void invalidInputsAndPartitionOverflowFailInsteadOfWrapping() {
    List<GroupStats> none = Collections.emptyList();
    assertThrows(IllegalArgumentException.class, () -> TaskPlan.plan(none, group -> 1L, 0));
    List<GroupStats> one = Collections.singletonList(new GroupStats("pdf", 1, 1, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> TaskPlan.plan(one, group -> 0L, 1));
    List<GroupStats> pastIntRange = Collections.singletonList(new GroupStats("pdf", 2147483648L, 2147483648L, 2147483648L, 2147483648L));
    assertThrows(IllegalStateException.class, () -> TaskPlan.plan(pastIntRange, group -> 1L, 1));
    List<GroupStats> overflowingSum = Arrays.asList(
        new GroupStats("a", Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
        new GroupStats("b", 1, 1, 1, 1));
    assertThrows(IllegalStateException.class, () -> TaskPlan.plan(overflowingSum, group -> 1L, 1));
  }

  @Test
  void soloRanksTakeOnePartitionEachAndTheRestAreDealtInSnakeOrder() {
    // pdf block [0, 5): 2 solo files on 0 and 1, the rest snake-dealt over 2, 3, 4; video block [5, 7)
    TaskPlan plan = TaskPlan.plan(Arrays.asList(new GroupStats(BalanceGroups.PDF, 20, 1300 * MIB, 2, 600 * MIB),
        new GroupStats(BalanceGroups.VIDEO, 4, 40 * MIB, 0, 0)), budgets(256 * MIB, GIB), 2);
    assertEquals(5, plan.tasks(BalanceGroups.PDF));
    assertEquals(2, plan.soloTasks(BalanceGroups.PDF));
    int[] pdf = new int[11];
    for (int rank = 0; rank < pdf.length; rank++) {
      pdf[rank] = plan.partitionOf(BalanceGroups.PDF, rank);
    }
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 4, 3, 2, 2, 3, 4}, pdf);
    assertEquals(5, plan.partitionOf(BalanceGroups.VIDEO, 0));
    assertEquals(6, plan.partitionOf(BalanceGroups.VIDEO, 1));
    assertEquals(6, plan.partitionOf(BalanceGroups.VIDEO, 2));
    assertEquals(5, plan.partitionOf(BalanceGroups.VIDEO, 3));
  }

  @Test
  void soloOnlyGroupAndUnmappedRowsPlaceEveryRank() {
    TaskPlan plan = TaskPlan.plan(Arrays.asList(new GroupStats(BalanceGroups.VIDEO, 2, 4 * GIB, 2, 4 * GIB),
        new GroupStats(BalanceGroups.UNMAPPED, 3, 0, 0, 0)), budgets(256 * MIB, GIB), 8);
    assertEquals(0, plan.partitionOf(BalanceGroups.UNMAPPED, 0));
    assertEquals(0, plan.partitionOf(BalanceGroups.UNMAPPED, 2));
    assertEquals(1, plan.partitionOf(BalanceGroups.VIDEO, 0));
    assertEquals(2, plan.partitionOf(BalanceGroups.VIDEO, 1));
    assertThrows(IllegalArgumentException.class, () -> plan.partitionOf(BalanceGroups.VIDEO, -1));
    assertThrows(IllegalArgumentException.class, () -> plan.partitionOf(BalanceGroups.PDF, 0));
  }
}
