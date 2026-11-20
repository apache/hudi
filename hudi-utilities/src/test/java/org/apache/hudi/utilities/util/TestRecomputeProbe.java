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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The probe's contract, asked of a real accumulator rather than a mock: the whole point of the
 * mechanism is that the driver cannot set the number for itself, so a test that set it for itself
 * would assert nothing about what the mechanism does.
 */
class TestRecomputeProbe {

  private static JavaSparkContext jsc;

  @BeforeAll
  static void startSpark() {
    jsc = new JavaSparkContext(new SparkConf()
        .setAppName("TestRecomputeProbe")
        .setMaster("local[2]"));
  }

  @AfterAll
  static void stopSpark() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  /**
   * The barrier, asked of the executors. A phase 1 whose forcing action was replaced by a lazy
   * transformation arrives here having run no task at all, and that is the regression this answers.
   *
   * <p>{@code seal()} reports rather than throws: the two dimensions raise their own refusals, with
   * their own wording, and a probe that threw first would take that wording away from them.
   */
  @Test
  void phaseOneNoExecutorRanIsNotMaterialised() {
    RecomputeProbe.Baseline baseline = RecomputeProbe.start(jsc, "testRecomputeNoTasks", 4).seal();

    assertFalse(baseline.materialised());
    assertEquals(0L, baseline.observedTasks());
    assertEquals(4L, baseline.expectedTasks());
  }

  @Test
  void partiallyRunPhaseOneIsNotMaterialised() {
    RecomputeProbe probe = RecomputeProbe.start(jsc, "testRecomputePartial", 4);
    probe.counter().add(3);

    assertFalse(probe.seal().materialised(), "\"some tasks ran\" is not the property");
  }

  /** A fan-out with no tasks at all can prove nothing, so it is not materialised either. */
  @Test
  void anEmptyFanOutIsNotMaterialised() {
    assertFalse(RecomputeProbe.start(jsc, "testRecomputeNoPartitions", 0).seal().materialised());
  }

  /**
   * Over-counting at the barrier is accepted for the reason {@code PhaseOne.isMaterialised} already
   * documents: a speculative or retried task means the listings certainly happened, which is what
   * is being asked. Under-counting cannot mean that.
   */
  @Test
  void anOverCountedPhaseOneIsMaterialised() {
    RecomputeProbe probe = RecomputeProbe.start(jsc, "testRecomputeOverCounted", 4);
    probe.counter().add(5);

    RecomputeProbe.Baseline baseline = probe.seal();

    assertTrue(baseline.materialised());
    assertEquals(5L, baseline.observedTasks());
    assertEquals(4L, baseline.expectedTasks());
    assertFalse(baseline.recomputed(), "the baseline is the reading itself and cannot exceed itself");
  }

  @Test
  void sealedBaselineReportsNoRecomputeUntilTheCounterMoves() {
    RecomputeProbe probe = RecomputeProbe.start(jsc, "testRecomputeQuiet", 4);
    probe.counter().add(4);
    RecomputeProbe.Baseline baseline = probe.seal();

    assertFalse(baseline.recomputed());

    probe.counter().add(1);

    assertTrue(baseline.recomputed(), "one extra invocation of the RDD's lambda is one recompute");
    assertTrue(baseline.describe().contains("1"), baseline.describe());
    assertTrue(baseline.describe().contains("4"), baseline.describe());
  }

  /**
   * The counter reaches the driver from an executor, which is the only reading that matters: a
   * probe that only worked for driver-side {@code add} calls would pass every test here and detect
   * nothing in production.
   */
  @Test
  void theCounterCountsInvocationsOnExecutors() {
    RecomputeProbe probe = RecomputeProbe.start(jsc, "testRecomputeOnExecutors", 3);
    LongAccumulator counter = probe.counter();
    jsc.parallelize(Arrays.asList("a", "b", "c"), 3)
        .mapPartitions(partition -> {
          counter.add(1);
          return partition;
        })
        .count();

    RecomputeProbe.Baseline baseline = probe.seal();

    assertEquals(3L, baseline.observedTasks());
    assertFalse(baseline.recomputed());
  }
}
