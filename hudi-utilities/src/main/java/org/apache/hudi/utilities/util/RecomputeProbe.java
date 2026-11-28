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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 * Tells whether a cached RDD's lambda ran again after its barrier action, which would mean a lost
 * block was recomputed after the timeline pin. A cache hit never invokes the lambda, and Spark merges
 * a {@code ResultTask}'s accumulator update exactly when it keeps that output, so no rise proves no
 * recomputed record was consumed; shuffle tasks, retries and speculation can only over-report.
 */
public final class RecomputeProbe {

  private final LongAccumulator tasks;
  private final long expectedTasks;

  private RecomputeProbe(LongAccumulator tasks, long expectedTasks) {
    this.tasks = tasks;
    this.expectedTasks = expectedTasks;
  }

  /**
   * Registers the counter, before the RDD that will bump it is built.
   *
   * @param expectedTasks how many tasks the phase-1 fan-out is split across
   */
  public static RecomputeProbe start(JavaSparkContext jsc, String name, int expectedTasks) {
    return new RecomputeProbe(jsc.sc().longAccumulator(name), expectedTasks);
  }

  /**
   * The counter to hand into the RDD's lambda, which must bump it exactly once per invocation and
   * before anything that can throw.
   */
  public LongAccumulator counter() {
    return tasks;
  }

  /** Takes the barrier reading; call only after the action forcing phase 1 has returned. */
  public Baseline seal() {
    return new Baseline(tasks.value());
  }

  /** The barrier reading; later calls re-read the live counter, so ask only between actions. */
  public final class Baseline {

    private final long baseline;

    private Baseline(long baseline) {
      this.baseline = baseline;
    }

    /** Whether every phase-1 task has run; over-counting from retries still proves that. */
    public boolean materialised() {
      return expectedTasks > 0 && baseline >= expectedTasks;
    }

    /** Whether the RDD's lambda has been invoked again since the seal. */
    public boolean recomputed() {
      return tasks.value() > baseline;
    }

    /** What ran at the barrier; over-counted by any speculative or retried task. */
    public long observedTasks() {
      return baseline;
    }

    /** How many tasks the fan-out was split across. */
    public long expectedTasks() {
      return expectedTasks;
    }

    /** Why the run is inconclusive, in the form an operator can act on. */
    public String describe() {
      return "phase 1 recomputed " + (tasks.value() - baseline) + " more time(s) after the "
          + "barrier had already forced it to completion in " + baseline + " task run(s) across "
          + expectedTasks + " task(s); a cached block was lost and its partition recomputed, so at "
          + "least one record phase 2 consumed describes storage after the timeline this run "
          + "pins to";
    }
  }
}
