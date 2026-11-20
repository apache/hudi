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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.exception.HoodieValidationException;

/**
 * The per-task ceiling on an inline threshold, shared by the blob table and the embedding input store. An inline value is read
 * whole into a task's heap and the writer's buffers grow with it, so a threshold a task cannot hold would exhaust the heap on
 * the first value large enough. The ceiling is
 * <pre>
 *   ceiling = fraction x share,  share = (executor heap - reserved memory) / task slots,  slots = executor cores / spark.task.cpus
 * </pre>
 * This class holds the terms and the maths only; it names no table's keys: {@link #refusal} and {@link #check} take them.
 */
public final class InlineCeiling {

  private static final String SPARK_TASK_CPUS = "spark.task.cpus";

  private final double fraction;
  private final long executorHeapBytes;
  /** Where the executor heap was read, for the derivation text. */
  private final String executorHeapSource;
  private final long reservedBytes;
  private final int executorCores;
  private final int taskCpus;

  public InlineCeiling(double fraction, long executorHeapBytes, String executorHeapSource, long reservedBytes, int executorCores,
                       int taskCpus) {
    this.fraction = fraction;
    this.executorHeapBytes = executorHeapBytes;
    this.executorHeapSource = executorHeapSource;
    this.reservedBytes = reservedBytes;
    this.executorCores = executorCores;
    this.taskCpus = taskCpus;
  }

  /** Task slots of one executor: executor cores / spark.task.cpus, at least 1. */
  public int slots() {
    return Math.max(1, executorCores / Math.max(1, taskCpus));
  }

  /** The heap one task may count on: (executor heap - reserved memory) / slots, at least 0. */
  public long shareBytes() {
    return Math.max(0L, executorHeapBytes - reservedBytes) / slots();
  }

  /** The largest inline threshold allowed: floor(fraction x share). */
  public long ceilingBytes() {
    return (long) Math.floor(fraction * shareBytes());
  }

  /** How the ceiling was computed, every term with its value; {@code fractionKey} names the fraction. */
  public String derivation(String fractionKey) {
    return "ceiling = " + fractionKey + " " + fraction + " x per-task heap share "
        + shareBytes() + " bytes; share = (executor heap " + executorHeapBytes + " bytes [" + executorHeapSource
        + "] - reserved " + reservedBytes + " bytes) / " + slots() + " task slots (executor cores " + executorCores + " / "
        + SPARK_TASK_CPUS + " " + taskCpus + ")";
  }

  /**
   * The refusal of {@code inlineMaxBytes}, or null when it is at or below {@link #ceilingBytes()}.
   *
   * @param where          where the check runs, for the message
   * @param maxBytesKey    the key of the inline threshold
   * @param fractionKey    the key of the heap fraction
   * @param enforceKey     the key that turns the refusal into a warning, named in the remedy; null when there is none
   */
  public String refusal(long inlineMaxBytes, String where, String maxBytesKey, String fractionKey, String enforceKey) {
    long ceiling = ceilingBytes();
    if (inlineMaxBytes <= ceiling) {
      return null;
    }
    return maxBytesKey + "=" + inlineMaxBytes + " is above the per-task inline ceiling "
        + ceiling + " bytes (checked " + where + "): " + derivation(fractionKey)
        + ". An inline file is held whole in one task's heap and the writer's buffers grow with it, so this threshold can exhaust "
        + "the heap. Lower " + maxBytesKey + ", give each task more heap (executor memory, fewer cores "
        + "per executor or more " + SPARK_TASK_CPUS + ")"
        + (enforceKey == null ? "." : ", or set " + enforceKey + "=false to run with it anyway: the hard memory cap then no longer "
        + "holds.");
  }

  /**
   * Refuses {@code inlineMaxBytes} above {@link #ceilingBytes()}; there is no switch that turns the refusal into a warning.
   *
   * @param where       where the check runs, for the message
   * @param maxBytesKey the key of the inline threshold
   * @param fractionKey the key of the heap fraction
   * @throws HoodieValidationException when the threshold is above the ceiling
   */
  public void check(long inlineMaxBytes, String where, String maxBytesKey, String fractionKey) {
    String refusal = refusal(inlineMaxBytes, where, maxBytesKey, fractionKey, null);
    if (refusal != null) {
      throw new HoodieValidationException(refusal);
    }
  }
}
