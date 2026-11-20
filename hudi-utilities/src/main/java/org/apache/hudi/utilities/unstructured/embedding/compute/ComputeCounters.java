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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

/**
 * Embedding-compute counters, summed across Spark tasks (a retried Spark task counts again). A batch is failed when its call
 * threw or answered malformed (its inputs may still be embedded by their later tries); each processed row counts once, by its
 * final outcome: embedded or skipped for good. {@link #getRowsRetried()} counts the tries after a
 * row's first.
 */
public class ComputeCounters implements Serializable {

  private static final long serialVersionUID = 1L;

  private final LongAccumulator batchesOk;
  private final LongAccumulator batchesFailed;
  private final LongAccumulator rowsEmbedded;
  private final LongAccumulator rowsRetried;
  private final LongAccumulator rowsPermanent;
  private final LongAccumulator rateLimitWaitMillis;
  private final LongAccumulator callsRefused;

  /**
   * Counters with fresh, unregistered rate-limit wait and refusal accumulators: driver-local only (tests that run the iterator in
   * the driver). Spark refuses to ship an unregistered accumulator to executors; production uses {@link #register}.
   */
  public ComputeCounters(LongAccumulator batchesOk, LongAccumulator batchesFailed, LongAccumulator rowsEmbedded,
                         LongAccumulator rowsRetried, LongAccumulator rowsPermanent) {
    this(batchesOk, batchesFailed, rowsEmbedded, rowsRetried, rowsPermanent, new LongAccumulator());
  }

  public ComputeCounters(LongAccumulator batchesOk, LongAccumulator batchesFailed, LongAccumulator rowsEmbedded,
                         LongAccumulator rowsRetried, LongAccumulator rowsPermanent, LongAccumulator rateLimitWaitMillis) {
    this(batchesOk, batchesFailed, rowsEmbedded, rowsRetried, rowsPermanent, rateLimitWaitMillis, new LongAccumulator());
  }

  public ComputeCounters(LongAccumulator batchesOk, LongAccumulator batchesFailed, LongAccumulator rowsEmbedded,
                         LongAccumulator rowsRetried, LongAccumulator rowsPermanent, LongAccumulator rateLimitWaitMillis,
                         LongAccumulator callsRefused) {
    this.batchesOk = batchesOk;
    this.batchesFailed = batchesFailed;
    this.rowsEmbedded = rowsEmbedded;
    this.rowsRetried = rowsRetried;
    this.rowsPermanent = rowsPermanent;
    this.rateLimitWaitMillis = rateLimitWaitMillis;
    this.callsRefused = callsRefused;
  }

  public static ComputeCounters register(SparkContext sc) {
    return new ComputeCounters(sc.longAccumulator("embedding.compute.batchesOk"),
        sc.longAccumulator("embedding.compute.batchesFailed"), sc.longAccumulator("embedding.compute.rowsEmbedded"),
        sc.longAccumulator("embedding.compute.rowsRetried"), sc.longAccumulator("embedding.compute.rowsPermanent"),
        sc.longAccumulator("embedding.compute.rateLimitWaitMillis"), sc.longAccumulator("embedding.compute.callsRefused"));
  }

  public LongAccumulator getBatchesOk() {
    return batchesOk;
  }

  public LongAccumulator getBatchesFailed() {
    return batchesFailed;
  }

  public LongAccumulator getRowsEmbedded() {
    return rowsEmbedded;
  }

  /** Tries of a row after its first (in-step retries). */
  public LongAccumulator getRowsRetried() {
    return rowsRetried;
  }

  /** Rows skipped for good (PERMANENT). */
  public LongAccumulator getRowsPermanent() {
    return rowsPermanent;
  }

  /**
   * Milliseconds the model's clients waited for rate-limit tokens ({@link RequestRateLimiter}), summed over the calls of every
   * compute task: pacing, as opposed to a slow provider.
   */
  public LongAccumulator getRateLimitWaitMillis() {
    return rateLimitWaitMillis;
  }

  /**
   * Compute tasks the provider refused permanently ({@link EmbeddingClientConfigException}): each such task stops at its first
   * refusal and ends normally with one marker row ({@link ComputeRound#REFUSED_PREFIX}), and the driver fails the round.
   */
  public LongAccumulator getCallsRefused() {
    return callsRefused;
  }
}
