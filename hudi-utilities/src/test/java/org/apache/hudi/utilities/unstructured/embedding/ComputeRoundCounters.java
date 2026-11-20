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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.sql.SparkSession;

/**
 * The compute counters of one compute round on the streamer ({@link EmbeddingStreamers#round}): the compute transformer
 * registers fresh named accumulators every round ({@code ComputeCounters.register}, names {@code embedding.compute.*}) and
 * returns none of them to the caller, so this listener sums the updates the round's successful tasks report for those names
 * ({@link RoundAccumulators}). Rows embedded, rows retried in step, rows failed for good, batches
 * and the rate-limit wait are what a test asserts of a compute round (including rate-limit pacing).
 */
public final class ComputeRoundCounters extends RoundAccumulators {

  private ComputeRoundCounters(SparkSession spark) {
    super(spark, "embedding.compute.");
  }

  /** A round's result with the counters its tasks reported. */
  public static final class Counted {
    public final EmbeddingStreamers.Round round;
    public final ComputeRoundCounters counters;

    public Counted(EmbeddingStreamers.Round round, ComputeRoundCounters counters) {
      this.round = round;
      this.counters = counters;
    }

    @Override
    public String toString() {
      return round + " " + counters;
    }
  }

  /** Starts listening on {@code spark}; {@link #stop} before reading. */
  public static ComputeRoundCounters listen(SparkSession spark) {
    return register(new ComputeRoundCounters(spark));
  }

  /** One embedding round with its counters. */
  public static Counted round(SparkSession spark, TypedProperties props) {
    ComputeRoundCounters counters = listen(spark);
    try {
      return new Counted(EmbeddingStreamers.round(spark, props), counters);
    } finally {
      counters.stop();
    }
  }

  public long rowsEmbedded() {
    return get("rowsEmbedded");
  }

  public long rowsRetried() {
    return get("rowsRetried");
  }

  public long rowsPermanent() {
    return get("rowsPermanent");
  }

  public long batchesOk() {
    return get("batchesOk");
  }

  public long batchesFailed() {
    return get("batchesFailed");
  }

  public long rateLimitWaitMillis() {
    return get("rateLimitWaitMillis");
  }
}
