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

import org.apache.spark.sql.SparkSession;

/**
 * The prep counters of the rounds run while listening: the prep round registers fresh named accumulators every round
 * ({@code PrepCounters.register}, names {@code embedding.prep.*}) and returns none of them to the caller, so this listener sums
 * the updates its successful tasks report ({@link RoundAccumulators}). A file prepared twice in a round counts twice here,
 * though the write collapses its duplicate rows.
 */
public final class PrepRoundCounters extends RoundAccumulators {

  private PrepRoundCounters(SparkSession spark) {
    super(spark, "embedding.prep.");
  }

  /** Starts listening on {@code spark}; {@link #stop} before reading. */
  public static PrepRoundCounters listen(SparkSession spark) {
    return register(new PrepRoundCounters(spark));
  }

  /** Files prepared without a failure. */
  public long filesOk() {
    return get("filesOk");
  }

  public long filesFailed() {
    return get("filesFailed");
  }

  /** Chunks the preprocessors emitted. */
  public long chunks() {
    return get("chunks");
  }
}
