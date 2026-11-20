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

import org.apache.spark.Success$;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Sums the updates that a round's successful tasks report for the named accumulators under one prefix: the embedding
 * transformer registers fresh named counters every round and returns none of them to the caller (a retried Spark task counts
 * again, as the counters do). Listen before the round and {@link #stop} before reading.
 */
public abstract class RoundAccumulators extends SparkListener {

  private final Map<String, Long> totals = new ConcurrentHashMap<>();
  private final SparkSession spark;
  private final String prefix;
  private volatile boolean stopped;

  protected RoundAccumulators(SparkSession spark, String prefix) {
    this.spark = spark;
    this.prefix = prefix;
  }

  /** Registers {@code accumulators} on its session's listener bus. */
  protected static <T extends RoundAccumulators> T register(T accumulators) {
    RoundAccumulators listener = accumulators;
    listener.spark.sparkContext().addSparkListener(listener);
    return accumulators;
  }

  /** Waits for the listener bus to deliver every event so far, then stops listening. */
  public RoundAccumulators stop() {
    if (stopped) {
      return this;
    }
    try {
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } catch (TimeoutException e) {
      throw new IllegalStateException("The Spark listener bus did not drain", e);
    } finally {
      spark.sparkContext().removeSparkListener(this);
      stopped = true;
    }
    return this;
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    if (!Success$.MODULE$.equals(taskEnd.reason())) {
      return;
    }
    for (AccumulableInfo info : scala.collection.JavaConverters.seqAsJavaList(taskEnd.taskInfo().accumulables())) {
      if (info.name().isDefined() && info.name().get().startsWith(prefix) && info.update().isDefined()
          && info.update().get() instanceof Long) {
        totals.merge(info.name().get().substring(prefix.length()), (Long) info.update().get(), Long::sum);
      }
    }
  }

  /** The total of the counter {@code name} (without the prefix), 0 when no task reported it. */
  protected long get(String name) {
    return totals.getOrDefault(name, 0L);
  }

  @Override
  public String toString() {
    return "counters=" + totals;
  }
}
