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

package org.apache.hudi.metrics;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.metrics.Registry;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** What task retries and stage recomputation do to counters collected in a {@link DistributedRegistry}. */
public class TestDistributedRegistryUnderTaskRetry {

  private static final String COUNTER = "keys";

  private JavaSparkContext jsc;

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
  }

  /** {@code local[n, maxFailures]} -- the second number is what makes Spark re-attempt a failed task. */
  private JavaSparkContext retryingContext(String appName) {
    SparkConf conf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[2, 4]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.shuffle.partitions", "4");
    return new JavaSparkContext(conf);
  }

  private static List<Integer> partitionedInput(int numPartitions, int perPartition) {
    List<Integer> data = new ArrayList<>();
    for (int i = 0; i < numPartitions * perPartition; i++) {
      data.add(i);
    }
    return data;
  }

  /**
   * One task fails its first attempt and succeeds on the retry. The count must reflect one pass over
   * the data, not two -- Spark discards the failed attempt's accumulator updates.
   */
  @Test
  public void testFailedAttemptDoesNotInflateCounts() {
    jsc = retryingContext("rli-retry-failed-attempt");
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc);
    Registry registry = context.getOrCreateOwnedRegistry("retryRegistry", "retryRegistry");

    int numPartitions = 4;
    int perPartition = 25;

    long collected = jsc.parallelize(partitionedInput(numPartitions, perPartition), numPartitions)
        .mapPartitions(it -> {
          List<Integer> batch = new ArrayList<>();
          it.forEachRemaining(batch::add);
          registry.add(COUNTER, batch.size());
          // Partition 0 blows up the first time it is tried. maxFailures=4 lets the retry through.
          if (TaskContext.getPartitionId() == 0 && TaskContext.get().attemptNumber() == 0) {
            throw new RuntimeException("induced failure on first attempt of partition 0");
          }
          return Collections.singletonList((long) batch.size()).iterator();
        })
        .reduce(Long::sum);

    long counted = registry.getAllCounts(false).get(COUNTER);
    long expected = (long) numPartitions * perPartition;

    System.out.println("\n===== failed attempt =====");
    System.out.println("  rows actually processed  " + collected);
    System.out.println("  registry counted         " + counted);
    System.out.println("  expected                 " + expected);
    System.out.println("==========================\n");

    assertEquals(expected, collected, "sanity: the retry must have succeeded and produced every row");
    assertEquals(expected, counted,
        "a failed attempt must contribute nothing: Spark ships accumulator updates home only from "
            + "attempts that succeed, which is what makes plain task retries safe for these counters");
  }

  /**
   * The other half, asserted so the caveat is grounded rather than asserted in prose: evaluating the same uncached transformation twice counts twice.
   */
  @Test
  public void testRepeatedEvaluationDoubleCounts() {
    jsc = retryingContext("rli-retry-recompute");
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc);
    Registry registry = context.getOrCreateOwnedRegistry("recomputeRegistry", "recomputeRegistry");

    int numPartitions = 4;
    int perPartition = 25;
    long onePass = (long) numPartitions * perPartition;

    org.apache.spark.api.java.JavaRDD<Long> counted =
        jsc.parallelize(partitionedInput(numPartitions, perPartition), numPartitions)
            .mapPartitions(it -> {
              List<Integer> batch = new ArrayList<>();
              it.forEachRemaining(batch::add);
              registry.add(COUNTER, batch.size());
              return Collections.singletonList((long) batch.size()).iterator();
            });

    // Two actions over an uncached RDD: the transformation runs twice.
    counted.reduce(Long::sum);
    counted.reduce(Long::sum);

    long total = registry.getAllCounts(false).get(COUNTER);

    System.out.println("\n===== repeated evaluation =====");
    System.out.println("  one pass would be        " + onePass);
    System.out.println("  registry counted         " + total);
    System.out.println("===============================\n");

    assertEquals(onePass * 2, total,
        "counters incremented inside a transformation are at-least-once: a second evaluation of the "
            + "same uncached RDD counts again. Documented behaviour, pinned here so a change in either "
            + "direction is visible");
  }
}
