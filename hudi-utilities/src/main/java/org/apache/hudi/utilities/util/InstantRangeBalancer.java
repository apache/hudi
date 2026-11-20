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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Range-repartitions phase-2 findings by writing instant into contiguous, balanced partitions, so a
 * task that reaches the archived timeline reads one archive range rather than scattered instants.
 * An instant is never split, so a hot instant gets an oversized partition of its own.
 */
public class InstantRangeBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(InstantRangeBalancer.class);

  private InstantRangeBalancer() {
  }

  /** Upper bound on records per partition, and the absolute size at which an instant is hot. */
  static final int TARGET_RECORDS_PER_PARTITION = 10_000;

  /**
   * How many partitions phase 2 uses, floored by default parallelism because fewer partitions means
   * each task scans a wider archive range.
   *
   * @param totalRecords       every record that will cross the shuffle
   * @param defaultParallelism {@code JavaSparkContext#defaultParallelism()}
   * @return at least 1, whatever the inputs
   */
  @VisibleForTesting
  static int partitionCount(long totalRecords, int defaultParallelism) {
    long byRecords = (long) Math.ceil((double) totalRecords / TARGET_RECORDS_PER_PARTITION);
    long floored = Math.max(byRecords, defaultParallelism);
    return (int) Math.max(1L, Math.min(floored, Integer.MAX_VALUE));
  }

  /** The per-partition record target, shared by {@link #computeBounds} and {@link #logHotInstants}. */
  private static long targetPerPartition(long totalRecords, int numParts) {
    return (long) Math.ceil((double) totalRecords / numParts);
  }

  /**
   * The ascending, distinct last instant of each partition but the last, from a greedy sweep over
   * exact counts. The largest instant is never a bound, so no partition is empty; the result may
   * hold fewer or more than {@code numParts - 1} bounds.
   *
   * @param counts   records per writing instant, exact; values must be non-null
   * @param numParts the partition count {@link #partitionCount} chose
   */
  @VisibleForTesting
  static List<String> computeBounds(Map<String, Long> counts, int numParts) {
    if (counts.isEmpty() || numParts <= 1) {
      return Collections.emptyList();
    }
    List<String> sorted = new ArrayList<>(counts.keySet());
    Collections.sort(sorted);

    long total = 0;
    for (Long count : counts.values()) {
      total += count;
    }
    long target = targetPerPartition(total, numParts);

    // Isolating a hot instant can cost two bounds, so widen the budget or the tail collapses into one
    // partition. The target stays fixed because it defines what "hot" means.
    int hotInstants = 0;
    for (Long count : counts.values()) {
      if (count >= target) {
        hotInstants++;
      }
    }
    int budget = numParts + hotInstants;

    List<String> bounds = new ArrayList<>();
    long running = 0;
    for (int i = 0; i < sorted.size() - 1 && bounds.size() < budget - 1; i++) {
      running += counts.get(sorted.get(i));
      long next = counts.get(sorted.get(i + 1));
      // Close on reaching the share, or ahead of a hot next instant so it gets a partition to itself.
      if (running >= target || (running > 0 && next >= target)) {
        bounds.add(sorted.get(i));
        running = 0;
      }
    }
    return Collections.unmodifiableList(bounds);
  }

  /**
   * Exact record count per writing instant, as a mutable driver-side map. Runs its own Spark job.
   *
   * @param keyed phase-2 records keyed by the instant that wrote each one; keys must be non-null
   */
  public static <V> Map<String, Long> countByInstant(JavaPairRDD<String, V> keyed) {
    return new HashMap<>(keyed.mapValues(value -> 1L).reduceByKey(Long::sum).collectAsMap());
  }

  /**
   * Range-repartitions {@code keyed} by instant, sorted within each partition so archive reads batch.
   * Persist {@code keyed}'s lineage first: this and {@link #countByInstant} each run a job over it.
   *
   * @param jsc    the context whose {@code defaultParallelism} floors the partition count
   * @param keyed  phase-2 records keyed by writing instant; keys must be non-null
   * @param counts records per instant, from {@link #countByInstant}
   * @return the repartitioned records with the instant key dropped
   */
  public static <V> JavaRDD<V> repartition(JavaSparkContext jsc,
                                           JavaPairRDD<String, V> keyed,
                                           Map<String, Long> counts) {
    ValidationUtils.checkArgument(counts != null, "InstantRangeBalancer requires instant counts");
    long total = 0;
    for (Long count : counts.values()) {
      total += count;
    }
    int numParts = partitionCount(total, jsc.defaultParallelism());
    long target = targetPerPartition(total, numParts);
    List<String> bounds = computeBounds(counts, numParts);
    logHotInstants(counts, total, target);
    LOG.info("Range-repartitioning {} phase-2 record(s) over {} distinct instant(s) into {} "
            + "partition(s) (target {} record(s) each)",
        total, counts.size(), bounds.size() + 1, target);
    return keyed.repartitionAndSortWithinPartitions(new InstantRangePartitioner(bounds)).values();
  }

  /**
   * Warns for an instant at or above both the run's {@code target} and the absolute
   * {@link #TARGET_RECORDS_PER_PARTITION}; the run-relative test alone fires on large clusters.
   */
  private static void logHotInstants(Map<String, Long> counts, long total, long target) {
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      if (entry.getValue() >= target && entry.getValue() >= TARGET_RECORDS_PER_PARTITION) {
        LOG.warn("Instant {} holds {} of this run's {} phase-2 record(s), at least the {} record "
                + "target this run's sweep used; its partition is oversized and will dominate the "
                + "phase-2 wall clock. A single instant cannot be split across a contiguous range, so "
                + "this is expected on a table damaged wholesale at one instant and is not itself a "
                + "finding",
            entry.getKey(), entry.getValue(), total, target);
      }
    }
  }
}
