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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.apache.spark.SparkConf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The parallelism P each group's task floor uses in size/type-aware prep balancing:
 * {@link EmbeddingTableConfig#BALANCE_MIN_PARTITIONS} when set; in local mode ({@code local[n]}) n; else the cores of the most
 * executors the job can have, not only those registered when prep plans its tasks (an autoscaled job may have zero executors up
 * then, and planning for the maximum gives the autoscaler the backlog it scales on):
 * {@code P = max(registered executor cores, maxExecutors × spark.executor.cores)}, where maxExecutors is the first set of
 * {@code spark.dynamicAllocation.maxExecutors} (when dynamic allocation is enabled and the bound is not Spark's unbounded
 * default) and {@code spark.executor.instances}; {@code spark.executor.cores} defaults to 1. With none of them set, P is
 * {@code spark.default.parallelism}.
 */
public final class BalanceParallelism {

  static final String DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled";
  static final String DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors";
  static final String EXECUTOR_INSTANCES = "spark.executor.instances";
  static final String EXECUTOR_CORES = "spark.executor.cores";
  static final String DEFAULT_PARALLELISM = "spark.default.parallelism";
  private static final Pattern LOCAL_MASTER = Pattern.compile("local\\[\\s*(\\*|\\d+)\\s*(,\\s*\\d+\\s*)?]");

  private BalanceParallelism() {
  }

  /** P and the configuration it came from, for the strategy's log line. */
  public static final class Resolved {
    private final int p;
    private final String source;

    Resolved(int p, String source) {
      this.p = p;
      this.source = source;
    }

    public int parallelism() {
      return p;
    }

    public String source() {
      return source;
    }

    @Override
    public String toString() {
      return p + " (" + source + ")";
    }
  }

  /**
   * @param registeredCores    the cores of the executors registered now (the driver's own entry excluded)
   * @param defaultParallelism {@code SparkContext#defaultParallelism()}, the fallback
   * @throws IllegalArgumentException if {@link EmbeddingTableConfig#BALANCE_MIN_PARTITIONS} is set and not positive, or an
   *                                  executor count or {@code spark.executor.cores} read is not an integer
   */
  public static Resolved resolve(TypedProperties props, SparkConf conf, String master, int registeredCores,
                                 int defaultParallelism) {
    if (props.containsKey(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key())) {
      return new Resolved(EmbeddingTableConfig.from(props).balanceMinPartitions(defaultParallelism),
          EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key());
    }
    String trimmed = master == null ? "" : master.trim();
    if (trimmed.equals("local")) {
      return new Resolved(1, "master local");
    }
    Matcher local = LOCAL_MASTER.matcher(trimmed);
    if (local.matches()) {
      int threads = local.group(1).equals("*") ? Runtime.getRuntime().availableProcessors() : Integer.parseInt(local.group(1));
      return new Resolved(Math.max(1, threads), "master " + trimmed);
    }
    String maxKey = maxExecutorsKey(conf);
    if (maxKey == null) {
      return new Resolved(Math.max(1, defaultParallelism), DEFAULT_PARALLELISM);
    }
    long cores = Math.max(1, intConf(conf, EXECUTOR_CORES, 1));
    long configured = Math.max(0, intConf(conf, maxKey, 0)) * cores;
    int p = (int) Math.min(Integer.MAX_VALUE, Math.max(Math.max(1, registeredCores), configured));
    return new Resolved(p, "max(registered executor cores " + registeredCores + ", " + maxKey + " " + conf.get(maxKey)
        + " x " + EXECUTOR_CORES + " " + cores + ")");
  }

  /** @return the key the job's most executors come from, or null when none is set */
  private static String maxExecutorsKey(SparkConf conf) {
    if (conf.get(DYNAMIC_ALLOCATION_ENABLED, "false").trim().equalsIgnoreCase("true")
        && conf.contains(DYNAMIC_ALLOCATION_MAX_EXECUTORS) && intConf(conf, DYNAMIC_ALLOCATION_MAX_EXECUTORS, Integer.MAX_VALUE) != Integer.MAX_VALUE) {
      return DYNAMIC_ALLOCATION_MAX_EXECUTORS;
    }
    return conf.contains(EXECUTOR_INSTANCES) ? EXECUTOR_INSTANCES : null;
  }

  /**
   * @throws IllegalArgumentException naming the key, if its value is not an integer
   */
  private static int intConf(SparkConf conf, String key, int defaultValue) {
    if (!conf.contains(key)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(conf.get(key).trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Spark conf " + key + " must be an integer, got '" + conf.get(key) + "'", e);
    }
  }
}
