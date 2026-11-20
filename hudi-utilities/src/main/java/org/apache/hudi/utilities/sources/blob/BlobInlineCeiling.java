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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.unstructured.storage.InlineCeiling;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The per-task ceiling on the blob table's inline threshold ({@link BlobTableConfig#INLINE_MAX_BYTES}). An inline file is read
 * whole into a task's heap and the writer's buffers grow with the inline value size, so a threshold a task cannot hold would
 * exhaust the heap on the first file large enough. The ceiling is
 * <pre>
 *   ceiling = fraction x share,  share = (executor heap - reserved memory) / task slots,  slots = executor cores / spark.task.cpus
 * </pre>
 * with the fraction {@link BlobTableConfig#INLINE_MAX_HEAP_FRACTION}, the reserved memory Spark's own reserved system memory and
 * the executor heap and cores taken from the Spark conf (local masters: this JVM's heap and the master's thread count). A
 * threshold above the ceiling is refused, never clamped, unless {@link BlobTableConfig#INLINE_CEILING_ENFORCE} is off.
 *
 * <p>On a cluster the driver reads the executor heap from spark.executor.memory while a task reads its JVM's maximum heap, which can be
 * lower (by a survivor space under the parallel collector): a threshold within that margin passes the job-start check and is refused
 * by each task before its first inline read. Spark's system memory override spark.testing.memory is not honoured.
 *
 * <p>The maths and the refusal text are the shared, key-free {@link InlineCeiling}'s; this class reads the terms from the Spark
 * conf and names the blob table's keys.
 */
@Slf4j
@Getter
public final class BlobInlineCeiling {

  /** Spark's reserved system memory outside tests (UnifiedMemoryManager RESERVED_SYSTEM_MEMORY_BYTES). */
  static final long SPARK_RESERVED_SYSTEM_MEMORY_BYTES = 300L * 1024 * 1024;
  static final String SPARK_TESTING = "spark.testing";
  static final String SPARK_TESTING_RESERVED_MEMORY = "spark.testing.reservedMemory";
  static final String SPARK_MASTER = "spark.master";
  static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
  static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  static final String SPARK_TASK_CPUS = "spark.task.cpus";
  private static final long MIB = 1024L * 1024L;
  /** local, local[N], local[*], local[N,F], local[*,F] (spaces allowed around the comma, as Spark does); group 1 is N or *. */
  private static final Pattern LOCAL = Pattern.compile("local(?:\\[([0-9]+|\\*)(?:\\s*,\\s*[0-9]+)?\\])?");
  /** local-cluster[workers,coresPerWorker,memoryPerWorkerMiB]. */
  private static final Pattern LOCAL_CLUSTER = Pattern.compile("local-cluster\\[\\s*[0-9]+\\s*,\\s*([0-9]+)\\s*,\\s*[0-9]+\\s*]");

  private final double fraction;
  private final long executorHeapBytes;
  /** Where the executor heap was read: this JVM's maximum heap, or spark.executor.memory. */
  private final String executorHeapSource;
  private final long reservedBytes;
  private final int executorCores;
  private final int taskCpus;

  BlobInlineCeiling(double fraction, long executorHeapBytes, String executorHeapSource, long reservedBytes, int executorCores,
                    int taskCpus) {
    this.fraction = fraction;
    this.executorHeapBytes = executorHeapBytes;
    this.executorHeapSource = executorHeapSource;
    this.reservedBytes = reservedBytes;
    this.executorCores = executorCores;
    this.taskCpus = taskCpus;
  }

  /**
   * The ceiling as the driver sees it when the job starts, from the Spark conf: in local mode the executor is this JVM, otherwise
   * its heap is spark.executor.memory.
   */
  public static BlobInlineCeiling onDriver(SparkConf conf, double fraction) {
    String master = conf.get(SPARK_MASTER, "");
    if (isLocal(master)) {
      return new BlobInlineCeiling(fraction, Runtime.getRuntime().maxMemory(), "this JVM's max heap", reservedBytes(conf),
          executorCores(conf, master), taskCpus(conf));
    }
    return new BlobInlineCeiling(fraction, conf.getSizeAsMb(SPARK_EXECUTOR_MEMORY, "1g") * MIB, SPARK_EXECUTOR_MEMORY,
        reservedBytes(conf), executorCores(conf, master), taskCpus(conf));
  }

  /** The ceiling inside a task: the executor's own max heap, the slots and reserve from the executor's Spark conf. */
  public static BlobInlineCeiling inTask(SparkConf conf, double fraction) {
    String master = conf.get(SPARK_MASTER, "");
    return new BlobInlineCeiling(fraction, Runtime.getRuntime().maxMemory(), "the executor JVM's max heap", reservedBytes(conf),
        executorCores(conf, master), taskCpus(conf));
  }

  /**
   * Refuses a configured inline threshold above the ceiling of the job described by {@code conf} ({@link #onDriver}).
   *
   * @throws HoodieValidationException when the threshold is above the ceiling and the ceiling is enforced
   * @throws BlobSourceConfigException when the fraction is not in (0, 1] (an IllegalArgumentException)
   */
  public static void checkOnDriver(TypedProperties props, SparkConf conf) {
    onDriver(conf, fraction(props)).check(inlineMaxBytes(props),
        ConfigUtils.getBooleanWithAltKeys(props, BlobTableConfig.INLINE_CEILING_ENFORCE), "on the driver at job start");
  }

  /**
   * The configured inline threshold, refusing by name a value that is not a number or not in range; the blob source runs it
   * when it is built, so those two refusals keep their text and place while the ceiling check runs at the fetch.
   *
   * @throws IllegalArgumentException naming the key when the value is not a number of bytes or not in [0, 2^31-9]
   */
  static long inlineMaxBytes(TypedProperties props) {
    String raw = ConfigUtils.getStringWithAltKeys(props, BlobTableConfig.INLINE_MAX_BYTES,
        String.valueOf(BlobTableConfig.INLINE_MAX_BYTES.defaultValue()));
    try {
      Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(BlobTableConfig.INLINE_MAX_BYTES.key()
          + " must be a number of bytes, got '" + raw + "'", e);
    }
    return BlobTableConfig.inlineMaxBytes(props);
  }

  /** The configured fraction, validated to lie in (0, 1]. */
  public static double fraction(TypedProperties props) {
    String raw = ConfigUtils.getStringWithAltKeys(props, BlobTableConfig.INLINE_MAX_HEAP_FRACTION,
        String.valueOf(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.defaultValue()));
    double fraction;
    try {
      fraction = Double.parseDouble(raw.trim());
    } catch (NumberFormatException e) {
      throw new BlobSourceConfigException(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key()
          + " must be a fraction in (0, 1], got '" + raw + "'", e);
    }
    if (!(fraction > 0 && fraction <= 1)) {
      throw new BlobSourceConfigException(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key() + " must be in (0, 1]: " + fraction);
    }
    return fraction;
  }

  /** True when the executor heap is this JVM's own maximum heap (local mode on the driver, or inside a task). */
  public boolean readsThisJvm() {
    return !SPARK_EXECUTOR_MEMORY.equals(executorHeapSource);
  }

  /** Task slots of one executor: executor cores / spark.task.cpus, at least 1. */
  public int slots() {
    return inlineCeiling().slots();
  }

  /** The heap one task may count on: (executor heap - reserved memory) / slots, at least 0. */
  public long shareBytes() {
    return inlineCeiling().shareBytes();
  }

  /** The largest inline threshold allowed: floor(fraction x share). */
  public long ceilingBytes() {
    return inlineCeiling().ceilingBytes();
  }

  /**
   * Refuses {@code inlineMaxBytes} above {@link #ceilingBytes()}; with {@code enforce} off it is only logged at WARN.
   *
   * @param where where the check runs, for the message
   * @throws HoodieValidationException when the threshold is above the ceiling and {@code enforce} is on
   */
  public void check(long inlineMaxBytes, boolean enforce, String where) {
    String message = inlineCeiling().refusal(inlineMaxBytes, where, BlobTableConfig.INLINE_MAX_BYTES.key(),
        BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), BlobTableConfig.INLINE_CEILING_ENFORCE.key());
    if (message == null) {
      return;
    }
    if (enforce) {
      throw new HoodieValidationException(message);
    }
    log.warn("{} is false, so the threshold is used as configured: {}", BlobTableConfig.INLINE_CEILING_ENFORCE.key(), message);
  }

  /** How the ceiling was computed, every term with its value. */
  public String derivation() {
    return inlineCeiling().derivation(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key());
  }

  /** These terms as the shared, key-free ceiling. */
  private InlineCeiling inlineCeiling() {
    return new InlineCeiling(fraction, executorHeapBytes, executorHeapSource, reservedBytes, executorCores, taskCpus);
  }

  static boolean isLocal(String master) {
    return LOCAL.matcher(master).matches();
  }

  /**
   * Executor cores: local → 1, local[N] → N, local[*] → available processors, local-cluster[n,c,m] → c, any other master →
   * spark.executor.cores (default 1, the YARN and Kubernetes default).
   */
  static int executorCores(SparkConf conf, String master) {
    Matcher local = LOCAL.matcher(master);
    if (local.matches()) {
      String threads = local.group(1);
      if (threads == null) {
        return 1;
      }
      return threads.equals("*") ? Runtime.getRuntime().availableProcessors() : Integer.parseInt(threads);
    }
    Matcher cluster = LOCAL_CLUSTER.matcher(master);
    if (cluster.matches()) {
      return Integer.parseInt(cluster.group(1));
    }
    return conf.getInt(SPARK_EXECUTOR_CORES, 1);
  }

  static int taskCpus(SparkConf conf) {
    return conf.getInt(SPARK_TASK_CPUS, 1);
  }

  /**
   * Spark's reserved system memory, by the rule of Spark's UnifiedMemoryManager, never negative: Spark's testing keys
   * (spark.testing, spark.testing.reservedMemory) move it, and with it the ceiling.
   */
  static long reservedBytes(SparkConf conf) {
    return Math.max(0L,
        conf.getLong(SPARK_TESTING_RESERVED_MEMORY, conf.contains(SPARK_TESTING) ? 0L : SPARK_RESERVED_SYSTEM_MEMORY_BYTES));
  }
}
