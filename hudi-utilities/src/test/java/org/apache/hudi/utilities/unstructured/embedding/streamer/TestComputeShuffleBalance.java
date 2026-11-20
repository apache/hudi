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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeRound;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compute shuffle: one hash shuffle over {@code hoodie.unstructured.embedding.compute.parallelism} tasks (default the Spark
 * context's default parallelism), so every row reaches exactly one task, task row counts are balanced in expectation whatever the
 * skew of the prepared rows, and each row's task depends on its own key columns and the parallelism alone.
 */
@Tag("functional")
public class TestComputeShuffleBalance extends SparkClientFunctionalTestHarness {

  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false);

  /** {@code sizes[i]} rows in input partition i, keys k, then i and j with i and j zero-padded. */
  private Dataset<Row> skewed(int... sizes) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < sizes.length; i++) {
      for (int j = 0; j < sizes[i]; j++) {
        rows.add(RowFactory.create(String.format("k%02d_%04d", i, j), 101L, 17L, "text", 0L));
      }
    }
    // one input partition per size, in order
    List<JavaRDD<Row>> parts = new ArrayList<>();
    int from = 0;
    for (int size : sizes) {
      parts.add(jsc().parallelize(rows.subList(from, from + size), 1));
      from += size;
    }
    JavaRDD<Row> rdd = parts.get(0);
    for (int i = 1; i < parts.size(); i++) {
      rdd = rdd.union(parts.get(i));
    }
    return spark().createDataFrame(rdd, SCHEMA);
  }

  private static List<Integer> counts(JavaRDD<Row> rows) {
    return rows.mapPartitions(iterator -> {
      int n = 0;
      while (iterator.hasNext()) {
        iterator.next();
        n++;
      }
      return Collections.singletonList(n).iterator();
    }).collect();
  }

  /** The rows of {@code dealt}, sorted, checking that each input row arrives exactly once. */
  private static void assertComplete(Dataset<Row> input, JavaRDD<Row> dealt) {
    List<String> expected = new ArrayList<>();
    input.collectAsList().forEach(row -> expected.add(row.toString()));
    List<String> actual = new ArrayList<>();
    dealt.collect().forEach(row -> actual.add(row.toString()));
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual, "every row dealt exactly once");
  }

  @Test
  public void skewedInputIsDealtToEveryTaskOnce() {
    Dataset<Row> input = skewed(61, 1, 1, 1);
    List<Integer> before = counts(input.javaRDD());
    assertEquals(61, (int) Collections.max(before), "the fixture is skewed: " + before);
    assertEquals(1, (int) Collections.min(before), before.toString());

    JavaRDD<Row> dealt = ComputeRound.hashPartitionByKey(input, 4);
    List<Integer> after = counts(dealt);

    assertEquals(4, after.size(), "one task per configured parallelism");
    assertEquals(64, after.stream().mapToInt(Integer::intValue).sum());
    assertTrue(Collections.min(after) > 0, "every task gets rows: " + after);
    assertComplete(input, dealt);
  }

  /** At N/P = 1000 rows per task the task counts stay within 10% of the mean, and every task has rows. */
  @Test
  public void largeInputIsBalancedInExpectation() {
    int parallelism = 4;
    Dataset<Row> input = skewed(4000, 0, 0);
    List<Integer> after = counts(ComputeRound.hashPartitionByKey(input, parallelism));
    assertEquals(parallelism, after.size());
    assertEquals(4000, after.stream().mapToInt(Integer::intValue).sum());
    double mean = 4000.0 / parallelism;
    assertTrue(Collections.max(after) <= 1.1 * mean, "max/mean above 1.1: " + after);
    assertTrue(Collections.min(after) > 0, after.toString());
  }

  /** {@code rows} in {@code partitions} Spark partitions, each emitting its rows in a new random order every time it is computed. */
  private Dataset<Row> reshuffledOnEveryCompute(List<Row> rows, int partitions) {
    return spark().createDataFrame(jsc().parallelize(rows, partitions).mapPartitions(iterator -> {
      List<Row> partition = new ArrayList<>();
      iterator.forEachRemaining(partition::add);
      Collections.shuffle(partition, new Random(System.nanoTime()));
      return partition.iterator();
    }), SCHEMA);
  }

  /** The task each dealt row is in, by row, checking that no row is dealt twice. */
  private static Map<Row, Integer> tasksOf(JavaRDD<Row> dealt) {
    List<Map.Entry<Row, Integer>> pairs = dealt.mapPartitionsWithIndex((task, iterator) -> {
      List<Map.Entry<Row, Integer>> out = new ArrayList<>();
      iterator.forEachRemaining(row -> out.add(new AbstractMap.SimpleImmutableEntry<>(row, task)));
      return out.iterator();
    }, false).collect();
    Map<Row, Integer> tasks = new HashMap<>();
    for (Map.Entry<Row, Integer> pair : pairs) {
      assertNull(tasks.put(pair.getKey(), pair.getValue()), "a row dealt twice: " + pair.getKey());
    }
    return tasks;
  }

  private static void assertEveryTask(Map<Row, Integer> tasks, int parallelism, int rows) {
    assertEquals(rows, tasks.size());
    assertTrue(tasks.values().stream().allMatch(task -> task >= 0 && task < parallelism), tasks.values().toString());
  }

  @Test
  public void twoDealsOfTheSameInputAgree() {
    Dataset<Row> input = skewed(20, 5, 0, 7).cache();
    try {
      assertEquals(tasksOf(ComputeRound.hashPartitionByKey(input, 3)), tasksOf(ComputeRound.hashPartitionByKey(input, 3)));
    } finally {
      input.unpersist();
    }
  }

  /**
   * A row's task is a function of the set of keys and the parallelism: the same rows in other input partitionings and orders,
   * a cached input whose row order changes on every compute, and that input read again once its cached blocks are dropped all
   * deal every row to the same task.
   */
  @Test
  public void eachRowsTaskDependsOnTheKeysAndTheParallelismAlone() {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 61; i++) {
      rows.add(RowFactory.create(String.format("sha-%02d", i % 23), 100L + i % 3, 10L + i % 5, i % 4 == 0 ? "image" : "text",
          (long) i));
    }
    List<Row> reversed = new ArrayList<>(rows);
    Collections.reverse(reversed);
    int parallelism = 4;

    Map<Row, Integer> reference = tasksOf(ComputeRound.hashPartitionByKey(spark().createDataFrame(jsc().parallelize(rows, 1), SCHEMA),
        parallelism));
    assertEveryTask(reference, parallelism, rows.size());
    assertEquals(reference, tasksOf(ComputeRound.hashPartitionByKey(spark().createDataFrame(jsc().parallelize(reversed, 5), SCHEMA),
        parallelism)), "the same rows reversed, in five input partitions");
    assertEquals(reference, tasksOf(ComputeRound.hashPartitionByKey(skewedOf(rows, 40, 1, 0, 20), parallelism)),
        "the same rows in skewed input partitions");

    Dataset<Row> reshuffled = reshuffledOnEveryCompute(rows, 3).persist(StorageLevel.MEMORY_ONLY());
    try {
      assertEquals(reference, tasksOf(ComputeRound.hashPartitionByKey(reshuffled, parallelism)), "a cached input that reorders on compute");
      // dropped so the next deal reads the input again, in a new order
      reshuffled.unpersist(true);
      assertEquals(reference, tasksOf(ComputeRound.hashPartitionByKey(reshuffled, parallelism)),
          "that input dealt again once its cache is dropped, so it is read again in a new order");
    } finally {
      reshuffled.unpersist(true);
    }
  }

  /** {@code rows}, in order, in input partitions of {@code sizes} rows each. */
  private Dataset<Row> skewedOf(List<Row> rows, int... sizes) {
    List<JavaRDD<Row>> parts = new ArrayList<>();
    int from = 0;
    for (int size : sizes) {
      parts.add(jsc().parallelize(rows.subList(from, from + size), 1));
      from += size;
    }
    assertEquals(rows.size(), from);
    JavaRDD<Row> rdd = parts.get(0);
    for (int i = 1; i < parts.size(); i++) {
      rdd = rdd.union(parts.get(i));
    }
    return spark().createDataFrame(rdd, SCHEMA);
  }

  /** The number of tasks of each stage that completes while it is registered. */
  private static final class StageTasks extends SparkListener {
    private final List<Integer> tasks = new CopyOnWriteArrayList<>();

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
      tasks.add(stageCompleted.stageInfo().numTasks());
    }
  }

  /**
   * Over a multi-partition input dealt to four tasks, every stage of the deal runs in more than one task, so none runs in a
   * single task over the whole input.
   */
  @Test
  public void noStageOfTheDealRunsInOneTask() throws Exception {
    Map<String, String> settings = new HashMap<>();
    settings.put("spark.sql.adaptive.enabled", "false");
    settings.put("spark.sql.shuffle.partitions", "4");
    Map<String, String> previous = new HashMap<>();
    settings.forEach((key, value) -> {
      previous.put(key, spark().conf().get(key, null));
      spark().conf().set(key, value);
    });
    StageTasks stages = new StageTasks();
    try {
      Dataset<Row> input = skewed(30, 30, 30);
      jsc().sc().listenerBus().waitUntilEmpty();
      jsc().sc().addSparkListener(stages);
      assertEquals(90, ComputeRound.hashPartitionByKey(input, 4).count());
      jsc().sc().listenerBus().waitUntilEmpty();
    } finally {
      jsc().sc().removeSparkListener(stages);
      previous.forEach((key, value) -> {
        if (value == null) {
          spark().conf().unset(key);
        } else {
          spark().conf().set(key, value);
        }
      });
    }
    assertFalse(stages.tasks.isEmpty(), "no stage was observed");
    assertTrue(stages.tasks.stream().allMatch(n -> n > 1), "tasks per stage: " + stages.tasks);
  }

  /**
   * A row's task is pmod(murmur3(the five key columns), P), Spark's hash partitioning, so it depends on the row's own key values
   * and P alone.
   */
  @Test
  public void eachRowsTaskIsTheHashOfItsKeyColumns() {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      rows.add(RowFactory.create("sha-" + i, 100L + i % 3, 10L + i % 7, i % 2 == 0 ? "image" : "text", (long) (i % 4)));
    }
    Dataset<Row> input = spark().createDataFrame(jsc().parallelize(rows, 3), SCHEMA);
    int parallelism = 5;
    Column[] key = EmbeddingTableSchema.KEY_COLUMNS.stream().map(functions::col).toArray(Column[]::new);
    Map<Row, Integer> expected = new HashMap<>();
    input.withColumn("_task", functions.pmod(functions.hash(key), functions.lit(parallelism))).collectAsList()
        .forEach(row -> expected.put(RowFactory.create(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)),
            row.getInt(5)));
    Map<Row, Integer> actual = new HashMap<>();
    tasksOf(ComputeRound.hashPartitionByKey(input, parallelism)).forEach((row, task) -> actual.put(
        RowFactory.create(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)), task));
    assertEquals(expected, actual);
  }

  /** Shuffle bytes written by the tasks that end while it is registered. */
  private static final class ShuffleWrites extends SparkListener {
    private final AtomicLong bytes = new AtomicLong();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.taskMetrics() != null) {
        bytes.addAndGet(taskEnd.taskMetrics().shuffleWriteMetrics().bytesWritten());
      }
    }
  }

  /**
   * The deal is one shuffle of the rows: the shuffle bytes written are those of one copy of the rows, and adaptive execution,
   * which would coalesce a shuffle to its advisory partition size, keeps the P tasks the deal asks for.
   */
  @Test
  public void theDealShufflesTheRowsOnceIntoExactlyPTasksWithAdaptiveExecutionOn() throws Exception {
    // random letters, so the shuffle's compression cannot shrink the rows below their size
    Random random = new Random(7);
    int keyChars = 512;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 400; i++) {
      StringBuilder key = new StringBuilder(String.format("sha-%03d-", i));
      for (int c = 0; c < keyChars; c++) {
        key.append((char) ('a' + random.nextInt(26)));
      }
      rows.add(RowFactory.create(key.toString(), 101L, 17L, "text", (long) i));
    }
    Dataset<Row> input = spark().createDataFrame(jsc().parallelize(rows, 3), SCHEMA).cache();
    assertEquals(400, input.count());
    Map<String, String> settings = new HashMap<>();
    settings.put("spark.sql.adaptive.enabled", "true");
    settings.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
    settings.put("spark.sql.adaptive.advisoryPartitionSizeInBytes", String.valueOf(64L * 1024 * 1024));
    Map<String, String> previous = new HashMap<>();
    settings.forEach((key, value) -> {
      previous.put(key, spark().conf().get(key, null));
      spark().conf().set(key, value);
    });
    ShuffleWrites writes = new ShuffleWrites();
    List<Integer> counts;
    try {
      jsc().sc().listenerBus().waitUntilEmpty();
      jsc().sc().addSparkListener(writes);
      counts = counts(ComputeRound.hashPartitionByKey(input, 7));
      jsc().sc().listenerBus().waitUntilEmpty();
    } finally {
      jsc().sc().removeSparkListener(writes);
      input.unpersist(true);
      previous.forEach((key, value) -> {
        if (value == null) {
          spark().conf().unset(key);
        } else {
          spark().conf().set(key, value);
        }
      });
    }
    assertEquals(7, counts.size(), "adaptive execution keeps the requested task count: " + counts);
    assertEquals(400, counts.stream().mapToInt(Integer::intValue).sum());
    long rowBytes = 400L * keyChars;
    long written = writes.bytes.get();
    assertTrue(written >= rowBytes, "the deal writes every row: " + written + " bytes of " + rowBytes);
    assertTrue(written < rowBytes + rowBytes / 2, "the rows are shuffled once: " + written + " bytes of " + rowBytes);
  }

  @Test
  public void theParallelismDefaultsToTheDefaultParallelismAndMustBePositive() {
    TypedProperties props = new TypedProperties();
    int defaultParallelism = jsc().defaultParallelism();
    assertEquals(defaultParallelism, EmbeddingTableConfig.from(props).computeParallelism(defaultParallelism));
    props.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "7");
    assertEquals(7, EmbeddingTableConfig.from(props).computeParallelism(defaultParallelism));
    props.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "0");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingTableConfig.from(props).computeParallelism(defaultParallelism));
    assertThrows(IllegalArgumentException.class, () -> ComputeRound.hashPartitionByKey(skewed(1), 0));
  }
}
