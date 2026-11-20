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
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HashBalancingStrategy}: one hash shuffle of the blob rows by their source version over exactly P partitions, with
 * adaptive execution on, so a row's partition is {@code pmod(murmur3(version), P)} whatever the input's partitioning or order, a
 * row with a null version column is kept, and partition counts are balanced in expectation.
 */
public class TestHashBalancingStrategy extends SparkClientFunctionalTestHarness {

  private static final StructType ROWS = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.BLOB, DataTypes.BinaryType, false)});

  private static final String[] ADAPTIVE_KEYS = {"spark.sql.adaptive.enabled", "spark.sql.adaptive.coalescePartitions.enabled",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes"};

  private final Map<String, String> previous = new HashMap<>();

  /** Adaptive execution on, coalescing to a 64 MiB advisory size: it would merge every small shuffle into one partition. */
  @BeforeEach
  public void adaptiveExecutionOn() {
    String[] values = {"true", "true", String.valueOf(64L * 1024 * 1024)};
    for (int i = 0; i < ADAPTIVE_KEYS.length; i++) {
      previous.put(ADAPTIVE_KEYS[i], spark().conf().get(ADAPTIVE_KEYS[i], null));
      spark().conf().set(ADAPTIVE_KEYS[i], values[i]);
    }
  }

  @AfterEach
  public void restoreConf() {
    previous.forEach((key, value) -> {
      if (value == null) {
        spark().conf().unset(key);
      } else {
        spark().conf().set(key, value);
      }
    });
    previous.clear();
  }

  private static TypedProperties props(int partitions) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), String.valueOf(partitions));
    return props;
  }

  private static Row file(String sha, Long modificationTime, Long size, int blob) {
    return RowFactory.create(sha, modificationTime, size, "pdf", new byte[] {(byte) blob});
  }

  private static List<Row> files(int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(file("sha-" + i, 1000L + i % 7, 10L + i % 3, i));
    }
    return rows;
  }

  private static String versionOf(Row row) {
    return row.get(0) + "/" + row.get(1) + "/" + row.get(2);
  }

  private static Column[] versionKey() {
    return BalancingAssignment.KEY_COLUMNS.stream().map(functions::col).toArray(Column[]::new);
  }

  /** The partition each row is in, by version, checking that no version arrives twice. */
  private static Map<String, Integer> placed(JavaRDD<Row> rows) {
    List<Tuple2<String, Integer>> pairs = rows.mapPartitionsWithIndex((index, iterator) -> {
      List<Tuple2<String, Integer>> out = new ArrayList<>();
      iterator.forEachRemaining(row -> out.add(new Tuple2<>(versionOf(row), index)));
      return out.iterator();
    }, false).collect();
    Map<String, Integer> result = new HashMap<>();
    for (Tuple2<String, Integer> pair : pairs) {
      assertNull(result.put(pair._1, pair._2), "a version placed twice: " + pair._1);
    }
    return result;
  }

  private static List<Integer> counts(JavaRDD<Row> rows) {
    return rows.glom().collect().stream().map(List::size).collect(Collectors.toList());
  }

  @Test
  public void givesExactlyPPartitionsWithAdaptiveExecutionOn() {
    Dataset<Row> batch = spark().createDataFrame(jsc().parallelize(files(40), 3), ROWS);
    try (BalancedRows balanced = new HashBalancingStrategy().balance(batch, props(7))) {
      assertEquals(7, balanced.getRows().getNumPartitions());
      List<Integer> counts = counts(balanced.getRows());
      assertEquals(7, counts.size(), "adaptive execution keeps the requested partition count: " + counts);
      assertEquals(40, counts.stream().mapToInt(Integer::intValue).sum());
      assertFalse(balanced.getAssignment().isPresent(), "the hash strategy has no assignment");
    }
  }

  @Test
  public void defaultsToTheDefaultParallelism() {
    Dataset<Row> batch = spark().createDataFrame(jsc().parallelize(files(5), 2), ROWS);
    try (BalancedRows balanced = new HashBalancingStrategy().balance(batch, new TypedProperties())) {
      assertEquals(jsc().defaultParallelism(), balanced.getRows().getNumPartitions());
    }
  }

  /**
   * A row's partition is {@code pmod(murmur3(sha, modification time, size), P)}, so the same rows in another order, in other input
   * partitionings, in an input whose order changes on every compute, and in that input read again once its cache is dropped, land
   * in the same partition.
   */
  @Test
  public void theSameRowLandsInTheSamePartitionAcrossReshuffledInput() {
    List<Row> rows = files(60);
    List<Row> reversed = new ArrayList<>(rows);
    Collections.reverse(reversed);
    int partitions = 4;
    Map<String, Integer> expected = new HashMap<>();
    spark().createDataFrame(rows, ROWS).withColumn("_p", functions.pmod(functions.hash(versionKey()), functions.lit(partitions)))
        .collectAsList().forEach(row -> expected.put(versionOf(row), row.getInt(5)));
    HashBalancingStrategy strategy = new HashBalancingStrategy();

    assertEquals(expected, placed(strategy.balance(spark().createDataFrame(jsc().parallelize(rows, 1), ROWS), props(partitions))
        .getRows()), "one input partition");
    assertEquals(expected, placed(strategy.balance(spark().createDataFrame(jsc().parallelize(reversed, 5), ROWS), props(partitions))
        .getRows()), "reversed, in five input partitions");
    Dataset<Row> reshuffled = spark().createDataFrame(jsc().parallelize(rows, 3).mapPartitions(iterator -> {
      List<Row> partition = new ArrayList<>();
      iterator.forEachRemaining(partition::add);
      Collections.shuffle(partition, new Random(System.nanoTime()));
      return partition.iterator();
    }), ROWS).persist(StorageLevel.MEMORY_ONLY());
    try {
      assertEquals(expected, placed(strategy.balance(reshuffled, props(partitions)).getRows()),
          "an input that reorders on every compute");
      // dropped, so the next spread reads the input again, in a new order
      reshuffled.unpersist(true);
      assertEquals(expected, placed(strategy.balance(reshuffled, props(partitions)).getRows()),
          "that input spread again once its cache is dropped");
    } finally {
      reshuffled.unpersist(true);
    }
  }

  /** The hash skips a null key column, so a row whose modification time or size is null is kept, whole. */
  @Test
  public void rowWithANullKeyColumnIsKept() {
    Dataset<Row> batch = spark().createDataFrame(Arrays.asList(file("sha-a", 1000L, 5L, 1), file("sha-b", null, 5L, 2),
        file("sha-c", 1000L, null, 3), file("sha-d", null, null, 4)), ROWS);
    try (BalancedRows balanced = new HashBalancingStrategy().balance(batch, props(3))) {
      List<Row> rows = balanced.getRows().collect().stream().sorted((a, b) -> a.getString(0).compareTo(b.getString(0)))
          .collect(Collectors.toList());
      assertEquals(Arrays.asList("sha-a/1000/5", "sha-b/null/5", "sha-c/1000/null", "sha-d/null/null"),
          rows.stream().map(TestHashBalancingStrategy::versionOf).collect(Collectors.toList()));
      for (int i = 0; i < rows.size(); i++) {
        assertEquals(ROWS, rows.get(i).schema());
        assertEquals("pdf", rows.get(i).getString(3));
        assertArrayEquals(new byte[] {(byte) (i + 1)}, (byte[]) rows.get(i).get(4));
      }
    }
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
   * The physical plan is one hash exchange over the version key and no join, sort or aggregate, and running it writes the blob
   * rows to the shuffle once.
   */
  @Test
  public void thePhysicalPlanHasOneShuffleAndNoJoin() throws Exception {
    int files = 200;
    int blobBytes = 4096;
    Random random = new Random(11);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < files; i++) {
      byte[] blob = new byte[blobBytes];
      random.nextBytes(blob);
      rows.add(RowFactory.create(String.format("sha-%03d", i), 1000L, (long) blobBytes, "pdf", blob));
    }
    Dataset<Row> batch = spark().createDataFrame(rows, ROWS).repartition(3).cache();
    try {
      assertEquals(files, batch.count());
      String plan = HashSpread.of(batch, 3, versionKey()).queryExecution().executedPlan().toString();
      String spread = plan.substring(0, plan.indexOf("InMemoryTableScan"));
      assertEquals(1, spread.split("Exchange hashpartitioning", -1).length - 1, plan);
      for (String operator : Arrays.asList("Join", "Sort", "Aggregate")) {
        assertFalse(spread.contains(operator), operator + " in " + plan);
      }

      ShuffleWrites writes = new ShuffleWrites();
      jsc().sc().listenerBus().waitUntilEmpty();
      jsc().sc().addSparkListener(writes);
      try (BalancedRows balanced = new HashBalancingStrategy().balance(batch, props(3))) {
        assertEquals(files, balanced.getRows().count());
        jsc().sc().listenerBus().waitUntilEmpty();
      } finally {
        jsc().sc().removeSparkListener(writes);
      }
      long blobTotal = (long) files * blobBytes;
      long written = writes.bytes.get();
      assertTrue(written >= blobTotal, "every blob row is shuffled: " + written + " bytes of " + blobTotal);
      assertTrue(written < blobTotal + blobTotal / 2, "the blob rows are shuffled once: " + written + " bytes of " + blobTotal);
    } finally {
      batch.unpersist(true);
    }
  }

  /** At N/P = 1000 rows per partition the partition counts stay within 10% of the mean. */
  @Test
  public void partitionCountsAreBalancedInExpectation() {
    int partitions = 8;
    int rows = 8000;
    Dataset<Row> batch = spark().createDataFrame(jsc().parallelize(files(rows), 4), ROWS);
    try (BalancedRows balanced = new HashBalancingStrategy().balance(batch, props(partitions))) {
      List<Integer> counts = counts(balanced.getRows());
      assertEquals(partitions, counts.size());
      assertEquals(rows, counts.stream().mapToInt(Integer::intValue).sum());
      double mean = (double) rows / partitions;
      assertTrue(Collections.max(counts) <= 1.1 * mean, "max/mean above 1.1: " + counts);
    }
  }

  @Test
  public void anEmptyBatchGivesNoRowsAndNoFailure() {
    Dataset<Row> batch = spark().createDataFrame(Collections.emptyList(), ROWS);
    BalancedRows balanced = new HashBalancingStrategy().balance(batch, props(3));
    assertEquals(0L, balanced.getRows().count());
    balanced.close();
    // close releases nothing, so a second close and a read after it are harmless
    balanced.close();
    assertEquals(0L, balanced.getRows().count());
  }

  @Test
  public void theSpreadNeedsAPartitionAndAKey() {
    Dataset<Row> batch = spark().createDataFrame(Collections.emptyList(), ROWS);
    assertThrows(IllegalArgumentException.class, () -> HashSpread.of(batch, 0, versionKey()));
    assertThrows(IllegalArgumentException.class, () -> HashSpread.of(batch, 3));
    assertThrows(IllegalArgumentException.class, () -> new HashBalancingStrategy().balance(batch, props(0)));
  }
}
