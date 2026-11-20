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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rebalancing of a prep batch, in two groups. The assignment contract, held by a ranked and a failing double of
 * {@link AssignmentBalancingStrategy}: {@code assign} names every version once with an INT partition in range, from the
 * batch's versions alone, and the base class persists that assignment, joins it back and moves every full blob row, bytes included,
 * into exactly the partition it was assigned. The balance contract, held by both shipped strategies through
 * {@link RowBalancer#balance}: every row arrives exactly once with its blob, in a partition that depends on its version alone.
 */
public class TestRowBalancer extends SparkClientFunctionalTestHarness {

  private static final StructType ROWS = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.BLOB, DataTypes.BinaryType, false)});

  private Dataset<Row> batch(int files) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < files; i++) {
      rows.add(RowFactory.create("sha-" + i, 1000L, 1L, "pdf", new byte[] {(byte) i}));
    }
    return spark().createDataFrame(rows, ROWS).repartition(3);
  }

  private static TypedProperties props(int partitions) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), String.valueOf(partitions));
    return props;
  }

  /** The source_path_sha of every row, per Spark partition of {@code balanced}. */
  private static Map<Integer, List<String>> byPartition(BalancedRows balanced) {
    List<Tuple2<Integer, String>> placed = balanced.getRows()
        .mapPartitionsWithIndex((index, rows) -> {
          List<Tuple2<Integer, String>> out = new ArrayList<>();
          rows.forEachRemaining(row -> out.add(new Tuple2<>(index, row.<String>getAs(BlobColumns.SOURCE_PATH_SHA))));
          return out.iterator();
        }, false).collect();
    Map<Integer, List<String>> result = new HashMap<>();
    for (Tuple2<Integer, String> entry : placed) {
      result.computeIfAbsent(entry._1, k -> new ArrayList<>()).add(entry._2);
    }
    result.values().forEach(Collections::sort);
    return result;
  }

  @Test
  public void balancerAppliesTheAssignmentExactlyAndKeepsEveryColumn() {
    Dataset<Row> batch = batch(5);
    BalancedRows balanced = RowBalancer.balance(batch, new RankedAssignmentStrategy(), props(2));

    assertEquals(2, balanced.getRows().getNumPartitions());
    Map<String, Integer> assigned = balanced.getAssignment().get().collectAsList().stream()
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getAs(BalancingAssignment.PARTITION)));
    Map<Integer, List<String>> placed = byPartition(balanced);
    for (Map.Entry<Integer, List<String>> partition : placed.entrySet()) {
      for (String sha : partition.getValue()) {
        assertEquals(assigned.get(sha), partition.getKey(), sha);
      }
    }
    assertEquals(Arrays.asList("sha-0", "sha-1", "sha-2", "sha-3", "sha-4"),
        placed.values().stream().flatMap(List::stream).sorted().collect(Collectors.toList()));
    Row row = balanced.getRows().filter(r -> r.getString(0).equals("sha-3")).first();
    assertEquals(batch.schema(), row.schema());
    assertEquals(1000L, row.getLong(1));
    assertEquals(1L, row.getLong(2));
    assertEquals("pdf", row.getString(3));
    assertArrayEquals(new byte[] {3}, (byte[]) row.get(4));
  }

  private static final long MIB = 1024L * 1024L;

  /**
   * An assignment strategy for the assignment contract: ranks the batch's versions over the whole batch ({@link SortedRanks}) and
   * assigns rank i to partition {@code i mod P}, P = {@code hoodie.unstructured.embedding.prep.num.partitions}.
   */
  static final class RankedAssignmentStrategy extends AssignmentBalancingStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public BalancingAssignment assign(Dataset<Row> rowsWithoutBlob, TypedProperties writerProps) {
      int numPartitions = EmbeddingTableConfig.from(writerProps)
          .balanceNumPartitions(rowsWithoutBlob.sparkSession().sparkContext().defaultParallelism());
      Column[] key = BalancingAssignment.KEY_COLUMNS.stream().map(functions::col).toArray(Column[]::new);
      Dataset<Row> versions = rowsWithoutBlob.select(key);
      JavaRDD<Row> assignment = SortedRanks.of(versions, key)
          .map(rowAndRank -> RowFactory.create(rowAndRank._1.get(0), rowAndRank._1.get(1), rowAndRank._1.get(2),
              (int) (rowAndRank._2 % numPartitions)));
      return new BalancingAssignment(numPartitions, rowsWithoutBlob.sparkSession().createDataFrame(assignment,
          SortedRanks.assignmentSchema(versions.schema())));
    }
  }

  private static Row file(String sha, Long modificationTime, Long size, String extension, int blob) {
    return RowFactory.create(sha, modificationTime, size, extension, new byte[] {(byte) blob});
  }

  /** The ranked assignment strategy, with the balancer's join planned by Spark and forced to a shuffle (sort-merge) join. */
  static Stream<Arguments> assignmentStrategies() {
    return assignmentStrategiesOnly()
        .flatMap(strategy -> Stream.of(Arguments.of(strategy, false), Arguments.of(strategy, true)));
  }

  private static final String BROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold";
  private static final String ADAPTIVE_BROADCAST_THRESHOLD = "spark.sql.adaptive.autoBroadcastJoinThreshold";

  /** Partition counts the contract is held at: one task, a few, and more than any batch has files except the random one. */
  private static final int[] CONTRACT_PARTITIONS = {1, 3, 64};

  /**
   * Batches for the strategy contract: empty, small, larger than the partitions, mixed extensions and sizes, versions of a path,
   * and a seeded random batch of 500 versions over 350 paths with sizes up to 400 MiB and a mix of extensions.
   */
  private static Map<String, List<Row>> contractBatches() {
    Map<String, List<Row>> batches = new LinkedHashMap<>();
    batches.put("empty", Collections.emptyList());
    batches.put("one file", Collections.singletonList(file("sha-0", 1000L, MIB, "pdf", 0)));
    batches.put("fewer files than partitions", Arrays.asList(file("sha-0", 1000L, MIB, "pdf", 0), file("sha-1", 1000L, 2 * MIB, "pdf", 1)));
    List<Row> many = new ArrayList<>();
    for (int i = 0; i < 23; i++) {
      many.add(file("sha-" + i, 1000L, (i % 5 + 1) * MIB, "pdf", i));
    }
    batches.put("more files than partitions", many);
    List<Row> mixed = new ArrayList<>();
    String[] extensions = {"pdf", "pdf", "pdf", "png", "png", "jpg", "mkv", "mp4", "docx"};
    for (int i = 0; i < extensions.length; i++) {
      mixed.add(file("mixed-" + i, 1000L, (i + 1) * MIB, extensions[i], i));
    }
    mixed.add(file("solo", 1000L, 300 * MIB, "pdf", 20));
    batches.put("mixed extensions and sizes", mixed);
    batches.put("versions of one path", Arrays.asList(file("same", 1000L, 10L, "pdf", 0),
        file("same", 2000L, 10L, "pdf", 1), file("same", 1000L, 20L, "pdf", 2)));
    Random random = new Random(42);
    String[] randomExtensions = {"pdf", "png", "jpg", "mp4", "mkv", "docx", "txt"};
    List<Row> randomBatch = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      // a distinct modification time per row keeps every version distinct while paths repeat
      randomBatch.add(file("random-" + random.nextInt(350), 1000L + i, random.nextInt(400) * MIB + random.nextInt(1024),
          randomExtensions[random.nextInt(randomExtensions.length)], i));
    }
    batches.put("random", randomBatch);
    return batches;
  }

  /** The version of a row: by name when it has a schema, else by the {@link #ROWS} positions (a scenario's input row). */
  private static String versionOf(Row row) {
    if (row.schema() == null) {
      return row.get(0) + "/" + row.get(1) + "/" + row.get(2);
    }
    return row.getAs(BlobColumns.SOURCE_PATH_SHA) + "/" + row.getAs(BlobColumns.SOURCE_MODIFICATION_TIME) + "/"
        + row.getAs(BlobColumns.SOURCE_FILE_SIZE);
  }

  private static List<String> sorted(List<String> values) {
    List<String> copy = new ArrayList<>(values);
    Collections.sort(copy);
    return copy;
  }

  /**
   * The contract {@link AssignmentBalancingStrategy} trusts a subclass to keep, held for the ranked assignment strategy: an INT
   * partition column, every input version assigned exactly once to a partition in [0, numPartitions), and through the balancer
   * every input row arriving once, whole, in the Spark partition it was assigned; at each of {@link #CONTRACT_PARTITIONS}. A batch
   * whose assignment is over the broadcast threshold joins it by shuffle (sort-merge), so the contract is held under that join as
   * well.
   */
  @ParameterizedTest(name = "{0}, shuffle join: {1}")
  @MethodSource("assignmentStrategies")
  public void assignmentStrategyAssignsEveryVersionOnceToAPartitionInRange(AssignmentBalancingStrategy strategy,
                                                                           boolean shuffleJoin) {
    Map<String, String> previous = new HashMap<>();
    for (String key : Arrays.asList(BROADCAST_THRESHOLD, ADAPTIVE_BROADCAST_THRESHOLD)) {
      previous.put(key, spark().conf().get(key, null));
      if (shuffleJoin) {
        spark().conf().set(key, "-1");
      }
    }
    try {
      for (int contractPartitions : CONTRACT_PARTITIONS) {
        for (Map.Entry<String, List<Row>> scenario : contractBatches().entrySet()) {
          assertContract(strategy, shuffleJoin, contractPartitions, scenario.getKey(), scenario.getValue());
        }
      }
    } finally {
      previous.forEach((key, value) -> {
        if (value == null) {
          spark().conf().unset(key);
        } else {
          spark().conf().set(key, value);
        }
      });
    }
  }

  private void assertContract(AssignmentBalancingStrategy strategy, boolean shuffleJoin, int contractPartitions, String scenario,
                              List<Row> input) {
    String label = strategy.getClass().getSimpleName() + ", P = " + contractPartitions + ", shuffle join " + shuffleJoin + ", "
        + scenario;
    Dataset<Row> batch = spark().createDataFrame(input, ROWS).repartition(3);
    List<String> inputVersions = input.stream().map(TestRowBalancer::versionOf).collect(Collectors.toList());

    BalancingAssignment assignment = strategy.assign(batch.drop(BlobColumns.BLOB), props(contractPartitions));
    int numPartitions = assignment.getNumPartitions();
    assertTrue(numPartitions >= 1, label);
    assertEquals(DataTypes.IntegerType, assignment.getAssignment().schema().apply(BalancingAssignment.PARTITION).dataType(), label);
    Map<String, Integer> partitionOf = new HashMap<>();
    List<String> assignedVersions = new ArrayList<>();
    for (Row row : assignment.getAssignment().collectAsList()) {
      int index = row.fieldIndex(BalancingAssignment.PARTITION);
      assertFalse(row.isNullAt(index), label + ": " + row);
      int partition = row.getInt(index);
      assertTrue(partition >= 0 && partition < numPartitions, label + ": " + row + " of " + numPartitions);
      assignedVersions.add(versionOf(row));
      partitionOf.put(versionOf(row), partition);
    }
    assertEquals(sorted(inputVersions), sorted(assignedVersions), label + ": every input version exactly once");

    try (BalancedRows balanced = RowBalancer.balance(batch, strategy, props(contractPartitions))) {
      List<List<Row>> partitions = balanced.getRows().glom().collect();
      assertEquals(balanced.getRows().getNumPartitions(), partitions.size(), label);
      List<String> arrived = new ArrayList<>();
      for (int k = 0; k < partitions.size(); k++) {
        for (Row row : partitions.get(k)) {
          assertEquals(ROWS, row.schema(), label);
          assertEquals(partitionOf.get(versionOf(row)), Integer.valueOf(k), label + ": " + versionOf(row));
          arrived.add(versionOf(row) + "#" + ((byte[]) row.getAs(BlobColumns.BLOB))[0]);
        }
      }
      List<String> expected = new ArrayList<>();
      for (Row row : input) {
        expected.add(versionOf(row) + "#" + ((byte[]) row.get(ROWS.fieldIndex(BlobColumns.BLOB)))[0]);
      }
      assertEquals(sorted(expected), sorted(arrived), label + ": every input row once, whole");
      if (shuffleJoin && !input.isEmpty()) {
        // the balancer's join
        String plan = AssignmentBalancingStrategy.joinAssignment(batch, balanced.getAssignment().get()).queryExecution()
            .executedPlan().toString();
        assertTrue(plan.contains("SortMergeJoin"), label + ": " + plan);
      }
    }
  }

  /**
   * The balance contract, held for both shipped strategies through {@link RowBalancer#balance}: every input row arrives exactly
   * once, whole (every column, its own blob bytes), at each of {@link #CONTRACT_PARTITIONS} and for every contract batch.
   */
  @ParameterizedTest
  @MethodSource("strategies")
  public void shippedStrategyBalancesEveryRowOnceWithItsBlob(RowBalancingStrategy strategy) {
    for (int contractPartitions : CONTRACT_PARTITIONS) {
      for (Map.Entry<String, List<Row>> scenario : contractBatches().entrySet()) {
        String label = strategy.getClass().getSimpleName() + ", P = " + contractPartitions + ", " + scenario.getKey();
        Dataset<Row> batch = spark().createDataFrame(scenario.getValue(), ROWS).repartition(3);
        try (BalancedRows balanced = RowBalancer.balance(batch, strategy, props(contractPartitions))) {
          assertEquals(strategy instanceof AssignmentBalancingStrategy, balanced.getAssignment().isPresent(), label);
          List<String> arrived = balanced.getRows().collect().stream()
              .map(row -> {
                assertEquals(ROWS, row.schema(), label);
                return versionOf(row) + "#" + ((byte[]) row.getAs(BlobColumns.BLOB))[0];
              }).collect(Collectors.toList());
          List<String> expected = scenario.getValue().stream()
              .map(row -> versionOf(row) + "#" + ((byte[]) row.get(ROWS.fieldIndex(BlobColumns.BLOB)))[0]).collect(Collectors.toList());
          assertEquals(sorted(expected), sorted(arrived), label + ": every input row once, whole");
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("strategies")
  public void twoVersionsOfOnePathAreTwoFiles(RowBalancingStrategy strategy) {
    Dataset<Row> batch = spark().createDataFrame(Arrays.asList(
        RowFactory.create("sha-a", 1000L, 5L, "txt", new byte[] {1}),
        RowFactory.create("sha-a", 2000L, 5L, "txt", new byte[] {2}),
        RowFactory.create("sha-a", 2000L, 9L, "txt", new byte[] {3})), ROWS);
    BalancedRows balanced = RowBalancer.balance(batch, strategy, props(2));
    try {
      if (balanced.getAssignment().isPresent()) {
        assertEquals(Arrays.asList(
            RowFactory.create("sha-a", 1000L, 5L), RowFactory.create("sha-a", 2000L, 5L), RowFactory.create("sha-a", 2000L, 9L)),
            balanced.getAssignment().get().orderBy(BlobColumns.SOURCE_MODIFICATION_TIME, BlobColumns.SOURCE_FILE_SIZE)
                .select(BlobColumns.SOURCE_PATH_SHA, BlobColumns.SOURCE_MODIFICATION_TIME, BlobColumns.SOURCE_FILE_SIZE).collectAsList());
        assertEquals(3L, AssignmentBalancingStrategy.joinAssignment(batch, balanced.getAssignment().get()).count());
      }
      List<Row> rows = balanced.getRows().sortBy(row -> row.getLong(1) * 10 + row.getLong(2), true, 1).collect();
      assertEquals(3, rows.size());
      List<Row> expectedRows = batch.orderBy(BlobColumns.SOURCE_MODIFICATION_TIME, BlobColumns.SOURCE_FILE_SIZE).collectAsList();
      for (int i = 0; i < rows.size(); i++) {
        Row expected = expectedRows.get(i);
        assertEquals(expected.getString(0), rows.get(i).getString(0));
        assertEquals(expected.getLong(1), rows.get(i).getLong(1));
        assertEquals(expected.getLong(2), rows.get(i).getLong(2));
        assertEquals(expected.getString(3), rows.get(i).getString(3));
        assertArrayEquals((byte[]) expected.get(4), (byte[]) rows.get(i).get(4));
      }
    } finally {
      balanced.close();
    }
  }

  @Test
  public void closeReleasesTheAssignment() {
    Dataset<Row> batch = batch(2);
    BalancedRows balanced = RowBalancer.balance(batch, new RankedAssignmentStrategy(), props(2));
    assertNotEquals(StorageLevel.NONE(), balanced.getAssignment().get().storageLevel());

    balanced.close();

    assertEquals(StorageLevel.NONE(), balanced.getAssignment().get().storageLevel());
  }

  @ParameterizedTest
  @MethodSource("strategies")
  public void anEmptyBatchBalancesToNoRow(RowBalancingStrategy strategy) {
    BalancedRows balanced = RowBalancer.balance(batch(0), strategy, props(2));

    assertEquals(0L, balanced.getRows().count());
    balanced.close();
  }

  @Test
  public void strategyComesFromTheConfiguredClass() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), HashBalancingStrategy.class.getName());
    assertInstanceOf(HashBalancingStrategy.class, RowBalancer.strategyFrom(props));
  }

  @Test
  public void exactPartitionerSendsKeyKToPartitionK() {
    ExactPartitioner partitioner = new ExactPartitioner(3);

    assertEquals(3, partitioner.numPartitions());
    assertEquals(0, partitioner.getPartition(0));
    assertEquals(2, partitioner.getPartition(2));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(3));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(-1));
    assertEquals(new ExactPartitioner(3), partitioner);
    assertEquals(new ExactPartitioner(3).hashCode(), partitioner.hashCode());
    assertNotEquals(new ExactPartitioner(4), partitioner);
  }

  /** The hash strategy and the ranked double, each held to a partition per version that depends on the batch's versions alone. */
  static Stream<RowBalancingStrategy> strategies() {
    return Stream.of(new HashBalancingStrategy(), new RankedAssignmentStrategy());
  }

  /** The ranked assignment strategy, held to an assignment that depends on the batch's versions alone. */
  static Stream<AssignmentBalancingStrategy> assignmentStrategiesOnly() {
    return Stream.of(new RankedAssignmentStrategy());
  }

  /** {@code rows} in {@code partitions} Spark partitions, each emitting its rows in a new random order every time it is computed. */
  private Dataset<Row> reshuffledOnEveryCompute(List<Row> rows, int partitions) {
    return spark().createDataFrame(jsc().parallelize(rows, partitions).mapPartitions(iterator -> {
      List<Row> partition = new ArrayList<>();
      iterator.forEachRemaining(partition::add);
      Collections.shuffle(partition, new Random(System.nanoTime()));
      return partition.iterator();
    }), ROWS);
  }

  private static Map<String, Integer> partitionsOf(Dataset<Row> assignment) {
    return assignment.collectAsList().stream()
        .collect(Collectors.toMap(TestRowBalancer::versionOf, row -> row.<Integer>getAs(BalancingAssignment.PARTITION)));
  }

  /**
   * A version's partition depends on the batch's versions and P alone, so a recomputed join can neither drop a row nor place one
   * twice. Each of these puts every version in the same partition: the same rows in another partition order; an input whose row
   * order changes on every compute; that assignment computed again, from the sort's shuffle output, once its cached blocks are
   * dropped; and a new assignment, which reads that input again in a new order.
   */
  @ParameterizedTest
  @MethodSource("assignmentStrategiesOnly")
  public void assignmentIsIndependentOfRowOrderAndRecomputation(AssignmentBalancingStrategy strategy) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      rows.add(file(String.format("sha-%02d", i), 1000L + i, (i % 7 + 1) * MIB, i % 4 == 0 ? "png" : "pdf", i));
    }
    List<Row> reversed = new ArrayList<>(rows);
    Collections.reverse(reversed);
    TypedProperties props = props(3);

    Map<String, Integer> forward = partitionsOf(strategy.assign(spark().createDataFrame(rows, ROWS)
        .drop(BlobColumns.BLOB), props).getAssignment());
    Map<String, Integer> backward = partitionsOf(strategy.assign(spark().createDataFrame(reversed, ROWS)
        .drop(BlobColumns.BLOB), props).getAssignment());
    assertEquals(40, forward.size());
    assertEquals(forward, backward, "the same versions in reverse order");

    Dataset<Row> reshuffled = reshuffledOnEveryCompute(rows, 3).drop(BlobColumns.BLOB);
    Dataset<Row> assignment = strategy.assign(reshuffled, props).getAssignment().persist(StorageLevel.MEMORY_ONLY());
    try {
      assertEquals(forward, partitionsOf(assignment), "the first compute over a reshuffled input");
      assignment.unpersist(true);
      assertEquals(forward, partitionsOf(assignment), "a second compute after the cached assignment is dropped");
    } finally {
      assignment.unpersist(true);
    }
    // a new assignment reads the input again, in a new order
    assertEquals(forward, partitionsOf(strategy.assign(reshuffled, props).getAssignment()), "a new assignment of the input");
  }

  /** The Spark partition each balanced row is in, by version, checking that no version arrives twice. */
  private static Map<String, Integer> placedPartitions(JavaRDD<Row> rows) {
    List<Tuple2<String, Integer>> placed = rows.mapPartitionsWithIndex((index, iterator) -> {
      List<Tuple2<String, Integer>> out = new ArrayList<>();
      iterator.forEachRemaining(row -> out.add(new Tuple2<>(versionOf(row), index)));
      return out.iterator();
    }, false).collect();
    Map<String, Integer> result = new HashMap<>();
    for (Tuple2<String, Integer> entry : placed) {
      assertNull(result.put(entry._1, entry._2), "a version placed twice: " + entry._1);
    }
    return result;
  }

  /**
   * Through {@link RowBalancer#balance}, a version's partition depends on the batch's versions and P alone, for both shipped
   * strategies: the same rows in reverse order and in another partitioning, an input whose row order changes on every compute,
   * and that input balanced again once its cache is dropped all put every version in the same partition.
   */
  @ParameterizedTest
  @MethodSource("strategies")
  public void balancedPartitionsAreIndependentOfRowOrderAndRecomputation(RowBalancingStrategy strategy) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      rows.add(file(String.format("sha-%02d", i), 1000L + i, (i % 7 + 1) * MIB, i % 4 == 0 ? "png" : "pdf", i));
    }
    List<Row> reversed = new ArrayList<>(rows);
    Collections.reverse(reversed);
    TypedProperties props = props(3);
    Map<String, Integer> forward;
    try (BalancedRows balanced = RowBalancer.balance(spark().createDataFrame(jsc().parallelize(rows, 1), ROWS), strategy, props)) {
      forward = placedPartitions(balanced.getRows());
    }
    assertEquals(40, forward.size());
    try (BalancedRows balanced = RowBalancer.balance(spark().createDataFrame(jsc().parallelize(reversed, 5), ROWS), strategy,
        props)) {
      assertEquals(forward, placedPartitions(balanced.getRows()), "the same versions in reverse order, in five partitions");
    }
    Dataset<Row> reshuffled = reshuffledOnEveryCompute(rows, 3).persist(StorageLevel.MEMORY_ONLY());
    try {
      try (BalancedRows balanced = RowBalancer.balance(reshuffled, strategy, props)) {
        assertEquals(forward, placedPartitions(balanced.getRows()), "the first balance of a reshuffled input");
      }
      // dropped, so the next balance reads the input again, in a new order
      reshuffled.unpersist(true);
      try (BalancedRows balanced = RowBalancer.balance(reshuffled, strategy, props)) {
        assertEquals(forward, placedPartitions(balanced.getRows()), "that input balanced again once its cache is dropped");
      }
    } finally {
      reshuffled.unpersist(true);
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

  /** The plans {@link #smallBatchShufflesItsBlobRowsOnce} balances under. */
  enum JoinPlanning {
    DEFAULT, ADAPTIVE_OFF, BROADCAST_OFF
  }

  static Stream<Arguments> strategiesAndJoinPlanning() {
    return strategies().flatMap(strategy -> Stream.of(JoinPlanning.values()).map(planning -> Arguments.of(strategy, planning)));
  }

  /**
   * A batch over the broadcast threshold whose assignment is under it joins the assignment by broadcast, with or without
   * adaptive execution, so its blob rows are shuffled once, into their assigned partitions; the strategy shuffles metadata only. With broadcast joins off,
   * the shuffle join writes every blob row once more, which shows the byte count tells the two plans apart. The hash strategy has
   * no join, so it shuffles the blob rows once under every planning.
   */
  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("strategiesAndJoinPlanning")
  public void smallBatchShufflesItsBlobRowsOnce(RowBalancingStrategy strategy, JoinPlanning planning) throws Exception {
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
    assertEquals(files, batch.count());
    // a threshold the assignment (a few KiB) is under and the batch (800 KiB of blobs) is over, as in a real prep round
    String threshold = planning == JoinPlanning.BROADCAST_OFF ? "-1" : String.valueOf(100 * 1024);
    Map<String, String> settings = new HashMap<>();
    settings.put(BROADCAST_THRESHOLD, threshold);
    settings.put(ADAPTIVE_BROADCAST_THRESHOLD, threshold);
    if (planning == JoinPlanning.ADAPTIVE_OFF) {
      settings.put("spark.sql.adaptive.enabled", "false");
    }
    Map<String, String> previous = new HashMap<>();
    settings.forEach((key, value) -> {
      previous.put(key, spark().conf().get(key, null));
      spark().conf().set(key, value);
    });
    ShuffleWrites writes = new ShuffleWrites();
    jsc().sc().listenerBus().waitUntilEmpty();
    jsc().sc().addSparkListener(writes);
    try (BalancedRows balanced = RowBalancer.balance(batch, strategy, props(3))) {
      assertEquals(files, balanced.getRows().count());
      jsc().sc().listenerBus().waitUntilEmpty();
    } finally {
      jsc().sc().removeSparkListener(writes);
      batch.unpersist(true);
      previous.forEach((key, value) -> {
        if (value == null) {
          spark().conf().unset(key);
        } else {
          spark().conf().set(key, value);
        }
      });
    }
    long blobTotal = (long) files * blobBytes;
    long written = writes.bytes.get();
    assertTrue(written >= blobTotal, "the exact partitioner writes every blob row: " + written + " bytes of " + blobTotal);
    if (planning == JoinPlanning.BROADCAST_OFF && strategy instanceof AssignmentBalancingStrategy) {
      assertTrue(written >= 2 * blobTotal, "a shuffle join writes the blob rows once more: " + written + " bytes");
    } else {
      assertTrue(written < blobTotal + blobTotal / 2, "the blob rows are shuffled once: " + written + " bytes of " + blobTotal);
    }
  }

  /**
   * A row whose modification time or size is null, which the source never builds, still reaches its partition instead of being
   * dropped without a trace: the assignment join matches the version columns null-safe, and the hash skips a null column.
   */
  @ParameterizedTest
  @MethodSource("strategies")
  public void rowWithANullVersionColumnStillReachesItsPartition(RowBalancingStrategy strategy) {
    Dataset<Row> batch = spark().createDataFrame(Arrays.asList(file("sha-a", 1000L, MIB, "pdf", 1),
        file("sha-b", null, MIB, "pdf", 2), file("sha-c", 1000L, null, "pdf", 3)), ROWS);
    try (BalancedRows balanced = RowBalancer.balance(batch, strategy, props(2))) {
      List<String> arrived = balanced.getRows().collect().stream().map(row -> versionOf(row) + "#" + ((byte[]) row.getAs(BlobColumns.BLOB))[0])
          .sorted().collect(Collectors.toList());
      assertEquals(Arrays.asList("sha-a/1000/" + MIB + "#1", "sha-b/null/" + MIB + "#2", "sha-c/1000/null#3"), arrived);
    }
  }

  /** A strategy whose assignment fails in a Spark task when computed; it keeps the Dataset it returned. */
  private static final class FailingAssignmentStrategy extends AssignmentBalancingStrategy {
    private transient Dataset<Row> returned;

    @Override
    public BalancingAssignment assign(Dataset<Row> rowsWithoutBlob, TypedProperties writerProps) {
      JavaRDD<Row> failing = rowsWithoutBlob.select(BlobColumns.SOURCE_PATH_SHA, BlobColumns.SOURCE_MODIFICATION_TIME,
          BlobColumns.SOURCE_FILE_SIZE).javaRDD().map(row -> {
            throw new IllegalStateException("assignment task fails");
          });
      returned = rowsWithoutBlob.sparkSession().createDataFrame(failing, BalancingAssignment.SCHEMA);
      return new BalancingAssignment(2, returned);
    }
  }

  /** A failed Spark job while the balancer computes the assignment releases the assignment it persisted. */
  @Test
  public void failedAssignmentJobReleasesTheAssignment() {
    FailingAssignmentStrategy strategy = new FailingAssignmentStrategy();
    Dataset<Row> batch = batch(3);
    Exception failure = assertThrows(Exception.class, () -> RowBalancer.balance(batch, strategy, props(2)));
    assertTrue(String.valueOf(failure.getMessage()).contains("assignment task fails"), String.valueOf(failure.getMessage()));
    assertEquals(StorageLevel.NONE(), strategy.returned.storageLevel());
  }

  @Test
  public void anAssignmentNeedsAPartition() {
    assertThrows(IllegalArgumentException.class, () -> new BalancingAssignment(0, batch(0)));
  }

  /** A strategy whose assignment leaves out one version: {@link #balance} trusts every version to be named. */
  private static final class IncompleteAssignmentStrategy extends AssignmentBalancingStrategy {
    @Override
    public BalancingAssignment assign(Dataset<Row> rowsWithoutBlob, TypedProperties writerProps) {
      Dataset<Row> named = rowsWithoutBlob.filter(rowsWithoutBlob.col(BlobColumns.SOURCE_PATH_SHA).notEqual("sha-1"))
          .select(rowsWithoutBlob.col(BlobColumns.SOURCE_PATH_SHA), rowsWithoutBlob.col(BlobColumns.SOURCE_MODIFICATION_TIME),
              rowsWithoutBlob.col(BlobColumns.SOURCE_FILE_SIZE), org.apache.spark.sql.functions.lit(0).as(BalancingAssignment.PARTITION));
      return new BalancingAssignment(2, named);
    }
  }

  /** A version the assignment does not name fails the round with the strategy and the version named, not a bare NPE (RV32-4). */
  @Test
  public void anUnassignedVersionNamesTheStrategyAndTheVersion() {
    Dataset<Row> batch = batch(3);
    try (BalancedRows balanced = RowBalancer.balance(batch, new IncompleteAssignmentStrategy(), props(2))) {
      Exception failure = assertThrows(Exception.class, () -> balanced.getRows().collect());
      String message = String.valueOf(failure.getMessage());
      assertTrue(message.contains(IncompleteAssignmentStrategy.class.getName()) && message.contains("sha-1"), message);
    }
  }
}
