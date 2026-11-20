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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link SizeTypeAwareBalancingStrategy} over metadata-only batches in a local Spark session: preprocessor groups in disjoint
 * partition blocks, the byte budget as a ceiling (within one file), solo files alone, the per-group floor, determinism, and
 * batches whose {@code source_file_size} is null, negative or missing; and, through {@link RowBalancer}, that Spark partition k holds
 * exactly the rows assigned to k.
 */
@Tag("functional")
class TestSizeTypeAwareBalancingStrategy extends SparkClientFunctionalTestHarness {

  private static final long MIB = 1024L * 1024L;
  private static final long GIB = 1024L * MIB;
  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true)});

  private static final class File {
    private final String sha;
    private final String extension;
    private final Long size;

    File(String sha, String extension, Long size) {
      this.sha = sha;
      this.extension = extension;
      this.size = size;
    }
  }

  private static List<File> files(String prefix, String extension, int count, long size) {
    List<File> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(new File(String.format("%s-%05d", prefix, i), extension, size));
    }
    return files;
  }

  private Dataset<Row> batch(List<File> files) {
    List<Row> rows = files.stream().map(f -> RowFactory.create(f.sha, 1L, f.extension, f.size)).collect(Collectors.toList());
    return spark().createDataFrame(rows, SCHEMA);
  }

  private static TypedProperties mediaProps() {
    TypedProperties props = new TypedProperties();
    for (String extension : Arrays.asList("png", "jpg")) {
      props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension
          + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
          "org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor");
    }
    for (String extension : Arrays.asList("mp4", "mov", "mkv")) {
      props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension
          + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
          "org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor");
    }
    return props;
  }

  private static TypedProperties props(int numPartitions) {
    TypedProperties props = mediaProps();
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), String.valueOf(numPartitions));
    return props;
  }

  private static TypedProperties budget(TypedProperties props, String group, long bytes) {
    props.setProperty(EmbeddingTableConfig.BALANCE_PREFIX + group + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX,
        String.valueOf(bytes));
    return props;
  }

  /** sha → partition, checked: every sha once, every partition in range. */
  private static Map<String, Integer> collect(BalancingAssignment assignment, List<File> files) {
    Map<String, Integer> partitions = new TreeMap<>();
    for (Row row : assignment.getAssignment().collectAsList()) {
      assertEquals(null, partitions.put(row.getString(0), row.<Integer>getAs(BalancingAssignment.PARTITION)), "assigned twice: " + row.getString(0));
      assertTrue(row.<Integer>getAs(BalancingAssignment.PARTITION) >= 0 && row.<Integer>getAs(BalancingAssignment.PARTITION) < assignment.getNumPartitions(), "partition out of range: " + row);
    }
    assertEquals(files.stream().map(f -> f.sha).collect(Collectors.toSet()), partitions.keySet());
    return partitions;
  }

  private Map<String, Integer> assign(List<File> files, TypedProperties props) {
    return collect(new SizeTypeAwareBalancingStrategy().assign(batch(files), props), files);
  }

  private static Map<Integer, Long> totals(List<File> files, Map<String, Integer> partitions) {
    Map<Integer, Long> totals = new TreeMap<>();
    for (File f : files) {
      totals.merge(partitions.get(f.sha), f.size == null ? 0 : f.size, Long::sum);
    }
    return totals;
  }

  @Test
  void missingAssignmentFailsInsteadOfDroppingASourceVersion() {
    Dataset<Row> input = batch(Arrays.asList(new File("keep", "pdf", 1L), new File("missing", "pdf", 2L)));
    TypedProperties props = props(1);
    try (BalancedRows balanced = new SizeTypeAwareBalancingStrategy().balance(input, props)) {
      assertEquals(2, balanced.getRows().collect().size(), "control: every input version is assigned");
    }
    AssignmentBalancingStrategy incomplete = new AssignmentBalancingStrategy() {
      @Override
      public BalancingAssignment assign(Dataset<Row> metadata, TypedProperties writerProps) {
        Dataset<Row> assignment = metadata.filter(functions.col(BlobColumns.SOURCE_PATH_SHA).equalTo("keep"))
            .selectExpr("source_path_sha", "source_modification_time", "source_file_size", "0 AS _balance_partition");
        return new BalancingAssignment(1, assignment);
      }
    };
    assertThrows(Exception.class, () -> {
      try (BalancedRows balanced = incomplete.balance(input, props)) {
        balanced.getRows().collect();
      }
    });
  }

  @Test
  void groupColumnMatchesGroupOf() {
    List<File> files = Arrays.asList(new File("a", "pdf", 1L), new File("b", "PNG", 1L), new File("c", "mov", 1L),
        new File("d", null, 1L), new File("e", "docx", 1L));
    BalanceGroups groups = BalanceGroups.from(mediaProps());
    List<String> viaColumn = batch(files).orderBy(BlobColumns.SOURCE_PATH_SHA)
        .select(groups.groupColumn(functions.col(BlobColumns.SOURCE_FILE_EXTENSION))).collectAsList().stream()
        .map(row -> row.getString(0)).collect(Collectors.toList());
    assertEquals(Arrays.asList(BalanceGroups.PDF, BalanceGroups.IMAGE, BalanceGroups.VIDEO, BalanceGroups.UNMAPPED,
        BalanceGroups.UNMAPPED), viaColumn);
    assertEquals(viaColumn, files.stream().map(f -> groups.groupOf(f.extension)).collect(Collectors.toList()));
  }

  @Test
  void groupsNeverShareAPartition() {
    List<File> files = new ArrayList<>();
    files.addAll(files("pdf", "pdf", 12, 3 * MIB));
    files.addAll(files("img", "JPG", 9, 2 * MIB));
    files.addAll(files("vid", "mp4", 5, 40 * MIB));
    files.addAll(files("doc", "docx", 7, MIB));
    files.add(new File("none", null, MIB));
    Map<String, Integer> partitions = assign(files, props(4));
    Map<String, Set<Integer>> byGroup = new HashMap<>();
    BalanceGroups groups = BalanceGroups.from(mediaProps());
    for (File f : files) {
      byGroup.computeIfAbsent(groups.groupOf(f.extension), g -> new HashSet<>()).add(partitions.get(f.sha));
    }
    assertEquals(4, byGroup.size());
    Set<Integer> seen = new HashSet<>();
    for (Set<Integer> block : byGroup.values()) {
      for (Integer partition : block) {
        assertTrue(seen.add(partition), "partition " + partition + " holds two groups: " + byGroup);
      }
    }
    assertEquals(4, byGroup.get(BalanceGroups.PDF).size());
    assertEquals(4, byGroup.get(BalanceGroups.IMAGE).size());
    assertEquals(4, byGroup.get(BalanceGroups.VIDEO).size());
    assertEquals(1, byGroup.get(BalanceGroups.UNMAPPED).size());
  }

  @Test
  void soloFileIsAlone() {
    List<File> files = new ArrayList<>(files("vid", "mp4", 5, 100 * MIB));
    files.add(new File("big", "mov", 3 * GIB));
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files),
        budget(props(1), BalanceGroups.VIDEO, GIB));
    Map<String, Integer> partitions = collect(assignment, files);
    // 1 solo + ceil(500 MiB / 1 GiB) = 2 tasks
    assertEquals(2, assignment.getNumPartitions());
    int big = partitions.get("big");
    assertEquals(Collections.singletonList("big"), partitions.entrySet().stream().filter(e -> e.getValue() == big)
        .map(Map.Entry::getKey).collect(Collectors.toList()));
  }

  @Test
  void budgetRespectedWithinOneFile() {
    Random random = new Random(42);
    List<File> files = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      files.add(new File(String.format("pdf-%03d", i), "pdf", (1 + random.nextInt(50)) * MIB));
    }
    long total = files.stream().mapToLong(f -> f.size).sum();
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files),
        budget(props(2), BalanceGroups.PDF, 256 * MIB));
    Map<String, Integer> partitions = collect(assignment, files);
    assertTrue(assignment.getNumPartitions() >= (total + 256 * MIB - 1) / (256 * MIB), "too few tasks for " + total);
    Map<Integer, Long> largest = new TreeMap<>();
    for (File f : files) {
      largest.merge(partitions.get(f.sha), f.size, Math::max);
    }
    for (Map.Entry<Integer, Long> entry : totals(files, partitions).entrySet()) {
      assertTrue(entry.getValue() <= 256 * MIB + largest.get(entry.getKey()),
          "partition " + entry.getKey() + " holds " + entry.getValue() + " bytes");
    }
  }

  @Test
  void snakeEvensTotals() {
    List<File> files = new ArrayList<>();
    for (int i = 1; i <= 40; i++) {
      files.add(new File(String.format("pdf-%02d", i), "pdf", i * MIB));
    }
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files),
        budget(props(4), BalanceGroups.PDF, GIB));
    assertEquals(4, assignment.getNumPartitions());
    Map<Integer, Long> totals = totals(files, collect(assignment, files));
    assertEquals(4, totals.size());
    // sizes 40..1 dealt 0,1,2,3 then 3,2,1,0: every pair of rounds adds 73 MiB to each task, so the totals are equal (205 MiB), where
    // a plain round-robin would leave a 30 MiB spread
    assertEquals(Collections.singleton(205 * MIB), new HashSet<>(totals.values()), "task totals " + totals);
  }

  @Test
  void soloFileOfALaterBlockIsAlone() {
    // block order: image [0, 4), pdf after it; the pdf ranks start after the image files in the global sort
    List<File> files = new ArrayList<>(files("img", "png", 10, MIB));
    files.addAll(files("pdf", "pdf", 6, MIB));
    files.add(new File("big", "pdf", 300 * MIB));
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files), props(4));
    Map<String, Integer> partitions = collect(assignment, files);
    // image 4 tasks; pdf the floor min(7, 4) = 4 tasks: its solo file alone, the other six dealt over 3
    assertEquals(8, assignment.getNumPartitions());
    int big = partitions.get("big");
    assertEquals(4, big, "the pdf block's first partition holds its solo file");
    assertEquals(Collections.singletonList("big"), partitions.entrySet().stream().filter(e -> e.getValue() == big)
        .map(Map.Entry::getKey).collect(Collectors.toList()));
    assertEquals(8, new HashSet<>(partitions.values()).size(), "no empty partition");
  }

  @Test
  void assignmentKeepsTheBatchKeyColumnTypes() {
    StructType intSized = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, true),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.IntegerType, true)});
    Dataset<Row> batch = spark().createDataFrame(Arrays.asList(RowFactory.create("a", 1L, "pdf", 3),
        RowFactory.create("b", 1L, "pdf", 5)), intSized);
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch, props(2));
    assertEquals(DataTypes.IntegerType, assignment.getAssignment().schema().apply(BlobColumns.SOURCE_FILE_SIZE).dataType());
    assertEquals(Arrays.asList(RowFactory.create("a", 1L, 3, 1), RowFactory.create("b", 1L, 5, 0)),
        assignment.getAssignment().orderBy(BlobColumns.SOURCE_PATH_SHA).collectAsList());
  }

  @Test
  void smallBatchSpreads() {
    List<File> files = files("pdf", "pdf", 50, 2 * MIB);
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files), props(8));
    assertEquals(8, assignment.getNumPartitions());
    assertEquals(8, new HashSet<>(collect(assignment, files).values()).size());
  }

  @Test
  void severalGroupsEachTakeTheirFloor() {
    List<File> files = new ArrayList<>(files("pdf", "pdf", 1000, 100 * 1024L));
    files.add(new File("video", "mp4", 2 * GIB));
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files), props(8));
    assertEquals(9, assignment.getNumPartitions());
    Map<String, Integer> partitions = collect(assignment, files);
    // name order: pdf block [0, 8), video block [8, 9)
    assertEquals(8, (int) partitions.get("video"));
    assertEquals(8, partitions.entrySet().stream().filter(e -> !e.getKey().equals("video")).map(Map.Entry::getValue)
        .collect(Collectors.toSet()).size());
  }

  @Test
  void fewerFilesThanParallelismCreatesNoEmptyPartition() {
    List<File> files = files("vid", "mp4", 3, 10 * MIB);
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files), props(40));
    assertEquals(3, assignment.getNumPartitions());
    assertEquals(3, new HashSet<>(collect(assignment, files).values()).size());
  }

  @Test
  void parallelismFromTheSparkContextWhenUnset() {
    // the harness master is local[n, retries]: with num.partitions unset P is n, and 50 small PDFs take n tasks
    String master = spark().sparkContext().master();
    Matcher local = Pattern.compile("local\\[(\\d+)(,\\s*\\d+)?]").matcher(master);
    assumeTrue(local.matches(), "local master with a thread count, got " + master);
    int threads = Integer.parseInt(local.group(1));
    List<File> files = files("pdf", "pdf", 50, 2 * MIB);
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files), new TypedProperties());
    assertEquals(Math.min(50, threads), assignment.getNumPartitions());
    assertEquals(threads, SizeTypeAwareBalancingStrategy.parallelism(spark().sparkContext(), new TypedProperties()).parallelism());
    collect(assignment, files);
  }

  @Test
  void deterministicWithTies() {
    List<File> files = files("pdf", "pdf", 30, 5 * MIB);
    Collections.shuffle(files, new Random(7));
    Map<String, Integer> first = assign(files, props(4));
    Collections.shuffle(files, new Random(8));
    assertEquals(first, assign(files, props(4)));
  }

  @Test
  void missingVersionKeyCannotProduceAnAssignment() {
    Dataset<Row> unsized = batch(files("pdf", "pdf", 6, MIB)).drop(BlobColumns.SOURCE_FILE_SIZE);
    Exception failure = assertThrows(Exception.class, () -> new SizeTypeAwareBalancingStrategy().assign(unsized, props(4)));
    assertTrue(failure.getMessage().contains(BlobColumns.SOURCE_FILE_SIZE), failure.getMessage());
  }

  @Test
  void nullAndNegativeSizes() {
    List<File> files = Arrays.asList(new File("a", "pdf", null), new File("b", "pdf", -1L), new File("c", "pdf", 3 * MIB),
        new File("d", "png", null));
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(files),
        budget(props(1), BalanceGroups.PDF, 2 * MIB));
    Map<String, Integer> partitions = collect(assignment, files);
    // pdf: c is solo (3 MiB >= 2 MiB), a and b weigh 0 and share one dealt task; image: one task
    assertEquals(3, assignment.getNumPartitions());
    assertEquals(partitions.get("a"), partitions.get("b"));
    assertNotEquals(partitions.get("a"), partitions.get("c"));
  }

  @Test
  void emptyBatch() {
    BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch(Collections.emptyList()), props(4));
    assertEquals(1, assignment.getNumPartitions());
    assertEquals(0, assignment.getAssignment().count());
  }

  @Test
  void throughRowBalancer() {
    List<File> files = new ArrayList<>();
    files.addAll(files("pdf", "pdf", 10, 3 * MIB));
    files.addAll(files("img", "png", 4, MIB));
    files.addAll(files("vid", "mkv", 2, 30 * MIB));
    files.add(new File("doc", "docx", MIB));
    Dataset<Row> withBlob = batch(files).withColumn(BlobColumns.BLOB, functions.lit("bytes").cast(DataTypes.BinaryType));
    TypedProperties props = props(3);
    Map<String, Integer> expected = assign(files, props);
    try (BalancedRows balanced = RowBalancer.balance(withBlob, new SizeTypeAwareBalancingStrategy(), props)) {
      List<List<Row>> partitions = balanced.getRows().glom().collect();
      // P = 3: pdf 3, image 3, video 2, unmapped 1
      assertEquals(9, partitions.size());
      Map<String, Integer> actual = new TreeMap<>();
      for (int k = 0; k < partitions.size(); k++) {
        for (Row row : partitions.get(k)) {
          actual.put(row.getString(row.fieldIndex(BlobColumns.SOURCE_PATH_SHA)), k);
          assertEquals(5, row.length());
        }
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  void samePathVersionsAreAssignedOnceAndDeterministically() {
    List<Row> versions = Arrays.asList(RowFactory.create("same", 100L, "pdf", 5 * MIB),
        RowFactory.create("same", 200L, "pdf", 5 * MIB), RowFactory.create("same", 100L, "pdf", 7 * MIB));
    Dataset<Row> batch = spark().createDataFrame(versions, SCHEMA);
    TypedProperties props = props(2);
    List<Row> expected = Arrays.asList(RowFactory.create("same", 100L, 5 * MIB, 1),
        RowFactory.create("same", 100L, 7 * MIB, 0), RowFactory.create("same", 200L, 5 * MIB, 1));
    for (Dataset<Row> input : Arrays.asList(batch.repartition(3), batch.orderBy(functions.col(BlobColumns.SOURCE_MODIFICATION_TIME).desc()))) {
      BalancingAssignment assigned = new SizeTypeAwareBalancingStrategy().assign(input, props);
      assertEquals(expected, assigned.getAssignment().orderBy(BlobColumns.SOURCE_PATH_SHA,
          BlobColumns.SOURCE_MODIFICATION_TIME, BlobColumns.SOURCE_FILE_SIZE).collectAsList());
      try (BalancedRows balanced = RowBalancer.balance(input, new SizeTypeAwareBalancingStrategy(), props)) {
        assertEquals(3, balanced.getRows().count(), "all three versions reach their prep tasks once");
      }
    }
  }

}
