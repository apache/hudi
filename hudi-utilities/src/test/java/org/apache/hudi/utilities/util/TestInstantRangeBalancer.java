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

import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link InstantRangeBalancer}'s shuffle against a real local Spark context.
 *
 * <p><b>Parameterized over both dimensions' record types on purpose.</b> The balancer is the one
 * thing the FS-MDT diff and the duplicate base-file dimension share, and the whole value of sharing
 * it is that neither can quietly acquire behaviour the other lacks. Running the identical
 * assertions over {@link FileDiffRecord} and {@link MismatchRecord} is what enforces that, and it
 * gives a third dimension a template rather than a precedent to guess at.
 */
class TestInstantRangeBalancer extends HoodieSparkClientTestHarness {

  private static final String PARTITION = "2024/01/01";

  /** Each case: a label, and a factory turning (instant, fileName) into that dimension's record. */
  static Stream<Object[]> recordTypes() {
    BiFunction<String, String, Serializable> diff =
        (instant, fileName) -> FileDiffRecord.newBuilder()
            .withPartition(PARTITION)
            .withSourcePattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
            .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
            .withFileGroupId("fg-" + fileName)
            .withFileName(fileName)
            .withFileType(MismatchRecord.FileType.BASE)
            .withWritingInstant(instant, MismatchRecord.InstantSource.FILE_NAME)
            .build();
    BiFunction<String, String, Serializable> duplicate =
        (instant, fileName) -> MismatchRecord.newBuilder()
            .withPartition(PARTITION)
            .withPattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
            .withFileName(fileName)
            .withFileType(MismatchRecord.FileType.BASE)
            .withBaseInstantTime(instant)
            .build();
    return Stream.of(
        new Object[] {"FileDiffRecord", diff},
        new Object[] {"MismatchRecord", duplicate});
  }

  @BeforeEach
  void setUp() {
    initSparkContexts("TestInstantRangeBalancer");
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recordTypes")
  void everyRecordSurvivesTheRepartition(
      String label, BiFunction<String, String, Serializable> factory) {
    List<Tuple2<String, Serializable>> input = new ArrayList<>();
    for (String instant : Arrays.asList("100", "200", "300", "400")) {
      input.add(new Tuple2<>(instant, factory.apply(instant, instant + "-a.parquet")));
      input.add(new Tuple2<>(instant, factory.apply(instant, instant + "-b.parquet")));
    }

    List<Serializable> balancedRecords = balanced(input).collect();
    assertEquals(input.size(), balancedRecords.size(),
        "a repartition that drops or duplicates a record changes the run's verdict");

    Set<String> expectedIdentities = new HashSet<>();
    for (Tuple2<String, Serializable> pair : input) {
      expectedIdentities.add(identityOf(pair._2()));
    }
    Set<String> actualIdentities = new HashSet<>();
    for (Serializable record : balancedRecords) {
      actualIdentities.add(identityOf(record));
    }
    assertEquals(expectedIdentities, actualIdentities,
        "the count alone would miss a record whose data got corrupted in transit");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recordTypes")
  void singleInstantKeySpaceIsNotASpecialCase(
      String label, BiFunction<String, String, Serializable> factory) {
    List<Tuple2<String, Serializable>> input = Arrays.asList(
        new Tuple2<>("100", factory.apply("100", "a.parquet")),
        new Tuple2<>("100", factory.apply("100", "b.parquet")));

    assertEquals(2L, balanced(input).count());
  }

  /**
   * Reads partition assignment from the actual shuffled output, not from calling
   * {@link InstantRangePartitioner#getPartition} directly on the input keys: that call is a
   * deterministic pure function of its key, so a {@code Set<Integer>} built by calling it once per
   * key is a singleton by construction whatever {@code bounds} contains, and could never have caught
   * a bug in the wiring between {@code computeBounds} and the real Spark shuffle. Also asserts each
   * partition's keys come back non-decreasing, since {@link InstantRangeBalancer#repartition}'s
   * archive-batching argument rests on that order actually holding.
   *
   * <p>The co-location check below is likewise true for <em>any</em> {@code bounds} -- routing by a
   * pure function of the key can never split one key across partitions, correct {@code bounds} or
   * not -- so the real guarantee this test stands on is asserted directly against {@code bounds}
   * itself: ascending and duplicate-free (or {@code binarySearch} misroutes), every bound an actual
   * instant (or one would be split), and the largest instant never a bound (or its partition would be
   * empty).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("recordTypes")
  void everyRecordOfOneInstantStaysInOnePartition(
      String label, BiFunction<String, String, Serializable> factory) {
    List<Tuple2<String, Serializable>> input = new ArrayList<>();
    for (String instant : Arrays.asList("100", "200", "300", "400", "500", "600")) {
      for (int i = 0; i < 20; i++) {
        input.add(new Tuple2<>(instant, factory.apply(instant, instant + "-" + i + ".parquet")));
      }
    }

    JavaPairRDD<String, Serializable> pairs = jsc.parallelize(input, 2).mapToPair(pair -> pair);
    Map<String, Long> counts = InstantRangeBalancer.countByInstant(pairs);
    List<String> bounds = InstantRangeBalancer.computeBounds(counts,
        InstantRangeBalancer.partitionCount(input.size(), jsc.defaultParallelism()));

    assertEquals(new ArrayList<>(new TreeSet<>(bounds)), bounds,
        "bounds must be ascending and duplicate-free or binarySearch misroutes records");
    assertTrue(counts.keySet().containsAll(bounds),
        "every bound must be an actual instant, or an instant would be split across partitions");
    assertNotEquals(Collections.max(counts.keySet()), bounds.isEmpty() ? null
        : bounds.get(bounds.size() - 1), "the largest instant must never close a partition");

    List<Tuple2<Integer, String>> assignments = pairs
        .repartitionAndSortWithinPartitions(new InstantRangePartitioner(bounds))
        .mapPartitionsWithIndex((index, iterator) -> {
          List<Tuple2<Integer, String>> seen = new ArrayList<>();
          while (iterator.hasNext()) {
            seen.add(new Tuple2<>(index, iterator.next()._1()));
          }
          return seen.iterator();
        }, false)
        .collect();

    Map<String, Set<Integer>> partitionsByInstant = new HashMap<>();
    Map<Integer, List<String>> keysByPartition = new HashMap<>();
    for (Tuple2<Integer, String> assignment : assignments) {
      partitionsByInstant.computeIfAbsent(assignment._2(), k -> new HashSet<>()).add(assignment._1());
      keysByPartition.computeIfAbsent(assignment._1(), k -> new ArrayList<>()).add(assignment._2());
    }

    for (Map.Entry<String, Set<Integer>> entry : partitionsByInstant.entrySet()) {
      assertEquals(1, entry.getValue().size(),
          "instant " + entry.getKey() + " was split across " + entry.getValue()
              + "; two tasks would each scan the archived timeline for it");
    }

    for (Map.Entry<Integer, List<String>> entry : keysByPartition.entrySet()) {
      List<String> expectedOrder = new ArrayList<>(entry.getValue());
      Collections.sort(expectedOrder);
      assertEquals(expectedOrder, entry.getValue(),
          "partition " + entry.getKey() + "'s keys did not come back non-decreasing: "
              + entry.getValue());
    }
  }

  /**
   * The regression test for the deleted subset filter. A record whose instant the driver's count
   * map never held must still route and still come back — under the old partitioner this killed the
   * whole validation, which is why both dimensions bounded their shuffled population to a sampled
   * subset and reported the excluded records without a verdict.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("recordTypes")
  void recordWhoseInstantTheDriverNeverCountedIsStillShuffled(
      String label, BiFunction<String, String, Serializable> factory) {
    List<Tuple2<String, Serializable>> input = Arrays.asList(
        new Tuple2<>("100", factory.apply("100", "a.parquet")),
        new Tuple2<>("200", factory.apply("200", "b.parquet")),
        new Tuple2<>("999", factory.apply("999", "unseen.parquet")));

    Map<String, Long> partialCounts = new HashMap<>();
    partialCounts.put("100", 1L);
    partialCounts.put("200", 1L);

    JavaPairRDD<String, Serializable> pairs = jsc.parallelize(input, 2).mapToPair(pair -> pair);
    assertEquals(3L, InstantRangeBalancer.repartition(jsc, pairs, partialCounts).count(),
        "an instant absent from the counts must route rather than fail the job");
  }

  /**
   * The hot instant is sanctioned: it cannot be split by a boundary array (that guarantee is
   * exercised against the real shuffle output in {@link #everyRecordOfOneInstantStaysInOnePartition}
   * above), so it gets an oversized partition to itself rather than dragging other instants into it.
   *
   * <p>This test reads partition assignment via {@link InstantRangePartitioner#getPartition}
   * directly on the keys rather than the real shuffled output, so it cannot observe whether the hot
   * instant itself stayed in one partition -- that call is a deterministic pure function of its key,
   * so {@code indicesByInstant} builds a singleton set for every instant by construction, whatever
   * {@code bounds} contains. What it can and does observe is which instants share a partition index
   * with the hot one.
   */
  @Test
  void hotInstantSharesItsPartitionWithNoOtherInstant() {
    List<Tuple2<String, Serializable>> input = new ArrayList<>();
    for (String instant : Arrays.asList("100", "200", "400", "500")) {
      for (int i = 0; i < 10; i++) {
        input.add(new Tuple2<>(instant, record(instant, instant + "-" + i + ".parquet")));
      }
    }
    for (int i = 0; i < InstantRangeBalancer.TARGET_RECORDS_PER_PARTITION * 2; i++) {
      input.add(new Tuple2<>("300", record("300", "hot-" + i + ".parquet")));
    }

    Map<String, Set<Integer>> indicesByInstant = indicesByInstant(input);
    Set<Integer> hotIndices = indicesByInstant.get("300");
    int hot = hotIndices.iterator().next();

    Set<Integer> othersSharingWithHot = new HashSet<>();
    for (Map.Entry<String, Set<Integer>> entry : indicesByInstant.entrySet()) {
      if (!"300".equals(entry.getKey()) && entry.getValue().contains(hot)) {
        othersSharingWithHot.add(hot);
      }
    }
    assertTrue(othersSharingWithHot.isEmpty(),
        "the hot instant dragged other instants into its oversized partition: " + indicesByInstant);
  }

  @Test
  void countByInstantCountsEveryRecordOfEveryInstant() {
    List<Tuple2<String, Serializable>> input = Arrays.asList(
        new Tuple2<>("100", record("100", "a.parquet")),
        new Tuple2<>("100", record("100", "b.parquet")),
        new Tuple2<>("200", record("200", "c.parquet")));

    JavaPairRDD<String, Serializable> pairs = jsc.parallelize(input, 2).mapToPair(pair -> pair);
    Map<String, Long> counts = InstantRangeBalancer.countByInstant(pairs);

    assertEquals(2, counts.size());
    assertEquals(Long.valueOf(2L), counts.get("100"));
    assertEquals(Long.valueOf(1L), counts.get("200"));
  }

  @Test
  void countByInstantOfAnEmptyRddIsEmpty() {
    JavaPairRDD<String, Serializable> pairs =
        jsc.<Tuple2<String, Serializable>>parallelize(new ArrayList<>(), 2).mapToPair(pair -> pair);
    assertTrue(InstantRangeBalancer.countByInstant(pairs).isEmpty());
  }

  private org.apache.spark.api.java.JavaRDD<Serializable> balanced(
      List<Tuple2<String, Serializable>> input) {
    JavaPairRDD<String, Serializable> pairs = jsc.parallelize(input, 2).mapToPair(pair -> pair);
    return InstantRangeBalancer.repartition(jsc, pairs, InstantRangeBalancer.countByInstant(pairs));
  }

  /** Which partition index each instant's records landed in, after the shuffle. */
  private Map<String, Set<Integer>> indicesByInstant(List<Tuple2<String, Serializable>> input) {
    JavaPairRDD<String, Serializable> pairs = jsc.parallelize(input, 2).mapToPair(pair -> pair);
    Map<String, Long> counts = InstantRangeBalancer.countByInstant(pairs);
    List<String> bounds = InstantRangeBalancer.computeBounds(counts,
        InstantRangeBalancer.partitionCount(input.size(), jsc.defaultParallelism()));
    InstantRangePartitioner partitioner = new InstantRangePartitioner(bounds);

    Map<String, Set<Integer>> indices = new HashMap<>();
    for (Tuple2<String, Serializable> pair : input) {
      indices.computeIfAbsent(pair._1(), k -> new HashSet<>())
          .add(partitioner.getPartition(pair._1()));
    }
    return indices;
  }

  /** The identifying fields of a record, independent of dimension, as {@code instant|fileName}. */
  private static String identityOf(Serializable record) {
    if (record instanceof FileDiffRecord) {
      FileDiffRecord diff = (FileDiffRecord) record;
      return diff.getWritingInstant() + "|" + diff.getFileName();
    }
    MismatchRecord duplicate = (MismatchRecord) record;
    return duplicate.getBaseInstantTime() + "|" + duplicate.getFileName();
  }

  private static Serializable record(String instant, String fileName) {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withSourcePattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withFileGroupId("fg-" + fileName)
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withWritingInstant(instant, MismatchRecord.InstantSource.FILE_NAME)
        .build();
  }
}
