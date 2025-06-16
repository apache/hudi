/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import scala.Tuple2;
import scala.Tuple3;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HoodieJavaPairRDDTest {

  private static final Logger LOG = LogManager.getLogger(HoodieJavaPairRDDTest.class);

  private JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    jsc = new JavaSparkContext("local[2]", "test");
  }

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
  }

  @Test
  public void testRangeBasedRepartitionForEachKey() {
    // Create initial RDD with uneven distribution
    Map<Integer, Long> maxValueByKey = new HashMap<>();
    maxValueByKey.put(1, 12L);  // Key 1 has 10 values
    maxValueByKey.put(2, 5L);   // Key 2 has 5 values
    maxValueByKey.put(3, 18L);  // Key 3 has 15 values

    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, Arrays.asList(0.6, 0.3, 0.1));
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Apply range-based repartitioning
    HoodiePairData<Integer, String> repartitionedRdd = hoodieRdd.rangeBasedRepartitionForEachKey(
        30,  // keyRange
        1.0, // sampleFraction
        6,   // maxKeyPerBucket
        42L  // seed
    );

    Map<Integer, Map<Integer, List<String>>> expectedPartitionContents = Collections.unmodifiableMap(
        new HashMap<Integer, Map<Integer, List<String>>>() {{
          put(0, Collections.singletonMap(1, Arrays.asList("1", "10", "11", "12", "2", "3")));
          put(1, Collections.singletonMap(1, Arrays.asList("4", "5", "6", "7", "8", "9")));
          put(2, Collections.singletonMap(2, Arrays.asList("1", "2", "3", "4", "5")));
          put(3, Collections.singletonMap(3, Arrays.asList("1", "10", "11", "12", "13", "14")));
          put(4, Collections.singletonMap(3, Arrays.asList("15", "16", "17", "18", "2", "3")));
          put(5, Collections.singletonMap(3, Arrays.asList("4", "5", "6", "7", "8", "9")));
        }});

    // Collect actual partition contents without any extra processing
    JavaPairRDD<Integer, String> javaPairRDD = HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd);
    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = dumpRDDContent(javaPairRDD);

    // Directly compare the maps
    assertEquals(expectedPartitionContents, actualPartitionContents,
        "Partition contents should match exactly");
  }

//  @Test
  public void testUnderSampling() {
    // Create initial RDD with uneven distribution
    Map<Integer, Long> maxValueByKey = new HashMap<>();
    maxValueByKey.put(1, 16L);
    maxValueByKey.put(2, 16L);
    maxValueByKey.put(3, 10L);

    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, Arrays.asList(0.6, 0.3, 0.1));
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Apply range-based repartitioning
    HoodiePairData<Integer, String> repartitionedRdd = hoodieRdd.rangeBasedRepartitionForEachKey(
        30,  // keyRange
        0.0625, // per key only sample at max 1 value
        4,   // need to split into 4 buckets per key per value
        42L  // seed
    );

    // Collect actual partition contents without any extra processing
    validateRepartitionedRDDProperties(hoodieRdd, repartitionedRdd, Option.of(Collections.unmodifiableMap(
        new HashMap<Integer,Integer>() {{
          put(1, 2); // it will sample at least 1 value no matter what, in that case, can have empty partitions. This validation needs to be modified.
          put(2, 2);
          put(3, 2);
        }})));
  }

//  @Test
  public void testLargeScaleRandomized() {
    // Validation needs modifications. Need more exhaustive testing. List all of them first.
    Random rand = new Random(42);

    // Random number of keys (1 to 50)
    int numKeys = 1 + rand.nextInt(50);
    int maxKeyVal = 400;
    Map<Integer, Long> maxValueByKey = new HashMap<>();

    for (int i = 0; i < numKeys; i++) {
      int key = 1 + rand.nextInt(maxKeyVal); // keys in [1, 400]
      long numValues = 50 + rand.nextInt(351); // values in [50, 400]
      maxValueByKey.put(key, numValues);
    }

    // Random number of partitions [10, 100]
    int numPartitions = 10 + rand.nextInt(91);

    // Random partition weight percentages, normalized to sum to 1.0
    List<Double> partitionWeights = new ArrayList<>();
    double totalWeight = 0.0;
    for (int i = 0; i < numPartitions; i++) {
      double weight = 0.1 + rand.nextDouble(); // avoid zero-weight partitions
      partitionWeights.add(weight);
      totalWeight += weight;
    }

    // Normalize
    for (int i = 0; i < partitionWeights.size(); i++) {
      partitionWeights.set(i, partitionWeights.get(i) / totalWeight);
    }

    // Generate initial RDD
    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, partitionWeights);
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Repartition
    HoodiePairData<Integer, String> repartitionedRdd = hoodieRdd.rangeBasedRepartitionForEachKey(
        maxKeyVal,     // keyRange
        0.05,          // sample rate
        1 + rand.nextInt(25),  // target split buckets per key
        rand.nextLong()     // random seed
    );

    validateRepartitionedRDDProperties(hoodieRdd, repartitionedRdd, Option.empty());
  }

  /** Modified RDD generator that supports weighted partition distribution */
  public static JavaPairRDD<Integer, String> generateRandomRDDWithWeightedPartitions(
      JavaSparkContext sc,
      Map<Integer, Long> maxValueByKey,
      List<Double> partitionWeights) {

    List<Tuple2<Integer, String>> allPairs = new ArrayList<>();
    for (Map.Entry<Integer, Long> e : maxValueByKey.entrySet()) {
      for (long v = 1; v <= e.getValue(); v++) {
        allPairs.add(new Tuple2<>(e.getKey(), Long.toString(v)));
      }
    }

    Collections.shuffle(allPairs, new Random(42));

    int total = allPairs.size();
    List<JavaPairRDD<Integer, String>> rdds = new ArrayList<>();
    int start = 0;

    for (int i = 0; i < partitionWeights.size(); i++) {
      int end = (i == partitionWeights.size() - 1)
          ? total
          : Math.min(total, start + (int) Math.round(partitionWeights.get(i) * total));

      List<Tuple2<Integer, String>> slice = allPairs.subList(start, end);
      JavaPairRDD<Integer, String> sliceRdd = sc.parallelize(slice, 1).mapToPair(t -> t);
      rdds.add(sliceRdd);
      start = end;
      if (start >= total) break;
    }

    JavaPairRDD<Integer, String> combined = rdds.get(0);
    for (int i = 1; i < rdds.size(); i++) {
      combined = combined.union(rdds.get(i));
    }

    return combined;
  }

  /**
   * Validates various properties of a repartitioned RDD.
   *
   * @param repartitionedRdd           the RDD to inspect
   * @param expectedPartitionsPerKey   map <key, expected #partitions>
   * @return actualPartitionContents   map <partitionId, map<key, list<values>>> – useful for debugging
   *
   * @throws AssertionError if any check fails
   */
  private static Map<Integer, Map<Integer, List<String>>> validateRepartitionedRDDProperties(
      HoodiePairData<Integer, String> originalRdd,
      HoodiePairData<Integer, String> repartitionedRdd,
      Option<Map<Integer, Integer>> expectedPartitionsPerKey) {
    JavaPairRDD<Integer, String> javaPairRDD = HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd);

    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = dumpRDDContent(javaPairRDD);

    try {
      /* ------------------------------------------------------------------
       Step 2:  Per-partition validations
       ------------------------------------------------------------------ */
      for (Map.Entry<Integer, Map<Integer, List<String>>> p : actualPartitionContents.entrySet()) {
        int partitionId = p.getKey();
        Map<Integer, List<String>> keyToValues = p.getValue();

        // (b) each value list is sorted (String natural order)
        for (Map.Entry<Integer, List<String>> kv : keyToValues.entrySet()) {
          List<String> values = kv.getValue();
          List<String> sorted = new ArrayList<>(values);
          Collections.sort(sorted);
          if (!values.equals(sorted)) {
            throw new AssertionError(
                "Partition " + partitionId + ", key " + kv.getKey() +
                    " has unsorted values: " + values);
          }
        }
      }

    /* ------------------------------------------------------------------
       Step 3:  Build key → list<(partitionId, min, max)>
       ------------------------------------------------------------------ */
      Map<Integer, List<Tuple3<Integer, String, String>>> keyToPartitionRanges = new HashMap<>();

      for (Map.Entry<Integer, Map<Integer, List<String>>> p : actualPartitionContents.entrySet()) {
        int partitionId = p.getKey();
        for (Map.Entry<Integer, List<String>> kv : p.getValue().entrySet()) {
          List<String> sorted = new ArrayList<>(kv.getValue());
          Collections.sort(sorted);
          keyToPartitionRanges
              .computeIfAbsent(kv.getKey(), k -> new ArrayList<>())
              .add(new Tuple3<>(partitionId, sorted.get(0), sorted.get(sorted.size() - 1)));
        }
      }

    /* ------------------------------------------------------------------
       Step 4:  Range-overlap check *and* expected-partition-count check
       ------------------------------------------------------------------ */
      for (Map.Entry<Integer, List<Tuple3<Integer, String, String>>> e : keyToPartitionRanges.entrySet()) {
        int key = e.getKey();
        List<Tuple3<Integer, String, String>> ranges = e.getValue();

        // 4a. confirm expected #partitions
        if (expectedPartitionsPerKey.isPresent()) {
          Integer expectedCnt = expectedPartitionsPerKey.get().get(key);
          if (expectedCnt == null) {
            throw new AssertionError("Unexpected key " + key +
                " appeared in RDD but not in expectedPartitionsPerKey map");
          }
          if (ranges.size() != expectedCnt) {
            throw new AssertionError("Key " + key + " should occupy " + expectedCnt +
                " partitions but actually occupies " + ranges.size());
          }
        }

        // 4b. check that ranges do not overlap (string order)
        ranges.sort(Comparator.comparing(t -> t._2()));   // sort by min
        for (int i = 1; i < ranges.size(); i++) {
          Tuple3<Integer, String, String> prev = ranges.get(i - 1);
          Tuple3<Integer, String, String> curr = ranges.get(i);
          if (curr._2().compareTo(prev._3()) <= 0) {
            throw new AssertionError(
                String.format(
                    "Key %d has overlapping ranges: partition %d [%s-%s] vs partition %d [%s-%s]",
                    key,
                    prev._1(), prev._2(), prev._3(),
                    curr._1(), curr._2(), curr._3()));
          }
        }
      }

    /* ------------------------------------------------------------------
       Step 5:  Verify no key is missing from actual data
       ------------------------------------------------------------------ */
      if (expectedPartitionsPerKey.isPresent()) {
        for (Integer expectedKey : expectedPartitionsPerKey.get().keySet()) {
          if (!keyToPartitionRanges.containsKey(expectedKey)) {
            throw new AssertionError("Expected key " + expectedKey + " never appeared in the RDD");
          }
        }
      }
    } catch (AssertionError e) {
      logRDDContent("Original RDD", HoodieJavaPairRDD.getJavaPairRDD(originalRdd));
      logRDDContent("Repartitioned RDD", HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd));
      LOG.error("Validation failed: " + e.getMessage(), e);
      throw e; // rethrow to fail the test
    }
    return actualPartitionContents;   // handy for unit-test callers
  }

  private static Map<Integer, Map<Integer, List<String>>> dumpRDDContent(JavaPairRDD<Integer, String> javaPairRDD) {
    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = new HashMap<>();

    javaPairRDD
        .mapPartitionsWithIndex((idx, iter) -> {
          Map<Integer, List<String>> keyToValues = new HashMap<>();
          while (iter.hasNext()) {
            Tuple2<Integer, String> row = iter.next();
            keyToValues
                .computeIfAbsent(row._1(), k -> new ArrayList<>())
                .add(row._2());
          }
          return Collections.singletonList(new Tuple2<>(idx, keyToValues)).iterator();
        }, true)
        .collect()
        .forEach(t -> actualPartitionContents.put(t._1(), t._2()));
    return actualPartitionContents;
  }

  private static void logRDDContent(String label, JavaPairRDD<Integer, String> rdd) {
    LOG.info("===== " + label + " =====");
    rdd
        .mapPartitionsWithIndex((idx, iter) -> {
          StringBuilder builder = new StringBuilder();
          builder.append("Partition ").append(idx).append(": [");

          while (iter.hasNext()) {
            Tuple2<Integer, String> kv = iter.next();
            builder.append("(").append(kv._1).append(", ").append(kv._2).append(")").append(", ");
          }
          builder.append("]");
          return Collections.singletonList(builder.toString()).iterator();
        }, true)
        .collect()
        .forEach(line -> LOG.info(line));
    LOG.info("============================\n");
  }
}