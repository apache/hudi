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

package org.apache.hudi.client.common;

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.metadata.HoodieBackedTestDelayedTableMetadata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TestHoodieSparkEngineDynamicRepartition {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTestDelayedTableMetadata.class);

  private JavaSparkContext jsc;

  /**
   * Generates a random RDD with unbalanced data distribution across partitions.
   *
   * @param sc Spark context
   * @param maxValueByKey Map of key to maximum number of values
   * @param partitionWeights List of weights for each partition
   * @param seed seed used for randomization
   * @return RDD with weighted partition distribution
   */
  public static JavaPairRDD<Integer, String> generateRandomRDDWithWeightedPartitions(
      JavaSparkContext sc,
      Map<Integer, Long> maxValueByKey,
      List<Double> partitionWeights,
      long seed) {

    // Generate all possible pairs of key and value in a single list.
    List<Tuple2<Integer, String>> allPairs = new ArrayList<>();
    for (Map.Entry<Integer, Long> e : maxValueByKey.entrySet()) {
      for (long v = 1; v <= e.getValue(); v++) {
        allPairs.add(new Tuple2<>(e.getKey(), Long.toString(v)));
      }
    }

    Collections.shuffle(allPairs, new Random(seed));

    int total = allPairs.size();
    List<JavaPairRDD<Integer, String>> rdds = new ArrayList<>();
    int start = 0;

    // Split the list into partitions based on the weights.
    for (int i = 0; i < partitionWeights.size(); i++) {
      int end = (i == partitionWeights.size() - 1)
          ? total
          : Math.min(total, start + (int) Math.round(partitionWeights.get(i) * total));

      List<Tuple2<Integer, String>> slice = allPairs.subList(start, end);
      JavaPairRDD<Integer, String> sliceRdd = sc.parallelize(slice, 1).mapToPair(t -> t);
      rdds.add(sliceRdd);
      start = end;
      if (start >= total) {
        break;
      }
    }

    // Combine all the partitions into a single RDD.
    JavaPairRDD<Integer, String> combined = rdds.get(0);
    for (int i = 1; i < rdds.size(); i++) {
      combined = combined.union(rdds.get(i));
    }

    return combined;
  }

  /**
   * Validates various properties of a repartitioned RDD, including:
   * 1. Each key is in exactly one partition.
   * 2. The keys are sorted within each partition.
   * 3. For partitions containing entries of the same key, the value ranges are not overlapping.
   * 4. Number of keys per partition is probably at most maxKeyPerBucket.
   *
   * @param originalRdd            Original RDD
   * @param repartitionedRdd       Repartitioned RDD
   * @param maxPartitionCountByKey Map of key to maximum number of partitions
   * @throws AssertionError if any check fails
   */
  private static void validateRepartitionedRDDProperties(
      HoodiePairData<Integer, String> originalRdd,
      HoodiePairData<Integer, String> repartitionedRdd,
      Option<Map<Integer, Integer>> maxPartitionCountByKey) {
    JavaPairRDD<Integer, String> javaPairRDD = HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd);

    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = dumpRDDContent(javaPairRDD);

    try {
      // Values in each partition are sorted.
      for (Map.Entry<Integer, Map<Integer, List<String>>> p : actualPartitionContents.entrySet()) {
        int partitionId = p.getKey();
        Map<Integer, List<String>> keyToValues = p.getValue();

        if (keyToValues.size() != 1) {
          assertEquals(1, keyToValues.size(),
              "Each partition should contain exactly one key, but found keys " + keyToValues.keySet()
                  + " in partition " + partitionId);
          logRDDContent("validation failure, original rdd ", originalRdd);
          logRDDContent("validation failure, repartitioned rdd ", repartitionedRdd);
        }

        for (Map.Entry<Integer, List<String>> kv : keyToValues.entrySet()) {
          List<String> values = kv.getValue();
          List<String> sorted = new ArrayList<>(values);
          Collections.sort(sorted);
          if (!values.equals(sorted)) {
            throw new AssertionError(
                "Partition " + partitionId + ", key " + kv.getKey()
                    + " has unsorted values: " + values);
          }
        }
      }

      // Build key → list<(partitionId, min, max)>
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

      // Range-overlap check and expected-partition-count check
      for (Map.Entry<Integer, List<Tuple3<Integer, String, String>>> e : keyToPartitionRanges.entrySet()) {
        int key = e.getKey();
        List<Tuple3<Integer, String, String>> ranges = e.getValue();

        // Confirm expected #partitions
        if (maxPartitionCountByKey.isPresent()) {
          Integer maxPartitionCnt = maxPartitionCountByKey.get().get(key);
          if (maxPartitionCnt == null) {
            throw new AssertionError("Unexpected key " + key
                + " appeared in RDD but not in expectedPartitionsPerKey map");
          }
          if (ranges.size() > maxPartitionCnt) {
            throw new AssertionError("Key " + key + " should occupy at most " + maxPartitionCnt
                + " partitions but actually occupies " + ranges.size());
          }
        }

        // Check that ranges do not overlap (string order)
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

      // Verify no key is missing from actual data
      if (maxPartitionCountByKey.isPresent()) {
        for (Integer expectedKey : maxPartitionCountByKey.get().keySet()) {
          if (!keyToPartitionRanges.containsKey(expectedKey)) {
            throw new AssertionError("Expected key " + expectedKey + " never appeared in the RDD");
          }
        }
      }
    } catch (AssertionError e) {
      logRDDContent("Original RDD", originalRdd);
      logRDDContent("Repartitioned RDD", repartitionedRdd);
      LOG.error("Validation failed: " + e.getMessage(), e);
      throw e; // rethrow to fail the test
    }
  }

  /**
   * Dumps the content of an RDD to a map of partition id to key to values.
   *
   * @param javaPairRDD RDD to dump
   * @return Map of partition id to key to values
   */
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

  /**
   * Logs the content of an RDD to the console.
   *
   * @param label Label for the RDD
   * @param pairData RDD to log
   */
  private static void logRDDContent(String label, HoodiePairData<Integer, String> pairData) {
    JavaPairRDD<Integer, String> rdd = HoodieJavaPairRDD.getJavaPairRDD(pairData);

    LOG.info("===== {} =====", label);
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
        .forEach(LOG::info);
    LOG.info("============================\n");
  }

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

    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, Arrays.asList(0.6, 0.3, 0.1), 42);
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Apply range-based repartitioning
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<Integer> shardIndices = new ArrayList<>(maxValueByKey.keySet());
    HoodiePairData<Integer, String> repartitionedRdd = engineContext.rangeBasedRepartitionForEachKey(
        hoodieRdd, shardIndices, 1.0, // Use the full data for computing the optimal partitioner
        6,   // maxKeyPerBucket
        42L  // fixed seed to ensure deterministic result for validation
    );

    Map<Integer, Map<Integer, List<String>>> expectedPartitionContents = Collections.unmodifiableMap(
        new HashMap<Integer, Map<Integer, List<String>>>() {
          {
          put(0, Collections.singletonMap(1, Arrays.asList("1", "10", "11", "12", "2", "3")));
          put(1, Collections.singletonMap(1, Arrays.asList("4", "5", "6", "7", "8", "9")));
          put(2, Collections.singletonMap(2, Arrays.asList("1", "2", "3", "4", "5")));
          put(3, Collections.singletonMap(3, Arrays.asList("1", "10", "11", "12", "13", "14")));
          put(4, Collections.singletonMap(3, Arrays.asList("15", "16", "17", "18", "2", "3")));
          put(5, Collections.singletonMap(3, Arrays.asList("4", "5", "6", "7", "8", "9")));
        }
      }
    );

    // Collect actual partition contents without any extra processing
    JavaPairRDD<Integer, String> javaPairRDD = HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd);
    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = dumpRDDContent(javaPairRDD);

    // Directly compare the maps
    assertEquals(expectedPartitionContents, actualPartitionContents, "Partition contents should match exactly");
  }

  @Test
  public void testUnderSampling() {
    // Create initial RDD with uneven distribution
    Map<Integer, Long> maxValueByKey = new HashMap<>();
    maxValueByKey.put(1, 16L);
    maxValueByKey.put(2, 16L);
    maxValueByKey.put(3, 10L);

    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, Arrays.asList(0.6, 0.3, 0.1), 42);
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Apply range-based repartitioning
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<Integer> shardIndices = new ArrayList<>(maxValueByKey.keySet());
    HoodiePairData<Integer, String> repartitionedRdd = engineContext.rangeBasedRepartitionForEachKey(
        hoodieRdd, shardIndices, 0.0625, // per key only sample at max 1 value
        4,   // need to split into 4 buckets per key per value
        42L  // seed
    );

    // With under sampling, num of split points = num of data points we sampled.
    Map<Integer, Map<Integer, List<String>>> expectedPartitionContents = Collections.unmodifiableMap(
        new HashMap<Integer, Map<Integer, List<String>>>() {
          {
            put(0, Collections.singletonMap(1, Arrays.asList("1", "10", "11", "12", "13", "14", "15", "16", "2", "3", "4")));
            put(1, Collections.singletonMap(1, Arrays.asList("5", "6", "7", "8", "9")));
            put(2, Collections.singletonMap(2, Arrays.asList("1", "10", "11", "12", "13", "14", "15", "16", "2", "3", "4", "5", "6", "7", "8", "9")));
            put(3, Collections.emptyMap()); // Under-sampling leads to unnessarily allocating partitions to keys that do not have many entries.
            put(4, Collections.singletonMap(3, Arrays.asList("1", "10", "2", "3", "4", "5")));
            put(5, Collections.singletonMap(3, Arrays.asList("6", "7", "8", "9")));
          }
        }
    );

    // Collect actual partition contents without any extra processing
    JavaPairRDD<Integer, String> javaPairRDD = HoodieJavaPairRDD.getJavaPairRDD(repartitionedRdd);
    Map<Integer, Map<Integer, List<String>>> actualPartitionContents = dumpRDDContent(javaPairRDD);

    // Directly compare the maps
    assertEquals(expectedPartitionContents, actualPartitionContents, "Partition contents should match exactly");
    assertEquals(initialRdd.count(), repartitionedRdd.count(), "Partition contents should match exactly");
  }

  @Test
  public void testLargeScaleRandomized() {
    // Validation needs modifications. Need more exhaustive testing. List all of them first.
    Random rand = new Random(System.nanoTime());

    // Random number of keys (1 to 50)
    int numKeys = 1 + rand.nextInt(50);
    int maxKeyVal = 200;
    double sampleRate = 1.0; // It leads to optimal partitioning where no empty partitions + per partition key count < maxPartitionCountByKey.
    Map<Integer, Long> maxValueByKey = new HashMap<>();
    Map<Integer, Integer> maxPartitionCountByKey = new HashMap<>();
    int maxKeyPerBucket = 10 + rand.nextInt(40); // target split buckets per key [10, 50]
    for (int i = 0; i < numKeys; i++) {
      int key = 1 + rand.nextInt(maxKeyVal); // keys in [1, 400]
      long numValues = 50 + rand.nextInt(351); // values in [50, 400]
      maxValueByKey.put(key, numValues);
      maxPartitionCountByKey.put(key, (int) Math.ceil((double) numValues / maxKeyPerBucket));
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
    JavaPairRDD<Integer, String> initialRdd = generateRandomRDDWithWeightedPartitions(jsc, maxValueByKey, partitionWeights, System.nanoTime());
    HoodieJavaPairRDD<Integer, String> hoodieRdd = HoodieJavaPairRDD.of(initialRdd);

    // Repartition
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<Integer> shardIndices = new ArrayList<>(maxValueByKey.keySet());
    HoodiePairData<Integer, String> repartitionedRdd = engineContext.rangeBasedRepartitionForEachKey(
        hoodieRdd, shardIndices,
        sampleRate,
        maxKeyPerBucket,
        rand.nextLong()
    );

    validateRepartitionedRDDProperties(hoodieRdd, repartitionedRdd, Option.of(maxPartitionCountByKey));
  }

  @Test
  public void testEmptyRdd() {
    JavaPairRDD<Integer, String> rdd = jsc.parallelize(Collections.<Tuple2<Integer, String>>emptyList(), 1).mapToPair(p -> p);
    HoodiePairData<Integer, String> pairRdd = HoodieJavaPairRDD.of(rdd);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodiePairData<Integer, String> repartitionedRdd = engineContext.rangeBasedRepartitionForEachKey(
        pairRdd, Collections.emptyList(),
        1.0,
        10,
        System.nanoTime()
    );
    assertEquals(0, repartitionedRdd.deduceNumPartitions());
    assertEquals(0, repartitionedRdd.count());
  }
}