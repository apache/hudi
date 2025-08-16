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

package org.apache.hudi.data.partitioner;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Partitioner that partitions the data based on the value ranges of the keys.
 * 
 * It is used to partition the data into a fixed number of partitions, so that:
 * 1. The keys are sorted within each partition.
 * 2. There is at most only 1 key per partition.
 * 3. For partitions containing entries of the same key, the value ranges are not overlapping.
 */
public class ConditionalRangePartitioner<S extends Comparable<S>, V extends Comparable<V>> extends Partitioner {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionalRangePartitioner.class);

  private final Map<S, List<V>> splitPoints;
  private final Map<S, Integer> startIndex;
  private final int totalPartitions;

  public ConditionalRangePartitioner(Map<S, List<V>> splitPoints) {
    this.splitPoints = splitPoints;
    this.startIndex = new HashMap<>();
    int idx = 0;
    // Sort the key to ensure consistent key processing order across executors.
    List<S> sortedKeys = splitPoints.keySet().stream().sorted().collect(Collectors.toList());
    for (S sortedKey : sortedKeys) {
      startIndex.put(sortedKey, idx);
      idx += splitPoints.get(sortedKey).size() + 1;
    }
    this.totalPartitions = idx;
    LOG.info("Total num of partitions to be enforced is {}", totalPartitions);
  }

  @Override
  public int numPartitions() {
    return totalPartitions;
  }

  @Override
  public int getPartition(Object keyObject) {
    Tuple2<S, V> compositeKey = (Tuple2<S, V>) keyObject;
    S key = compositeKey._1();
    V value = compositeKey._2();
    ValidationUtils.checkArgument(startIndex.containsKey(key),
        "ConditionalRangePartitioner does not expect key " + key);

    List<V> splits = splitPoints.getOrDefault(key, Collections.emptyList());
    // binary search to find the right bucket id.
    int bucket = Collections.binarySearch(splits, value);
    if (bucket < 0) {
      bucket = -bucket - 1;
    }
    // If a given key is not expected by the partitioner, this line will error out.
    return startIndex.get(key) + bucket;
  }

  /**
   * Comparator that sort first on key, then on value.
   */
  public static class CompositeKeyComparator<S extends Comparable<S>, V extends Comparable<V>> implements Comparator<Tuple2<S, V>>, Serializable {
    @Override
    public int compare(Tuple2<S, V> o1, Tuple2<S, V> o2) {
      int cmp = o1._1().compareTo(o2._1());
      if (cmp != 0) {
        return cmp;
      }
      return o1._2().compareTo(o2._2());
    }
  }

  /**
   * Computes split point map per key, with all computation on executors.
   *
   * @param sampled         Sampled RDD of (key, value), it must contain all keys from the original rdd.
   * @param sampleFraction  Fraction used to estimate total size, if the fraction is so small that
   *                        actual total size * fraction < 1, we assume at least 1 key is sampled.
   * @param maxKeyPerBucket Max number of items allowed per partition
   * @param <V> Type of the value in the input data
   * @return Map of key -> list of split points specifying value range per partition.
   */
  public static <S extends Comparable<S>, V extends Comparable<V>> Map<S, List<V>> computeSplitPointMapDistributed(
      JavaPairRDD<S, V> sampled,
      double sampleFraction,
      int maxKeyPerBucket) {

    return sampled
        .groupByKey()
        .mapValues(values -> {
          List<V> sortedValues = new ArrayList<>();
          values.forEach(sortedValues::add);
          Collections.sort(sortedValues);

          int estimatedTotal = (int) Math.ceil(sortedValues.size() / sampleFraction);
          int splitCount = (int) Math.ceil((double) estimatedTotal / maxKeyPerBucket) - 1;

          if (splitCount > 0) {
            // If we only have x data points yet we need to split into > x + 1 parts, then only split into x + 1
            // pieces using every single sampled data as a split point.
            return computeSplitPoints(sortedValues, Math.min(splitCount, sortedValues.size()));
          } else {
            // If estimatedTotal of a given key < maxKeyPerBucket, don't create split points, meaning that all entries
            // of the key goes into 1 single partition.
            return Collections.<V>emptyList();
          }
        })
        .collectAsMap(); // Now the result is small enough to collect to driver
  }

  public static <V extends Comparable<V>> List<V> computeSplitPoints(List<V> sortedValues, int numSplits) {
    // If we have way less data points to split, then use every value as a split point.
    if (sortedValues.size() < numSplits) {
      return sortedValues;
    }
    List<V> splits = new ArrayList<>();
    int n = sortedValues.size();
    for (double i = 1; i <= numSplits; i++) {
      int index = (int) Math.ceil(i * n / (numSplits + 1)) - 1;
      splits.add(sortedValues.get(index));
    }
    return splits;
  }

  public static <S extends Comparable<S>, V extends Comparable<V>> JavaPairRDD<Tuple2<S, V>, V> repartitionWithSplits(
      JavaPairRDD<S, V> baseRdd, ConditionalRangePartitioner<S, V> partitioner) {
    JavaPairRDD<Tuple2<S, V>, V> compositeKeyRdd = baseRdd.mapToPair(t -> new Tuple2<>(t, t._2()));
    return compositeKeyRdd.repartitionAndSortWithinPartitions(partitioner, new CompositeKeyComparator<S, V>());
  }
}
