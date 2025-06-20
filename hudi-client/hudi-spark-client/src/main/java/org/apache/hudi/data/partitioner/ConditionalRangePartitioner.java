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

import scala.Tuple2;

public class ConditionalRangePartitioner extends Partitioner {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionalRangePartitioner.class);

  private final Map<Integer, List<String>> splitPoints;
  private final Map<Integer, Integer> startIndex;
  private final int totalPartitions;

  public ConditionalRangePartitioner(Map<Integer, List<String>> splitPoints, int keyRange) {
    this.splitPoints = splitPoints;
    this.startIndex = new HashMap<>();
    int idx = 0;
    for (int i = 0; i <= keyRange; i++) {
      if (!splitPoints.containsKey(i)) {
        continue;
      }
      startIndex.put(i, idx);
      idx += splitPoints.get(i).size() + 1;
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
    Tuple2<Integer, String> compositeKey = (Tuple2<Integer, String>) keyObject;
    int key = compositeKey._1();
    String value = compositeKey._2();
    ValidationUtils.checkArgument(startIndex.containsKey(key),
        "ConditionalRangePartitioner does not expect key " + key);

    List<String> splits = splitPoints.getOrDefault(key, Collections.emptyList());
    // binary search to find the right bucket id.
    int bucket = Collections.binarySearch(splits, value);
    if (bucket < 0) {
      bucket = -bucket - 1;
    }
    // If a given key is not expected by the partitioner, this line will error out.
    return startIndex.get(key) + bucket;
  }

  public static class CompositeKeyComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
      int cmp = Long.compare(o1._1(), o2._1());
      if (cmp != 0) {
        return cmp;
      }
      return o1._2().compareTo(o2._2());
    }
  }

  /**
   * Computes split point map per key, with all computation on executors.
   *
   * @param sampled         Sampled RDD of (key, value)
   * @param sampleFraction  Fraction used to estimate total size
   * @param maxKeyPerBucket Max number of items allowed per partition
   * @return Map of key -> list of split points
   */
  public static Map<Integer, List<String>> computeSplitPointMapDistributed(
      JavaPairRDD<Integer, String> sampled,
      double sampleFraction,
      int maxKeyPerBucket) {

    return sampled
        .groupByKey()
        .mapValues(values -> {
          List<String> sortedValues = new ArrayList<>();
          values.forEach(sortedValues::add);
          Collections.sort(sortedValues);

          int estimatedTotal = (int) Math.ceil(sortedValues.size() / sampleFraction);
          int splitCount = (int) Math.ceil((double) estimatedTotal / maxKeyPerBucket) - 1;

          if (splitCount > 0) {
            return computeSplitPoints(sortedValues, Math.min(splitCount, sortedValues.size()));
          } else {
            return Collections.<String>emptyList();
          }
        })
        .collectAsMap(); // Now result is small enough to collect to driver
  }

  public static List<String> computeSplitPoints(List<String> sortedValues, int numSplits) {
    // If we have way less data points to split, then use every value as a split point.
    if (sortedValues.size() < numSplits) {
      return sortedValues;
    }
    List<String> splits = new ArrayList<>();
    int n = sortedValues.size();
    for (double i = 1; i <= numSplits; i++) {
      int index = (int) Math.ceil(i * n / (numSplits + 1)) - 1;
      splits.add(sortedValues.get(index));
    }
    return splits;
  }

  public static JavaPairRDD<Tuple2<Integer, String>, String> repartitionWithSplits(
      JavaPairRDD<Integer, String> baseRdd, ConditionalRangePartitioner partitioner) {
    JavaPairRDD<Tuple2<Integer, String>, String> compositeKeyRdd = baseRdd.mapToPair(t -> new Tuple2<>(t, t._2()));
    return compositeKeyRdd.repartitionAndSortWithinPartitions(partitioner, new CompositeKeyComparator());
  }
}
