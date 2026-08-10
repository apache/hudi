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

package org.apache.hudi.common.util.hash;

import org.apache.hudi.common.util.Functions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBucketIndexUtil {

  // (int parallelism, int bucketNumber)
  private static Stream<Arguments> partitionParams() {
    List<Arguments> argsList = new ArrayList<>();
    argsList.add(Arguments.of(10, 5, true));
    argsList.add(Arguments.of(20, 5, true));
    argsList.add(Arguments.of(21, 5, true));
    argsList.add(Arguments.of(40, 5, true));
    argsList.add(Arguments.of(41, 5, true));
    argsList.add(Arguments.of(100, 5, true));
    argsList.add(Arguments.of(101, 5, true));
    argsList.add(Arguments.of(20, 100, true));
    argsList.add(Arguments.of(21, 100, true));
    argsList.add(Arguments.of(100, 100, true));
    argsList.add(Arguments.of(101, 100, true));
    argsList.add(Arguments.of(200, 100, true));
    argsList.add(Arguments.of(201, 100, true));
    argsList.add(Arguments.of(400, 1000, true));
    argsList.add(Arguments.of(401, 1000, true));
    argsList.add(Arguments.of(65536, 65536, true));

    return argsList.stream();
  }

  private static Stream<Arguments> testPartitionWithPartitionLevelBucketIndex() {
    List<Arguments> argsList = new ArrayList<>();
    List<String> total40 = Arrays.asList(
        "year=2021/month=01/day=01,18",
        "year=2021/month=01/day=02,11",
        "year=2021/month=01/day=03,11");
    List<String> total100 = Arrays.asList(
        "year=2021/month=01/day=01,18",
        "year=2021/month=01/day=02,21",
        "year=2021/month=01/day=03,61");
    List<String> total1000 = Arrays.asList(
        "year=2021/month=01/day=01,18",
        "year=2021/month=01/day=02,21",
        "year=2021/month=01/day=03,961");
    argsList.add(Arguments.of(10, total40));
    argsList.add(Arguments.of(20, total40));
    argsList.add(Arguments.of(21, total40));
    argsList.add(Arguments.of(100, total40));
    argsList.add(Arguments.of(101, total40));
    argsList.add(Arguments.of(20, total100));
    argsList.add(Arguments.of(21, total100));
    argsList.add(Arguments.of(300, total100));
    argsList.add(Arguments.of(201, total100));
    argsList.add(Arguments.of(400, total1000));
    argsList.add(Arguments.of(401, total1000));

    return argsList.stream();
  }

  private static Stream<Arguments> noPartitionParams() {
    List<Arguments> argsList = new ArrayList<>();

    argsList.add(Arguments.of(10, 50, false));
    argsList.add(Arguments.of(11, 50, false));
    argsList.add(Arguments.of(100, 50, false));
    argsList.add(Arguments.of(101, 50, false));

    return argsList.stream();
  }

  @ParameterizedTest
  @MethodSource("testPartitionWithPartitionLevelBucketIndex")
  void testPartitionWithPartitionLevelBucketIndex(int parallelism, List<String> partition2Bucket) {
    int totalBucketNumber = 0;
    for (String s : partition2Bucket) {
      totalBucketNumber = totalBucketNumber + Integer.parseInt(s.split(",")[1]);
    }
    Map<Integer, Integer> parallelism2TaskCount = new HashMap<>();
    final Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc =
        BucketIndexUtil.getPartitionIndexFunc(parallelism);
    initPartitionDataPartitionBucket(parallelism2TaskCount, partitionIndexFunc, partition2Bucket);

    checkResult(parallelism2TaskCount, parallelism, totalBucketNumber, true, false);
  }

  @ParameterizedTest
  @MethodSource("partitionParams")
  void testPartition(int parallelism, int bucketNumber, boolean partitioned) {
    Map<Integer, Integer> parallelism2TaskCount = new HashMap<>();
    final Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc =
        BucketIndexUtil.getPartitionIndexFunc(parallelism);
    initPartitionData(parallelism2TaskCount, bucketNumber, partitionIndexFunc);
    checkResult(parallelism2TaskCount, parallelism, bucketNumber, partitioned);
  }

  @ParameterizedTest
  @MethodSource("noPartitionParams")
  void testNoPartition(int parallelism, int bucketNumber, boolean partitioned) {
    Map<Integer, Integer> parallelism2TaskCount = new HashMap<>();
    final Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc =
        BucketIndexUtil.getPartitionIndexFunc(parallelism);
    initNoPartitionData(parallelism2TaskCount, bucketNumber, partitionIndexFunc);
    checkResult(parallelism2TaskCount, parallelism, bucketNumber, partitioned);
  }

  private static void putIndexCount(Map<Integer, Integer> parallelism2TaskCount, int workIndex) {
    if (parallelism2TaskCount.containsKey(workIndex)) {
      parallelism2TaskCount.put(workIndex, parallelism2TaskCount.get(workIndex) + 1);
    } else {
      parallelism2TaskCount.put(workIndex, 1);
    }
  }

  private void checkResult(Map<Integer, Integer> parallelism2TaskCount, int parallelism, int bucketNumber, boolean partitioned, boolean needExpand) {
    int sum = 0;
    assertTrue(parallelism2TaskCount.keySet().stream().allMatch(id -> id >= 0 && id < parallelism));
    for (int v : parallelism2TaskCount.values()) {
      sum = sum + v;
    }
    int avg = sum / parallelism;
    double minToleranceValue = avg * 0.8;
    double maxToleranceValue = avg * 1.2;
    final ArrayList<Integer> outOfLimit = new ArrayList<>();
    final ArrayList<Integer> inLimit = new ArrayList<>();
    for (int v : parallelism2TaskCount.values()) {
      // if parallelism is too bigger, first condition will false although the diff is 1
      // for example, avg is 4, v is 5 or 3 all will out of limit, so add second condition
      if ((v >= minToleranceValue) && (v <= maxToleranceValue) || ((Math.abs(v - avg) <= 2))) {
        inLimit.add(v);
      } else {
        outOfLimit.add(v);
      }
    }
    assertEquals(0, outOfLimit.size());

    // parallelism2TaskCount indicates the number of data split assigned each parallelism,
    // so it's size need infinitely close parallelism or (bucketNumber * 8), try to take advantage of available resources as much as possible
    int totalBucketFileNumber = bucketNumber;
    if (partitioned && needExpand) {
      totalBucketFileNumber = bucketNumber * 8;
    }
    if (parallelism >= totalBucketFileNumber) {
      assertTrue(parallelism2TaskCount.size() >= (totalBucketFileNumber * 0.9));
    } else {
      assertTrue(parallelism2TaskCount.size() >= parallelism * 0.9);
    }
  }

  private void checkResult(Map<Integer, Integer> parallelism2TaskCount, int parallelism, int bucketNumber, boolean partitioned) {
    checkResult(parallelism2TaskCount, parallelism, bucketNumber, partitioned, true);
  }

  private void initPartitionData(Map<Integer, Integer> parallelism2TaskCount, int bucketNumber,
                                 Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc) {
    parallelism2TaskCount.clear();
    Arrays.asList("year=2021/month=01/day=01", "year=2021/month=01/day=02", "year=2021/month=01/day=03", "year=2021/month=01/day=04",
        "year=2021/month=01/day=05", "year=2021/month=01/day=06", "year=2021/month=01/day=07", "year=2021/month=01/day=08").forEach(partition -> {
          for (int bucketIndex = 0; bucketIndex < bucketNumber; bucketIndex++) {
            putIndexCount(parallelism2TaskCount, partitionIndexFunc.apply(bucketNumber, partition, bucketIndex));
          }
        });
  }

  private void initPartitionDataPartitionBucket(Map<Integer, Integer> parallelism2TaskCount, Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc,
                                                List<String> partitionToBucket) {
    partitionToBucket.forEach(partitionPair -> {
      String[] pair = partitionPair.split(",");
      int bucketNumber = Integer.parseInt(pair[1]);
      for (int bucketIndex = 0; bucketIndex < bucketNumber; bucketIndex++) {
        putIndexCount(parallelism2TaskCount, partitionIndexFunc.apply(bucketNumber, pair[0], bucketIndex));
      }
    });
  }

  private void initNoPartitionData(Map<Integer, Integer> parallelism2TaskCount, int bucketNumber,
                                   Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc) {
    parallelism2TaskCount.clear();
    for (int bucketIndex = 0; bucketIndex < bucketNumber; bucketIndex++) {
      putIndexCount(parallelism2TaskCount, partitionIndexFunc.apply(bucketNumber, "", bucketIndex));
    }
  }
}
