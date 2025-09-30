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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCoalescingPartitioner extends HoodieClientTestBase {

  @Test
  public void simpleCoalescingPartitionerTest() {
    int numPartitions = 100;
    HoodieData<Integer> rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));

    // 100 keys spread across 10 partitions.
    CoalescingPartitioner coalescingPartitioner = new CoalescingPartitioner(10);
    assertEquals(10, coalescingPartitioner.numPartitions());
    rddData.collectAsList().forEach(entry -> {
      assertEquals(entry.hashCode() % 10, coalescingPartitioner.getPartition(entry));
    });

    // 1 partition
    CoalescingPartitioner coalescingPartitioner1 = new CoalescingPartitioner(1);
    assertEquals(1, coalescingPartitioner1.numPartitions());
    rddData.collectAsList().forEach(entry -> {
      assertEquals(0, coalescingPartitioner1.getPartition(entry));
    });

    // empty rdd
    rddData = HoodieJavaRDD.of(jsc.emptyRDD());
    CoalescingPartitioner coalescingPartitioner2 = new CoalescingPartitioner(1);
    assertEquals(1, coalescingPartitioner2.numPartitions());
    rddData.collectAsList().forEach(entry -> {
      // since there is only one partition, any getPartition will return just the same partition index
      assertEquals(0, coalescingPartitioner2.getPartition(entry));
    });
  }

  private static Stream<Arguments> coalesceTestArgs() {
    return Arrays.stream(new Object[][] {
        {100, 1},
        {1, 1},
        {1000, 10},
        {100, 7},
        {10, 2}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("coalesceTestArgs")
  public void testCoalescingPartitionerWithRDD(int inputNumPartitions, int targetPartitions) {
    List<Integer> inputData = IntStream.rangeClosed(0, inputNumPartitions).boxed().collect(Collectors.toList());
    JavaRDD<Integer> data = jsc.parallelize(inputData, inputNumPartitions);
    JavaRDD<Integer> coalescedData = data.mapToPair(new PairFunc()).partitionBy(new CoalescingPartitioner(targetPartitions)).map(new MapFunc());

    assertEquals(targetPartitions, coalescedData.getNumPartitions());
    List<Integer> result = coalescedData.collect();
    assertEquals(inputData, result);
  }

  static class PairFunc implements PairFunction<Integer, Boolean, Integer> {
    @Override
    public Tuple2<Boolean, Integer> call(Integer integer) throws Exception {
      return Tuple2.apply(true, integer);
    }
  }

  static class MapFunc implements Function<Tuple2<Boolean, Integer>, Integer> {

    @Override
    public Integer call(Tuple2<Boolean, Integer> booleanIntegerTuple2) throws Exception {
      return booleanIntegerTuple2._2;
    }
  }
}
