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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.apache.hudi.client.TestCoalescingPartitioner.FlatMapFunc.getWriteStatusForPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        {1200, 50},
        {1000, 23},
        {10, 2}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("coalesceTestArgs")
  public void testCoalescingPartitionerWithRDD(int inputNumPartitions, int targetPartitions) {
    int totalHudiPartitions = Math.max(1, inputNumPartitions / targetPartitions);
    String partitionPathPrefix = "pPath";
    List<String> partitionPaths = IntStream.rangeClosed(1, totalHudiPartitions).boxed().map(integer -> partitionPathPrefix + "_" + integer).collect(Collectors.toList());
    List<WriteStatus> writeStatuses = new ArrayList<>(jsc.parallelize(partitionPaths, partitionPaths.size()).flatMap(new FlatMapFunc(targetPartitions != 1
        ? inputNumPartitions / totalHudiPartitions : targetPartitions)).collect());

    // for pending files, add to last partition.
    if (targetPartitions != 1 && inputNumPartitions - writeStatuses.size() > 0) {
      writeStatuses.addAll(getWriteStatusForPartition("/tmp/", partitionPathPrefix + "_" + (totalHudiPartitions - 1), inputNumPartitions - writeStatuses.size()));
    }

    assertEquals(writeStatuses.size(), inputNumPartitions);

    JavaRDD<WriteStatus> data = jsc.parallelize(writeStatuses, inputNumPartitions);
    JavaRDD<WriteStatus> coalescedData = data.mapToPair(new PairFunc()).partitionBy(new CoalescingPartitioner(targetPartitions)).map(new MapFunc());
    coalescedData.cache();

    List<Pair<Integer, Integer>> countsPerPartition = coalescedData.mapPartitionsWithIndex((partitionIndex, rows) -> {
      int count = 0;
      while (rows.hasNext()) {
        rows.next();
        count++;
      }
      return Collections.singletonList(Pair.of(partitionIndex, count)).iterator();
    }, true).collect();

    assertEquals(targetPartitions, countsPerPartition.size());
    // lets validate that atleast we have 50% of data in each spark partition compared to ideal scenario (we can't assume hash of strings will evenly distribute).
    countsPerPartition.forEach(pair -> {
      int numElements = pair.getValue();
      int idealExpectedCount = inputNumPartitions / targetPartitions;
      assertTrue(numElements > idealExpectedCount * 0.5);
    });
    assertEquals(targetPartitions, coalescedData.getNumPartitions());
    List<WriteStatus> result = new ArrayList<>(coalescedData.collect());
    // lets validate all paths from input are present in output as well.
    List<String> expectedInputPaths = writeStatuses.stream().map(writeStatus -> writeStatus.getStat().getPath()).collect(Collectors.toList());
    List<String> actualPaths = result.stream().map(writeStatus -> writeStatus.getStat().getPath()).collect(Collectors.toList());
    Collections.sort(expectedInputPaths);
    Collections.sort(actualPaths);
    assertEquals(expectedInputPaths, actualPaths);
    coalescedData.unpersist();
  }

  static class FlatMapFunc implements FlatMapFunction<String, WriteStatus> {

    private int numWriteStatuses;

    FlatMapFunc(int numWriteStatuses) {
      this.numWriteStatuses = numWriteStatuses;
    }

    @Override
    public Iterator<WriteStatus> call(String s) throws Exception {
      return getWriteStatusForPartition("/tmp", s, numWriteStatuses).iterator();
    }

    static List<WriteStatus> getWriteStatusForPartition(String basePath, String partititionPath, int numWriteStatuses) {
      String randomPrefix = UUID.randomUUID().toString() + "_";
      List<WriteStatus> writeStatuses = new ArrayList<>();
      for (int i = 0; i < numWriteStatuses; i++) {
        String fileName = randomPrefix + i;
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partititionPath);
        String fullFilePath = basePath + "/" + partititionPath + "/" + fileName;
        writeStat.setPath(fullFilePath);
        WriteStatus writeStatus = new WriteStatus();
        writeStatus.setStat(writeStat);
        writeStatuses.add(writeStatus);
      }
      return writeStatuses;
    }
  }

  static class PairFunc implements PairFunction<WriteStatus, String, WriteStatus> {
    @Override
    public Tuple2<String, WriteStatus> call(WriteStatus writeStatus) throws Exception {
      return Tuple2.apply(writeStatus.getStat().getPath(), writeStatus);
    }
  }

  static class MapFunc implements Function<Tuple2<String, WriteStatus>, WriteStatus> {

    @Override
    public WriteStatus call(Tuple2<String, WriteStatus> booleanIntegerTuple2) throws Exception {
      return booleanIntegerTuple2._2;
    }
  }
}
