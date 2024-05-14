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
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("unchecked")
public class TestHoodieJavaPairRDD {

  private static JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    // Initialize Spark context and JavaPairRDD mock
    SparkConf conf = new SparkConf().setAppName("HoodieJavaPairRDDJoinTest").setMaster("local[2]");
    jsc = new JavaSparkContext(conf);
  }

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testJoinOperation() {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "003"),
        new Tuple2<>("2017/10/22", "002"),
        new Tuple2<>("2017/10/22", "005"),
        new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    JavaPairRDD<String, String> otherPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "value1"),
        new Tuple2<>("2017/10/22", "value2"))).mapToPair(t -> t);

    HoodieJavaPairRDD<String, String> hoodiePairData = HoodieJavaPairRDD.of(partitionRecordKeyPairRDD);
    HoodieJavaPairRDD<String, String> otherHoodiePairData = HoodieJavaPairRDD.of(otherPairRDD);

    HoodiePairData<String, Pair<String, String>> result = hoodiePairData.join(otherHoodiePairData);

    List<Pair<String, Pair<String, String>>> resultList = result.collectAsList();
    assertEquals(8, resultList.size());
    resultList.forEach(item -> {
      assertEquals("2017/10/22", item.getLeft());
      assertTrue(Arrays.asList("003", "002", "005", "004").contains(item.getRight().getLeft()));
      assertTrue(Arrays.asList("value1", "value2").contains(item.getRight().getRight()));
    });
  }

  @Test
  public void testLeftOuterJoinOperation() {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "003"),
        new Tuple2<>("2017/10/22", "002"),
        new Tuple2<>("2017/10/22", "005"),
        new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    JavaPairRDD<String, String> otherPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "value1"))).mapToPair(t -> t);

    HoodieJavaPairRDD<String, String> hoodiePairData = HoodieJavaPairRDD.of(partitionRecordKeyPairRDD);
    HoodieJavaPairRDD<String, String> otherHoodiePairData = HoodieJavaPairRDD.of(otherPairRDD);

    HoodiePairData<String, Pair<String, Option<String>>> result = hoodiePairData.leftOuterJoin(otherHoodiePairData);

    List<Pair<String, Pair<String, Option<String>>>> resultList = result.collectAsList();
    assertEquals(4, resultList.size());
    resultList.forEach(item -> {
      assertEquals("2017/10/22", item.getLeft());
      assertTrue(Arrays.asList("003", "002", "005", "004").contains(item.getRight().getLeft()));
      assertEquals(Option.of("value1"), item.getRight().getRight());
    });
  }
}
