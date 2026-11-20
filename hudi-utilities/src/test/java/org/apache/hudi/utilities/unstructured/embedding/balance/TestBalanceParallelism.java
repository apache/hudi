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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Plans for the configured capacity even before autoscaled executors register. */
class TestBalanceParallelism {

  private static final String CLUSTER = "k8s://https://kubernetes.default.svc";

  private static BalanceParallelism.Resolved resolve(TypedProperties props, SparkConf conf, String master, int registeredCores,
                                                     int defaultParallelism) {
    return BalanceParallelism.resolve(props, conf, master, registeredCores, defaultParallelism);
  }

  @Test
  void parallelismFromMinPartitions() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.unstructured.embedding.prep.balance.min.partitions", "7");
    BalanceParallelism.Resolved p = resolve(props, new SparkConf(false).set("spark.executor.instances", "50"), CLUSTER, 100, 2);
    assertEquals(7, p.parallelism());
    assertEquals("hoodie.unstructured.embedding.prep.balance.min.partitions", p.source());
  }

  @Test
  void sizeAwareFloorUsesItsOwnKey() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.unstructured.embedding.prep.balance.min.partitions", "7");
    props.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "19");
    assertEquals(7, resolve(props, new SparkConf(false), "local[2]", 0, 2).parallelism());
    assertEquals(19, EmbeddingTableConfig.from(props).balanceNumPartitions(2));
  }

  @Test
  void hashPartitionCountDoesNotSetSizeAwareFloor() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "19");
    assertEquals(2, resolve(props, new SparkConf(false), "local[2]", 0, 2).parallelism());
    props.setProperty("hoodie.unstructured.embedding.prep.balance.min.partitions", "0");
    SparkConf local = new SparkConf(false);
    assertThrows(IllegalArgumentException.class, () -> resolve(props, local, "local[2]", 0, 2));
  }

  @Test
  public void parallelismFromDynamicAllocation() {
    SparkConf conf = new SparkConf(false).set("spark.dynamicAllocation.enabled", "true")
        .set("spark.dynamicAllocation.maxExecutors", "5").set("spark.executor.cores", "4");
    BalanceParallelism.Resolved p = resolve(new TypedProperties(), conf, CLUSTER, 0, 2);
    assertEquals(20, p.parallelism());
    assertTrue(p.source().contains("spark.dynamicAllocation.maxExecutors"), p.source());
    // unbounded (unset, or Spark's Int.MaxValue default written out): falls through to spark.executor.instances
    SparkConf unbounded = new SparkConf(false).set("spark.dynamicAllocation.enabled", "true")
        .set("spark.executor.instances", "2").set("spark.executor.cores", "4");
    assertEquals(8, resolve(new TypedProperties(), unbounded, CLUSTER, 0, 2).parallelism());
    unbounded.set("spark.dynamicAllocation.maxExecutors", String.valueOf(Integer.MAX_VALUE));
    assertEquals(8, resolve(new TypedProperties(), unbounded, CLUSTER, 0, 2).parallelism());
  }

  @Test
  void parallelismFromExecutorInstances() {
    SparkConf conf = new SparkConf(false).set("spark.executor.instances", "3");
    BalanceParallelism.Resolved p = resolve(new TypedProperties(), conf, CLUSTER, 0, 2);
    assertEquals(3, p.parallelism());
    assertTrue(p.source().contains("spark.executor.instances"), p.source());
  }

  @Test
  void registeredCoresAboveTheConfiguredMax() {
    SparkConf conf = new SparkConf(false).set("spark.executor.instances", "3").set("spark.executor.cores", "4");
    assertEquals(64, resolve(new TypedProperties(), conf, CLUSTER, 64, 2).parallelism());
  }

  @Test
  void parallelismInLocalMode() {
    assertEquals(6, resolve(new TypedProperties(), new SparkConf(false), "local[6]", 0, 2).parallelism());
    assertEquals(Runtime.getRuntime().availableProcessors(),
        resolve(new TypedProperties(), new SparkConf(false), "local[*]", 0, 2).parallelism());
    assertEquals(1, resolve(new TypedProperties(), new SparkConf(false), "local", 0, 2).parallelism());
    assertEquals(3, resolve(new TypedProperties(), new SparkConf(false), "local[3, 2]", 0, 2).parallelism());
  }

  @Test
  void malformedExecutorConfNamesTheKey() {
    SparkConf conf = new SparkConf(false).set("spark.executor.instances", "3").set("spark.executor.cores", "four");
    TypedProperties none = new TypedProperties();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> resolve(none, conf, CLUSTER, 0, 2));
    assertTrue(e.getMessage().contains("spark.executor.cores"), e.getMessage());
  }

  @Test
  void parallelismFallsBackToDefaultParallelism() {
    BalanceParallelism.Resolved p = resolve(new TypedProperties(), new SparkConf(false), CLUSTER, 0, 2);
    assertEquals(2, p.parallelism());
    assertEquals("spark.default.parallelism", p.source());
  }

  @Test
  void explicitParallelismWinsAndMustBePositive() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "7");
    assertEquals(7, BalanceParallelism.resolve(props, new SparkConf(false), "local[2]", 30, 9).parallelism());
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "0");
    SparkConf local = new SparkConf(false);
    assertThrows(IllegalArgumentException.class, () -> BalanceParallelism.resolve(props, local, "local[2]", 0, 1));
  }

  @Test
  void fallbackClampsAndOverflowDoesNotWrap() {
    assertEquals(9, resolve(new SparkConf(false), "yarn", 0, 9));
    assertEquals(1, resolve(new SparkConf(false), null, 0, 0));
    SparkConf conf = new SparkConf(false).set(BalanceParallelism.EXECUTOR_INSTANCES, String.valueOf(Integer.MAX_VALUE))
        .set(BalanceParallelism.EXECUTOR_CORES, "4");
    assertEquals(Integer.MAX_VALUE, resolve(conf, "yarn", 0, 1));
  }

  private static int resolve(SparkConf conf, String master, int registered, int fallback) {
    return BalanceParallelism.resolve(new TypedProperties(), conf, master, registered, fallback).parallelism();
  }
}
