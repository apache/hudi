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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.Option;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SuccessfulRecordCounter}. Covers the driver-side (collected list)
 * counting and error-table unification paths, plus null safety on public entry points.
 */
public class TestSuccessfulRecordCounter {

  private static JavaSparkContext jsc;

  @BeforeAll
  public static void setUp() {
    SparkConf conf = new SparkConf()
        .setAppName("TestSuccessfulRecordCounter")
        .setMaster("local[2]");
    jsc = new JavaSparkContext(conf);
  }

  @AfterAll
  public static void tearDown() {
    if (jsc != null) {
      jsc.close();
    }
  }

  @Test
  public void testEmptyInputReturnsZero() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.emptyList(), Option.empty(), false);

    assertEquals(0L, counts.getTotalRecords());
    assertEquals(0L, counts.getTotalErroredRecords());
    assertEquals(0L, counts.getTotalSuccessfulRecords());
    assertFalse(counts.hasErrors());
  }

  @Test
  public void testSingleWriteStatusNoErrors() {
    WriteStatus ws = Mockito.mock(WriteStatus.class);
    Mockito.when(ws.getTotalRecords()).thenReturn(1000L);
    Mockito.when(ws.getTotalErrorRecords()).thenReturn(0L);

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(ws), Option.empty(), false);

    assertEquals(1000L, counts.getTotalRecords());
    assertEquals(0L, counts.getTotalErroredRecords());
    assertEquals(1000L, counts.getTotalSuccessfulRecords());
    assertFalse(counts.hasErrors());
  }

  @Test
  public void testMultipleWriteStatusesAreSummed() {
    WriteStatus a = Mockito.mock(WriteStatus.class);
    Mockito.when(a.getTotalRecords()).thenReturn(100L);
    Mockito.when(a.getTotalErrorRecords()).thenReturn(5L);

    WriteStatus b = Mockito.mock(WriteStatus.class);
    Mockito.when(b.getTotalRecords()).thenReturn(200L);
    Mockito.when(b.getTotalErrorRecords()).thenReturn(10L);

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Arrays.asList(a, b), Option.empty(), false);

    assertEquals(300L, counts.getTotalRecords());
    assertEquals(15L, counts.getTotalErroredRecords());
    assertEquals(285L, counts.getTotalSuccessfulRecords());
    assertTrue(counts.hasErrors());
  }

  @Test
  public void testUnificationDisabledIgnoresErrorTableRdd() {
    WriteStatus ws = Mockito.mock(WriteStatus.class);
    Mockito.when(ws.getTotalRecords()).thenReturn(50L);
    Mockito.when(ws.getTotalErrorRecords()).thenReturn(2L);

    // Even if an error-table RDD is provided, unification=false means it must be ignored.
    // Pass an "always throws" mock to prove the helper never touches it.
    @SuppressWarnings("unchecked")
    JavaRDD<WriteStatus> rdd = (JavaRDD<WriteStatus>) Mockito.mock(JavaRDD.class);
    Mockito.when(rdd.mapToDouble(Mockito.any())).thenThrow(new AssertionError("RDD must not be consulted when unification is disabled"));
    Mockito.when(rdd.map(Mockito.any())).thenThrow(new AssertionError("RDD must not be consulted when unification is disabled"));

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(ws), Option.of(rdd), false);

    assertEquals(50L, counts.getTotalRecords());
    assertEquals(2L, counts.getTotalErroredRecords());
    assertEquals(48L, counts.getTotalSuccessfulRecords());
  }

  @Test
  public void testHasErrorsBoundary() {
    WriteStatus ws = Mockito.mock(WriteStatus.class);
    Mockito.when(ws.getTotalRecords()).thenReturn(10L);
    Mockito.when(ws.getTotalErrorRecords()).thenReturn(1L);

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(ws), Option.empty(), false);

    assertTrue(counts.hasErrors());
  }

  // ========== Unification path (real Spark) ==========

  @Test
  public void testUnificationEnabledSumsErrorTable() {
    WriteStatus dataA = stat(100L, 5L);
    WriteStatus dataB = stat(200L, 10L);
    WriteStatus errA = stat(50L, 50L);
    WriteStatus errB = stat(25L, 25L);
    JavaRDD<WriteStatus> errorRdd = jsc.parallelize(Arrays.asList(errA, errB));

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Arrays.asList(dataA, dataB), Option.of(errorRdd), true);

    assertEquals(375L, counts.getTotalRecords());        // 100 + 200 + 50 + 25
    assertEquals(90L, counts.getTotalErroredRecords());  // 5 + 10 + 50 + 25
    assertEquals(285L, counts.getTotalSuccessfulRecords());
    assertTrue(counts.hasErrors());
  }

  // ========== RDD-based path (real Spark) ==========

  @Test
  public void testUnificationWithRealSparkErrorRdd() {
    WriteStatus dataA = stat(100L, 5L);
    JavaRDD<WriteStatus> errorRdd = jsc.parallelize(Collections.singletonList(stat(50L, 50L)));

    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(dataA), Option.of(errorRdd), true);

    assertEquals(150L, counts.getTotalRecords());
    assertEquals(55L, counts.getTotalErroredRecords());
    assertEquals(95L, counts.getTotalSuccessfulRecords());
  }

  // ========== Null safety ==========

  @Test
  public void testNullDataTableListRejected() {
    assertThrows(NullPointerException.class, () ->
        SuccessfulRecordCounter.compute(null, Option.empty(), false));
  }

  @Test
  public void testNullErrorTableOptionRejected() {
    assertThrows(NullPointerException.class, () ->
        SuccessfulRecordCounter.compute(Collections.emptyList(), null, false));
  }

  // ========== Helper ==========

  private static WriteStatus stat(long totalRecords, long totalErrorRecords) {
    // Use a real WriteStatus so it serializes for Spark closures (Mockito mocks are not Serializable).
    // @Data on WriteStatus exposes setters for totalRecords/totalErrorRecords.
    WriteStatus ws = new WriteStatus(false, 0.0);
    ws.setTotalRecords(totalRecords);
    ws.setTotalErrorRecords(totalErrorRecords);
    return ws;
  }
}
