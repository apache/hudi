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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ErrorTableCommitter} covering the two write paths (unification on/off),
 * the success/failure pass-through contract, and the no-op when no RDD is present.
 */
public class TestErrorTableCommitter {

  private static final String INSTANT = "20260520120000000";

  private static JavaSparkContext jsc;

  @BeforeAll
  public static void setUpSpark() {
    jsc = new JavaSparkContext(new SparkConf().setAppName("TestErrorTableCommitter").setMaster("local[2]"));
  }

  @AfterAll
  public static void tearDownSpark() {
    if (jsc != null) {
      jsc.close();
    }
  }

  /**
   * Models the writer's commit: it consumes the cached statuses and then releases them, the way
   * the write client's commit unpersists what the write persisted.
   */
  private static BaseErrorTableWriter<?> committingWriter(boolean success) {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    Mockito.when(writer.commit(Mockito.any())).thenAnswer(invocation -> {
      JavaRDD<WriteStatus> rdd = invocation.getArgument(0);
      rdd.collect();
      rdd.unpersist(true);
      return success;
    });
    return writer;
  }

  private static WriteStatus stat(long totalRecords, long totalErrorRecords) {
    WriteStatus ws = new WriteStatus(false, 0.0);
    ws.setTotalRecords(totalRecords);
    ws.setTotalErrorRecords(totalErrorRecords);
    return ws;
  }

  @Test
  public void testCollectAndCommitEvaluatesTheWriteOnce() {
    LongAccumulator evaluations = jsc.sc().longAccumulator("evaluations");
    JavaRDD<WriteStatus> rdd = jsc.parallelize(Arrays.asList(stat(3L, 3L), stat(2L, 2L)), 2)
        .map(ws -> {
          evaluations.add(1L);
          return ws;
        })
        .persist(StorageLevel.MEMORY_ONLY());
    ErrorTableCommitter.ErrorTableCommitResult result =
        ErrorTableCommitter.collectAndCommit(committingWriter(true), Option.of(rdd), true, INSTANT, Option.empty());
    assertTrue(result.isSuccess());
    List<WriteStatus> statuses = result.getWriteStatuses().get();
    assertEquals(2, statuses.size());
    assertEquals(5L, statuses.stream().mapToLong(WriteStatus::getTotalRecords).sum());
    // Two partitions, each computed exactly once: the collect ran before the commit released the cache.
    assertEquals(2L, evaluations.value());
  }

  @Test
  public void testCollectAndCommitEvaluatesAnUnpersistedWriteOnce() {
    LongAccumulator evaluations = jsc.sc().longAccumulator("evaluations");
    JavaRDD<WriteStatus> rdd = jsc.parallelize(Arrays.asList(stat(3L, 3L), stat(2L, 2L)), 2)
        .map(ws -> {
          evaluations.add(1L);
          return ws;
        });
    ErrorTableCommitter.ErrorTableCommitResult result =
        ErrorTableCommitter.collectAndCommit(committingWriter(true), Option.of(rdd), true, INSTANT, Option.empty());
    assertTrue(result.isSuccess());
    assertEquals(2, result.getWriteStatuses().get().size());
    assertEquals(2L, evaluations.value());
    assertEquals(StorageLevel.NONE(), rdd.getStorageLevel());
  }

  @Test
  public void testCollectAndCommitReportsCommitFailureWithStatuses() {
    JavaRDD<WriteStatus> rdd = jsc.parallelize(Arrays.asList(stat(1L, 1L)), 1).persist(StorageLevel.MEMORY_ONLY());
    ErrorTableCommitter.ErrorTableCommitResult result =
        ErrorTableCommitter.collectAndCommit(committingWriter(false), Option.of(rdd), true, INSTANT, Option.empty());
    assertFalse(result.isSuccess());
    assertEquals(1, result.getWriteStatuses().get().size());
  }

  @Test
  public void testCollectAndCommitLegacyPathCollectsNothing() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    Mockito.when(writer.upsertAndCommit(Mockito.anyString(), Mockito.any())).thenReturn(true);
    ErrorTableCommitter.ErrorTableCommitResult result =
        ErrorTableCommitter.collectAndCommit(writer, Option.of(rdd()), false, INSTANT, Option.empty());
    assertTrue(result.isSuccess());
    assertFalse(result.getWriteStatuses().isPresent());
    Mockito.verify(writer).upsertAndCommit(INSTANT, Option.empty());
  }

  @Test
  public void testCollectAndCommitWithoutRddIsNoOp() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    ErrorTableCommitter.ErrorTableCommitResult result =
        ErrorTableCommitter.collectAndCommit(writer, Option.empty(), true, INSTANT, Option.empty());
    assertTrue(result.isSuccess());
    assertFalse(result.getWriteStatuses().isPresent());
    Mockito.verifyNoInteractions(writer);
  }

  @SuppressWarnings("unchecked")
  private static JavaRDD<WriteStatus> rdd() {
    return (JavaRDD<WriteStatus>) Mockito.mock(JavaRDD.class);
  }

  @Test
  public void testUnificationCommitSuccess() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    JavaRDD<WriteStatus> rdd = rdd();
    Mockito.when(writer.commit(rdd)).thenReturn(true);

    boolean result = ErrorTableCommitter.commit(writer, Option.of(rdd), true, INSTANT, Option.empty());

    assertTrue(result);
    Mockito.verify(writer).commit(rdd);
    Mockito.verify(writer, Mockito.never()).upsertAndCommit(Mockito.any(), Mockito.any());
  }

  @Test
  public void testUnificationCommitFailurePropagates() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    JavaRDD<WriteStatus> rdd = rdd();
    Mockito.when(writer.commit(rdd)).thenReturn(false);

    boolean result = ErrorTableCommitter.commit(writer, Option.of(rdd), true, INSTANT, Option.empty());

    assertFalse(result);
  }

  @Test
  public void testUnificationWithoutRddIsNoOpAndReturnsTrue() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);

    boolean result = ErrorTableCommitter.commit(writer, Option.empty(), true, INSTANT, Option.empty());

    assertTrue(result);
    Mockito.verifyNoInteractions(writer);
  }

  @Test
  public void testLegacyPathUsesUpsertAndCommit() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    Option<String> latest = Option.of("20260520115959000");
    Mockito.when(writer.upsertAndCommit(INSTANT, latest)).thenReturn(true);

    boolean result = ErrorTableCommitter.commit(writer, Option.empty(), false, INSTANT, latest);

    assertTrue(result);
    Mockito.verify(writer).upsertAndCommit(INSTANT, latest);
    Mockito.verify(writer, Mockito.never()).commit(Mockito.any());
  }

  @Test
  public void testLegacyPathFailurePropagates() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    Mockito.when(writer.upsertAndCommit(Mockito.anyString(), Mockito.any())).thenReturn(false);

    boolean result = ErrorTableCommitter.commit(writer, Option.empty(), false, INSTANT, Option.empty());

    assertFalse(result);
  }

  @Test
  public void testLegacyPathIgnoresRddEvenWhenProvided() {
    // When unification is OFF, the RDD must not be touched even if accidentally passed in.
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    JavaRDD<WriteStatus> rdd = rdd();
    Mockito.when(writer.upsertAndCommit(Mockito.anyString(), Mockito.any())).thenReturn(true);

    ErrorTableCommitter.commit(writer, Option.of(rdd), false, INSTANT, Option.empty());

    Mockito.verify(writer, Mockito.never()).commit(Mockito.any());
    Mockito.verify(writer).upsertAndCommit(INSTANT, Option.empty());
  }

  @Test
  public void testNullWriterRejected() {
    assertThrows(NullPointerException.class, () ->
        ErrorTableCommitter.commit(null, Option.empty(), false, INSTANT, Option.empty()));
  }

  @Test
  public void testNullRddOptionRejected() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    assertThrows(NullPointerException.class, () ->
        ErrorTableCommitter.commit(writer, null, false, INSTANT, Option.empty()));
  }

  @Test
  public void testNullInstantRejected() {
    BaseErrorTableWriter<?> writer = Mockito.mock(BaseErrorTableWriter.class);
    assertThrows(NullPointerException.class, () ->
        ErrorTableCommitter.commit(writer, Option.empty(), false, null, Option.empty()));
  }
}
