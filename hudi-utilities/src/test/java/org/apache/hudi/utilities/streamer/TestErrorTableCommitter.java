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

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ErrorTableCommitter} covering the two write paths (unification on/off),
 * the success/failure pass-through contract, and the no-op when no RDD is present.
 */
public class TestErrorTableCommitter {

  private static final String INSTANT = "20260520120000000";

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
