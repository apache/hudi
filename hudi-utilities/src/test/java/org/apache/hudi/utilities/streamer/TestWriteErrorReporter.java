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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests for {@link WriteErrorReporter}. Verifies the no-op contract for null/empty inputs and
 * that the List overload short-circuits without touching a Spark RDD. Logging output itself is
 * intentionally not asserted — the value of the logger is human triage, not test assertions.
 */
public class TestWriteErrorReporter {

  @Test
  public void testNullListIsNoOp() {
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors((List<WriteStatus>) null));
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors((List<WriteStatus>) null, 10));
  }

  @Test
  public void testNullRddIsNoOp() {
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors((org.apache.spark.api.java.JavaRDD<WriteStatus>) null));
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors((org.apache.spark.api.java.JavaRDD<WriteStatus>) null, 10));
  }

  @Test
  public void testEmptyListIsNoOp() {
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors(Collections.emptyList()));
  }

  @Test
  public void testZeroMaxErrorsIsNoOp() {
    WriteStatus ws = errored("global err");
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors(Collections.singletonList(ws), 0));
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors(Collections.singletonList(ws), -5));
  }

  @Test
  public void testListWithErrorsLogsWithoutThrowing() {
    List<WriteStatus> statuses = Arrays.asList(
        errored("err1"),
        clean(),
        errored("err2"));
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors(statuses, 10));
  }

  @Test
  public void testMaxCapLimitsIteration() {
    // Build a list with 5 errored statuses; cap at 2. Should iterate only the first 2
    // (no throw, no interaction beyond the first 2 verified by reaching the end of the call).
    WriteStatus a = errored("a");
    WriteStatus b = errored("b");
    WriteStatus c = errored("c");
    WriteStatus d = errored("d");
    WriteStatus e = errored("e");
    assertDoesNotThrow(() -> WriteErrorReporter.logTopErrors(Arrays.asList(a, b, c, d, e), 2));
    // The other statuses must not have been touched. Their getErrors() should not have been called.
    Mockito.verify(c, Mockito.never()).getErrors();
    Mockito.verify(d, Mockito.never()).getErrors();
    Mockito.verify(e, Mockito.never()).getErrors();
  }

  // ========== Helpers ==========

  private static WriteStatus errored(String globalError) {
    WriteStatus ws = Mockito.mock(WriteStatus.class);
    Mockito.when(ws.hasErrors()).thenReturn(true);
    Mockito.when(ws.getGlobalError()).thenReturn(new RuntimeException(globalError));
    Mockito.when(ws.getErrors()).thenReturn(new HashMap<>());
    return ws;
  }

  private static WriteStatus clean() {
    WriteStatus ws = Mockito.mock(WriteStatus.class);
    Mockito.when(ws.hasErrors()).thenReturn(false);
    return ws;
  }
}
