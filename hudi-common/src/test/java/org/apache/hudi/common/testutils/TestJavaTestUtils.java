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

package org.apache.hudi.common.testutils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the null-message handling of {@link JavaTestUtils#checkNestedExceptionContains}: a throwable
 * with no message must neither NPE the walk nor match an errorMsg of "null". The helper's callers all
 * live in other modules, so the test lives beside the helper to keep it covered where it is defined.
 */
public class TestJavaTestUtils {

  @Test
  public void testNullMessageOnHeadStillMatchesDeeperCause() {
    // Pre-fix this NPE'd on the head. The explicit (String) null cast matters: new RuntimeException(cause)
    // would set the message to the cause's toString and hide the null-message case entirely.
    Throwable t = new RuntimeException((String) null, new IllegalStateException("boom"));
    assertTrue(JavaTestUtils.checkNestedExceptionContains(t, "boom"));
  }

  @Test
  public void testNullMessageMidChainDoesNotStopTheWalk() {
    Throwable deepest = new IllegalArgumentException("boom");
    Throwable t = new RuntimeException("head", new RuntimeException((String) null, deepest));
    assertTrue(JavaTestUtils.checkNestedExceptionContains(t, "boom"));
  }

  @Test
  public void testErrorMsgNullDoesNotMatchMessagelessThrowable() {
    assertFalse(JavaTestUtils.checkNestedExceptionContains(new RuntimeException((String) null), "null"));
  }

  @Test
  public void testMessageContainingTextMatchesOnHead() {
    assertTrue(JavaTestUtils.checkNestedExceptionContains(new RuntimeException("a boom happened"), "boom"));
  }

  @Test
  public void testNoMatchAnywhereInChainReturnsFalse() {
    Throwable t = new RuntimeException("head", new IllegalStateException("cause"));
    assertFalse(JavaTestUtils.checkNestedExceptionContains(t, "boom"));
  }
}
