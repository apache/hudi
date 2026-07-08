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

package org.apache.hudi.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the thin client-side exception types that wrap {@link HoodieException}.
 */
public class TestClientExceptions {

  @Test
  void keyGeneratorExceptionPreservesMessageAndCause() {
    Throwable cause = new IllegalStateException("boom");
    HoodieKeyGeneratorException withCause = new HoodieKeyGeneratorException("bad key", cause);
    assertEquals("bad key", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieKeyGeneratorException messageOnly = new HoodieKeyGeneratorException("no cause");
    assertEquals("no cause", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void compactionExceptionPreservesMessageAndCause() {
    Throwable cause = new RuntimeException("io");
    HoodieCompactionException withCause = new HoodieCompactionException("compaction failed", cause);
    assertEquals("compaction failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieCompactionException messageOnly = new HoodieCompactionException("compaction failed");
    assertEquals("compaction failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void rollbackExceptionPreservesMessageAndCause() {
    Throwable cause = new RuntimeException("io");
    HoodieRollbackException withCause = new HoodieRollbackException("rollback failed", cause);
    assertEquals("rollback failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieRollbackException messageOnly = new HoodieRollbackException("rollback failed");
    assertEquals("rollback failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }
}
