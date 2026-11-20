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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** StreamSync's rollback before it throws a pre-commit refusal: the refusal stays the thrown exception. */
class TestStreamSyncRollbackOnRefusal {

  @Test
  void refusalIsReturnedAfterTheRollback() {
    AtomicInteger rollbacks = new AtomicInteger();
    HoodiePreCommitRefusalException refusal = new HoodiePreCommitRefusalException("refused");
    assertSame(refusal, StreamSync.rolledBack(rollbacks::incrementAndGet, refusal));
    assertEquals(1, rollbacks.get());
    assertEquals(0, refusal.getSuppressed().length);
  }

  @Test
  void failedRollbackIsSuppressedUnderTheRefusal() {
    HoodieRollbackException rollbackFailure = new HoodieRollbackException("rollback failed");
    HoodiePreCommitRefusalException refusal = new HoodiePreCommitRefusalException("refused");
    HoodiePreCommitRefusalException thrown = StreamSync.rolledBack(() -> {
      throw rollbackFailure;
    }, refusal);
    assertSame(refusal, thrown, "the refusal, not the rollback failure, is what the round fails with");
    assertArrayEquals(new Throwable[] {rollbackFailure}, thrown.getSuppressed());
  }
}
