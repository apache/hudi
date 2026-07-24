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

package org.apache.hudi.hive.transaction.lock;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestHeartbeat {

  private final List<Exception> lockLostCauses = new ArrayList<>();

  /**
   * Failures that mean the metastore has already expired or aborted the lock. They are declared by
   * {@code IMetaStoreClient.heartbeat(long, long)} and cannot be recovered from by retrying.
   */
  private static Stream<Exception> terminalFailures() {
    return Stream.of(
        new NoSuchLockException("lock does not exist"),
        new NoSuchTxnException("txn does not exist"),
        new TxnAbortedException("txn was aborted"));
  }

  @Test
  void runDoesNotRethrowWhenHeartbeatFails() throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    doThrow(new TException("transient failure")).when(client).heartbeat(anyLong(), anyLong());

    Heartbeat heartbeat = new Heartbeat(client, 7L, lockLostCauses::add);

    // Rethrowing here would cancel every subsequent execution of a scheduleAtFixedRate task,
    // silently stopping lock renewal. The fix must swallow the failure so the next tick retries.
    assertDoesNotThrow(heartbeat::run);
    assertTrue(lockLostCauses.isEmpty(), "a transient failure must not be reported as a lost lock");
  }

  @Test
  void runKeepsRetryingAfterTransientFailure() throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    doThrow(new TException("transient failure")).when(client).heartbeat(anyLong(), anyLong());

    Heartbeat heartbeat = new Heartbeat(client, 7L, lockLostCauses::add);
    heartbeat.run();
    heartbeat.run();

    verify(client, times(2)).heartbeat(0L, 7L);
    assertTrue(lockLostCauses.isEmpty());
  }

  @Test
  void runHeartbeatsTheLockOnSuccess() throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);

    new Heartbeat(client, 99L, lockLostCauses::add).run();

    verify(client, times(1)).heartbeat(0L, 99L);
    assertTrue(lockLostCauses.isEmpty());
  }

  @ParameterizedTest
  @MethodSource("terminalFailures")
  void runReportsLockLossOnTerminalFailure(Exception terminalFailure) throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    doThrow(terminalFailure).when(client).heartbeat(anyLong(), anyLong());

    Heartbeat heartbeat = new Heartbeat(client, 11L, lockLostCauses::add);

    assertDoesNotThrow(heartbeat::run);
    assertEquals(1, lockLostCauses.size());
    assertSame(terminalFailure, lockLostCauses.get(0));
  }

  @ParameterizedTest
  @MethodSource("terminalFailures")
  void runStopsHeartbeatingAfterTerminalFailure(Exception terminalFailure) throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    doThrow(terminalFailure).when(client).heartbeat(anyLong(), anyLong());

    Heartbeat heartbeat = new Heartbeat(client, 11L, lockLostCauses::add);
    heartbeat.run();
    // A tick already queued when the schedule was cancelled must not renew a lock that is gone,
    // nor report the loss a second time.
    heartbeat.run();

    verify(client, times(1)).heartbeat(0L, 11L);
    assertEquals(1, lockLostCauses.size());
  }
}
