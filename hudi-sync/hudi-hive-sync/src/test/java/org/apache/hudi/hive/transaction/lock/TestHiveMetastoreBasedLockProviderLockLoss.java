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

import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for how {@link HiveMetastoreBasedLockProvider} reacts to the metastore reporting its
 * lock as gone, with a mocked {@link IMetaStoreClient} and no live metastore or ZooKeeper.
 */
class TestHiveMetastoreBasedLockProviderLockLoss extends HiveMetastoreBasedLockProviderTestBase {

  private static final long LOCK_ID = 42L;
  private static final long OTHER_LOCK_ID = 43L;
  private static final long HEARTBEAT_INTERVAL_MS = 100L;
  private static final long AWAIT_TIMEOUT_MS = 30_000L;

  @Override
  protected long heartbeatIntervalMs() {
    // Keep the ticks short so the scheduled heartbeat fires within the test.
    return HEARTBEAT_INTERVAL_MS;
  }

  @Test
  void terminalHeartbeatFailureStopsRenewalAndDropsTheLock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      // The metastore has expired the lock, so the provider must stop claiming to hold it.
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // The heartbeat task latches the failure on its own, so the count below would stay at one
      // even if the schedule were left running. Assert the cancellation itself as well.
      assertTrue(heartbeatFutureOf(provider).isCancelled(), "the heartbeat schedule must be cancelled");

      // Give the scheduler several more intervals: no further heartbeat may be attempted, since
      // both the schedule and the heartbeat task itself are stopped after a terminal failure.
      Thread.sleep(HEARTBEAT_INTERVAL_MS * 5);
      verify(client, times(1)).heartbeat(0L, LOCK_ID);

      // The writer must learn that it no longer holds exclusivity, and the provider must not send
      // a doomed unlock for a lock the metastore has already dropped.
      assertThrows(HoodieLockException.class, provider::unlock);
      verify(client, never()).unlock(anyLong());
    } finally {
      provider.close();
    }
  }

  @Test
  void lockLostAfterTryLockIsReportedOnUnlock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      // Same loss, but driven through the entry point the LockManager actually calls rather than
      // the test-only acquireLock overload.
      assertTrue(provider.tryLock(1000L, TimeUnit.MILLISECONDS));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      assertThrows(HoodieLockException.class, provider::unlock);
      verify(client, never()).unlock(anyLong());
    } finally {
      provider.close();
    }
  }

  @Test
  void closeAfterALostLockSendsNoUnlockAndShutsDownTheExecutor() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");
    } finally {
      provider.close();
    }

    // There is nothing left to release at the metastore, but the heartbeat pool must still go away.
    verify(client, never()).unlock(anyLong());
    ScheduledExecutorService executor = readField(provider, "executor");
    assertTrue(executor.isShutdown(), "the heartbeat pool must be shut down after a lost lock");
  }

  @Test
  void transientHeartbeatFailureKeepsRenewingTheLock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    CountDownLatch heartbeats = new CountDownLatch(2);
    doAnswer(invocation -> {
      heartbeats.countDown();
      throw new TException("transient failure");
    }).when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      assertTrue(heartbeats.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          "a transient failure must not stop the heartbeat schedule");
      assertNotNull(provider.getLock(), "a transient failure must not drop the lock");

      provider.unlock();
      verify(client).unlock(LOCK_ID);
      assertNull(provider.getLock());
    } finally {
      provider.close();
    }
  }

  @Test
  void lockCanBeAcquiredAgainAfterItWasLost() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID), acquiredLock(OTHER_LOCK_ID));
    // Only the first lock is expired by the metastore; heartbeating the second one succeeds.
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .doNothing()
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // Acquiring again must clear the lost-lock state, otherwise the provider would keep failing
      // to release locks it holds perfectly well.
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      provider.unlock();

      verify(client).unlock(OTHER_LOCK_ID);
      assertNull(provider.getLock());
    } finally {
      provider.close();
    }
  }

  @Test
  void lostLockStateDoesNotLeakIntoTheNextAcquire() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID), waitingLock(OTHER_LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .doNothing()
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // The second attempt only got queued, so it leaves no lock behind and nothing was expired
      // by the metastore this time round.
      assertFalse(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      verify(client).unlock(OTHER_LOCK_ID);

      // Releasing must not report the loss that belonged to the previous lock.
      assertDoesNotThrow(provider::unlock);
    } finally {
      provider.close();
    }
  }

  @Test
  void lockThatWasOnlyQueuedIsNeverHeartbeated() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(waitingLock(LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " was never granted"))
        .when(client).heartbeat(anyLong(), anyLong());
    // Releasing the queued lock is the provider's very next step, and it is an RPC: parking inside
    // it holds open exactly the window in which a heartbeat scheduled for that lock would tick.
    doAnswer(invocation -> {
      Thread.sleep(HEARTBEAT_INTERVAL_MS * 5);
      return null;
    }).when(client).unlock(anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertFalse(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      // A lock that was never granted must never be renewed: a tick failing for it would be read
      // as the metastore taking away exclusivity that the writer never had in the first place.
      verify(client, never()).heartbeat(anyLong(), anyLong());
      assertNull(heartbeatFutureOf(provider), "a queued lock must not be given a heartbeat schedule");
      assertDoesNotThrow(provider::unlock);
    } finally {
      provider.close();
    }
  }

  @Test
  void unlockStaysSilentWhenNoLockWasEverHeld() {
    IMetaStoreClient client = mock(IMetaStoreClient.class);

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      // Releasing a lock that was never acquired is still a no-op: only a lock the metastore took
      // away is reported as a failure.
      assertDoesNotThrow(provider::unlock);
    } finally {
      provider.close();
    }
  }

  @Test
  void staleHeartbeatFromAReleasedLockDoesNotDropTheNextLock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID), acquiredLock(OTHER_LOCK_ID));
    CountDownLatch staleTickStarted = new CountDownLatch(1);
    CountDownLatch releaseStaleTick = new CountDownLatch(1);
    CountDownLatch newLockHeartbeats = new CountDownLatch(3);
    doAnswer(invocation -> {
      long heartbeatedLockId = invocation.getArgument(1);
      if (heartbeatedLockId == LOCK_ID) {
        // Park this tick inside the RPC until the lock it renews has been released and another one
        // taken, then fail it the way the metastore fails a heartbeat for a lock it no longer has.
        staleTickStarted.countDown();
        releaseStaleTick.await();
        throw new NoSuchLockException("lock " + LOCK_ID + " does not exist");
      }
      newLockHeartbeats.countDown();
      return null;
    }).when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      assertTrue(staleTickStarted.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          "the heartbeat for the first lock must be in flight before it is released");

      provider.unlock();
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      // Only now does the tick scheduled for the first lock come back, failing because that lock
      // was released here. Cancelling its schedule could never have stopped it.
      releaseStaleTick.countDown();

      assertTrue(newLockHeartbeats.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          "the stale tick must not stop the renewal of the lock held now, which would let the "
              + "metastore expire a perfectly healthy lock mid-commit");
      assertNotNull(provider.getLock(), "the stale tick must not drop the lock held now");
      assertFalse(heartbeatFutureOf(provider).isCancelled(),
          "the stale tick must not cancel the schedule of the lock held now");

      assertDoesNotThrow(provider::unlock);
      verify(client).unlock(OTHER_LOCK_ID);
    } finally {
      // Free the parked tick even when an assertion above failed: close() only shuts the executor
      // down, which neither interrupts the await nor lets the non-daemon thread exit, so leaving
      // it parked would turn a failing test into a hanging JVM.
      releaseStaleTick.countDown();
      provider.close();
    }
  }

  @Test
  void releasingALockNormallyIsNotReportedAsLost() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    CountDownLatch tickStarted = new CountDownLatch(1);
    CountDownLatch releaseTick = new CountDownLatch(1);
    doAnswer(invocation -> {
      tickStarted.countDown();
      releaseTick.await();
      throw new NoSuchLockException("lock " + LOCK_ID + " does not exist");
    }).when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      assertTrue(tickStarted.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          "the heartbeat must be in flight before the lock is released");

      provider.unlock();
      verify(client).unlock(LOCK_ID);
      releaseTick.countDown();

      // Let the released tick finish and run whatever it makes of the failure.
      Thread.sleep(HEARTBEAT_INTERVAL_MS * 5);

      // The heartbeat failed only because the lock was released here, so releasing again stays the
      // no-op it has always been instead of reporting a loss that never happened.
      assertDoesNotThrow(provider::unlock);
    } finally {
      // See the note in staleHeartbeatFromAReleasedLockDoesNotDropTheNextLock: a parked tick must
      // never outlive a failed assertion.
      releaseTick.countDown();
      provider.close();
    }
  }

  private static ScheduledFuture<?> heartbeatFutureOf(HiveMetastoreBasedLockProvider provider) throws Exception {
    return readField(provider, "future");
  }

  private static void awaitUntil(BooleanSupplier condition, String message) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20L);
    }
    fail(message);
  }
}
