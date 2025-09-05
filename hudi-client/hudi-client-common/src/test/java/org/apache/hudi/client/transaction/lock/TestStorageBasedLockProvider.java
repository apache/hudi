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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.client.transaction.lock.models.HeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.StorageBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test class for StorageBasedLockProvider
 */
class TestStorageBasedLockProvider {
  private StorageBasedLockProvider lockProvider;
  private StorageLockClient mockLockService;
  private HeartbeatManager mockHeartbeatManager;
  private Logger mockLogger;
  private final String ownerId = UUID.randomUUID().toString();
  private static final int DEFAULT_LOCK_VALIDITY_MS = 10000;

  @BeforeEach
  void setupLockProvider() {
    mockLockService = mock(StorageLockClient.class);
    mockHeartbeatManager = mock(HeartbeatManager.class);
    mockLogger = mock(Logger.class);
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(true);
    // Mock the readObject method to return Option.empty() to prevent NPE in audit service creation
    when(mockLockService.readObject(anyString(), anyBoolean())).thenReturn(Option.empty());
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-default");

    lockProvider = spy(new StorageBasedLockProvider(
        ownerId,
        props,
        (a,b,c) -> mockHeartbeatManager,
        (a,b,c) -> mockLockService,
        mockLogger,
        null));
  }

  @AfterEach
  void cleanupLockProvider() {
    lockProvider.close();
  }

  @Test
  void testValidLockStorageLocation() {
    TypedProperties props = new TypedProperties();
    props.put(BASE_PATH.key(), "s3://bucket/lake/db/tbl-default");

    LockConfiguration lockConf = new LockConfiguration(props);
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new StorageBasedLockProvider(lockConf, storageConf));
    assertTrue(ex.getMessage().contains("Failed to load and initialize StorageLock"));
  }

  @ParameterizedTest
  @ValueSource(strings = { "gs://bucket/lake/db/tbl-default", "s3://bucket/lake/db/tbl-default",
      "s3a://bucket/lake/db/tbl-default" })
  void testNonExistentWriteServiceWithDefaults(String tableBasePathString) {
    TypedProperties props = new TypedProperties();
    props.put(BASE_PATH.key(), tableBasePathString);

    LockConfiguration lockConf = new LockConfiguration(props);
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new StorageBasedLockProvider(lockConf, storageConf));
    assertTrue(ex.getMessage().contains("Failed to load and initialize StorageLock"));
  }

  @Test
  void testTryLockForTimeUnitThrowsOnInterrupt() throws Exception {
    doReturn(false).when(lockProvider).tryLock();
    CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try {
        lockProvider.tryLock(1, TimeUnit.SECONDS);
      } catch (HoodieLockException e) {
        latch.countDown();
      }
    });
    t.start();
    Thread.sleep(50);
    t.interrupt();
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  void testTryLockForTimeUnitAcquiresLockEventually() throws Exception {
    AtomicInteger count = new AtomicInteger(0);
    doAnswer(inv -> count.incrementAndGet() > 2).when(lockProvider).tryLock();
    CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      assertTrue(lockProvider.tryLock(4, TimeUnit.SECONDS));
      latch.countDown();
    });
    t.start();
    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  @Test
  void testTryLockForTimeUnitFailsToAcquireLockEventually() throws Exception {
    AtomicInteger count = new AtomicInteger(0);
    doAnswer(inv -> count.incrementAndGet() > 2).when(lockProvider).tryLock();
    CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      assertFalse(lockProvider.tryLock(1, TimeUnit.SECONDS));
      latch.countDown();
    });
    t.start();
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  void testTryLockSuccess() {
    long t0 = 1_000L;
    when(lockProvider.getCurrentEpochMs())
        .thenReturn(t0);
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, t0 + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(refEq(data), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    assertEquals(realLockFile, lockProvider.getLock());
    verify(mockLockService, atLeastOnce()).tryUpsertLockFile(any(), any());
  }

  @Test
  void testTryLockSuccessButFailureToStartHeartbeat() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(false);
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(realLockFile))))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsFromOwnerMismatch() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockFile returnedLockFile = new StorageLockFile(
        new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "different-owner"), "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(returnedLockFile)));

    HoodieLockException ex = assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
    assertTrue(ex.getMessage().contains("Owners do not match"));
  }

  @Test
  void testTryLockFailsDueToExistingLock() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS,
        "other-owner");
    StorageLockFile existingLock = new StorageLockFile(data, "v2");
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.SUCCESS, Option.of(existingLock)));

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsToUpdateFile() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.ACQUIRED_BY_OTHERS, Option.empty()));
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockFailsDueToUnknownState() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.UNKNOWN_ERROR, Option.empty()));
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockSucceedsWhenExistingLockExpiredByTime() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS,
        "other-owner");
    StorageLockFile existingLock = new StorageLockFile(data, "v2");
    StorageLockData newData = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS,
        ownerId);
    StorageLockFile realLockFile = new StorageLockFile(newData, "v1");
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.SUCCESS, Option.of(existingLock)));
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(existingLock))))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
  }

  @Test
  void testTryLockReentrancySucceeds() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    // Re-entrancy succeeds
    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockReentrancyAfterLockExpiredByTime() {
    // In an extremely unlikely scenario, we could have a local reference to a lock
    // which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not
    // successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce
    // some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the lock is still "unexpired" but the time shows expired.
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile expiredLock = new StorageLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData validData = new StorageLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS,
        ownerId);
    StorageLockFile validLock = new StorageLockFile(validData, "v2");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(validLock)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockReentrancyAfterLockSetExpired() {
    // In an extremely unlikely scenario, we could have a local reference to a lock
    // which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not
    // successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce
    // some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the lock is "expired" but the time shows unexpired.
    StorageLockData data = new StorageLockData(true, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile expiredLock = new StorageLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData validData = new StorageLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS,
        ownerId);
    StorageLockFile validLock = new StorageLockFile(validData, "v2");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(validLock)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockHeartbeatStillActive() {
    // In an extremely unlikely scenario, we could have a local reference to a lock
    // which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not
    // successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce
    // some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the heartbeat is still active, so we have to error out.
    StorageLockData data = new StorageLockData(true, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile expiredLock = new StorageLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
  }

  @Test
  void testUnlockSucceedsAndReentrancy() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(true);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(realLockFile))))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS,
            Option.of(new StorageLockFile(new StorageLockData(true, data.getValidUntil(), ownerId), "v2"))));
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.hasActiveHeartbeat())
        .thenReturn(true) // when we try to stop the heartbeat we will check if heartbeat is active return true.
        .thenReturn(false); // when try to set lock to expire we will assert no active heartbeat as a precondition.
    lockProvider.unlock();
    assertNull(lockProvider.getLock());
    lockProvider.unlock();
  }

  @Test
  void testUnlockFailsToStopHeartbeat() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(false);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.unlock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
  }

  @Test
  void testCloseFailsToStopHeartbeat() {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(false);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    // Should wrap the exception and log error.
    lockProvider.close();
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
  }

  @Test
  void testRenewLockReturnsFalseWhenNoLockHeld() {
    doReturn(null).when(lockProvider).getLock();
    assertFalse(lockProvider.renewLock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    verify(mockLogger).warn("Owner {}: Cannot renew, no lock held by this process", this.ownerId);
  }

  @Test
  void testRenewLockWithoutHoldingLock() {
    doReturn(null).when(lockProvider).getLock();
    assertFalse(lockProvider.renewLock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
    verify(mockLogger).warn("Owner {}: Cannot renew, no lock held by this process", this.ownerId);
  }

  @Test
  void testRenewLockWithFullyExpiredLock() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile nearExpiredLockFile = new StorageLockFile(data, "v1");
    doReturn(nearExpiredLockFile).when(lockProvider).getLock();
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(nearExpiredLockFile))))
        .thenReturn(Pair.of(LockUpsertResult.ACQUIRED_BY_OTHERS, null));
    assertFalse(lockProvider.renewLock());
    verify(mockLogger).error("Owner {}: Unable to renew lock as it is acquired by others.", this.ownerId);
  }

  @Test
  void testRenewLockUnableToUpsertLockFileButNotFatal() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for
    // more details.
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(lockFile))))
        .thenReturn(Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty()));
    assertTrue(lockProvider.renewLock());
  }

  @Test
  void testRenewLockUnableToUpsertLockFileFatal() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for
    // more details.
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(lockFile))))
        .thenReturn(Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty()));
    // renewLock return true so it will be retried.
    assertTrue(lockProvider.renewLock());

    verify(mockLogger).warn("Owner {}: Unable to renew lock due to unknown error, could be transient.", this.ownerId);
  }

  @Test
  void testRenewLockSucceedsButRenewalWithinExpirationWindow() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    StorageLockData nearExpirationData = new StorageLockData(false, System.currentTimeMillis(), ownerId);
    StorageLockFile lockFileNearExpiration = new StorageLockFile(nearExpirationData, "v2");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(lockFile))))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(lockFileNearExpiration)));

    // We used to fail in this case before, but since we are only modifying a single
    // lock file, this is ok now.
    // Therefore, this can be a happy path variation.
    assertTrue(lockProvider.renewLock());
  }

  @Test
  void testRenewLockSucceeds() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    StorageLockData successData = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS,
        ownerId);
    StorageLockFile successLockFile = new StorageLockFile(successData, "v2");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(lockFile))))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(successLockFile)));
    assertTrue(lockProvider.renewLock());

    verify(mockLogger).info(
        eq("Owner {}: Lock renewal successful. The renewal completes {} ms before expiration for lock {}."),
        eq(this.ownerId), anyLong(), eq("gs://bucket/lake/db/tbl-default/.hoodie/.locks/table_lock.json"));
  }

  @Test
  void testRenewLockFails() {
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    when(mockLockService.tryUpsertLockFile(any(), eq(Option.of(lockFile))))
        .thenThrow(new RuntimeException("Failure"));
    assertFalse(lockProvider.renewLock());

    verify(mockLogger).error(eq("Owner {}: Exception occurred while renewing lock"), eq(ownerId),
        any(RuntimeException.class));
  }

  @Test
  void testCloseCallsDependencies() throws Exception {
    lockProvider.close();
    verify(mockLockService, atLeastOnce()).close();
    verify(mockHeartbeatManager, atLeastOnce()).close();
    assertNull(lockProvider.getLock());
  }

  @Test
  void testCloseWithErrorForLockService() throws Exception {
    doThrow(new RuntimeException("Some failure")).when(mockLockService).close();
    lockProvider.close();
    verify(mockLogger).error(eq("Owner {}: Lock service failed to close."), eq(ownerId), any(RuntimeException.class));
    assertNull(lockProvider.getLock());
  }

  @Test
  void testCloseWithErrorForHeartbeatManager() throws Exception {
    doThrow(new RuntimeException("Some failure")).when(mockHeartbeatManager).close();
    lockProvider.close();
    verify(mockLogger).error(eq("Owner {}: Heartbeat manager failed to close."), eq(ownerId),
        any(RuntimeException.class));
    assertNull(lockProvider.getLock());
  }

  @Test
  public void testShutdownHookViaReflection() throws Exception {
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile realLockFile = new StorageLockFile(data, "v1");
    when(mockLockService.tryUpsertLockFile(any(), eq(Option.empty())))
            .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    assertEquals(realLockFile, lockProvider.getLock());
    verify(mockLockService, atLeastOnce()).tryUpsertLockFile(any(), any());

    when(mockLockService.tryUpsertLockFile(any(StorageLockData.class), eq(Option.of(realLockFile))))
            .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(realLockFile)));

    // Mock shutdown
    Method shutdownMethod = lockProvider.getClass().getDeclaredMethod("shutdown", boolean.class);
    shutdownMethod.setAccessible(true);
    shutdownMethod.invoke(lockProvider, true);

    // Verify that the expected shutdown behaviors occurred.
    assertNull(lockProvider.getLock());
    // We do not execute additional actions
    verify(mockLockService, never()).close();
    verify(mockHeartbeatManager, never()).close();
  }

  @Test
  public void testShutdownHookWhenNoLockPresent() throws Exception {
    // Now, when calling shutdown(true), the method should immediately return.
    Method shutdownMethod = lockProvider.getClass().getDeclaredMethod("shutdown", boolean.class);
    shutdownMethod.setAccessible(true);
    shutdownMethod.invoke(lockProvider, true);

    // Verify that unlock or close methods are NOT invoked, or adjust expectations accordingly.
    verify(mockLockService, never()).close();
    verify(mockHeartbeatManager, never()).close();
  }

  @Test
  public void testShutdownHookFailsToBeRemoved() throws Exception {
    doThrow(new IllegalStateException("Shutdown already in progress")).when(lockProvider).tryRemoveShutdownHook();
    lockProvider.close();
    verify(mockLockService, atLeastOnce()).close();
    verify(mockHeartbeatManager, atLeastOnce()).close();
    assertNull(lockProvider.getLock());
  }

  @Test
  void testShutdownHookFiresDuringTryLockWithTimeout() throws Exception {
    // This test simulates the scenario where the shutdown hook fires while tryLock(long time, TimeUnit unit) 
    // is in progress, and expects that HoodieLockException is thrown when tryLock is called after shutdown.

    // Setup mocks to simulate lock being held by another owner (to keep tryLock looping)
    StorageLockData otherOwnerData = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "other-owner");
    StorageLockFile otherOwnerLock = new StorageLockFile(otherOwnerData, "v1");
    when(mockLockService.readCurrentLockFile()).thenReturn(Pair.of(LockGetResult.SUCCESS, Option.of(otherOwnerLock)));

    CountDownLatch tryLockStarted = new CountDownLatch(1);
    CountDownLatch proceedWithShutdown = new CountDownLatch(1);
    CountDownLatch shutdownCompleted = new CountDownLatch(1);
    CountDownLatch tryLockCompleted = new CountDownLatch(1);
    CountDownLatch exceptionThrown = new CountDownLatch(1);

    // Spy on the real tryLock to know when it's been called and coordinate with shutdown
    AtomicInteger tryLockCallCount = new AtomicInteger(0);
    doAnswer(inv -> {
      int count = tryLockCallCount.incrementAndGet();
      if (count == 1) {
        // First call - signal that tryLock has started
        tryLockStarted.countDown();
        // Wait for shutdown to be triggered
        assertTrue(proceedWithShutdown.await(2, TimeUnit.SECONDS));
      } else {
        // Subsequent calls - wait briefly for shutdown to complete
        assertTrue(shutdownCompleted.await(100, TimeUnit.MILLISECONDS));
      }
      // Call the real method
      return inv.callRealMethod();
    }).when(lockProvider).tryLock();

    // Start a thread that will call tryLock with timeout
    Thread tryLockThread = new Thread(() -> {
      try {
        lockProvider.tryLock(2, TimeUnit.SECONDS);
        // Should not reach here - exception should be thrown after shutdown
        fail("Should have thrown HoodieLockException after shutdown");
      } catch (HoodieLockException e) {
        // Expected - tryLock should throw exception after shutdown
        exceptionThrown.countDown();
      } finally {
        tryLockCompleted.countDown();
      }
    });

    tryLockThread.start();

    // Wait for tryLock to start
    assertTrue(tryLockStarted.await(2, TimeUnit.SECONDS), "tryLock should have started");

    // Now invoke the shutdown hook while tryLock is in progress
    Method shutdownMethod = lockProvider.getClass().getDeclaredMethod("shutdown", boolean.class);
    shutdownMethod.setAccessible(true);

    // Invoke shutdown in a separate thread to simulate shutdown hook
    Thread shutdownThread = new Thread(() -> {
      try {
        proceedWithShutdown.countDown();  // Signal tryLock to proceed
        shutdownMethod.invoke(lockProvider, true);
        shutdownCompleted.countDown();  // Signal that shutdown is complete
      } catch (Exception ignored) {
        // do nothing
      }
    });
    shutdownThread.start();

    // Wait for both operations to complete
    assertTrue(tryLockCompleted.await(5, TimeUnit.SECONDS), "tryLock should complete");
    assertTrue(exceptionThrown.await(1, TimeUnit.SECONDS), "HoodieLockException should have been thrown");
    shutdownThread.join(2000);

    // Verify the state after shutdown
    // The lock should be null after shutdown
    assertNull(lockProvider.getLock(), "Lock should be null after shutdown hook fires");

    // Verify that tryLock was called at least once
    verify(lockProvider, atLeastOnce()).tryLock();
  }

  @Test
  public void testStorageBasedLockProviderWithMetricsConstructor() {
    // Create test configuration
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-default");
    LockConfiguration lockConfiguration = new LockConfiguration(props);
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    
    // Create a mock HoodieLockMetrics object  
    HoodieLockMetrics mockMetrics = mock(HoodieLockMetrics.class);
    
    // Test that constructor with metrics works by using the internal constructor to avoid scheme issues
    StorageBasedLockProvider lockProviderWithMetrics = null;
    try {
      // First test that the public constructor with metrics compiles and can be called
      // We expect this to fail due to lock client instantiation, but it validates the constructor exists
      assertThrows(Exception.class, () -> {
        new StorageBasedLockProvider(lockConfiguration, storageConf, mockMetrics);
      }, "Constructor should exist but fail during lock client instantiation");
      
      // Now create a working instance using the internal constructor for proper validation
      lockProviderWithMetrics = new StorageBasedLockProvider(
          UUID.randomUUID().toString(),
          props,
          (a,b,c) -> mock(HeartbeatManager.class),
          (a,b,c) -> new StubStorageLockClient(a, b, new Properties()),
          mock(Logger.class),
          mockMetrics);
      
      // Verify the lock provider was created successfully
      assertNotNull(lockProviderWithMetrics, "StorageBasedLockProvider should be created successfully");
      
      // Verify that it can perform basic operations
      assertNull(lockProviderWithMetrics.getLock(), "Initially should have no lock");
      
    } catch (Exception e) {
      fail("StorageBasedLockProvider creation should not throw unexpected exception: " + e.getMessage());
    } finally {
      if (lockProviderWithMetrics != null) {
        lockProviderWithMetrics.close();
      }
    }
  }

  @Test 
  public void testStorageBasedLockProviderStandardConstructor() {
    // Create test configuration
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-default");
    LockConfiguration lockConfiguration = new LockConfiguration(props);
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    
    // Test that standard constructor works by using the internal constructor to avoid scheme issues
    StorageBasedLockProvider lockProviderStandard = null;
    try {
      // First test that the public standard constructor compiles and can be called  
      // We expect this to fail due to lock client instantiation, but it validates the constructor exists
      assertThrows(Exception.class, () -> {
        new StorageBasedLockProvider(lockConfiguration, storageConf);
      }, "Standard constructor should exist but fail during lock client instantiation");
      
      // Now create a working instance using the internal constructor for proper validation
      lockProviderStandard = new StorageBasedLockProvider(
          UUID.randomUUID().toString(),
          props,
          (a,b,c) -> mock(HeartbeatManager.class),
          (a,b,c) -> new StubStorageLockClient(a, b, new Properties()),
          mock(Logger.class),
          null);  // No metrics for standard constructor test
      
      // Verify the lock provider was created successfully
      assertNotNull(lockProviderStandard, "StorageBasedLockProvider should be created successfully");
      
      // Verify that it can perform basic operations  
      assertNull(lockProviderStandard.getLock(), "Initially should have no lock");
      
    } catch (Exception e) {
      fail("StorageBasedLockProvider creation should not throw unexpected exception: " + e.getMessage());
    } finally {
      if (lockProviderStandard != null) {
        lockProviderStandard.close();
      }
    }
  }

  @Test
  void testAuditServiceIntegrationWhenConfigNotPresent() {
    // Test that lock provider works correctly when audit config is not present
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-audit-test");
    
    // Mock client that returns empty for audit config
    StorageLockClient auditMockClient = mock(StorageLockClient.class);
    when(auditMockClient.readObject(anyString(), eq(true)))
        .thenReturn(Option.empty());
    when(auditMockClient.readCurrentLockFile())
        .thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    
    StorageBasedLockProvider auditLockProvider = new StorageBasedLockProvider(
        ownerId,
        props,
        (a,b,c) -> mockHeartbeatManager,
        (a,b,c) -> auditMockClient,
        mockLogger,
        null);
    
    // Verify audit config was checked
    verify(auditMockClient, times(1)).readObject(
        contains(".locks/audit_enabled.json"), eq(true));
    
    // Lock provider should work normally even without audit
    StorageLockData data = new StorageLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    StorageLockFile lockFile = new StorageLockFile(data, "v1");
    when(auditMockClient.tryUpsertLockFile(any(), eq(Option.empty())))
        .thenReturn(Pair.of(LockUpsertResult.SUCCESS, Option.of(lockFile)));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    
    assertTrue(auditLockProvider.tryLock());
    auditLockProvider.close();
  }
  
  @Test
  void testAuditServiceIntegrationWhenConfigDisabled() {
    // Test that lock provider works correctly when audit is explicitly disabled
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-audit-disabled");
    
    // Mock client that returns disabled config
    StorageLockClient auditMockClient = mock(StorageLockClient.class);
    String disabledConfig = "{\"STORAGE_LOCK_AUDIT_SERVICE_ENABLED\": false}";
    when(auditMockClient.readObject(anyString(), eq(true)))
        .thenReturn(Option.of(disabledConfig));
    when(auditMockClient.readCurrentLockFile())
        .thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    
    StorageBasedLockProvider auditLockProvider = new StorageBasedLockProvider(
        ownerId,
        props,
        (a,b,c) -> mockHeartbeatManager,
        (a,b,c) -> auditMockClient,
        mockLogger,
        null);
    
    // Verify audit config was checked
    verify(auditMockClient, times(1)).readObject(
        contains(".locks/audit_enabled.json"), eq(true));
    
    auditLockProvider.close();
  }
  
  @Test
  void testAuditServiceIntegrationWhenConfigEnabled() {
    // Test that lock provider works correctly when audit is enabled
    TypedProperties props = new TypedProperties();
    props.put(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.put(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    props.put(BASE_PATH.key(), "gs://bucket/lake/db/tbl-audit-enabled");
    
    // Mock client that returns enabled config
    StorageLockClient auditMockClient = mock(StorageLockClient.class);
    String enabledConfig = "{\"STORAGE_LOCK_AUDIT_SERVICE_ENABLED\": true}";
    when(auditMockClient.readObject(anyString(), eq(true)))
        .thenReturn(Option.of(enabledConfig));
    when(auditMockClient.readCurrentLockFile())
        .thenReturn(Pair.of(LockGetResult.NOT_EXISTS, Option.empty()));
    
    StorageBasedLockProvider auditLockProvider = new StorageBasedLockProvider(
        ownerId,
        props,
        (a,b,c) -> mockHeartbeatManager,
        (a,b,c) -> auditMockClient,
        mockLogger,
        null);
    
    // Verify audit config was checked
    verify(auditMockClient, times(1)).readObject(
        contains(".locks/audit_enabled.json"), eq(true));
    
    // Note: Actual audit service implementation would be instantiated here
    // Currently returns empty since no concrete implementation yet
    
    auditLockProvider.close();
  }
  
  public static class StubStorageLockClient implements StorageLockClient {
    public StubStorageLockClient(String ownerId, String lockFileUri, Properties props) {
      assertTrue(lockFileUri.endsWith("table_lock.json"));
    }

    @Override
    public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
        StorageLockData newLockData,
        Option<StorageLockFile> previousLockFile) {
      return null;
    }

    @Override
    public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
      return null;
    }

    @Override
    public Option<String> readObject(String filePath, boolean checkExistsFirst) {
      // Stub implementation for testing
      return Option.empty();
    }

    @Override
    public void close() throws Exception {
      // stub, no-op
    }
  }

}
