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

import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.client.transaction.lock.models.HeartbeatManager;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test class for ConditionalWriteLockProvider
 */
class TestConditionalWriteLockProvider {
  private ConditionalWriteLockProvider lockProvider;
  private ConditionalWriteLockService mockLockService;
  private HeartbeatManager mockHeartbeatManager;
  private Logger mockLogger;
  private final String ownerId = UUID.randomUUID().toString();
  private static final int DEFAULT_LOCK_VALIDITY_MS = 5000;

  @BeforeEach
  void setupLockProvider() {
    mockLockService = mock(ConditionalWriteLockService.class);
    mockHeartbeatManager = mock(HeartbeatManager.class);
    mockLogger = mock(Logger.class);
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(true);
    lockProvider = spy(new ConditionalWriteLockProvider(
        1000,
        DEFAULT_LOCK_VALIDITY_MS,
        "my-bucket",
        "gs://bucket/lake/db/tbl-default",
        ownerId,
        mockHeartbeatManager,
        mockLockService,
        mockLogger
    ));
  }

  @AfterEach
  void cleanupLockProvider() {
    lockProvider.close();
  }

  @Test
  void testUnsupportedLockStorageLocation() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "hdfs://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "gs://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new ConditionalWriteLockProvider(lockConf, conf));
    assertTrue(ex.getCause().getMessage().contains("No implementation of ConditionalWriteLockService supports this scheme"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://test-bucket/locks", "s3://test-bucket/locks", "s3a://test-bucket/locks"})
  void testNonExistentWriteService(String lockStorageLocation) {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), lockStorageLocation);
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "gs://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new ConditionalWriteLockProvider(lockConf, conf));
    assertTrue(ex.getMessage().contains("Failed to load and initialize ConditionalWriteLockService"));
  }

  @Test
  void testInvalidLocksLocationForWriteService() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "not a uri");
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "gs://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new ConditionalWriteLockProvider(lockConf, conf));
    Throwable cause = ex.getCause();
    assertNotNull(cause);
    assertTrue(cause instanceof URISyntaxException);
    assertTrue(ex.getMessage().contains("Unable to parse locks location as a URI"));
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
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    assertEquals(realLockFile, lockProvider.getLock());
    verify(mockLockService, atLeastOnce()).tryCreateOrUpdateLockFile(any(), any());
  }

  @Test
  void testTryLockSuccessButFailureToStartHeartbeat() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(false);
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(realLockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsFromOwnerMismatch() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockFile returnedLockFile = new ConditionalWriteLockFile(new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "different-owner"), "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, returnedLockFile));

    HoodieLockException ex = assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
    assertTrue(ex.getMessage().contains("Owners do not match!"));
  }

  @Test
  void testTryLockFailsDueToExistingLock() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "other-owner");
    ConditionalWriteLockFile existingLock = new ConditionalWriteLockFile(data, "v2");
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.SUCCESS, existingLock));

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsToUpdateFile() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null));
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockFailsDueToUnknownState() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.UNKNOWN_ERROR, null));
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockSucceedsWhenExistingLockExpiredByTime() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, "other-owner");
    ConditionalWriteLockFile existingLock = new ConditionalWriteLockFile(data, "v2");
    ConditionalWriteLockData newData = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(newData, "v1");
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.SUCCESS, existingLock));
    when(mockLockService.tryCreateOrUpdateLockFile(any(), eq(existingLock))).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
  }

  @Test
  void testTryLockReentrancySucceeds() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    // Re-entrancy succeeds
    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockReentrancyAfterLockExpiredByTime() {
    // In an extremely unlikely scenario, we could have a local reference to a lock which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the lock is still "unexpired" but the time shows expired.
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile expiredLock = new ConditionalWriteLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData validData = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile validLock = new ConditionalWriteLockFile(validData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, validLock));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockReentrancyAfterLockSetExpired() {
    // In an extremely unlikely scenario, we could have a local reference to a lock which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the lock is "expired" but the time shows unexpired.
    ConditionalWriteLockData data = new ConditionalWriteLockData(true, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile expiredLock = new ConditionalWriteLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData validData = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile validLock = new ConditionalWriteLockFile(validData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, validLock));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    assertTrue(lockProvider.tryLock());
  }

  @Test
  void testTryLockHeartbeatStillActive() {
    // In an extremely unlikely scenario, we could have a local reference to a lock which is present but expired,
    // and because we were unable to stop the heartbeat properly, we did not successfully set it to null.
    // Due to the nature of the heartbeat manager, this is expected to introduce some delay, but not be permanently blocking.
    // There are a few variations of this edge case, so we must test them all.

    // Here the heartbeat is still active, so we have to error out.
    ConditionalWriteLockData data = new ConditionalWriteLockData(true, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile expiredLock = new ConditionalWriteLockFile(data, "v1");
    doReturn(expiredLock).when(lockProvider).getLock();
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
  }

  @Test
  void testUnlockSucceedsAndReentrancy() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(true);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(realLockFile), anyLong()))
        .thenReturn(Pair.of(LockUpdateResult.SUCCESS, new ConditionalWriteLockFile(new ConditionalWriteLockData(true, data.getValidUntil(), ownerId), "v2")));
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.hasActiveHeartbeat())
        .thenReturn(true) // when we try to stop the heartbeat we will check if heartbeat is active, return true.
        .thenReturn(false); // when try to set lock to expire we will assert no active heartbeat as a precondition.
    lockProvider.unlock();
    assertNull(lockProvider.getLock());
    lockProvider.unlock();
  }

  @Test
  void testUnlockFailsToStopHeartbeat() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(false);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.unlock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
  }

  @Test
  void testCloseFailsToStopHeartbeat() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Pair.of(LockGetResult.NOT_EXISTS, null));
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, realLockFile));
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
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile nearExpiredLockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(nearExpiredLockFile).when(lockProvider).getLock();
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(nearExpiredLockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null));
    assertFalse(lockProvider.renewLock());
    verify(mockLogger).error("Owner {}: Unable to renew lock as it is acquired by others.", this.ownerId);
  }

  @Test
  void testRenewLockUnableToUpsertLockFileButNotFatal() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for more details.
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.UNKNOWN_ERROR, null));
    assertTrue(lockProvider.renewLock());
  }

  @Test
  void testRenewLockUnableToUpsertLockFileFatal() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for more details.
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.UNKNOWN_ERROR, null));
    // renewLock return true so it will be retried.
    assertTrue(lockProvider.renewLock());

    verify(mockLogger).warn("Owner {}: Unable to renew lock due to unknown error, could be transient.", this.ownerId);
  }

  @Test
  void testRenewLockSucceedsButRenewalWithinExpirationWindow() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    ConditionalWriteLockData nearExpirationData = new ConditionalWriteLockData(false, System.currentTimeMillis(), ownerId);
    ConditionalWriteLockFile lockFileNearExpiration = new ConditionalWriteLockFile(nearExpirationData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, lockFileNearExpiration));

    // We used to fail in this case before, but since we are only modifying a single lock file, this is ok now.
    // Therefore, this can be a happy path variation.
    assertTrue(lockProvider.renewLock());
  }

  @Test
  void testRenewLockSucceeds() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    ConditionalWriteLockData successData = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile successLockFile = new ConditionalWriteLockFile(successData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Pair.of(LockUpdateResult.SUCCESS, successLockFile));
    assertTrue(lockProvider.renewLock());

    verify(mockLogger).info(eq("Owner {}: Lock renewal successful. The renewal completes {} ms before expiration for lock {}."), eq(this.ownerId), anyLong(), eq("gs://bucket/lake/db/tbl-default"));
  }

  @Test
  void testRenewLockFails() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenThrow(new RuntimeException("Failure"));
    assertFalse(lockProvider.renewLock());

    verify(mockLogger).error(eq("Owner {}: Exception occurred while renewing lock"), eq(ownerId), any(RuntimeException.class));
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
    verify(mockLogger).error(eq("Owner {}: Heartbeat manager failed to close."), eq(ownerId), any(RuntimeException.class));
    assertNull(lockProvider.getLock());
  }

  public static class StubConditionalWriteLockService implements ConditionalWriteLockService {
    public StubConditionalWriteLockService(String arg1, String arg2, String arg3) {
      // No-op constructor for reflection
    }

    @Override
    public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFile(ConditionalWriteLockData newLockData, ConditionalWriteLockFile previousLockFile) {
      return null;
    }

    @Override
    public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(
        Supplier<ConditionalWriteLockData> newLockDataSupplier,
        ConditionalWriteLockFile previousLockFile,
        long retryExpiration) {
      return null;
    }

    @Override
    public Pair<LockGetResult, ConditionalWriteLockFile> getCurrentLockFile() {
      return null;
    }

    @Override
    public void close() throws Exception {
      // stub, no-op
    }
  }

}


