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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.net.URI;
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
import static org.junit.jupiter.api.Assertions.fail;
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
        "s3a://bucket/lake/db/tbl-default",
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
  void testValidDefaultConstructor() {

    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3a://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.WRITE_SERVICE_CLASS_NAME.key(), StubConditionalWriteLockService.class.getName());
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "s3a://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    ConditionalWriteLockProvider provider = new ConditionalWriteLockProvider(lockConf, conf);
    assertNull(provider.getLock());
    provider.close();
  }

  @Test
  void testValidDefaultConstructorWithWeirdBasePath() {

    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3a://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.WRITE_SERVICE_CLASS_NAME.key(), StubConditionalWriteLockService.class.getName());
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "//中文/路径//测试/emoji-u83DuDE0E-text//\\\\invalid*chars%$#end\n");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    ConditionalWriteLockProvider provider = new ConditionalWriteLockProvider(lockConf, conf);
    try {
      Field field = provider.getClass().getDeclaredField("lockFilePath");
      field.setAccessible(true);
      String lockFilePath = (String) field.get(provider);
      new URI(lockFilePath);
    } catch (URISyntaxException | NoSuchFieldException | IllegalAccessException e) {
      fail("Should not throw exception!");
    }
  }

  @Test
  void testInvalidWriteService() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3a://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.WRITE_SERVICE_CLASS_NAME.key(), String.class.getName());
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "s3a://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> new ConditionalWriteLockProvider(lockConf, conf));
    Throwable cause = ex.getCause();
    assertNotNull(cause);
    assertTrue(cause instanceof HoodieException);
  }

  @Test
  void testNonExistentWriteService() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3a://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.WRITE_SERVICE_CLASS_NAME.key(), "com.nonexistent.ClassName");
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "s3a://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    HoodieLockException ex = assertThrows(HoodieLockException.class,
        () -> new ConditionalWriteLockProvider(lockConf, conf));
    assertTrue(ex.getMessage().contains("Failed to load and initialize ConditionalWriteLockService"));
  }

  @Test
  void testInvalidLocksLocation() {
    ConditionalWriteLockService lockService = mock(ConditionalWriteLockService.class);
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "not a uri");
    props.put(ConditionalWriteLockConfig.WRITE_SERVICE_CLASS_NAME.key(), lockService.getClass().getName());
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "s3a://bucket/lake/db/tbl-default");
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
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(realLockFile);
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);

    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
    assertEquals(realLockFile, lockProvider.getLock());
    verify(mockLockService, atLeastOnce()).tryCreateOrUpdateLockFile(any(), any());
  }

  @Test
  void testTryLockSuccessButFailureToStartHeartbeat() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(realLockFile);
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(false);

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsFromOwnerMismatch() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockFile returnedLockFile = new ConditionalWriteLockFile(new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "different-owner"), "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(returnedLockFile);

    HoodieLockException ex = assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
    assertTrue(ex.getMessage().contains("Owners do not match!"));
  }

  @Test
  void testTryLockFailsDueToExistingLock() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, "other-owner");
    ConditionalWriteLockFile existingLock = new ConditionalWriteLockFile(data, "v2");
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.of(existingLock));

    boolean acquired = lockProvider.tryLock();
    assertFalse(acquired);
  }

  @Test
  void testTryLockFailsToUpdateFile() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(null);
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockFailsDueToUnknownState() {
    when(mockLockService.getCurrentLockFile()).thenReturn(null);
    assertFalse(lockProvider.tryLock());
  }

  @Test
  void testTryLockSucceedsWhenExistingLockExpiredByTime() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, "other-owner");
    ConditionalWriteLockFile existingLock = new ConditionalWriteLockFile(data, "v2");
    ConditionalWriteLockData newData = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(newData, "v1");
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.of(existingLock));
    when(mockLockService.tryCreateOrUpdateLockFile(any(), eq(existingLock))).thenReturn(realLockFile);
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    boolean acquired = lockProvider.tryLock();
    assertTrue(acquired);
  }

  @Test
  void testTryLockReentrancySucceeds() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(realLockFile);
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
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData validData = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile validLock = new ConditionalWriteLockFile(validData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(validLock);
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
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData validData = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile validLock = new ConditionalWriteLockFile(validData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(validLock);
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
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.tryLock());
  }

  @Test
  void testUnlockSucceedsAndReentrancy() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(realLockFile);
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(true);
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(realLockFile), anyLong()))
        .thenReturn(Option.of(new ConditionalWriteLockFile(new ConditionalWriteLockData(true, data.getValidUntil(), ownerId), "v2")));
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    lockProvider.unlock();
    assertNull(lockProvider.getLock());
    lockProvider.unlock();
  }

  @Test
  void testUnlockFailsToStopHeartbeat() {
    when(mockLockService.getCurrentLockFile()).thenReturn(Option.empty());
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile realLockFile = new ConditionalWriteLockFile(data, "v1");
    when(mockLockService.tryCreateOrUpdateLockFile(any(), isNull())).thenReturn(realLockFile);
    when(mockHeartbeatManager.startHeartbeatForThread(any())).thenReturn(true);
    assertTrue(lockProvider.tryLock());
    when(mockHeartbeatManager.stopHeartbeat(true)).thenReturn(false);
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(true);
    assertThrows(HoodieLockException.class, () -> lockProvider.unlock());
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
  void testRenewLockWithoutActiveHeartbeat() {
    doReturn(null).when(lockProvider).getLock();
    assertFalse(lockProvider.renewLock());
    when(mockHeartbeatManager.hasActiveHeartbeat()).thenReturn(false);
    verify(mockLogger).warn("Owner {}: Another process has already shutdown the heartbeat after this task fired.", this.ownerId);
  }

  @Test
  void testRenewLockWithNearlyExpiredLock() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis(), ownerId);
    ConditionalWriteLockFile nearExpiredLockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(nearExpiredLockFile).when(lockProvider).getLock();
    assertFalse(lockProvider.renewLock());
    verify(mockLogger).error("Owner {}: Cannot renew unexpired lock which is within clock drift of expiration!", this.ownerId);
  }

  @Test
  void testRenewLockWithFullyExpiredLock() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() - DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile nearExpiredLockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(nearExpiredLockFile).when(lockProvider).getLock();
    assertFalse(lockProvider.renewLock());
    verify(mockLogger).error("Owner {}: Cannot renew unexpired lock which is within clock drift of expiration!", this.ownerId);
  }

  @Test
  void testRenewLockUnableToUpsertLockFileButNotFatal() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for more details.
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Option.empty());
    assertTrue(lockProvider.renewLock());
  }

  @Test
  void testRenewLockUnableToUpsertLockFileFatal() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();
    // Signal the upsert attempt failed, but may be transient. See interface for more details.
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(null);
    assertFalse(lockProvider.renewLock());

    verify(mockLogger).error("Owner {}: Unable to upload metadata to renew lock due to fatal error.", this.ownerId);
  }

  @Test
  void testRenewLockSucceedsButRenewalWithinExpirationWindow() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, System.currentTimeMillis() + DEFAULT_LOCK_VALIDITY_MS, ownerId);
    ConditionalWriteLockFile lockFile = new ConditionalWriteLockFile(data, "v1");
    doReturn(lockFile).when(lockProvider).getLock();

    ConditionalWriteLockData nearExpirationData = new ConditionalWriteLockData(false, System.currentTimeMillis(), ownerId);
    ConditionalWriteLockFile lockFileNearExpiration = new ConditionalWriteLockFile(nearExpirationData, "v2");
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Option.of(lockFileNearExpiration));

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
    when(mockLockService.tryCreateOrUpdateLockFileWithRetry(any(), eq(lockFile), anyLong())).thenReturn(Option.of(successLockFile));
    assertTrue(lockProvider.renewLock());

    verify(mockLogger).info(eq("Owner {}: renewal succeeded for lock: {} with {} ms left until expiration."), eq(this.ownerId), eq("s3a://bucket/lake/db/tbl-default"), anyLong());
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
    public ConditionalWriteLockFile tryCreateOrUpdateLockFile(ConditionalWriteLockData newLockData, ConditionalWriteLockFile previousLockFile) {
      return null;
    }

    @Override
    public Option<ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(
        Supplier<ConditionalWriteLockData> newLockDataSupplier,
        ConditionalWriteLockFile previousLockFile,
        long retryExpiration) {
      return null;
    }

    @Override
    public Option<ConditionalWriteLockFile> getCurrentLockFile() {
      return null;
    }

    @Override
    public void close() throws Exception {
      // stub, no-op
    }
  }

}


