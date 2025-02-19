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

package org.apache.hudi.gcp.transaction.lock;

import org.apache.hudi.client.transaction.lock.LockGetResult;
import org.apache.hudi.client.transaction.lock.LockUpdateResult;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test class for GCSConditionalWriteLockService.
 */
@ExtendWith(MockitoExtension.class)
public class TestGCSConditionalWriteLockService {

  private static final String OWNER_ID = "ownerId";
  private static final String BUCKET_NAME = "bucketName";
  private static final String LOCK_FILE_PATH = "lockFilePath";

  @Mock
  private Storage mockStorage;

  @Mock
  private Blob mockBlob;

  @Mock
  private Logger mockLogger;

  private GCSConditionalWriteLockService lockService;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  /**
   * Mocks the given {@link Blob} so that when {@code blob.reader()} is called,
   * it returns a {@link ReadChannel} whose first read supplies JSON
   * for the provided {@link ConditionalWriteLockData}, followed by EOF on subsequent reads.
   */
  public static void mockBlobReaderWithLockData(Blob mockBlob, ConditionalWriteLockData data) throws IOException {
    String json = OBJECT_MAPPER.writeValueAsString(data);
    byte[] jsonBytes = json.getBytes();

    ReadChannel mockReadChannel = mock(ReadChannel.class);

    Answer<Integer> fillBufferWithJson = invocation -> {
      ByteBuffer buffer = invocation.getArgument(0);
      buffer.put(jsonBytes);
      return jsonBytes.length;
    };
    doAnswer(fillBufferWithJson)
        .doAnswer(invocation -> -1)
        .when(mockReadChannel).read(any(ByteBuffer.class));
    
    when(mockBlob.reader()).thenReturn(mockReadChannel);
  }

  @BeforeEach
  void setUp() {
    lockService = new GCSConditionalWriteLockService(OWNER_ID, BUCKET_NAME, LOCK_FILE_PATH, mockStorage, mockLogger);
  }

  @Test
  void testTryCreateOrUpdateLockFile_noPreviousLock_success() throws IOException {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 123L, "test-owner");

    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class))
    ).thenReturn(mockBlob);

    mockBlobReaderWithLockData(mockBlob, lockData);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertNotNull(result.getRight(), "Expected a valid ConditionalWriteLockFile on success");
    verify(mockStorage).create(
        any(BlobInfo.class),
        any(byte[].class),
        eq(Storage.BlobTargetOption.generationMatch(0))
    );
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testTryCreateOrUpdateLockFile_withPreviousLock_success() throws IOException {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "existing-owner");
    ConditionalWriteLockFile previousLockFile = new ConditionalWriteLockFile(lockData, "123"); // versionId=123

    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class))
    ).thenReturn(mockBlob);

    mockBlobReaderWithLockData(mockBlob, lockData);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, previousLockFile);

    assertNotNull(result, "Expected a valid ConditionalWriteLockFile on success");
    verify(mockStorage).create(
        any(BlobInfo.class),
        any(byte[].class),
        eq(Storage.BlobTargetOption.generationMatch(123L))
    );
  }

  @Test
  void testTryCreateOrUpdateLockFile_preconditionFailed() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "owner");
    ConditionalWriteLockFile previousLockFile = new ConditionalWriteLockFile(lockData, "123");

    StorageException exception = new StorageException(412, "Precondition Failed");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFile(lockData, previousLockFile);

    assertEquals(LockUpdateResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertNull(result.getRight(), "Should return null when a 412 occurs");
    verify(mockLogger).info(contains("Unable to write new lock file. Another process has modified this lockfile"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_rateLimitExceeded() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "owner");
    StorageException exception = new StorageException(429, "Rate Limit Exceeded");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertEquals(LockUpdateResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight(), "Should return null when a 429 occurs");
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_serverError() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "owner");
    StorageException exception = new StorageException(503, "Service Unavailable");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertEquals(LockUpdateResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight(), "Should return null when a 5xx error occurs");
    verify(mockLogger).warn(contains("GCS returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(exception));
  }

  @Test
  void testTryCreateOrUpdateLockFile_unexpectedError() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "owner");
    StorageException exception = new StorageException(400, "Bad Request");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    assertThrows(StorageException.class, () ->
            lockService.tryCreateOrUpdateLockFile(lockData, null),
        "Expected the method to rethrow the exception"
    );
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_successWithinExpiration() throws IOException {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, 123L, "retry-owner");
    Supplier<ConditionalWriteLockData> supplier = () -> data;
    // We'll simulate a 500 error on the first call, 429 on the second, then succeed on the third.
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(new StorageException(500, "Internal Server Error"))
        .thenThrow(new StorageException(429, "Rate limit exceeded"))
        .thenReturn(mockBlob);

    mockBlobReaderWithLockData(mockBlob, data);
    ConditionalWriteLockFile result = lockService.tryCreateOrUpdateLockFileWithRetry(supplier, null, 5).getRight();

    assertNotNull(result, "Should eventually succeed and return a valid file");
    verify(mockStorage, times(3)).create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_preconditionFailed() {
    Supplier<ConditionalWriteLockData> supplier = () -> new ConditionalWriteLockData(false, 123L, "retry-owner");
    StorageException exception412 = new StorageException(412, "Precondition Failed");

    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception412);
    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFileWithRetry(supplier, null, 5);

    assertEquals(LockUpdateResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertNull(result.getRight(), "Should return null and stop retrying immediately on 412");
    verify(mockStorage, times(1)).create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_forbiddenErrors() {
    Supplier<ConditionalWriteLockData> supplier = () -> new ConditionalWriteLockData(false, 123L, "retry-owner");
    StorageException exception403 = new StorageException(403, "Forbidden Error");

    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception403);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFileWithRetry(supplier, null, 3);
    assertEquals(LockUpdateResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight(), "Should return empty after failing to update before expiration");
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_forbiddenErrors_thenInterrupt() throws InterruptedException {
    Supplier<ConditionalWriteLockData> supplier = () -> new ConditionalWriteLockData(false, 123L, "retry-owner");
    StorageException exception400 = new StorageException(400, "bad request");
    CountDownLatch exceptionLatch = new CountDownLatch(1);
    CountDownLatch threadLatch = new CountDownLatch(1);

    // We don't expect it to return a final time.
    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class)
    )).thenAnswer(invocation -> {
      exceptionLatch.countDown();
      throw exception400;
    }).thenReturn(mockBlob);

    Thread t = new Thread(() -> {
      Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFileWithRetry(supplier, null, 3);
      assertEquals(LockUpdateResult.UNKNOWN_ERROR, result.getLeft());
      assertNull(result.getRight(), "Should return empty after failing to update before expiration");
      threadLatch.countDown();
    });
    t.start();
    assertTrue(exceptionLatch.await(1000L, TimeUnit.MILLISECONDS), "Did not throw the exception in time.");
    // Interrupt right before we throw the exception.
    t.interrupt();
    assertTrue(threadLatch.await(1000, TimeUnit.MILLISECONDS), "Did not get a result in time.");
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_withPreviousLock_success() throws IOException {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 999L, "existing-owner");
    ConditionalWriteLockFile previousLockFile = new ConditionalWriteLockFile(lockData, "123"); // versionId=123

    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class))
    ).thenReturn(mockBlob);

    mockBlobReaderWithLockData(mockBlob, lockData);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFileWithRetry(() -> lockData, previousLockFile, 3);

    assertEquals(LockUpdateResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight(), "Expected a valid ConditionalWriteLockFile on success");
    verify(mockStorage).create(
        any(BlobInfo.class),
        any(byte[].class),
        eq(Storage.BlobTargetOption.generationMatch(123L))
    );
  }

  @Test
  void testGetCurrentLockFile_blobNotFound() {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(null);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();

    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertNull(result.getRight(), "Expected empty when no blob is found");
  }

  @Test
  void testGetCurrentLockFile_blobFound() throws IOException {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(mockBlob);
    mockBlobReaderWithLockData(mockBlob, new ConditionalWriteLockData(false, 123L, "owner"));
    when(mockBlob.getGeneration()).thenReturn(123L);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();

    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight(), "Should return a ConditionalWriteLockFile if blob is found");
    assertEquals("123", result.getRight().getVersionId(), "Version ID should match blob generation");
  }

  @Test
  void testGetCurrentLockFile_404Error() {
    StorageException exception404 = new StorageException(404, "Not Found");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception404);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();

    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertNull(result.getRight(), "Should return null on 404 error");
    verify(mockLogger).info(contains("Object not found"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_rateLimit() {
    StorageException exception429 = new StorageException(429, "Rate Limit Exceeded");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception429);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();

    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight(), "Should return null on 429 error");
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_serverError() {
    StorageException exception500 = new StorageException(503, "Service Unavailable");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception500);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();

    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight(), "Should return null on 5xx error");
    verify(mockLogger).warn(contains("GCS returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(exception500));
  }

  @Test
  void testGetCurrentLockFile_unexpectedError() {
    StorageException exception400 = new StorageException(400, "Bad Request");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception400);

    assertThrows(StorageException.class, () -> lockService.getCurrentLockFile(),
        "Should rethrow unexpected errors");
  }

  @Test
  void testClose() throws Exception {
    lockService.close();
    verify(mockStorage).close();
  }
}
