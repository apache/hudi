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

import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test class for GCSStorageLockClient.
 */
@ExtendWith(MockitoExtension.class)
public class TestGCSStorageLockClient {

  private static final String OWNER_ID = "ownerId";
  private static final String LOCK_FILE_URI = "gs://bucket/lockFilePath";
  private static final String LOCK_FILE_URI_WITH_UNDERSCORES = "gs://bucket_with_underscores/lockFilePath";
  private static final String LOCK_FILE_PATH = "lockFilePath";
  private static final String BUCKET_NAME = "bucket";

  @Mock
  private Storage mockStorage;

  @Mock
  private Blob mockBlob;

  @Mock
  private Logger mockLogger;

  private GCSStorageLockClient lockService;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  /**
   * Mocks the given {@link Blob} so that when {@code blob.reader()} is called,
   * it returns a {@link ReadChannel} whose first read supplies JSON
   * for the provided {@link StorageLockData}, followed by EOF on subsequent reads.
   */
  public static void mockBlobReaderWithLockData(Blob mockBlob, StorageLockData data) throws IOException {
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
    setUp(LOCK_FILE_URI);
  }

  private void setUp(String lockFileUri) {
    lockService = new GCSStorageLockClient(OWNER_ID, lockFileUri, new Properties(), (a) -> mockStorage, mockLogger);
  }

  @ParameterizedTest
  @ValueSource(strings = {LOCK_FILE_URI, LOCK_FILE_URI_WITH_UNDERSCORES})
  void testTryCreateOrUpdateLockFile_noPreviousLock_success(String lockFileUri) {
    setUp(lockFileUri);
    StorageLockData lockData = new StorageLockData(false, 123L, "test-owner");

    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class))
    ).thenReturn(mockBlob);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertNotNull(result.getRight(), "Expected a valid StorageLockFile on success");
    verify(mockStorage).create(
        any(BlobInfo.class),
        any(byte[].class),
        eq(Storage.BlobTargetOption.generationMatch(0))
    );
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testTryCreateOrUpdateLockFile_withPreviousLock_success() {
    StorageLockData lockData = new StorageLockData(false, 999L, "existing-owner");
    StorageLockFile previousLockFile = new StorageLockFile(lockData, "123"); // versionId=123

    when(mockStorage.create(
        any(BlobInfo.class),
        any(byte[].class),
        any(Storage.BlobTargetOption.class))
    ).thenReturn(mockBlob);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.of(previousLockFile));

    assertNotNull(result, "Expected a valid StorageLockFile on success");
    verify(mockStorage).create(
        any(BlobInfo.class),
        any(byte[].class),
        eq(Storage.BlobTargetOption.generationMatch(123L))
    );
  }

  @Test
  void testTryCreateOrUpdateLockFile_preconditionFailed() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    StorageLockFile previousLockFile = new StorageLockFile(lockData, "123");

    StorageException exception = new StorageException(412, "Precondition Failed");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.of(previousLockFile));

    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty when a 412 occurs");
    verify(mockLogger).info(contains("Unable to write new lock file. Another process has modified this lockfile"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_rateLimitExceeded() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    StorageException exception = new StorageException(429, "Rate Limit Exceeded");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty when a 429 occurs");
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_serverError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    StorageException exception = new StorageException(503, "Service Unavailable");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty when a 5xx error occurs");
    verify(mockLogger).warn(contains("GCS returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(exception));
  }

  @Test
  void testTryCreateOrUpdateLockFile_unexpectedError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    StorageException exception = new StorageException(400, "Bad Request");
    when(mockStorage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption.class)))
        .thenThrow(exception);

    assertThrows(StorageException.class, () ->
            lockService.tryUpsertLockFile(lockData, Option.empty()),
        "Expected the method to rethrow the exception"
    );
  }

  @Test
  void testGetCurrentLockFile_blobNotFound() {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(null);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();

    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Expected empty when no blob is found");
  }

  @Test
  void testGetCurrentLockFile_blobFound() throws IOException {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(mockBlob);
    mockBlobReaderWithLockData(mockBlob, new StorageLockData(false, 123L, "owner"));
    when(mockBlob.getGeneration()).thenReturn(123L);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();

    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight(), "Should return a StorageLockFile if blob is found");
    assertEquals("123", result.getRight().get().getVersionId(), "Version ID should match blob generation");
  }

  @Test
  void testGetCurrentLockFile_404Error() {
    StorageException exception404 = new StorageException(404, "Not Found");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception404);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();

    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty on 404 error");
    verify(mockLogger).info(contains("Object not found"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_rateLimit() {
    StorageException exception429 = new StorageException(429, "Rate Limit Exceeded");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception429);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();

    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty on 429 error");
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_serverError() {
    StorageException exception500 = new StorageException(503, "Service Unavailable");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception500);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();

    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty on 5xx error");
    verify(mockLogger).warn(contains("GCS returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(exception500));
  }

  @Test
  void testGetCurrentLockFile_unexpectedError() {
    StorageException exception400 = new StorageException(400, "Bad Request");
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenThrow(exception400);

    assertThrows(StorageException.class, () -> lockService.readCurrentLockFile(),
        "Should rethrow unexpected errors");
  }

  @Test
  void testGetCurrentLockFile_IOException() {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(mockBlob);
    when(mockBlob.reader()).thenThrow(new HoodieIOException("IO Error"));

    assertThrows(HoodieIOException.class, () -> lockService.readCurrentLockFile());
  }

  @Test
  void testGetCurrentLockFile_IOExceptionWrapping404() {
    when(mockStorage.get(BlobId.of(BUCKET_NAME, LOCK_FILE_PATH))).thenReturn(mockBlob);
    when(mockBlob.reader()).thenThrow(new HoodieIOException("IO Error", new IOException(new StorageException(404, "storage 404"))));

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty(), "Should return empty on IO exception wrapping 404 error");
  }

  @Test
  void testClose() throws Exception {
    lockService.close();
    verify(mockStorage).close();
  }
}
