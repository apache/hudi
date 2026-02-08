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
 * KIND, either express or implied.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.util.Properties;

import static org.apache.hudi.client.transaction.lock.models.LockUpsertResult.UNKNOWN_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestAzureStorageLockClient {

  private static final String OWNER_ID = "ownerId";
  private static final String LOCK_FILE_URI_WASB = "wasbs://container@account.blob.core.windows.net/lockFilePath";
  private static final String LOCK_FILE_URI_ABFS = "abfss://container@account.dfs.core.windows.net/lockFilePath";
  private static final String LOCK_FILE_PATH = "lockFilePath";

  @Mock
  private BlobClient mockBlobClient;

  @Mock
  private BlobContainerClient mockContainerClient;

  @Mock
  private Logger mockLogger;

  private AzureStorageLockClient lockService;

  @BeforeEach
  void setUp() {
    lockService = new AzureStorageLockClient(
        OWNER_ID,
        LOCK_FILE_URI_WASB,
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    );
  }

  @Test
  void testTryCreateOrUpdateLockFile_noPreviousLock_success() {
    StorageLockData lockData = new StorageLockData(false, System.currentTimeMillis(), "myTxOwner");
    BlockBlobItem mockItem = mock(BlockBlobItem.class);
    when(mockItem.getETag()).thenReturn("new-etag-123");
    @SuppressWarnings("unchecked")
    Response<BlockBlobItem> mockResponse = mock(Response.class);
    when(mockResponse.getValue()).thenReturn(mockItem);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenReturn(mockResponse);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("new-etag-123", result.getRight().get().getVersionId());
    verify(mockBlobClient, times(1)).uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any());
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testInitializeWithInvalidUri() {
    assertThrows(IllegalArgumentException.class, () -> new AzureStorageLockClient(
        OWNER_ID,
        "\\",
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    ));
  }

  @Test
  void testInitializeWithNoLockFilePath() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new AzureStorageLockClient(
        OWNER_ID,
        "wasbs://container@account.blob.core.windows.net/",
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    ));
    assertTrue(ex.getMessage().contains("path"));
  }

  @Test
  void testInitializeWithNoContainerName() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new AzureStorageLockClient(
        OWNER_ID,
        "wasbs:///path",
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    ));
    assertTrue(ex.getMessage().contains("authority"));
  }

  @Test
  void testInitializeWithInvalidWasbFormat() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new AzureStorageLockClient(
        OWNER_ID,
        "wasbs://container/path",
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    ));
    assertTrue(ex.getMessage().contains("WASB"));
  }

  @Test
  void testInitializeWithAbfsUri() {
    // Should not throw
    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID,
        LOCK_FILE_URI_ABFS,
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger
    );
    assertTrue(client != null);
  }

  @Test
  void testTryCreateOrUpdateLockFile_withPreviousLock_success() {
    StorageLockData lockData = new StorageLockData(false, 1000L, "myTxOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "old-etag-999");
    BlockBlobItem mockItem = mock(BlockBlobItem.class);
    when(mockItem.getETag()).thenReturn("new-etag-456");
    @SuppressWarnings("unchecked")
    Response<BlockBlobItem> mockResponse = mock(Response.class);
    when(mockResponse.getValue()).thenReturn(mockItem);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenReturn(mockResponse);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("new-etag-456", result.getRight().get().getVersionId());
    verify(mockBlobClient, times(1)).uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any());
  }

  @Test
  void testTryCreateOrUpdateLockFile_preconditionFailed() {
    StorageLockData lockData = new StorageLockData(false, 2000L, "txOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "some-etag");

    BlobStorageException ex412 = mockBlobStorageException(412, BlobErrorCode.CONDITION_NOT_MET);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenThrow(ex412);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Lock file modified by another process"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_conflict409() {
    StorageLockData lockData = new StorageLockData(false, 3000L, "myTxOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "some-etag-409");

    BlobStorageException ex409 = mockBlobStorageException(409, null);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenThrow(ex409);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Retriable conditional request conflict error"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_rateLimitExceeded() {
    StorageLockData lockData = new StorageLockData(false, 4000L, "myTxOwner");
    BlobStorageException ex429 = mockBlobStorageException(429, null);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenThrow(ex429);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_serverError() {
    StorageLockData lockData = new StorageLockData(false, 5000L, "myTxOwner");
    BlobStorageException ex503 = mockBlobStorageException(503, null);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenThrow(ex503);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex503));
  }

  @Test
  void testTryCreateLockFile_unexpectedError() {
    StorageLockData lockData = new StorageLockData(false, 8000L, "myTxOwner");
    BlobStorageException ex400 = mockBlobStorageException(400, null);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class),
        any(), any())).thenThrow(ex400);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Error writing lock file"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex400));
  }

  @Test
  void testGetCurrentLockFile_404Error() {
    BlobStorageException ex404 = mockBlobStorageException(404, BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlobClient.getProperties()).thenThrow(ex404);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Blob not found"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_409Error() {
    BlobStorageException ex409 = mockBlobStorageException(409, null);
    when(mockBlobClient.getProperties()).thenThrow(ex409);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Conflicting operation has occurred"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_objectFound() {
    StorageLockData lockData = new StorageLockData(false, 9999L, "myTxOwner");
    byte[] bytes = StorageLockFile.toByteArray(lockData);

    BlobProperties mockProps = mock(BlobProperties.class);
    when(mockProps.getETag()).thenReturn("abc-etag");
    when(mockBlobClient.getProperties()).thenReturn(mockProps);
    when(mockBlobClient.downloadContent()).thenReturn(BinaryData.fromBytes(bytes));

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("abc-etag", result.getRight().get().getVersionId());
    assertEquals("myTxOwner", result.getRight().get().getOwner());
  }

  @Test
  void testGetCurrentLockFile_rateLimit() {
    BlobStorageException ex429 = mockBlobStorageException(429, null);
    when(mockBlobClient.getProperties()).thenThrow(ex429);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_serverError() {
    BlobStorageException ex500 = mockBlobStorageException(500, null);
    when(mockBlobClient.getProperties()).thenThrow(ex500);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Azure internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex500));
  }

  @Test
  void testGetCurrentLockFile_unexpectedError() {
    BlobStorageException ex400 = mockBlobStorageException(400, null);
    when(mockBlobClient.getProperties()).thenThrow(ex400);

    assertThrows(BlobStorageException.class, () -> lockService.readCurrentLockFile());
  }

  @Test
  void testReadObject_found() {
    when(mockBlobClient.getContainerClient()).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(any())).thenReturn(mockBlobClient);
    String testContent = "{\"test\":\"data\"}";
    when(mockBlobClient.exists()).thenReturn(true);
    when(mockBlobClient.downloadContent()).thenReturn(BinaryData.fromString(testContent));

    Option<String> result = lockService.readObject(LOCK_FILE_URI_WASB, true);
    assertTrue(result.isPresent());
    assertEquals(testContent, result.get());
  }

  @Test
  void testReadObject_notFound() {
    when(mockBlobClient.getContainerClient()).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(any())).thenReturn(mockBlobClient);
    when(mockBlobClient.exists()).thenReturn(false);

    Option<String> result = lockService.readObject(LOCK_FILE_URI_WASB, true);
    assertTrue(result.isEmpty());
    verify(mockLogger).debug(contains("JSON config file not found"), eq(LOCK_FILE_URI_WASB));
  }

  @Test
  void testWriteObject_success() {
    when(mockBlobClient.getContainerClient()).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(any())).thenReturn(mockBlobClient);
    doNothing().when(mockBlobClient).upload(any(BinaryData.class), anyBoolean());

    boolean result = lockService.writeObject(LOCK_FILE_URI_WASB, "test content");
    assertTrue(result);
    verify(mockBlobClient).upload(any(BinaryData.class), eq(true));
    verify(mockLogger).debug(contains("Successfully wrote object"), eq(LOCK_FILE_URI_WASB));
  }

  @Test
  void testWriteObject_failure() {
    when(mockBlobClient.getContainerClient()).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(any())).thenReturn(mockBlobClient);
    doThrow(new RuntimeException("Upload failed"))
        .when(mockBlobClient).upload(any(BinaryData.class), anyBoolean());

    boolean result = lockService.writeObject(LOCK_FILE_URI_WASB, "test content");
    assertTrue(!result);
    verify(mockLogger).error(contains("Error writing object"), eq(LOCK_FILE_URI_WASB), any(RuntimeException.class));
  }

  @Test
  void testClose() {
    lockService.close();
    // BlobClient doesn't require explicit close, so nothing to verify
  }

  private BlobStorageException mockBlobStorageException(int statusCode, BlobErrorCode errorCode) {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(statusCode);
    if (errorCode != null) {
      org.mockito.Mockito.lenient().when(ex.getErrorCode()).thenReturn(errorCode);
    }
    return ex;
  }
}
