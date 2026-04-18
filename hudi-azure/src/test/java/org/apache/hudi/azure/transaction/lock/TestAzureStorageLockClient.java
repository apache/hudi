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

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieLockException;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAzureStorageLockClient {

  private static final String OWNER_ID = "ownerId";
  private static final String LOCK_FILE_URI =
      "abfs://container@account.dfs.core.windows.net/lockFilePath";
  private static final String LOCK_FILE_URI_WITH_NESTED_PATH =
      "abfs://container@account.dfs.core.windows.net/lake/db/tbl-default/.hoodie/.locks/table_lock.json";

  @Mock
  private BlobServiceClient mockBlobServiceClient;

  @Mock
  private BlobContainerClient mockContainerClient;

  @Mock
  private BlobClient mockBlobClient;

  @Mock
  private Logger mockLogger;

  private AzureStorageLockClient lockClient;

  @BeforeEach
  void setUp() {
    setUp(LOCK_FILE_URI);
  }

  private void setUp(String lockFileUri) {
    when(mockBlobServiceClient.getBlobContainerClient(eq("container"))).thenReturn(mockContainerClient);
    String expectedBlobPath = lockFileUri.replaceFirst("^abfss?://[^/]+/", "");
    when(mockContainerClient.getBlobClient(eq(expectedBlobPath))).thenReturn(mockBlobClient);

    lockClient = new AzureStorageLockClient(
        OWNER_ID,
        lockFileUri,
        new Properties(),
        (location) -> mockBlobServiceClient,
        mockLogger);
  }

  @Test
  void testTryUpsertLockFile_noPreviousLock_success_setsIfNoneMatchStar() throws Exception {
    StorageLockData lockData = new StorageLockData(false, 123L, "test-owner");
    @SuppressWarnings("unchecked")
    Response<BlockBlobItem> response = (Response<BlockBlobItem>) mock(Response.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().set("ETag", "\"etag-1\""));
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
        .thenReturn(response);

    ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("\"etag-1\"", result.getRight().get().getVersionId());

    verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), eq(Context.NONE));
    BlobParallelUploadOptions options = optionsCaptor.getValue();
    assertRequestCondition(options, "ifNoneMatch", "*");
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testTryUpsertLockFile_withPreviousLock_success_setsIfMatch() throws Exception {
    StorageLockData lockData = new StorageLockData(false, 999L, "existing-owner");
    StorageLockFile previousLockFile = new StorageLockFile(lockData, "\"etag-prev\"");

    @SuppressWarnings("unchecked")
    Response<BlockBlobItem> response = (Response<BlockBlobItem>) mock(Response.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().set("ETag", "\"etag-new\""));
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
        .thenReturn(response);

    ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockClient.tryUpsertLockFile(lockData, Option.of(previousLockFile));

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("\"etag-new\"", result.getRight().get().getVersionId());

    verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), eq(Context.NONE));
    BlobParallelUploadOptions options = optionsCaptor.getValue();
    assertRequestCondition(options, "ifMatch", "\"etag-prev\"");
  }

  @Test
  void testTryUpsertLockFile_fallsBackToBlockBlobItemEtag() {
    StorageLockData lockData = new StorageLockData(false, 123L, "test-owner");
    @SuppressWarnings("unchecked")
    Response<BlockBlobItem> response = (Response<BlockBlobItem>) mock(Response.class);
    BlockBlobItem blockBlobItem = mock(BlockBlobItem.class);
    when(response.getHeaders()).thenReturn(null);
    when(response.getValue()).thenReturn(blockBlobItem);
    when(blockBlobItem.getETag()).thenReturn("0x8DABC123");
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
        .thenReturn(response);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("\"0x8DABC123\"", result.getRight().get().getVersionId());
  }

  @Test
  void testTryUpsertLockFile_preconditionFailed_returnsAcquiredByOthers() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(412);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(
        contains("Unable to write new lock file. Another process has modified this lockfile"),
        eq(OWNER_ID),
        eq(LOCK_FILE_URI));
  }

  @Test
  void testTryUpsertLockFile_rateLimit_returnsUnknownError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(429);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_URI));
  }

  @Test
  void testTryUpsertLockFile_serverError_returnsUnknownError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(503);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Azure returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_URI), eq(ex));
  }

  @Test
  void testTryUpsertLockFile_unexpectedError_returnsUnknownError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(400);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockClient.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
  }

  @Test
  void testReadCurrentLockFile_notFound_returnsNotExists() {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(404);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();
    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Object not found"), eq(OWNER_ID), eq(LOCK_FILE_URI));
  }

  @Test
  void testReadCurrentLockFile_blobFound_success() {
    setUp(LOCK_FILE_URI_WITH_NESTED_PATH);

    StorageLockData data = new StorageLockData(false, 1700000000000L, "testOwner");
    byte[] json = StorageLockFile.toByteArray(data);
    BlobDownloadContentResponse response = mock(BlobDownloadContentResponse.class);
    when(response.getValue()).thenReturn(BinaryData.fromBytes(json));
    when(response.getHeaders()).thenReturn(new HttpHeaders().set("ETag", "\"etag-123\""));
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenReturn(response);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();

    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("\"etag-123\"", result.getRight().get().getVersionId());
    assertEquals("testOwner", result.getRight().get().getOwner());
  }

  @Test
  void testReadCurrentLockFile_missingEtag_throwsHoodieLockException() {
    StorageLockData data = new StorageLockData(false, 1700000000000L, "testOwner");
    byte[] json = StorageLockFile.toByteArray(data);
    BlobDownloadContentResponse response = mock(BlobDownloadContentResponse.class);
    when(response.getHeaders()).thenReturn(null);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenReturn(response);

    HoodieLockException exception =
        assertThrows(HoodieLockException.class, () -> lockClient.readCurrentLockFile());

    assertTrue(exception.getMessage().contains("Missing ETag in Azure download response for lock file"));
  }

  @Test
  void testReadCurrentLockFile_emptyEtag_throwsHoodieLockException() {
    StorageLockData data = new StorageLockData(false, 1700000000000L, "testOwner");
    byte[] json = StorageLockFile.toByteArray(data);
    BlobDownloadContentResponse response = mock(BlobDownloadContentResponse.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().set("ETag", ""));
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenReturn(response);

    HoodieLockException exception =
        assertThrows(HoodieLockException.class, () -> lockClient.readCurrentLockFile());

    assertTrue(exception.getMessage().contains("Missing ETag in Azure download response for lock file"));
  }

  @Test
  void testReadCurrentLockFile_malformedQuotedEtag_throwsHoodieLockException() {
    BlobDownloadContentResponse response = mock(BlobDownloadContentResponse.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().set("ETag", "\"etag-123"));
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenReturn(response);

    HoodieLockException exception =
        assertThrows(HoodieLockException.class, () -> lockClient.readCurrentLockFile());

    assertTrue(exception.getMessage().contains("Malformed ETag in Azure download response for lock file"));
  }

  @Test
  void testReadCurrentLockFile_download404_returnsNotExists() {
    BlobStorageException ex404 = mock(BlobStorageException.class);
    when(ex404.getStatusCode()).thenReturn(404);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenThrow(ex404);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();
    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
  }

  @Test
  void testClose_noop() {
    lockClient.close();
  }

  @Test
  void testReadObject_reusesSecondaryBlobServiceClientForSameEndpoint() {
    BlobServiceClient primaryServiceClient = mock(BlobServiceClient.class);
    BlobContainerClient primaryContainerClient = mock(BlobContainerClient.class);
    BlobClient primaryBlobClient = mock(BlobClient.class);
    when(primaryServiceClient.getBlobContainerClient(any(String.class))).thenReturn(primaryContainerClient);
    when(primaryContainerClient.getBlobClient(any(String.class))).thenReturn(primaryBlobClient);

    BlobServiceClient secondaryServiceClient = mock(BlobServiceClient.class);
    BlobContainerClient secondaryContainerClient = mock(BlobContainerClient.class);
    BlobClient secondaryBlobClient = mock(BlobClient.class);
    when(secondaryServiceClient.getBlobContainerClient(any(String.class))).thenReturn(secondaryContainerClient);
    when(secondaryContainerClient.getBlobClient(any(String.class))).thenReturn(secondaryBlobClient);
    when(secondaryBlobClient.exists()).thenReturn(false);

    AtomicInteger secondarySupplierInvocations = new AtomicInteger(0);
    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID,
        LOCK_FILE_URI,
        new Properties(),
        location -> {
          if ("https://secondary.blob.core.windows.net".equals(location.getBlobEndpoint())) {
            secondarySupplierInvocations.incrementAndGet();
            return secondaryServiceClient;
          }
          return primaryServiceClient;
        },
        mockLogger);

    client.readObject("abfs://container@secondary.dfs.core.windows.net/path1", true);
    client.readObject("abfs://container@secondary.dfs.core.windows.net/path2", true);

    assertEquals(1, secondarySupplierInvocations.get());
  }

  @Test
  void testTryUpsertLockFile_conflict409_returnsUnknownError() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "\"some-etag-409\"");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(409);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
        lockClient.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(LockUpsertResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Retriable conditional request conflict error"), eq(OWNER_ID), eq(LOCK_FILE_URI));
  }

  @Test
  void testReadCurrentLockFile_rateLimit429_returnsUnknownError() {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(429);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_URI));
  }

  @Test
  void testReadCurrentLockFile_serverError500_returnsUnknownError() {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(500);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenThrow(ex);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Azure returned internal server error code"), eq(OWNER_ID), eq(LOCK_FILE_URI), eq(ex));
  }

  @Test
  void testReadCurrentLockFile_unexpectedError400_throws() {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(400);
    when(mockBlobClient.downloadContentWithResponse(isNull(), isNull(), isNull(), eq(Context.NONE))).thenThrow(ex);

    assertThrows(BlobStorageException.class, () -> lockClient.readCurrentLockFile());
  }

  @Test
  void testReadObject_found_returnsContent() {
    BlobServiceClient svc = mock(BlobServiceClient.class);
    BlobContainerClient container = mock(BlobContainerClient.class);
    BlobClient configBlobClient = mock(BlobClient.class);
    when(svc.getBlobContainerClient(any(String.class))).thenReturn(container);
    when(container.getBlobClient(any(String.class))).thenReturn(configBlobClient);

    String expectedContent = "{\"key\":\"value\"}";
    when(configBlobClient.exists()).thenReturn(true);
    when(configBlobClient.downloadContent()).thenReturn(BinaryData.fromString(expectedContent));

    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID, LOCK_FILE_URI, new Properties(),
        (location) -> svc, mockLogger);

    Option<String> result = client.readObject(
        "abfs://container@account.dfs.core.windows.net/config.json", true);

    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
  }

  @Test
  void testReadObject_notFound_returnsEmpty() {
    BlobServiceClient svc = mock(BlobServiceClient.class);
    BlobContainerClient container = mock(BlobContainerClient.class);
    BlobClient configBlobClient = mock(BlobClient.class);
    when(svc.getBlobContainerClient(any(String.class))).thenReturn(container);
    when(container.getBlobClient(any(String.class))).thenReturn(configBlobClient);

    when(configBlobClient.exists()).thenReturn(false);

    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID, LOCK_FILE_URI, new Properties(),
        (location) -> svc, mockLogger);

    Option<String> result = client.readObject(
        "abfs://container@account.dfs.core.windows.net/config.json", true);

    assertFalse(result.isPresent());
    verify(configBlobClient, never()).downloadContent();
  }

  @Test
  void testReadObject_blobStorageException404_returnsEmpty() {
    BlobServiceClient svc = mock(BlobServiceClient.class);
    BlobContainerClient container = mock(BlobContainerClient.class);
    BlobClient configBlobClient = mock(BlobClient.class);
    when(svc.getBlobContainerClient(any(String.class))).thenReturn(container);
    when(container.getBlobClient(any(String.class))).thenReturn(configBlobClient);

    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(404);
    when(configBlobClient.downloadContent()).thenThrow(ex);

    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID, LOCK_FILE_URI, new Properties(),
        (location) -> svc, mockLogger);

    Option<String> result = client.readObject(
        "abfs://container@account.dfs.core.windows.net/config.json", false);

    assertFalse(result.isPresent());
  }

  @Test
  void testWriteObject_success() {
    BlobServiceClient svc = mock(BlobServiceClient.class);
    BlobContainerClient container = mock(BlobContainerClient.class);
    BlobClient configBlobClient = mock(BlobClient.class);
    when(svc.getBlobContainerClient(any(String.class))).thenReturn(container);
    when(container.getBlobClient(any(String.class))).thenReturn(configBlobClient);

    doNothing().when(configBlobClient).upload(any(BinaryData.class), anyBoolean());

    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID, LOCK_FILE_URI, new Properties(),
        (location) -> svc, mockLogger);

    boolean result = client.writeObject(
        "abfs://container@account.dfs.core.windows.net/audit.json", "test content");

    assertTrue(result);
    verify(configBlobClient).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void testWriteObject_failure() {
    BlobServiceClient svc = mock(BlobServiceClient.class);
    BlobContainerClient container = mock(BlobContainerClient.class);
    BlobClient configBlobClient = mock(BlobClient.class);
    when(svc.getBlobContainerClient(any(String.class))).thenReturn(container);
    when(container.getBlobClient(any(String.class))).thenReturn(configBlobClient);

    doThrow(new RuntimeException("Upload failed"))
        .when(configBlobClient).upload(any(BinaryData.class), anyBoolean());

    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID, LOCK_FILE_URI, new Properties(),
        (location) -> svc, mockLogger);

    boolean result = client.writeObject(
        "abfs://container@account.dfs.core.windows.net/audit.json", "test content");

    assertFalse(result);
  }

  @Test
  void testInitializeWithAbfsUri() {
    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID,
        "abfss://container@account.dfs.core.windows.net/table/.hoodie/.locks/lock.json",
        new Properties(),
        (location) -> mockBlobServiceClient,
        mockLogger);
    assertNotNull(client);
  }

  @Test
  void testInitializeWithWasbUri() {
    AzureStorageLockClient client = new AzureStorageLockClient(
        OWNER_ID,
        "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/lock.json",
        new Properties(),
        (location) -> mockBlobServiceClient,
        mockLogger);
    assertNotNull(client);
  }

  @Test
  void testInitializeWithInvalidUri() {
    assertThrows(Exception.class, () -> new AzureStorageLockClient(
        OWNER_ID,
        "not-a-valid-uri",
        new Properties(),
        (location) -> mockBlobServiceClient,
        mockLogger));
  }

  private static BlobStorageException mockBlobStorageException(int statusCode, BlobErrorCode errorCode) {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(statusCode);
    if (errorCode != null) {
      org.mockito.Mockito.lenient().when(ex.getErrorCode()).thenReturn(errorCode);
    }
    return ex;
  }

  private static void assertRequestCondition(Object blobParallelUploadOptions, String expectedField, String expectedValue) throws Exception {
    Object requestConditions = tryInvoke(blobParallelUploadOptions, "getRequestConditions");
    if (requestConditions == null) {
      Field f = blobParallelUploadOptions.getClass().getDeclaredField("requestConditions");
      f.setAccessible(true);
      requestConditions = f.get(blobParallelUploadOptions);
    }
    assertNotNull(requestConditions, "requestConditions should be set on upload options");

    Object actualIfMatch = tryInvoke(requestConditions, "getIfMatch");
    if (actualIfMatch == null) {
      actualIfMatch = getFieldIfExists(requestConditions, "ifMatch");
    }
    Object actualIfNoneMatch = tryInvoke(requestConditions, "getIfNoneMatch");
    if (actualIfNoneMatch == null) {
      actualIfNoneMatch = getFieldIfExists(requestConditions, "ifNoneMatch");
    }

    if ("ifMatch".equals(expectedField)) {
      assertEquals(expectedValue, actualIfMatch, "Expected If-Match to be set");
    } else if ("ifNoneMatch".equals(expectedField)) {
      assertEquals(expectedValue, actualIfNoneMatch, "Expected If-None-Match to be set");
    } else {
      throw new IllegalArgumentException("Unexpected expectedField: " + expectedField);
    }
  }

  private static Object tryInvoke(Object target, String methodName) {
    try {
      Method m = target.getClass().getMethod(methodName);
      return m.invoke(target);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static Object getFieldIfExists(Object target, String fieldName) {
    try {
      Field f = target.getClass().getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.get(target);
    } catch (Exception ignored) {
      return null;
    }
  }
}
