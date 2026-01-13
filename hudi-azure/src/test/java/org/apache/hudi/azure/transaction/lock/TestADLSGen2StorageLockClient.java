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

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestADLSGen2StorageLockClient {

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
  private BlobProperties mockBlobProperties;

  @Mock
  private Logger mockLogger;

  private ADLSGen2StorageLockClient lockClient;

  @BeforeEach
  void setUp() {
    setUp(LOCK_FILE_URI);
  }

  private void setUp(String lockFileUri) {
    when(mockBlobServiceClient.getBlobContainerClient(eq("container"))).thenReturn(mockContainerClient);
    String expectedBlobPath = lockFileUri.replaceFirst("^abfss?://[^/]+/", "");
    when(mockContainerClient.getBlobClient(eq(expectedBlobPath))).thenReturn(mockBlobClient);

    lockClient = new ADLSGen2StorageLockClient(
        OWNER_ID,
        lockFileUri,
        new Properties(),
        (location) -> mockBlobServiceClient,
        mockLogger);
  }

  @Test
  void testTryUpsertLockFile_noPreviousLock_success_setsIfNoneMatchStar() throws Exception {
    StorageLockData lockData = new StorageLockData(false, 123L, "test-owner");
    when(mockBlobClient.getProperties()).thenReturn(mockBlobProperties);
    when(mockBlobProperties.getETag()).thenReturn("\"etag-1\"");

    ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
        .thenReturn(null);

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

    when(mockBlobClient.getProperties()).thenReturn(mockBlobProperties);
    when(mockBlobProperties.getETag()).thenReturn("\"etag-new\"");

    ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
        .thenReturn(null);

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
  void testTryUpsertLockFile_unexpectedError_rethrows() {
    StorageLockData lockData = new StorageLockData(false, 999L, "owner");
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(400);
    when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE))).thenThrow(ex);

    assertThrows(BlobStorageException.class, () -> lockClient.tryUpsertLockFile(lockData, Option.empty()));
  }

  @Test
  void testReadCurrentLockFile_notFound_returnsNotExists() {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(404);
    when(mockBlobClient.getProperties()).thenThrow(ex);

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
    ByteArrayInputStream delegate = new ByteArrayInputStream(json);
    BlobInputStream stream = mock(BlobInputStream.class);
    try {
      doAnswer(inv -> delegate.read(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)))
          .when(stream).read(any(byte[].class), anyInt(), anyInt());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to stub BlobInputStream", ioe);
    }

    when(mockBlobClient.getProperties()).thenReturn(mockBlobProperties);
    when(mockBlobProperties.getETag()).thenReturn("\"etag-123\"");
    when(mockBlobClient.openInputStream()).thenReturn(stream);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();

    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("\"etag-123\"", result.getRight().get().getVersionId());
    assertEquals("testOwner", result.getRight().get().getOwner());
  }

  @Test
  void testReadCurrentLockFile_streamRead404_returnsUnknownError() {
    when(mockBlobClient.getProperties()).thenReturn(mockBlobProperties);
    when(mockBlobProperties.getETag()).thenReturn("\"etag-123\"");

    BlobStorageException ex404 = mock(BlobStorageException.class);
    when(ex404.getStatusCode()).thenReturn(404);
    when(mockBlobClient.openInputStream()).thenThrow(ex404);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockClient.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
  }

  @Test
  void testClose_noop() {
    lockClient.close();
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

