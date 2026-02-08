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

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.common.util.Option;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for AzureStorageLockClient audit operations (readObject and writeObject methods)
 */
public class TestAzureStorageLockClientAuditOperations {

  private BlobClient mockBlobClient;
  private BlobClient mockConfigBlobClient;
  private BlobContainerClient mockContainerClient;
  private Logger mockLogger;
  private AzureStorageLockClient lockClient;

  @BeforeEach
  void setUp() {
    mockBlobClient = mock(BlobClient.class);
    mockConfigBlobClient = mock(BlobClient.class);
    mockContainerClient = mock(BlobContainerClient.class);
    mockLogger = mock(Logger.class);

    when(mockBlobClient.getContainerClient()).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(any())).thenReturn(mockConfigBlobClient);

    String ownerId = "test-owner";
    String lockFileUri = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/table_lock.json";
    lockClient = new AzureStorageLockClient(
        ownerId,
        lockFileUri,
        new Properties(),
        (uri, props) -> mockBlobClient,
        mockLogger);
  }

  @Test
  void testReadConfigWithCheckExistsFirstFileNotFound() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";

    // exists() returns false
    when(mockConfigBlobClient.exists()).thenReturn(false);

    Option<String> result = lockClient.readObject(configPath, true);

    assertTrue(result.isEmpty());
    // Should only call exists(), not downloadContent()
    verify(mockConfigBlobClient, times(1)).exists();
    verify(mockConfigBlobClient, never()).downloadContent();
  }

  @Test
  void testReadConfigWithCheckExistsFirstFileExists() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": true}";

    // exists() returns true
    when(mockConfigBlobClient.exists()).thenReturn(true);

    // downloadContent() returns content
    when(mockConfigBlobClient.downloadContent()).thenReturn(BinaryData.fromString(expectedContent));

    Option<String> result = lockClient.readObject(configPath, true);

    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should call both exists() and downloadContent()
    verify(mockConfigBlobClient, times(1)).exists();
    verify(mockConfigBlobClient, times(1)).downloadContent();
  }

  @Test
  void testReadConfigWithoutCheckExistsFirstFileNotFound() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";

    // Direct downloadContent() throws BlobNotFound
    BlobStorageException notFoundException = mockBlobStorageException(404, BlobErrorCode.BLOB_NOT_FOUND);
    when(mockConfigBlobClient.downloadContent()).thenThrow(notFoundException);

    Option<String> result = lockClient.readObject(configPath, false);

    assertTrue(result.isEmpty());
    // Should not call exists(), only downloadContent()
    verify(mockConfigBlobClient, never()).exists();
    verify(mockConfigBlobClient, times(1)).downloadContent();
  }

  @Test
  void testReadConfigWithoutCheckExistsFirstFileExists() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": false}";

    // Direct downloadContent() returns content
    when(mockConfigBlobClient.downloadContent()).thenReturn(BinaryData.fromString(expectedContent));

    Option<String> result = lockClient.readObject(configPath, false);

    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should not call exists(), only downloadContent()
    verify(mockConfigBlobClient, never()).exists();
    verify(mockConfigBlobClient, times(1)).downloadContent();
  }

  @Test
  void testReadConfigWithCheckExistsFirstOtherError() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";

    // exists() throws non-404 error
    BlobStorageException serverError = mockBlobStorageException(500, null);
    when(mockConfigBlobClient.exists()).thenThrow(serverError);

    Option<String> result = lockClient.readObject(configPath, true);

    assertTrue(result.isEmpty());
    verify(mockConfigBlobClient, times(1)).exists();
    verify(mockConfigBlobClient, never()).downloadContent();
  }

  @Test
  void testReadConfigWithInvalidUri() {
    String invalidPath = "not-a-valid-uri";

    Option<String> result = lockClient.readObject(invalidPath, false);

    assertTrue(result.isEmpty());
    // Should not make any blob calls due to URI parsing error
    verify(mockConfigBlobClient, never()).exists();
  }

  @Test
  void testReadConfigWithRateLimitError() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_enabled.json";

    // downloadContent() returns rate limit error
    BlobStorageException rateLimitException = mockBlobStorageException(429, null);
    when(mockConfigBlobClient.downloadContent()).thenThrow(rateLimitException);

    Option<String> result = lockClient.readObject(configPath, false);

    assertTrue(result.isEmpty());
  }

  @Test
  void testWriteConfigSuccess() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_log.json";
    String content = "{\"operation\":\"START\",\"timestamp\":123456789}";

    doNothing().when(mockConfigBlobClient).upload(any(BinaryData.class), anyBoolean());

    boolean result = lockClient.writeObject(configPath, content);

    assertTrue(result);
    verify(mockConfigBlobClient, times(1)).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void testWriteConfigFailure() {
    String configPath = "wasbs://container@account.blob.core.windows.net/table/.hoodie/.locks/audit_log.json";
    String content = "{\"operation\":\"START\",\"timestamp\":123456789}";

    BlobStorageException uploadException = mockBlobStorageException(500, null);
    doThrow(uploadException).when(mockConfigBlobClient).upload(any(BinaryData.class), anyBoolean());

    boolean result = lockClient.writeObject(configPath, content);

    assertFalse(result);
    verify(mockConfigBlobClient, times(1)).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void testWriteConfigInvalidUri() {
    String invalidPath = "not-a-valid-uri";
    String content = "some content";

    boolean result = lockClient.writeObject(invalidPath, content);

    assertFalse(result);
    // Should not make any blob calls due to URI parsing error
    verify(mockConfigBlobClient, never()).upload(any(BinaryData.class), anyBoolean());
  }

  private BlobStorageException mockBlobStorageException(int statusCode, BlobErrorCode errorCode) {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getStatusCode()).thenReturn(statusCode);
    if (errorCode != null) {
      when(ex.getErrorCode()).thenReturn(errorCode);
    }
    return ex;
  }
}
