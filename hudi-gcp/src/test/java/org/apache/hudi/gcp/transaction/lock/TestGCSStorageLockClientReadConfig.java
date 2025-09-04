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

package org.apache.hudi.gcp.transaction.lock;

import org.apache.hudi.common.util.Option;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for GCSStorageLockClient.readObject method
 */
public class TestGCSStorageLockClientReadConfig {
  
  private Storage mockGcsClient;
  private Blob mockBlob;
  
  private GCSStorageLockClient lockClient;

  @BeforeEach
  void setUp() {
    mockGcsClient = mock(Storage.class);
    Logger mockLogger = mock(Logger.class);
    mockBlob = mock(Blob.class);
    String lockFileUri = "gs://test-bucket/table/.hoodie/.locks/table_lock.json";
    String ownerId = "test-owner";
    lockClient = new GCSStorageLockClient(
            ownerId,
            lockFileUri,
            new Properties(),
            props -> mockGcsClient,
            mockLogger);
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstFileNotFound() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // get() returns null for non-existent blob
    when(mockGcsClient.get(any(BlobId.class)))
        .thenReturn(null);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isEmpty());
    // Should only call get() for existence check, not readAllBytes
    verify(mockGcsClient, times(1)).get(any(BlobId.class));
    verify(mockGcsClient, never()).readAllBytes(any(BlobId.class));
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstBlobExistsFalse() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // get() returns blob but exists() is false
    when(mockGcsClient.get(any(BlobId.class)))
        .thenReturn(mockBlob);
    when(mockBlob.exists()).thenReturn(false);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isEmpty());
    // Should only check existence, not read content
    verify(mockGcsClient, times(1)).get(any(BlobId.class));
    verify(mockBlob, times(1)).exists();
    verify(mockBlob, never()).getContent();
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstFileExists() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": true}";
    
    // get() returns existing blob
    when(mockGcsClient.get(any(BlobId.class)))
        .thenReturn(mockBlob);
    when(mockBlob.exists()).thenReturn(true);
    when(mockBlob.getContent())
        .thenReturn(expectedContent.getBytes(StandardCharsets.UTF_8));
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should call get() for existence check and getContent() for reading
    verify(mockGcsClient, times(1)).get(any(BlobId.class));
    verify(mockBlob, times(1)).exists();
    verify(mockBlob, times(1)).getContent();
  }
  
  @Test
  void testReadConfigWithoutCheckExistsFirstFileNotFound() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // Direct readAllBytes throws 404 exception
    StorageException notFoundException = new StorageException(404, "Not Found");
    when(mockGcsClient.readAllBytes(any(BlobId.class)))
        .thenThrow(notFoundException);
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isEmpty());
    // Should not call get(), only readAllBytes
    verify(mockGcsClient, never()).get(any(BlobId.class));
    verify(mockGcsClient, times(1)).readAllBytes(any(BlobId.class));
  }
  
  @Test
  void testReadConfigWithoutCheckExistsFirstFileExists() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": false}";
    
    // Direct readAllBytes returns content
    when(mockGcsClient.readAllBytes(any(BlobId.class)))
        .thenReturn(expectedContent.getBytes(StandardCharsets.UTF_8));
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should not call get(), only readAllBytes
    verify(mockGcsClient, never()).get(any(BlobId.class));
    verify(mockGcsClient, times(1)).readAllBytes(any(BlobId.class));
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstOtherGcsError() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // get() throws non-404 error
    StorageException serverError = new StorageException(500, "Internal Server Error");
    when(mockGcsClient.get(any(BlobId.class)))
        .thenThrow(serverError);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isEmpty());
    verify(mockGcsClient, times(1)).get(any(BlobId.class));
    verify(mockGcsClient, never()).readAllBytes(any(BlobId.class));
  }
  
  @Test
  void testReadConfigWithInvalidUri() {
    String invalidPath = "not-a-valid-uri";
    
    Option<String> result = lockClient.readObject(invalidPath, false);
    
    assertTrue(result.isEmpty());
    // Should not make any GCS calls due to URI parsing error
    verify(mockGcsClient, never()).get(any(BlobId.class));
    verify(mockGcsClient, never()).readAllBytes(any(BlobId.class));
  }
  
  @Test
  void testReadConfigWithRateLimitError() {
    String configPath = "gs://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // readAllBytes returns rate limit error
    StorageException rateLimitException = new StorageException(429, "Too Many Requests");
    when(mockGcsClient.readAllBytes(any(BlobId.class)))
        .thenThrow(rateLimitException);
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isEmpty());
    verify(mockGcsClient, times(1)).readAllBytes(any(BlobId.class));
  }
}