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

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
 * Tests for S3StorageLockClient.readObject method
 */
public class TestS3StorageLockClientReadConfig {
  
  private S3Client mockS3Client;

  private S3StorageLockClient lockClient;

  @BeforeEach
  void setUp() {
    mockS3Client = mock(S3Client.class);
    Logger mockLogger = mock(Logger.class);
    String ownerId = "test-owner";
    String lockFileUri = "s3://test-bucket/table/.hoodie/.locks/table_lock.json";
    lockClient = new S3StorageLockClient(
            ownerId,
            lockFileUri,
            new Properties(),
            (bucket, props) -> mockS3Client,
            mockLogger);
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstFileNotFound() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // HEAD request returns 404
    S3Exception notFoundException = (S3Exception) S3Exception.builder()
        .statusCode(404)
        .message("Not Found")
        .build();
    when(mockS3Client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(notFoundException);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isEmpty());
    // Should only call HEAD, not GET
    verify(mockS3Client, times(1)).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstFileExists() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": true}";
    
    // HEAD request succeeds
    HeadObjectResponse headResponse = HeadObjectResponse.builder().build();
    when(mockS3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(headResponse);
    
    // GET request returns content
    ResponseBytes<GetObjectResponse> responseBytes = ResponseBytes.fromByteArray(
        GetObjectResponse.builder().build(),
        expectedContent.getBytes(StandardCharsets.UTF_8));
    when(mockS3Client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenReturn(responseBytes);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should call both HEAD and GET
    verify(mockS3Client, times(1)).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, times(1)).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithoutCheckExistsFirstFileNotFound() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // Direct GET request returns 404
    S3Exception notFoundException = (S3Exception) S3Exception.builder()
        .statusCode(404)
        .message("Not Found")
        .build();
    when(mockS3Client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenThrow(notFoundException);
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isEmpty());
    // Should not call HEAD, only GET
    verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, times(1)).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithoutCheckExistsFirstFileExists() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    String expectedContent = "{\"STORAGE_LP_AUDIT_SERVICE_ENABLED\": false}";
    
    // Direct GET request returns content
    ResponseBytes<GetObjectResponse> responseBytes = ResponseBytes.fromByteArray(
        GetObjectResponse.builder().build(),
        expectedContent.getBytes(StandardCharsets.UTF_8));
    when(mockS3Client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenReturn(responseBytes);
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isPresent());
    assertEquals(expectedContent, result.get());
    // Should not call HEAD, only GET
    verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, times(1)).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithCheckExistsFirstOtherS3Error() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // HEAD request returns non-404 error
    S3Exception serverError = (S3Exception) S3Exception.builder()
        .statusCode(500)
        .message("Internal Server Error")
        .build();
    when(mockS3Client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(serverError);
    
    Option<String> result = lockClient.readObject(configPath, true);
    
    assertTrue(result.isEmpty());
    verify(mockS3Client, times(1)).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithInvalidUri() {
    String invalidPath = "not-a-valid-uri";
    
    Option<String> result = lockClient.readObject(invalidPath, false);
    
    assertTrue(result.isEmpty());
    // Should not make any S3 calls due to URI parsing error
    verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
  }
  
  @Test
  void testReadConfigWithRateLimitError() {
    String configPath = "s3://test-bucket/table/.hoodie/.locks/audit_enabled.json";
    
    // GET request returns rate limit error
    S3Exception rateLimitException = (S3Exception) S3Exception.builder()
        .statusCode(429)
        .message("Too Many Requests")
        .build();
    when(mockS3Client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenThrow(rateLimitException);
    
    Option<String> result = lockClient.readObject(configPath, false);
    
    assertTrue(result.isEmpty());
    verify(mockS3Client, times(1)).getObjectAsBytes(any(GetObjectRequest.class));
  }
}