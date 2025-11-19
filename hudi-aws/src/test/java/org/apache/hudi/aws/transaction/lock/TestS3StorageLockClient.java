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
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hudi.exception.HoodieLockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.client.transaction.lock.models.LockUpsertResult.UNKNOWN_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestS3StorageLockClient {

  private static final String OWNER_ID = "ownerId";
  private static final String LOCK_FILE_URI = "s3://bucket/lockFilePath";
  private static final String LOCK_FILE_PATH = "lockFilePath";

  @Mock
  private S3Client mockS3Client;

  @Mock
  private Logger mockLogger;

  private S3StorageLockClient lockService;

  @BeforeEach
  void setUp() {
    lockService = new S3StorageLockClient(
            OWNER_ID,
            LOCK_FILE_URI,
            new Properties(),
            (a,b) -> mockS3Client,
            mockLogger
    );
  }

  private void mockS3ObjectWithLockData(StorageLockData lockData, String eTag) {
    byte[] bytes = StorageLockFile.toByteArray(lockData);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    ResponseInputStream<GetObjectResponse> mockResponseStream = new ResponseInputStream<>(
            GetObjectResponse.builder().eTag(eTag).build(),
            new MockAbortableInputStream(bais)
    );

    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockResponseStream);
  }

  private static class MockAbortableInputStream extends java.io.FilterInputStream {
    protected MockAbortableInputStream(ByteArrayInputStream in) {
      super(in);
    }
  }

  @Test
  void testTryCreateOrUpdateLockFile_noPreviousLock_success() {
    StorageLockData lockData = new StorageLockData(false, System.currentTimeMillis(), "myTxOwner");
    PutObjectResponse putResp = PutObjectResponse.builder().eTag("new-etag-123").build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putResp);

    Pair<LockUpsertResult, Option<StorageLockFile>> result = lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("new-etag-123", result.getRight().get().getVersionId());
    verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testInitializeWithInvalidUri() {
    assertThrows(HoodieLockException.class, () -> new S3StorageLockClient(
            OWNER_ID,
            "\\",
            new Properties(),
            (a,b) -> mockS3Client,
            mockLogger
    ));
  }

  @Test
  void testInitializeWithNoLockFilePath() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new S3StorageLockClient(
            OWNER_ID,
            "s3://bucket/",
            new Properties(),
            (a,b) -> mockS3Client,
            mockLogger
    ));
    assertTrue(ex.getMessage().contains("path"));
  }

  @Test
  void testInitializeWithNoBucketName() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new S3StorageLockClient(
            OWNER_ID,
            "s3:///path",
            new Properties(),
            (a,b) -> mockS3Client,
            mockLogger
    ));
    assertTrue(ex.getMessage().contains("bucket name"));
  }

  @Test
  void testTryCreateOrUpdateLockFile_withPreviousLock_success() {
    StorageLockData lockData = new StorageLockData(false, 1000L, "myTxOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "old-etag-999");
    PutObjectResponse putResp = PutObjectResponse.builder().eTag("new-etag-456").build();
    when(mockS3Client.putObject(
            eq(PutObjectRequest.builder().bucket("bucket")
                    .key(LOCK_FILE_PATH)
                    .ifMatch("old-etag-999")
                    .build()),
            any(RequestBody.class)
    )).thenReturn(putResp);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
            lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(LockUpsertResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("new-etag-456", result.getRight().get().getVersionId());
    verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  void testTryCreateOrUpdateLockFile_preconditionFailed() {
    StorageLockData lockData = new StorageLockData(false, 2000L, "txOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "some-etag");

    AwsServiceException ex412 = S3Exception.builder().statusCode(412).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex412);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
            lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Lockfile modified by another process"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_conflict409() {
    StorageLockData lockData = new StorageLockData(false, 3000L, "myTxOwner");
    StorageLockFile prevLockFile = new StorageLockFile(lockData, "some-etag-409");

    AwsServiceException ex409 = S3Exception.builder().statusCode(409).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex409);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
            lockService.tryUpsertLockFile(lockData, Option.of(prevLockFile));

    // This error is not necessarily unknown, but we don't know the state of the lockfile,
    // which is why the return result is UNKNOWN_ERROR
    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Retriable conditional request conflict error"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_rateLimitExceeded() {
    StorageLockData lockData = new StorageLockData(false, 4000L, "myTxOwner");
    AwsServiceException ex429 = S3Exception.builder().statusCode(429).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex429);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
            lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_serverError() {
    StorageLockData lockData = new StorageLockData(false, 5000L, "myTxOwner");
    AwsServiceException ex503 = S3Exception.builder().statusCode(503).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex503);

    Pair<LockUpsertResult, Option<StorageLockFile>> result =
            lockService.tryUpsertLockFile(lockData, Option.empty());

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex503));
  }

  @Test
  void testTryCreateLockFile_unexpectedError() {
    StorageLockData lockData = new StorageLockData(false, 8000L, "myTxOwner");
    AwsServiceException ex400 = AwsServiceException.builder().statusCode(400).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex400);

    assertThrows(AwsServiceException.class, () -> lockService.tryUpsertLockFile(lockData, Option.empty()));
  }

  @Test
  void testGetCurrentLockFile_404Error() {
    AwsServiceException ex404 = S3Exception.builder().statusCode(404).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex404);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Object not found"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_409Error() {
    AwsServiceException ex409 = S3Exception.builder().statusCode(409).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex409);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).info(contains("Conflicting operation has occurred"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_objectFound() {
    StorageLockData lockData = new StorageLockData(false, 9999L, "myTxOwner");
    mockS3ObjectWithLockData(lockData, "abc-etag");

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertTrue(result.getRight().isPresent());
    assertEquals("abc-etag", result.getRight().get().getVersionId());
    assertEquals("myTxOwner", result.getRight().get().getOwner());
  }

  @Test
  void testGetCurrentLockFile_rateLimit() {
    AwsServiceException ex429 = S3Exception.builder().statusCode(429).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex429);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_serverError() {
    AwsServiceException ex500 = S3Exception.builder().statusCode(500).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex500);

    Pair<LockGetResult, Option<StorageLockFile>> result = lockService.readCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertTrue(result.getRight().isEmpty());
    verify(mockLogger).warn(contains("S3 internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex500));
  }

  @Test
  void testGetCurrentLockFile_unexpectedError() {
    AwsServiceException ex400 = S3Exception.builder().statusCode(400).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex400);

    assertThrows(S3Exception.class, () -> lockService.readCurrentLockFile());
  }

  @Test
  void testGetCurrentLockFile_readFailure() {
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().eTag("some-etag").build();
    @SuppressWarnings("unchecked")
    ResponseInputStream<GetObjectResponse> mockStream = mock(ResponseInputStream.class);
    when(mockStream.response()).thenReturn(getObjectResponse);
    try {
      when(mockStream.read(any(byte[].class), anyInt(), anyInt()))
              .thenThrow(new IOException("Read error"));
    } catch (IOException ignored) {
      // ignore
    }
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockStream);
    assertThrows(HoodieIOException.class, () -> lockService.readCurrentLockFile());
  }

  @Test
  void testClose() throws Exception {
    lockService.close();
    verify(mockS3Client).close();
  }
}
