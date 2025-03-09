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

import org.apache.hudi.client.transaction.lock.LockGetResult;
import org.apache.hudi.client.transaction.lock.LockUpdateResult;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.client.transaction.lock.LockUpdateResult.UNKNOWN_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestS3ConditionalWriteLockService {

  private static final String OWNER_ID = "ownerId";
  private static final String BUCKET_NAME = "bucketName";
  private static final String LOCK_FILE_PATH = "lockFilePath";

  @Mock
  private S3Client mockS3Client;

  @Mock
  private Logger mockLogger;

  private S3ConditionalWriteLockService lockService;

  @BeforeEach
  void setUp() {
    lockService = new S3ConditionalWriteLockService(
        OWNER_ID,
        BUCKET_NAME,
        LOCK_FILE_PATH,
        mockS3Client,
        mockLogger
    );
  }

  private void mockS3ObjectWithLockData(ConditionalWriteLockData lockData, String eTag) {
    byte[] bytes = ConditionalWriteLockFile.toByteArray(lockData);
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
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, System.currentTimeMillis(), "myTxOwner");
    PutObjectResponse putResp = PutObjectResponse.builder().eTag("new-etag-123").build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putResp);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertEquals(LockUpdateResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight());
    assertEquals("new-etag-123", result.getRight().getVersionId());
    verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  void testTryCreateOrUpdateLockFile_withPreviousLock_success() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 1000L, "myTxOwner");
    ConditionalWriteLockFile prevLockFile = new ConditionalWriteLockFile(lockData, "old-etag-999");
    PutObjectResponse putResp = PutObjectResponse.builder().eTag("new-etag-456").build();
    when(mockS3Client.putObject(
        eq(PutObjectRequest.builder().bucket(BUCKET_NAME)
            .key(LOCK_FILE_PATH)
            .ifMatch("old-etag-999")
            .build()),
        any(RequestBody.class)
    )).thenReturn(putResp);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, prevLockFile);

    assertEquals(LockUpdateResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight());
    assertEquals("new-etag-456", result.getRight().getVersionId());
    verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  void testTryCreateOrUpdateLockFile_preconditionFailed() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 2000L, "txOwner");
    ConditionalWriteLockFile prevLockFile = new ConditionalWriteLockFile(lockData, "some-etag");

    AwsServiceException ex412 = S3Exception.builder().statusCode(412).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex412);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, prevLockFile);

    assertEquals(LockUpdateResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).info(contains("Lockfile modified by another process"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_conflict409() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 3000L, "myTxOwner");
    ConditionalWriteLockFile prevLockFile = new ConditionalWriteLockFile(lockData, "some-etag-409");

    AwsServiceException ex409 = S3Exception.builder().statusCode(409).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex409);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, prevLockFile);

    assertEquals(LockUpdateResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).info(contains("Lockfile modified by another process"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_rateLimitExceeded() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 4000L, "myTxOwner");
    AwsServiceException ex429 = S3Exception.builder().statusCode(429).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex429);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testTryCreateOrUpdateLockFile_serverError() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 5000L, "myTxOwner");
    AwsServiceException ex503 = S3Exception.builder().statusCode(503).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex503);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFile(lockData, null);

    assertEquals(UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).warn(contains("S3 internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex503));
  }

  @Test
  void testTryCreateOrUpdateLockFile_unexpectedError() {
    AwsServiceException ex400 = S3Exception.builder().statusCode(400).message("Bad Request").build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex400);

    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 6000L, "myTxOwner");
    assertThrows(S3Exception.class, () -> lockService.tryCreateOrUpdateLockFile(lockData, null));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_successWithinAttempts() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 7000L, "myTxOwner");
    AwsServiceException ex500 = S3Exception.builder().statusCode(500).build();
    AwsServiceException ex429 = S3Exception.builder().statusCode(429).build();
    PutObjectResponse successResp = PutObjectResponse.builder().eTag("retry-etag").build();

    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(ex500)
        .thenThrow(ex429)
        .thenReturn(successResp);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFileWithRetry(() -> lockData, null, 5);

    assertEquals(LockUpdateResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight());
    assertEquals("retry-etag", result.getRight().getVersionId());
    verify(mockS3Client, times(3)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_preconditionFailed() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 8000L, "myTxOwner");
    AwsServiceException ex412 = S3Exception.builder().statusCode(412).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex412);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFileWithRetry(() -> lockData, null, 5);

    assertEquals(LockUpdateResult.ACQUIRED_BY_OTHERS, result.getLeft());
    assertNull(result.getRight());
    verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_unexpectedError() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 8000L, "myTxOwner");
    AwsServiceException ex400 = AwsServiceException.builder().statusCode(400).build();
    when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(ex400);

    Pair<LockUpdateResult, ConditionalWriteLockFile> result =
        lockService.tryCreateOrUpdateLockFileWithRetry(() -> lockData, null, 3);

    assertEquals(LockUpdateResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockS3Client, times(3)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test
  void testTryCreateOrUpdateLockFileWithRetry_unexpectedThenInterrupt() throws InterruptedException {
    AwsServiceException ex400 = S3Exception.builder().statusCode(400).message("Bad Request").build();
    CountDownLatch attemptLatch = new CountDownLatch(1);
    CountDownLatch interruptLatch = new CountDownLatch(1);

    doAnswer(invocation -> {
      attemptLatch.countDown();
      throw ex400;
    }).when(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

    Thread t = new Thread(() -> {
      Pair<LockUpdateResult, ConditionalWriteLockFile> result = lockService.tryCreateOrUpdateLockFileWithRetry(
          () -> new ConditionalWriteLockData(false, 9999L, "myTxOwner"),
          null,
          3
      );
      assertEquals(UNKNOWN_ERROR, result.getLeft());
      assertNull(result.getRight());
      interruptLatch.countDown();
    });
    t.start();
    assertTrue(attemptLatch.await(2, TimeUnit.SECONDS));
    t.interrupt();
    t.join();
    assertTrue(interruptLatch.await(2, TimeUnit.SECONDS));
  }

  @Test
  void testGetCurrentLockFile_404Error() {
    AwsServiceException ex404 = S3Exception.builder().statusCode(404).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex404);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();
    assertEquals(LockGetResult.NOT_EXISTS, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).info(contains("Object not found"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_409Error() {
    AwsServiceException ex409 = S3Exception.builder().statusCode(409).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex409);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).info(contains("Conflicting operation has occurred"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_objectFound() {
    ConditionalWriteLockData lockData = new ConditionalWriteLockData(false, 9999L, "myTxOwner");
    mockS3ObjectWithLockData(lockData, "abc-etag");

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();
    assertEquals(LockGetResult.SUCCESS, result.getLeft());
    assertNotNull(result.getRight());
    assertEquals("abc-etag", result.getRight().getVersionId());
    assertEquals("myTxOwner", result.getRight().getOwner());
  }

  @Test
  void testGetCurrentLockFile_rateLimit() {
    AwsServiceException ex429 = S3Exception.builder().statusCode(429).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex429);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).warn(contains("Rate limit exceeded"), eq(OWNER_ID), eq(LOCK_FILE_PATH));
  }

  @Test
  void testGetCurrentLockFile_serverError() {
    AwsServiceException ex500 = S3Exception.builder().statusCode(500).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex500);

    Pair<LockGetResult, ConditionalWriteLockFile> result = lockService.getCurrentLockFile();
    assertEquals(LockGetResult.UNKNOWN_ERROR, result.getLeft());
    assertNull(result.getRight());
    verify(mockLogger).warn(contains("S3 internal server error"), eq(OWNER_ID), eq(LOCK_FILE_PATH), eq(ex500));
  }

  @Test
  void testGetCurrentLockFile_unexpectedError() {
    AwsServiceException ex400 = S3Exception.builder().statusCode(400).build();
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(ex400);

    assertThrows(S3Exception.class, () -> lockService.getCurrentLockFile());
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
    assertThrows(HoodieIOException.class, () -> lockService.getCurrentLockFile());
  }

  @Test
  void testClose() throws Exception {
    lockService.close();
    verify(mockS3Client).close();
  }
}
