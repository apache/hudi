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

package org.apache.hudi.utilities.exception;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionException;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the thin hudi-utilities exception types that wrap {@link HoodieException}.
 */
class TestUtilitiesExceptions {

  @Test
  void sourceTimeoutExceptionPreservesMessageAndCause() {
    Throwable cause = new InterruptedException("waited too long");
    HoodieSourceTimeoutException withCause = new HoodieSourceTimeoutException("source timed out", cause);
    assertEquals("source timed out", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieSourceTimeoutException messageOnly = new HoodieSourceTimeoutException("source timed out");
    assertEquals("source timed out", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void streamerExceptionsPreserveMessageAndCause() {
    Throwable cause = new IllegalStateException("boom");
    HoodieStreamerException withCause = new HoodieStreamerException("streamer failed", cause);
    assertEquals("streamer failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieStreamerException messageOnly = new HoodieStreamerException("streamer failed");
    assertEquals("streamer failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());

    HoodieStreamerWriteException writeWithCause = new HoodieStreamerWriteException("write failed", cause);
    assertEquals("write failed", writeWithCause.getMessage());
    assertSame(cause, writeWithCause.getCause());
    assertTrue(writeWithCause instanceof HoodieStreamerException);

    HoodieStreamerWriteException writeMessageOnly = new HoodieStreamerWriteException("write failed");
    assertEquals("write failed", writeMessageOnly.getMessage());
    assertNull(writeMessageOnly.getCause());
  }

  @Test
  void ingestionExceptionPreservesMessageAndCause() {
    HoodieIngestionException messageOnly = new HoodieIngestionException("ingestion failed");
    assertEquals("ingestion failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
    assertTrue(messageOnly instanceof HoodieException);

    Throwable cause = new IllegalArgumentException("bad config");
    HoodieIngestionException causeOnly = new HoodieIngestionException(cause);
    assertSame(cause, causeOnly.getCause());
    // Throwable-only ctor derives the message from the cause.
    assertEquals(cause.toString(), causeOnly.getMessage());
  }

  @Test
  void transformPlanExceptionPreservesMessageAndCause() {
    Throwable cause = new RuntimeException("bad sql");
    HoodieTransformPlanException withCause = new HoodieTransformPlanException("planning failed", cause);
    assertEquals("planning failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieTransformException);

    HoodieTransformPlanException messageOnly = new HoodieTransformPlanException("planning failed");
    assertEquals("planning failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void sourcePostProcessExceptionPreservesMessageAndCause() {
    Throwable cause = new RuntimeException("post process");
    HoodieSourcePostProcessException withCause = new HoodieSourcePostProcessException("source post process failed", cause);
    assertEquals("source post process failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieSourcePostProcessException messageOnly = new HoodieSourcePostProcessException("source post process failed");
    assertEquals("source post process failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void schemaPostProcessExceptionPreservesMessageAndCause() {
    Throwable cause = new RuntimeException("post process");
    HoodieSchemaPostProcessException withCause = new HoodieSchemaPostProcessException("schema post process failed", cause);
    assertEquals("schema post process failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieSchemaPostProcessException messageOnly = new HoodieSchemaPostProcessException("schema post process failed");
    assertEquals("schema post process failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());
  }

  @Test
  void schemaProviderExceptionsPreserveMessageAndCause() {
    Throwable cause = new RuntimeException("registry down");
    HoodieSchemaProviderException withCause = new HoodieSchemaProviderException("provider failed", cause);
    assertEquals("provider failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieSchemaProviderException messageOnly = new HoodieSchemaProviderException("provider failed");
    assertEquals("provider failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());

    HoodieSchemaFetchException fetchWithCause = new HoodieSchemaFetchException("fetch failed", cause);
    assertEquals("fetch failed", fetchWithCause.getMessage());
    assertSame(cause, fetchWithCause.getCause());
    assertTrue(fetchWithCause instanceof HoodieSchemaProviderException);

    HoodieSchemaFetchException fetchMessageOnly = new HoodieSchemaFetchException("fetch failed");
    assertEquals("fetch failed", fetchMessageOnly.getMessage());
    assertNull(fetchMessageOnly.getCause());
  }

  @Test
  void incrementalPullExceptionsPreserveMessageAndCause() {
    SQLException cause = new SQLException("syntax error");
    HoodieIncrementalPullException withCause = new HoodieIncrementalPullException("pull failed", cause);
    assertEquals("pull failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
    assertTrue(withCause instanceof HoodieException);

    HoodieIncrementalPullException messageOnly = new HoodieIncrementalPullException("pull failed");
    assertEquals("pull failed", messageOnly.getMessage());
    assertNull(messageOnly.getCause());

    HoodieIncrementalPullSQLException sqlWithCause = new HoodieIncrementalPullSQLException("sql pull failed", cause);
    assertEquals("sql pull failed", sqlWithCause.getMessage());
    assertSame(cause, sqlWithCause.getCause());
    assertTrue(sqlWithCause instanceof HoodieIncrementalPullException);

    HoodieIncrementalPullSQLException sqlMessageOnly = new HoodieIncrementalPullSQLException("sql pull failed");
    assertEquals("sql pull failed", sqlMessageOnly.getMessage());
    assertNull(sqlMessageOnly.getCause());
  }
}
