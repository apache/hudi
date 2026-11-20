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

package org.apache.hudi.utilities.unstructured.embedding.source;

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.googleapis.json.GoogleJsonResponseException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which storage failures are an access denial ({@link SourceFailures#isAccessDenial}). The GCS connector reports a refused request
 * (403) as an IOException caused by its JSON error response, not as the {@link AccessDeniedException} S3A raises; both are an
 * access denial, which is never a file's own fault.
 */
public class TestSourceFailuresAccessDenial {

  private static final String SOURCE = "gs://landing/docs/a.pdf";

  /** Whether any throwable of {@code failure}'s chain is an access denial. */
  private static boolean denial(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (SourceFailures.isAccessDenial(cause)) {
        return true;
      }
    }
    return false;
  }

  /** The connector's wording: the request error wrapped once, the JSON response as its cause. */
  private static IOException gcs(int statusCode, String statusMessage) {
    GoogleJsonResponseException response = new GoogleJsonResponseException(statusCode, statusMessage,
        "GET https://storage.googleapis.com/storage/v1/b/landing/o/docs%2Fa.pdf\n{\"code\": " + statusCode + "}");
    return new IOException("Error accessing " + SOURCE, response);
  }

  /** A 403 whose JSON error names {@code reason}, as the connector reports a rate limit: a quota, not a denial. */
  private static IOException gcs403(String reason) {
    GoogleJsonResponseException response = new GoogleJsonResponseException(403, "Forbidden",
        "GET https://storage.googleapis.com/storage/v1/b/landing/o/docs%2Fa.pdf\n{\"code\": 403, \"errors\": [{\"domain\": "
            + "\"usageLimits\", \"reason\": \"" + reason + "\"}]}");
    return new IOException("Error accessing " + SOURCE, response);
  }

  @Test
  void s3aHdfsAndGcsConnector403AreDenials() {
    assertTrue(denial(new AccessDeniedException(SOURCE, null, "s3a 403")), "the S3A denial");
    assertTrue(denial(new AccessControlException("hdfs permission")), "the HDFS permission check");
    assertTrue(denial(gcs(403, "Forbidden")), "the GCS connector's denial");
    assertTrue(denial(new IOException("outer", gcs(403, "Forbidden"))), "a wrapped one");
  }

  @Test
  void otherGcsResponsesAreNotDenials() {
    assertFalse(denial(gcs(429, "Too Many Requests")));
    assertFalse(denial(gcs(503, "Service Unavailable")));
    assertFalse(denial(new IOException("403 Forbidden, but no HTTP response")));
  }

  @Test
  void gcsRateLimit403IsNotADenial() {
    assertFalse(denial(gcs403("rateLimitExceeded")), "rateLimitExceeded");
    assertFalse(denial(gcs403("userRateLimitExceeded")), "userRateLimitExceeded");
    assertTrue(denial(gcs403("forbidden")), "a 403 naming another reason");
  }
}
