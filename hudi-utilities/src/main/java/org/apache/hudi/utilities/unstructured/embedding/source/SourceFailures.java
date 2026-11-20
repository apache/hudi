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

import org.apache.hudi.utilities.unstructured.SourceCauses;

import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Tells a storage failure that is a file's own fault from one a later run may not see: a
 * source that is gone ({@link FileNotFoundException}: at open {@code BlobBytesSource} re-checks it, and a file found present becomes
 * an access denial; a FileNotFoundException of a later read, e.g. an S3A lazy open, is re-checked the same way),
 * short ({@link BlobTruncatedException}, raised only by {@code BlobBytesSource}'s own range checks) or whose blob
 * row names no readable range ({@link BlobReferenceException}) gives the same answer on every read, so it is a content failure of
 * its file (its quarantine entry); any other storage failure is an interruption ({@code SourceReadException} of data prep). A
 * bare {@link java.io.EOFException} is not a content failure: Hadoop also reports a dropped connection as one. Used by
 * {@link BlobBytesSource#isTransient}.
 */
public final class SourceFailures {

  /** The simple name of the HTTP response exception the GCS connector's refused requests are caused by. */
  private static final String GCS_HTTP_RESPONSE_EXCEPTION = "HttpResponseException";
  /** The HTTP status of a refused request. */
  private static final int HTTP_FORBIDDEN = 403;
  /**
   * The reasons of a GCS 403 that refuses a request for its rate, not its access: the JSON error names one of them
   * ({@code "reason": "rateLimitExceeded"}), and a later attempt may pass.
   */
  private static final String[] GCS_RATE_LIMIT_REASONS = {"\"rateLimitExceeded\"", "\"userRateLimitExceeded\""};

  private SourceFailures() {
  }

  /**
   * A speculative read may fall back after ordinary I/O, but never after a JVM failure in its cause or suppressed graph.
   * Rethrows that original failure; a stack overflow keeps its existing input-content treatment.
   */
  public static void rethrowJvmFailure(Throwable failure) {
    Error jvmFailure = jvmFailure(failure);
    if (jvmFailure != null) {
      throw jvmFailure;
    }
  }

  /** The shared node rule for JVM failures; a stack overflow retains input-content classification. */
  public static boolean isJvmFailure(Throwable failure) {
    return (failure instanceof VirtualMachineError && !(failure instanceof StackOverflowError)) || failure instanceof LinkageError;
  }

  /** The first JVM failure in {@code failure}'s cause or suppressed graph ({@link #rethrowJvmFailure}'s rule), else null. */
  public static Error jvmFailure(Throwable failure) {
    if (failure == null) {
      return null;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(failure);
    while (!pending.isEmpty()) {
      Throwable current = pending.removeFirst();
      if (!visited.add(current)) {
        continue;
      }
      if (isJvmFailure(current)) {
        return (Error) current;
      }
      if (current.getCause() != null) {
        pending.addLast(current.getCause());
      }
      Collections.addAll(pending, current.getSuppressed());
    }
    return null;
  }

  /**
   * Whether {@code cause} itself is an access denial: {@link AccessDeniedException} (how S3A reports a 403 or a KMS denial),
   * Hadoop's {@link AccessControlException} (an HDFS permission check), or the GCS connector's HTTP 403 response unless it
   * names a rate-limit reason ({@code rateLimitExceeded}, {@code userRateLimitExceeded}): such a 403 is a quota a later attempt
   * may pass, so it takes the retry path. The GCS connector raises an IOException caused by its JSON error response, whose
   * class is relocated under the connector's own package in the shaded connector jar, so it is recognized by its class's
   * simple name in the hierarchy ({@code HttpResponseException}) and its message, which starts with the status code and
   * carries the JSON error with its reason.
   */
  public static boolean isAccessDenial(Throwable cause) {
    if (cause instanceof AccessDeniedException || cause instanceof AccessControlException) {
      return true;
    }
    for (Class<?> type = cause.getClass(); type != null; type = type.getSuperclass()) {
      if (GCS_HTTP_RESPONSE_EXCEPTION.equals(type.getSimpleName())) {
        String message = cause.getMessage();
        return message != null && message.startsWith(HTTP_FORBIDDEN + " ") && !namesRateLimit(message);
      }
    }
    return false;
  }

  private static boolean namesRateLimit(String message) {
    for (String reason : GCS_RATE_LIMIT_REASONS) {
      if (message.contains(reason)) {
        return true;
      }
    }
    return false;
  }

  /** Whether {@code failure} or any of its causes is an access denial ({@link #isAccessDenial}). */
  public static boolean isCausedByAccessDenial(Throwable failure) {
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (isAccessDenial(cause)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code failure}'s cause chain says the file itself is at fault: its source is gone or short, or its row malformed. An
   * access denial anywhere in the chain ({@link #isAccessDenial}: java.nio.file.AccessDeniedException, which is
   * a 403, a KMS denial or a file there but unreadable; Hadoop's AccessControlException; the GCS connector's 403 response,
   * unless it names a rate-limit reason) is never the file's fault, even when it carries the storage's FileNotFoundException.
   */
  public static boolean isContentFailure(Throwable failure) {
    boolean content = false;
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (isAccessDenial(cause)) {
        return false;
      }
      content |= cause instanceof FileNotFoundException || cause instanceof BlobTruncatedException
          || cause instanceof BlobReferenceException || cause instanceof BlobSourceChangedException;
    }
    return content;
  }
}
