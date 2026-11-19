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

import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The two classifiers of a failure to read a blob source: {@link SourceFailures#isContentFailure} (the file's own fault, its
 * quarantine entry) and {@link BlobBytesSource#isTransient} (an open the source retries in the round).
 */
public class TestSourceFailures {

  static Stream<Arguments> failures() {
    return Stream.of(
        // name, failure, content failure, transient
        Arguments.of("file not found", new FileNotFoundException("gone"), true, false),
        Arguments.of("file not found wrapped by the storage layer",
            new HoodieIOException("Exception creating input stream", new FileNotFoundException("gone")), true, false),
        Arguments.of("access denied carrying a file not found", denied(new FileNotFoundException("gone")), false, false),
        Arguments.of("access denied, wrapped, carrying a file not found",
            new IOException("open failed", denied(new FileNotFoundException("gone"))), false, false),
        Arguments.of("hdfs permission check", new AccessControlException("Permission denied"), false, false),
        Arguments.of("hdfs permission check carrying a file not found",
            withCause(new AccessControlException("Permission denied"), new FileNotFoundException("gone")), false, false),
        Arguments.of("truncated blob", new BlobTruncatedException("ended early"), true, false),
        Arguments.of("malformed reference", new BlobReferenceException("relative path"), true, false),
        Arguments.of("bare EOF: a dropped connection", new EOFException("connection closed"), false, true),
        Arguments.of("served scheme without its file system",
            new IOException("no storage", new UnsupportedFileSystemException("No FileSystem for scheme \"s3a\"")), false, false),
        Arguments.of("file system class missing",
            new IOException("no storage", new ClassNotFoundException("org.apache.hadoop.fs.s3a.S3AFileSystem")), false, false),
        Arguments.of("file system class not linkable", new IOException("no storage", new NoClassDefFoundError("S3AFileSystem")),
            false, false),
        Arguments.of("plain storage failure", new IOException("connection reset"), false, true),
        Arguments.of("storage failure inside an unchecked wrapper", new RuntimeException(new IOException("throttled")), false, true),
        Arguments.of("unchecked failure without storage cause", new IllegalStateException("bug"), false, false),
        Arguments.of("cause cycle", cycle(), false, true));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failures")
  void classifiesTheFailure(String name, Throwable failure, boolean content, boolean transientFailure) {
    assertEquals(content, SourceFailures.isContentFailure(failure), "content failure: " + name);
    assertEquals(transientFailure, BlobBytesSource.isTransient(failure), "transient: " + name);
  }

  private static AccessDeniedException denied(Throwable cause) {
    AccessDeniedException denied = new AccessDeniedException("s3a://bucket/a.pdf", null, "403 Forbidden");
    denied.initCause(cause);
    return denied;
  }

  /** The GCS connector's wording of a refused request: the request error, its JSON response as the cause. */

  /** A 403 whose JSON error names a rate-limit {@code reason}: a quota the next attempt may pass. */

  private static <T extends Throwable> T withCause(T failure, Throwable cause) {
    failure.initCause(cause);
    return failure;
  }

  /** Two storage failures, each the other's cause: the classifiers must stop. */
  private static IOException cycle() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);
    return first;
  }
}
