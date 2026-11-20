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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import java.io.IOException;

/**
 * A storage failure reading a file's source that is not the file's own fault — a throttled, reset or timed-out read, an access
 * denial, anything but the source's confirmed absence, its truncation or a malformed reference ({@link SourceFailures}). Thrown by
 * a preprocessor's {@code open} or while its chunks are read, after the source's in-round open retries, it is an interruption
 * (transient, or the platform's): the prep framework propagates it (also from a cause or suppressed
 * chain, {@link PrepFailures#roundFailure}), failing the round, instead of committing the file's quarantine entry past a source a
 * later run can still read.
 */
public class SourceReadException extends HoodieIOException {

  private static final long serialVersionUID = 1L;

  /** @param cause the storage failure; an unchecked one (e.g. a HoodieIOException of the blob reader) is wrapped */
  public SourceReadException(String message, Throwable cause) {
    super(message, cause instanceof IOException ? (IOException) cause : new IOException(cause));
  }
}
