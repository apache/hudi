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

import org.apache.hudi.exception.ExceptionUtil;

import java.io.IOException;

/**
 * A failure of the storage under an image (a connection reset, a timeout), as opposed to a property of the image's bytes.
 * A file shorter than its recorded range is NOT one: that EOF repeats on every attempt and reads as the file's bad input, in data
 * prep and in compute's stored inputs alike (SeekableBlobImageInputStream.openStoredInput, forStoredInput).
 * ImageIO readers wrap stream failures in their own {@link javax.imageio.IIOException}; this type survives in the cause
 * chain so {@link #isStorageFailure} can tell a transient read failure from corrupt pixel data.
 */
public class BlobReadException extends IOException {

  private static final long serialVersionUID = 1L;

  public BlobReadException(IOException cause) {
    super(cause.getClass().getSimpleName() + ": " + cause.getMessage(), cause);
  }

  /**
   * A storage failure the source reported unchecked, e.g. the prep framework's {@code SourceReadException}; kept as the cause,
   * so a search of the cause chain still finds it.
   */
  public BlobReadException(RuntimeException cause) {
    super(cause.getClass().getSimpleName() + ": " + cause.getMessage(), cause);
  }

  /** Whether {@code failure} or any of its causes is a {@link BlobReadException}. */
  public static boolean isStorageFailure(Throwable failure) {
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (cause instanceof BlobReadException) {
        return true;
      }
    }
    return false;
  }
}
