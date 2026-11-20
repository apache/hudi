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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The PDF chunker's local scratch files (the staged input), whose I/O failures are the executor's
 * local disk (full, unwritable, gone), never the file being chunked: each is a {@link PrepPlatformException} (a platform failure), which
 * fails the round instead of quarantining the file, or every file on that disk. The streams are unbuffered views whose
 * failures are unchecked, so a decoder that swallows or reclassifies {@link IOException}s cannot turn them into a page failure.
 */
final class ScratchFiles {

  private ScratchFiles() {
  }

  /** Writes {@code path}, created or truncated. */
  static OutputStream newOutputStream(Path path) {
    OutputStream out;
    try {
      out = Files.newOutputStream(path);
    } catch (IOException | RuntimeException e) {
      throw failure("cannot write the scratch file " + path, e);
    }
    return new OutputStream() {
      @Override
      public void write(int b) {
        try {
          out.write(b);
        } catch (IOException e) {
          throw failure("writing the scratch file " + path + " failed", e);
        }
      }

      @Override
      public void write(byte[] b, int off, int len) {
        try {
          out.write(b, off, len);
        } catch (IOException e) {
          throw failure("writing the scratch file " + path + " failed", e);
        }
      }

      @Override
      public void flush() {
        try {
          out.flush();
        } catch (IOException e) {
          throw failure("writing the scratch file " + path + " failed", e);
        }
      }

      @Override
      public void close() {
        try {
          out.close();
        } catch (IOException e) {
          throw failure("closing the scratch file " + path + " failed", e);
        }
      }
    };
  }

  /** A failure of the local scratch disk: the round fails. */
  static PrepPlatformException failure(String message, Throwable cause) {
    return new PrepPlatformException("PDF chunker local scratch disk: " + message, cause);
  }
}
