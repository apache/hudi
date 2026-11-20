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

import org.apache.pdfbox.io.RandomAccess;
import org.apache.pdfbox.io.RandomAccessReadView;
import org.apache.pdfbox.io.RandomAccessStreamCache;
import org.apache.pdfbox.io.RandomAccessStreamCache.StreamCacheCreateFunction;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The stream cache PDFBox keeps its scratch data in while it loads and extracts a PDF (the decrypted copy of every stream of
 * an encrypted file), with its local disk failures told apart from the file's own (a local
 * disk failure is a platform failure). PDFBox reports a full or failing scratch disk with the same {@link IOException} types as a parse
 * failure, which the chunker would record as
 * the file's {@code LOAD_ERROR} or {@code PAGE_ERROR}; on an executor out of disk every PDF would be quarantined.
 * <ul>
 *   <li>creating the cache fails only on the platform (no input is read yet): {@link PrepPlatformException}, unchecked, so
 *       PDFBox's document cannot swallow it and fall back to an unbounded heap cache, as it does for an {@link IOException};</li>
 *   <li>an {@link IOException} a cache buffer raises from beneath PDFBox (the JDK's file I/O: its top stack frame is outside
 *       PDFBox) is the local disk: {@link PrepPlatformException};</li>
 *   <li>an {@link IOException} PDFBox raises itself (its storage cap, a page past the cap or a closed buffer) passes through
 *       unchanged: the cap is reached by what the file asks PDFBox to keep, and stays the file's failure.</li>
 * </ul>
 * The first platform failure is kept ({@link #failure()}), so the caller can fail the round even where PDFBox catches the
 * exception on its way out.
 */
final class PdfBoxScratchCache implements StreamCacheCreateFunction {

  private static final String PDFBOX_PACKAGE = "org.apache.pdfbox.";
  private static final String WRITE_FAILED = "writing PDFBox's scratch failed";
  private static final String READ_FAILED = "reading PDFBox's scratch failed";

  private final StreamCacheCreateFunction delegate;
  private final AtomicReference<PrepPlatformException> failure = new AtomicReference<>();

  /** @param delegate PDFBox's own scratch cache, whose failures this one classifies */
  PdfBoxScratchCache(StreamCacheCreateFunction delegate) {
    this.delegate = delegate;
  }

  /** The first local disk failure met by the cache, or null. */
  PrepPlatformException failure() {
    return failure.get();
  }

  @Override
  public RandomAccessStreamCache create() {
    RandomAccessStreamCache cache;
    try {
      cache = delegate.create();
    } catch (IOException | RuntimeException e) {
      throw recordFailure("cannot create PDFBox's scratch cache", e);
    }
    return new ClassifyingCache(cache);
  }

  /** Owns {@code cache}: its buffers classify their failures, and closing it closes {@code cache}. */
  private final class ClassifyingCache implements RandomAccessStreamCache {
    private final RandomAccessStreamCache cache;

    ClassifyingCache(RandomAccessStreamCache cache) {
      this.cache = cache;
    }

    @Override
    public RandomAccess createBuffer() throws IOException {
      try {
        return new Buffer(cache.createBuffer());
      } catch (IOException e) {
        throw classify("creating a PDFBox scratch buffer failed", e);
      }
    }

    @Override
    public void close() throws IOException {
      cache.close();
    }
  }

  /**
   * Returns {@code e} for the caller to rethrow when PDFBox raised it itself; throws it as the recorded platform failure when it
   * came from beneath PDFBox. An exception with no stack trace cannot be told apart, and is the platform's; a JVM
   * run with stack traces switched off ({@code -XX:-StackTraceInThrowable}) would so turn PDFBox's own cap into a platform
   * failure, and is not supported.
   */
  private IOException classify(String message, IOException e) {
    StackTraceElement[] trace = e.getStackTrace();
    if (trace.length > 0 && trace[0].getClassName().startsWith(PDFBOX_PACKAGE)) {
      return e;
    }
    throw recordFailure(message, e);
  }

  private PrepPlatformException recordFailure(String message, Throwable cause) {
    PrepPlatformException platform = new PrepPlatformException("PDF chunker local scratch disk: " + message, cause);
    failure.compareAndSet(null, platform);
    return platform;
  }

  /** One cache buffer, every I/O failure classified. */
  private final class Buffer implements RandomAccess {
    private final RandomAccess access;

    Buffer(RandomAccess access) {
      this.access = access;
    }

    @Override
    public void write(int b) throws IOException {
      try {
        access.write(b);
      } catch (IOException e) {
        throw classify(WRITE_FAILED, e);
      }
    }

    @Override
    public void write(byte[] b) throws IOException {
      try {
        access.write(b);
      } catch (IOException e) {
        throw classify(WRITE_FAILED, e);
      }
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      try {
        access.write(b, offset, length);
      } catch (IOException e) {
        throw classify(WRITE_FAILED, e);
      }
    }

    @Override
    public void clear() throws IOException {
      try {
        access.clear();
      } catch (IOException e) {
        throw classify("clearing PDFBox's scratch failed", e);
      }
    }

    @Override
    public int read() throws IOException {
      try {
        return access.read();
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      try {
        return access.read(b, offset, length);
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public long getPosition() throws IOException {
      try {
        return access.getPosition();
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public void seek(long position) throws IOException {
      try {
        access.seek(position);
      } catch (IOException e) {
        throw classify("seeking in PDFBox's scratch failed", e);
      }
    }

    @Override
    public long length() throws IOException {
      try {
        return access.length();
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public boolean isClosed() {
      return access.isClosed();
    }

    @Override
    public boolean isEOF() throws IOException {
      try {
        return access.isEOF();
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public RandomAccessReadView createView(long startPosition, long streamLength) throws IOException {
      try {
        return access.createView(startPosition, streamLength);
      } catch (IOException e) {
        throw classify(READ_FAILED, e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        access.close();
      } catch (IOException e) {
        throw classify("closing PDFBox's scratch failed", e);
      }
    }
  }
}
