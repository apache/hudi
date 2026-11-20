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

import org.apache.pdfbox.io.RandomAccess;
import org.apache.pdfbox.io.RandomAccessReadView;
import org.apache.pdfbox.io.RandomAccessStreamCache;
import org.apache.pdfbox.io.RandomAccessStreamCache.StreamCacheCreateFunction;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PDFBox's own scratch cache whose writes fail, once {@link #arm() armed}, as a full local disk fails them: an
 * {@link IOException} "No space left on device". Nothing fills a real disk: the tests are functional only.
 */
final class FailingScratchCache implements PdfBoxPageSource.ScratchCacheFactory {

  static final String NO_SPACE = "No space left on device";

  private final AtomicBoolean armed = new AtomicBoolean();
  private final AtomicInteger failedWrites;

  FailingScratchCache() {
    this(new AtomicInteger());
  }

  /** @param failedWrites counts the scratch writes that failed */
  FailingScratchCache(AtomicInteger failedWrites) {
    this.failedWrites = failedWrites;
  }

  /** Fails every scratch write from now on. */
  FailingScratchCache arm() {
    armed.set(true);
    return this;
  }

  /** Scratch writes failed so far. */
  int failedWrites() {
    return failedWrites.get();
  }

  /** {@code source}, arming this cache once the file is loaded, so the scratch writes of page extraction fail. */
  PageSource armedAfterOpen(PageSource source) {
    return new PageSource() {
      @Override
      public void open() throws IOException {
        source.open();
        arm();
      }

      @Override
      public boolean hasMorePages() {
        return source.hasMorePages();
      }

      @Override
      public PageText next() throws IOException {
        return source.next();
      }

      @Override
      public long stagedBytes() {
        return source.stagedBytes();
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  @Override
  public StreamCacheCreateFunction create(File scratchDir, long maxBytes) {
    StreamCacheCreateFunction real = PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(scratchDir, maxBytes);
    return () -> {
      RandomAccessStreamCache cache = real.create();
      return new RandomAccessStreamCache() {
        @Override
        public RandomAccess createBuffer() throws IOException {
          return new FailingBuffer(cache.createBuffer());
        }

        @Override
        public void close() throws IOException {
          cache.close();
        }
      };
    };
  }

  private void writing() throws IOException {
    if (armed.get()) {
      failedWrites.incrementAndGet();
      throw new IOException(NO_SPACE);
    }
  }

  /** A scratch buffer whose writes fail while the cache is armed; reads pass through. */
  private final class FailingBuffer implements RandomAccess {
    private final RandomAccess buffer;

    FailingBuffer(RandomAccess buffer) {
      this.buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
      writing();
      buffer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      writing();
      buffer.write(b);
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      writing();
      buffer.write(b, offset, length);
    }

    @Override
    public void clear() throws IOException {
      buffer.clear();
    }

    @Override
    public int read() throws IOException {
      return buffer.read();
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      return buffer.read(b, offset, length);
    }

    @Override
    public long getPosition() throws IOException {
      return buffer.getPosition();
    }

    @Override
    public void seek(long position) throws IOException {
      buffer.seek(position);
    }

    @Override
    public long length() throws IOException {
      return buffer.length();
    }

    @Override
    public boolean isClosed() {
      return buffer.isClosed();
    }

    @Override
    public boolean isEOF() throws IOException {
      return buffer.isEOF();
    }

    @Override
    public RandomAccessReadView createView(long startPosition, long streamLength) throws IOException {
      return buffer.createView(startPosition, streamLength);
    }

    @Override
    public void close() throws IOException {
      buffer.close();
    }
  }
}
