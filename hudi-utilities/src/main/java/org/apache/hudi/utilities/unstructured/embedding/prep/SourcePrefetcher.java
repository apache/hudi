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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads the next file of a prep task ahead, on one background thread, while the task prepares the current file: the download
 * of file n + 1 overlaps the preparation of file n.
 *
 * <p>Only a whole object is read ahead: an OUT_OF_LINE source row without a range, whose listed size is at most
 * {@code maxBytes}. Its bytes are then held in memory until the task takes them for that row ({@link #take}), so a task holds at
 * most two such buffers (the file it prepares and the one read ahead). A read ahead is only a cache: a row it skipped, a read
 * that failed transiently, or an object whose bytes do not end at its listed size is opened by the task itself exactly as
 * without a read ahead, meeting the same failure under the same classification. The task's row order and its output are
 * unchanged. A confirmed source-version change is reported for that file instead of discarded as a failed cache read.
 *
 * <p>Not thread-safe beyond the task thread calling {@link #start} and {@link #take}; {@link #close} may run on another
 * thread (a task-completion listener). Closing cancels the read in flight and stops the thread.
 */
final class SourcePrefetcher implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SourcePrefetcher.class);
  private static final long IDLE_THREAD_SECONDS = 30L;
  private static final AtomicInteger THREADS = new AtomicInteger();

  private final BlobBytesSource source;
  private final long maxBytes;
  private ThreadPoolExecutor executor;
  private Row pendingRow;
  private Future<CachedRead> pending;
  private boolean closed;

  /**
   * @param maxBytes the largest object read ahead; 0 reads nothing ahead
   */
  SourcePrefetcher(BlobBytesSource source, long maxBytes) {
    // one try: a read ahead that fails is not retried here, the task's own open retries it
    this.source = source.withoutOpenRetry();
    this.maxBytes = maxBytes;
  }

  boolean enabled() {
    return maxBytes > 0;
  }

  /**
   * Starts reading {@code blobRow}'s object ahead, when it is a whole OUT_OF_LINE object of at most the bound and
   * {@code limit}; any read ahead not taken is cancelled first.
   *
   * @param objectLength the object's listed size
   * @param limit the largest object worth reading ahead for this row's preprocessor (an image's header bound)
   */
  synchronized void start(Row blobRow, long objectLength, long limit) {
    cancelPending();
    if (closed || !enabled() || objectLength <= 0 || objectLength > Math.min(maxBytes, limit)) {
      return;
    }
    Row blob = wholeObjectBlob(blobRow);
    if (blob == null) {
      return;
    }
    pendingRow = blobRow;
    long modificationTime = blobRow.getAs(BlobColumns.SOURCE_MODIFICATION_TIME);
    pending = executor().submit(() -> read(blob, modificationTime, objectLength));
  }

  /**
   * The bytes read ahead for {@code blobRow}, waiting for the read to end; empty when it was not read ahead or the read failed,
   * and the task then opens it itself.
   */
  Option<CachedRead> take(Row blobRow) throws BlobSourceChangedException {
    Future<CachedRead> read;
    synchronized (this) {
      if (pending == null || pendingRow != blobRow) {
        cancelPending();
        return Option.empty();
      }
      // Keep the awaited future reachable by close(); its worker may not immediately observe interruption.
      read = pending;
    }
    try {
      return Option.ofNullable(read.get());
    } catch (InterruptedException e) {
      // Interruption ends the task even when its preprocessor does not open a source.
      Thread.currentThread().interrupt();
      read.cancel(true);
      throw new HoodieException("Interrupted while waiting for source read ahead", e);
    } catch (ExecutionException e) {
      SourceFailures.rethrowJvmFailure(e);
      if (e.getCause() instanceof Error) {
        // an exhausted heap or a broken link on the read-ahead thread is the task's: never hidden behind a fallback
        throw (Error) e.getCause();
      }
      rethrowSourceChange(e);
      LOG.debug("Reading ahead failed; the task opens the file itself", e);
      return Option.empty();
    } catch (CancellationException e) {
      SourceFailures.rethrowJvmFailure(e);
      LOG.debug("Reading ahead was cancelled; the task opens the file itself", e);
      return Option.empty();
    } finally {
      synchronized (this) {
        if (pending == read) {
          pending = null;
          pendingRow = null;
        }
      }
    }
  }

  /** A close-time status mismatch can be suppressed by the read failure; neither may hide a confirmed change. */
  private static void rethrowSourceChange(Throwable failure) throws BlobSourceChangedException {
    BlobSourceChangedException changed = PrepFailures.findFailure(failure,
        cause -> cause instanceof BlobSourceChangedException ? (BlobSourceChangedException) cause : null);
    if (changed != null) {
      throw changed;
    }
  }

  /**
   * The object's bytes, exactly its listed size of them, or an IOException that hands the file back to the task: one that ends
   * early or runs past its listed size is not this read ahead's to judge.
   */
  private CachedRead read(Row blob, long modificationTime, long objectLength) throws IOException {
    source.checkVersion(blob, modificationTime, objectLength);
    BlobBytesSource.VersionCheck version = source.checkedVersionCheck(blob);
    byte[] bytes = new byte[(int) objectLength];
    try (InputStream in = source.open(blob, objectLength)) {
      int filled = 0;
      while (filled < bytes.length) {
        int n = in.read(bytes, filled, bytes.length - filled);
        if (n < 0) {
          throw new IOException("object ended after " + filled + " of its listed " + objectLength + " bytes");
        }
        filled += n;
      }
      if (in.read() >= 0) {
        throw new IOException("object is longer than its listed " + objectLength + " bytes");
      }
    }
    return new CachedRead(bytes, version);
  }

  static final class CachedRead {
    private final byte[] bytes;
    private final BlobBytesSource.VersionCheck version;

    private CachedRead(byte[] bytes, BlobBytesSource.VersionCheck version) {
      this.bytes = bytes;
      this.version = version;
    }

    byte[] bytes() {
      return bytes;
    }

    private void checkVersion(BlobBytesSource consumer) throws IOException {
      version.check(consumer);
    }

    InputStream open(BlobBytesSource consumer) throws IOException {
      checkVersion(consumer);
      return new FilterInputStream(new ByteArrayInputStream(bytes)) {
        private boolean verified;

        private void verify() throws IOException {
          if (!verified) {
            checkVersion(consumer);
            verified = true;
          }
        }

        @Override
        public int read() throws IOException {
          int value = in.read();
          if (value < 0) {
            verify();
          } else {
            verified = false;
          }
          return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          int count = in.read(buffer, offset, length);
          if (count < 0) {
            verify();
          } else if (count > 0) {
            verified = false;
          }
          return count;
        }

        @Override
        public void close() throws IOException {
          try (InputStream closing = in) {
            verify();
          }
        }
      };
    }

    SeekableBlob openSeekable(BlobBytesSource consumer, Row blob) throws IOException {
      checkVersion(consumer);
      SeekableBlob delegate = consumer.openSeekable(blob, bytes);
      return new SeekableBlob() {
        private boolean verified;

        private void verify() throws IOException {
          if (!verified) {
            checkVersion(consumer);
            verified = true;
          }
        }

        @Override
        public long length() {
          return delegate.length();
        }

        @Override
        public long getPos() throws IOException {
          return delegate.getPos();
        }

        @Override
        public void seek(long position) throws IOException {
          delegate.seek(position);
          verified = false;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          int count = delegate.read(buffer, offset, length);
          if (count < 0) {
            verify();
          } else if (count > 0) {
            verified = false;
          }
          return count;
        }

        @Override
        public void close() throws IOException {
          try (SeekableBlob closing = delegate) {
            verify();
          }
        }
      };
    }
  }

  /** The blob of a row naming a whole OUT_OF_LINE object (no range), or null. */
  static Row wholeObjectBlob(Row blobRow) {
    int blobIndex = blobRow.fieldIndex(BlobColumns.BLOB);
    if (blobRow.isNullAt(blobIndex)) {
      return null;
    }
    Row blob = blobRow.getStruct(blobIndex);
    if (!HoodieSchema.Blob.OUT_OF_LINE.equals(blob.getAs(HoodieSchema.Blob.TYPE))) {
      return null;
    }
    Row reference = blob.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    if (reference == null || reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET) != null
        || reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH) != null) {
      return null;
    }
    return blob;
  }

  private void cancelPending() {
    if (pending != null) {
      pending.cancel(true);
      pending = null;
      pendingRow = null;
    }
  }

  private ThreadPoolExecutor executor() {
    if (executor == null) {
      ThreadFactory threads = runnable -> {
        Thread thread = new Thread(runnable, "embedding-prep-prefetch-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      };
      // one thread, ending when idle: an iterator its caller never closes leaves no thread behind
      executor = new ThreadPoolExecutor(1, 1, IDLE_THREAD_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), threads);
      executor.allowCoreThreadTimeOut(true);
    }
    return executor;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    cancelPending();
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
