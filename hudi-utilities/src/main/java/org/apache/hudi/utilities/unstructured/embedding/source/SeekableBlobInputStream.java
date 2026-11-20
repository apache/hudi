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

import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} over a {@link SeekableBlob} whose {@link #mark}, {@link #reset} and {@link #skip} are seeks. JavaCV's
 * FFmpegFrameGrabber seeks a stream by {@code reset()} to the mark it sets at start, then {@code skip(offset)}; given a stream
 * without mark support it wraps it in a BufferedInputStream, which buffers everything read since the mark — up to the whole
 * video. This stream supports the mark without buffering anything, so the video is never held in memory.
 *
 * <p>A storage failure under a read (not an EOF, which a truncated stored file repeats on every attempt) is thrown as a
 * {@link BlobReadException} and remembered: JavaCV's callbacks swallow exceptions and FFmpeg only sees an I/O error, so the frame
 * source asks {@link #storageFailure()} to tell a transient read failure from corrupt video data. That holds for an unchecked
 * failure of the blob too, the prep framework's {@code SourceReadException} for a throttled, reset or denied read among them: it
 * is remembered as the cause of a {@link BlobReadException}, so the failure the frame source throws keeps it in its cause chain
 * and fails the round instead of reading as undecodable video.
 */
public final class SeekableBlobInputStream extends InputStream {

  private final SeekableBlob blob;
  private final byte[] single = new byte[1];
  private long mark;
  private IOException storageFailure;
  private boolean closed;

  public SeekableBlobInputStream(SeekableBlob blob) {
    this.blob = blob;
  }

  @Override
  public int read() throws IOException {
    int n = read(single, 0, 1);
    return n <= 0 ? -1 : single[0] & 0xff;
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    try {
      return blob.read(buffer, offset, len);
    } catch (IOException e) {
      throw classify(e);
    } catch (RuntimeException e) {
      throw classify(e);
    }
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  /** Remembers the position; the read limit is irrelevant, nothing is buffered. */
  @Override
  public synchronized void mark(int readLimit) {
    try {
      mark = blob.getPos();
    } catch (IOException e) {
      // StorageRangeBlob.getPos asks the stream, which may fail; the next read fails the same way
      storageFailure = storageFailure == null ? e : storageFailure;
      mark = 0;
    } catch (RuntimeException e) {
      classify(e);
      mark = 0;
    }
  }

  @Override
  public synchronized void reset() throws IOException {
    seek(mark);
  }

  /** Moves forward by up to {@code n} bytes, never past the end; a non-positive {@code n} moves nothing. */
  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    try {
      long pos = blob.getPos();
      long target = n >= blob.length() - pos ? blob.length() : pos + n;
      blob.seek(target);
      return target - pos;
    } catch (IOException e) {
      throw classify(e);
    } catch (RuntimeException e) {
      throw classify(e);
    }
  }

  @Override
  public int available() throws IOException {
    try {
      return (int) Math.min(Integer.MAX_VALUE, Math.max(0, blob.length() - blob.getPos()));
    } catch (IOException e) {
      throw classify(e);
    } catch (RuntimeException e) {
      throw classify(e);
    }
  }

  /**
   * Closes the blob once: a grabber that failed to start closes the stream it was given, and its owner closes the stream again.
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      blob.close();
      closed = true;
    }
  }

  /** The first storage failure under this stream, or null. */
  public IOException storageFailure() {
    return storageFailure;
  }

  private void seek(long pos) throws IOException {
    try {
      blob.seek(pos);
    } catch (IOException e) {
      throw classify(e);
    } catch (RuntimeException e) {
      throw classify(e);
    }
  }

  private IOException classify(IOException e) {
    if (e instanceof BlobTruncatedException || e instanceof BlobReadException) {
      if (e instanceof BlobReadException && storageFailure == null) {
        storageFailure = e;
      }
      return e;
    }
    BlobReadException failure = new BlobReadException(e);
    if (storageFailure == null) {
      storageFailure = failure;
    }
    return failure;
  }

  /** An unchecked failure of the blob: a storage failure (it cannot be told from one, so it never reads as video data). */
  private IOException classify(RuntimeException e) {
    BlobReadException failure = new BlobReadException(e);
    if (storageFailure == null) {
      storageFailure = failure;
    }
    return failure;
  }
}
