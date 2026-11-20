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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * A seekable supplier over a file that records every blob and stream it hands out and whether each was closed: the spy the
 * handle release tests use to tell which reader of a video was left open.
 */
public final class TrackedBlobSupplier implements SeekableBlobBytesSupplier {

  private final Path file;
  private final long failAt;
  private final IntFunction<Throwable> closeFailure;
  private final List<TrackedBlob> blobs = new ArrayList<>();
  private final List<TrackedStream> streams = new ArrayList<>();

  /** Every read past {@code failAt} fails as storage does; the blob numbered n (from 1) throws {@code closeFailure(n)} on its first close, when not null. */
  public TrackedBlobSupplier(Path file, long failAt, IntFunction<Throwable> closeFailure) {
    this.file = file;
    this.failAt = failAt;
    this.closeFailure = closeFailure;
  }

  public TrackedBlobSupplier(Path file) {
    this(file, Long.MAX_VALUE, n -> null);
  }

  @Override
  public synchronized SeekableBlob openSeekable() throws IOException {
    TrackedBlob blob = new TrackedBlob(file, failAt, blobs.size() + 1, closeFailure.apply(blobs.size() + 1));
    blobs.add(blob);
    return blob;
  }

  @Override
  public synchronized InputStream open() throws IOException {
    TrackedStream stream = new TrackedStream(Files.newInputStream(file), streams.size() + 1);
    streams.add(stream);
    return stream;
  }

  /** How many blobs and streams were handed out. */
  public synchronized int opened() {
    return blobs.size() + streams.size();
  }

  /** The blobs and streams handed out and never closed, e.g. "[blob #1]"; empty when every one was closed. */
  public synchronized List<String> unclosed() {
    List<String> open = new ArrayList<>();
    for (TrackedBlob blob : blobs) {
      if (!blob.closed) {
        open.add("blob #" + blob.number);
      }
    }
    for (TrackedStream stream : streams) {
      if (!stream.closed) {
        open.add("stream #" + stream.number);
      }
    }
    return open;
  }

  private static final class TrackedBlob extends VideoClips.FileBlob {
    private final int number;
    private Throwable closeFailure;
    private boolean closed;

    private TrackedBlob(Path path, long failAt, int number, Throwable closeFailure) throws IOException {
      super(path, failAt);
      this.number = number;
      this.closeFailure = closeFailure;
    }

    @Override
    public void close() throws IOException {
      Throwable failure = closeFailure;
      closeFailure = null;
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      if (failure instanceof IOException) {
        throw (IOException) failure;
      }
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      super.close();
      closed = true;
    }
  }

  private static final class TrackedStream extends InputStream {
    private final InputStream in;
    private final int number;
    private boolean closed;

    private TrackedStream(InputStream in, int number) {
      this.in = in;
      this.number = number;
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      return in.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
      in.close();
      closed = true;
    }
  }
}
