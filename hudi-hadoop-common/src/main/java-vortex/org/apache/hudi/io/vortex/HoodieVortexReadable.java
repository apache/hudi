/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.vortex;

import dev.vortex.io.NativeReadable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link NativeReadable} that reads a file through its Hadoop {@link FileSystem}.
 *
 * <p>Vortex reads with positional reads from many threads at the same time. Each read borrows an
 * input stream from a pool, and gives it back after the read. Thus, concurrent reads do not share
 * a stream, and a stream is opened only when all open streams are in use.
 *
 * <p>This class uses only the Hadoop 2 stream API, and no Guava.
 */
public final class HoodieVortexReadable implements NativeReadable {

  // A read that is larger than this uses a new array, so that the pool keeps no large arrays.
  private static final int SCRATCH_RETAIN_LIMIT = 1 << 20;

  private final String name;
  private final Path path;
  private final FileSystem fs;
  private final long length;
  private final Queue<PooledStream> pool = new ConcurrentLinkedQueue<>();
  private volatile boolean closed;

  private HoodieVortexReadable(String name, Path path, FileSystem fs, long length) {
    this.name = name;
    this.path = path;
    this.fs = fs;
    this.length = length;
  }

  /**
   * Opens a file for reading.
   *
   * @param conf   Hadoop configuration for the file system of the path
   * @param path   path of the file
   * @param length length of the file in bytes, or a negative value if it is not known. If the
   *               length is known, no metadata request is necessary.
   */
  public static HoodieVortexReadable open(Configuration conf, String path, long length) {
    Path file = new Path(path);
    try {
      FileSystem fs = file.getFileSystem(conf);
      long resolvedLength = length >= 0 ? length : fs.getFileStatus(file).getLen();
      return new HoodieVortexReadable(path, file, fs, resolvedLength);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open Vortex file " + path, e);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public void readFully(long position, ByteBuffer buffer) throws IOException {
    if (closed) {
      throw new IOException("Cannot read " + name + ": the readable is closed");
    }

    int requested = buffer.remaining();
    if (position < 0 || position + requested > length) {
      throw new EOFException("Cannot read " + requested + " bytes at position " + position
          + ": " + name + " is " + length + " bytes long");
    }

    PooledStream pooled = pool.poll();
    if (pooled == null) {
      pooled = new PooledStream(fs.open(path));
    }

    try {
      pooled.readFully(position, buffer);
    } catch (IOException | RuntimeException e) {
      closeAfterFailure(pooled, e);
      throw e;
    }

    pool.add(pooled);
    if (closed) {
      // The readable closed during this read. Close the stream that the read gave back.
      closeAllPooled();
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    closeAllPooled();
  }

  private void closeAllPooled() throws IOException {
    List<PooledStream> drained = new ArrayList<>();
    PooledStream pooled;
    while ((pooled = pool.poll()) != null) {
      drained.add(pooled);
    }

    IOException failure = null;
    for (PooledStream stream : drained) {
      try {
        stream.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  private static void closeAfterFailure(PooledStream stream, Exception failure) {
    try {
      stream.close();
    } catch (IOException e) {
      failure.addSuppressed(e);
    }
  }

  /**
   * An input stream and its scratch array. Only one read at a time uses it.
   */
  private static final class PooledStream implements Closeable {
    private final FSDataInputStream stream;
    private byte[] scratch = new byte[0];

    private PooledStream(FSDataInputStream stream) {
      this.stream = stream;
    }

    private void readFully(long position, ByteBuffer buffer) throws IOException {
      int length = buffer.remaining();
      if (buffer.hasArray()) {
        stream.readFully(position, buffer.array(), buffer.arrayOffset() + buffer.position(), length);
        buffer.position(buffer.position() + length);
        return;
      }

      byte[] target = scratch(length);
      stream.readFully(position, target, 0, length);
      buffer.put(target, 0, length);
    }

    private byte[] scratch(int length) {
      if (scratch.length >= length) {
        return scratch;
      }
      if (length > SCRATCH_RETAIN_LIMIT) {
        return new byte[length];
      }
      scratch = new byte[length];
      return scratch;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }
}
