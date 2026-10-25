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

package org.apache.hudi.parquet.io;

import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An in-memory implementation of SeekableInputStream backed by a byte array.
 * Supports reading from a specific offset and length within the buffer.
 */
public class ByteArraySeekableInputStream extends SeekableInputStream {
  private final byte[] buffer;
  private final long offset;
  private final int length;
  private int position;

  /**
   * Creates a stream reading from the entire buffer.
   *
   * @param buffer The byte array containing the data.
   */
  public ByteArraySeekableInputStream(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  /**
   * Creates a stream reading from a specific range within the buffer.
   *
   * @param buffer The byte array containing the data.
   * @param offset The starting offset in the buffer (logical position 0 of the stream).
   * @param length The number of bytes available to read.
   */
  public ByteArraySeekableInputStream(byte[] buffer, long offset, int length) {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
    this.position = 0;
  }

  @Override
  public long getPos() {
    return offset + position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < offset || newPos > offset + length) {
      throw new IOException("Seek outside of buffer: " + newPos);
    }
    this.position = (int) (newPos - offset);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    if (position + len > length) {
      throw new EOFException();
    }
    System.arraycopy(buffer, position, bytes, start, len);
    position += len;
  }

  @Override
  public int read() {
    if (position >= length) {
      return -1;
    }
    return buffer[position++] & 0xFF;
  }

  @Override
  public void readFully(ByteBuffer byteBuffer) throws IOException {
    int len = byteBuffer.remaining();
    if (position + len > length) {
      throw new EOFException();
    }
    byteBuffer.put(buffer, position, len);
    position += len;
  }

  public int read(ByteBuffer byteBuffer) throws IOException {
    if (position >= length) {
      return -1;
    }
    int toRead = Math.min(byteBuffer.remaining(), length - position);
    byteBuffer.put(buffer, position, toRead);
    position += toRead;
    return toRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (position >= length) {
      return -1;
    }
    int toRead = Math.min(len, length - position);
    System.arraycopy(buffer, position, b, off, toRead);
    position += toRead;
    return toRead;
  }
}
