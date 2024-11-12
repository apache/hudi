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

package org.apache.hudi.common.util.io;

import org.apache.hudi.common.util.ValidationUtils;

import javax.annotation.Nonnull;

import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Instance of {@link InputStream} backed by {@link ByteBuffer}, implementing following
 * functionality (on top of what's required by {@link InputStream})
 *
 * <ol>
 *   <li>Seeking: enables random access by allowing to seek to an arbitrary position w/in the stream</li>
 *   <li>(Thread-safe) Copying: enables to copy from the underlying buffer not modifying the state of the stream</li>
 * </ol>
 *
 * NOTE: Generally methods of this class are NOT thread-safe, unless specified otherwise
 */
public class ByteBufferBackedInputStream extends InputStream {

  private final ByteBuffer buffer;
  private final int bufferOffset;

  public ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buffer = buf.duplicate();
    // We're marking current buffer position, so that we will be able
    // to reset it later on appropriately (to support seek operations)
    this.buffer.mark();
    this.bufferOffset = buffer.position();
  }

  public ByteBufferBackedInputStream(byte[] array) {
    this(array, 0, array.length);
  }

  public ByteBufferBackedInputStream(byte[] array, int offset, int length) {
    this(ByteBuffer.wrap(array, offset, length));
  }

  @Override
  public int read() throws EOFException {
    if (!buffer.hasRemaining()) {
      throw new EOFException("Reached end of buffer");
    }
    return buffer.get() & 0xFF;
  }

  @Override
  public int read(@Nonnull byte[] bytes, int offset, int length) throws EOFException {
    if (!buffer.hasRemaining()) {
      throw new EOFException("Reached end of buffer");
    }
    // Determine total number of bytes available to read
    int available = Math.min(length, buffer.remaining());
    // Copy bytes into the target buffer
    buffer.get(bytes, offset, available);
    return available;
  }

  /**
   * Returns current position of the stream
   */
  public int getPosition() {
    return buffer.position() - bufferOffset;
  }

  /**
   * Seeks to a position w/in the stream
   *
   * NOTE: Position is relative to the start of the stream (ie its absolute w/in this stream),
   * with following invariant being assumed:
   * <p>0 <= pos <= length (of the stream)</p>
   *
   * This method is NOT thread-safe
   *
   * @param pos target position to seek to w/in the holding buffer
   */
  public void seek(long pos) throws EOFException {
    ValidationUtils.checkArgument(pos >= 0, "Position must be greater than or equal zero.");

    buffer.reset(); // to mark
    int offset = buffer.position();
    // NOTE: That the new pos is still relative to buffer's offset
    int newPos = offset + (int) pos;
    if (newPos > buffer.limit()) {
      throw new EOFException(String.format("Reached end of buffer (offset: %d, length: %d)", pos, buffer.remaining()));
    }

    buffer.position(newPos);
  }

  /**
   * Copies at most {@code length} bytes starting from position {@code pos} into the target
   * buffer with provided {@code offset}. Returns number of bytes copied from the backing buffer
   *
   * NOTE: This does not change the current position of the stream and is thread-safe
   *
   * @param pos absolute position w/in stream to read from
   * @param targetBuffer target buffer to copy into
   * @param offset target buffer offset to copy at
   * @param length length of the sequence to copy
   * @return number of bytes copied
   */
  public int copyFrom(long pos, byte[] targetBuffer, int offset, int length) throws EOFException {
    ValidationUtils.checkArgument(length <= targetBuffer.length, "Length must not exceed the target buffer's length");

    int bufferPos = bufferOffset + (int) pos;
    if (bufferPos > buffer.limit()) {
      throw new EOFException(String.format("Reached end of buffer (offset: %d, length: %d)", pos, buffer.limit() - bufferOffset));
    }
    // Determine total number of bytes available to read
    int available = Math.min(length, buffer.limit() - bufferPos);
    // Get current buffer position in the backing array
    System.arraycopy(buffer.array(), bufferPos, targetBuffer, offset, available);
    return available;
  }
}
