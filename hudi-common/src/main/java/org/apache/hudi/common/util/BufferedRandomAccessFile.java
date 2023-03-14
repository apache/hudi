/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Use a private buffer for the read/write/seek operations of the RandomAccessFile
 * to reduce the number of disk operations.
 *
 *                S    C   L/E
 * Buffer:        |----+---|
 *  File:  +---------------D-------------------+
 *     S. startPosition - file position of start of the buffer.
 *     C. currentPosition - file position of current pointer in the buffer.
 *     L. validLastPosition - file position of the last byte in the buffer. (This is same as
 *        endPosition of the buffer. Except for the last file block).
 *                                              S-C--L--E
 *                                                    (A)
 *        Buffer:                               |-+--+--|
 *        File:  +-----------------------------------D
 *     E. endPosition() - end file position of the current buffer.
 *     D. DiskPosition - Position in the file pointed by underlying RandomAccessFile.
 *        When reading from the file, diskPosition aligns with startPosition.  When writing to
 *        the file, diskPosition(D) aligns with validLastPosition(L/E).
 *
 *     A. AvailableSpace - Space between validLastPosition(L) and EndPosition(E)
 *        is the available space in buffer, when writing/appending records
 *        into the buffer/file.
 *
 * Note: Based on BufferedRandomAccessFile implementation in Apache/Cassandra.
 *         - adopted from org.apache.cassandra.io
 *           Copyright: 2015-2019 The Apache Software Foundation
 *           Home page: http://cassandra.apache.org/
 *           License: http://www.apache.org/licenses/LICENSE-2.0
 */
public final class BufferedRandomAccessFile extends RandomAccessFile {
  private static final Logger LOG = LoggerFactory.getLogger(BufferedRandomAccessFile.class);
  static final int DEFAULT_BUFFER_SIZE = (1 << 16); // 64K buffer
  static final int BUFFER_BOUNDARY_MASK = -DEFAULT_BUFFER_SIZE;

  private int capacity;
  private ByteBuffer dataBuffer;
  private long startPosition = 0L;
  private long currentPosition = 0L;
  private long validLastPosition = 0L;
  private long diskPosition = 0L;
  private boolean isDirty = false;
  private boolean isClosed = false;
  private boolean isEOF = false;

  /**
   *
   * @param file  - file name
   * @param mode - "r" for read only; "rw" for read write
   * @throws IOException
   */
  public BufferedRandomAccessFile(File file, String mode) throws IOException {
    super(file, mode);
    this.init(DEFAULT_BUFFER_SIZE);
  }

  /**
   *
   * @param file  - file name
   * @param mode - "r" for read only; "rw" for read write
   * @param size - size/capacity of the buffer.
   * @throws IOException
   */
  public BufferedRandomAccessFile(File file, String mode, int size) throws IOException {
    super(file, mode);
    this.init(size);
  }

  /**
   *
   * @param name - name of the file
   * @param mode - "r" for read only; "rw" for read write
   * @throws IOException
   */
  public BufferedRandomAccessFile(String name, String mode) throws IOException {
    super(name, mode);
    this.init(DEFAULT_BUFFER_SIZE);
  }

  /**
   *
   * @param name - name of the file
   * @param mode - "r" for read only; "rw" for read write
   * @param size - size/capacity of the buffer
   * @throws FileNotFoundException
   */
  public BufferedRandomAccessFile(String name, String mode, int size) throws FileNotFoundException {
    super(name, mode);
    this.init(size);
  }

  /**
   *
   * @param size - capacity of the buffer
   */
  private void init(int size) {
    this.capacity = Math.max(DEFAULT_BUFFER_SIZE, size);
    this.dataBuffer = ByteBuffer.wrap(new byte[this.capacity]);
  }

  /**
   * Close the file, after flushing data in the buffer.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (!isClosed) {
      this.flush();
      super.close();
      this.isClosed = true;
    }
  }

  /**
   * If the file is writable, flush any bytes in the buffer that have not yet been written to disk.
   * @throws IOException
   */
  public void flush() throws IOException {
    this.flushBuffer();
  }

  /**
   * Flush any dirty bytes in the buffer to disk.
   * @throws IOException
   */
  private void flushBuffer() throws IOException {
    if (this.isDirty) {
      alignDiskPositionToBufferStartIfNeeded();
      int len = (int) (this.currentPosition - this.startPosition);
      super.write(this.dataBuffer.array(), 0, len);
      this.diskPosition = this.currentPosition;
      this.isDirty = false;
    }
  }

  /**
   * read ahead file contents to buffer.
   * @return number of bytes filled
   * @throws IOException
   */
  private int fillBuffer() throws IOException {
    int cnt = 0;
    int bytesToRead = this.capacity;
    // blocking read, until buffer is filled or EOF reached
    while (bytesToRead > 0) {
      int n = super.read(this.dataBuffer.array(), cnt, bytesToRead);
      if (n < 0) {
        break;
      }
      cnt += n;
      bytesToRead -= n;
    }
    this.isEOF = (cnt < this.dataBuffer.array().length);
    this.diskPosition += cnt;
    return cnt;
  }

  /**
   * If the diskPosition differs from the startPosition, flush the data in the buffer
   * and realign/fill the buffer at startPosition.
   * @throws IOException
   */
  private void alignDiskPositionToBufferStartIfNeeded() throws IOException {
    if (this.diskPosition != this.startPosition) {
      super.seek(this.startPosition);
      this.diskPosition = this.startPosition;
    }
  }

  /**
   * If the new seek position is in the buffer, adjust the currentPosition.
   * If the new seek position is outside of the buffer, flush the contents to
   * the file and reload the buffer corresponding to the position.
   *
   * We logically view the file as group blocks, where each block will perfectly
   * fit into the buffer (except for the last block).  Given a position to seek,
   * we identify the block to be loaded using BUFFER_BOUNDARY_MASK.
   *
   * When dealing with the last block, we will have extra space between validLastPosition
   * and endPosition of the buffer.
   *
   * @param pos  - position in the file to be loaded to the buffer.
   * @throws IOException
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos >= this.validLastPosition || pos < this.startPosition) {
      // seeking outside of current buffer -- flush and read
      this.flushBuffer();
      this.startPosition = pos & BUFFER_BOUNDARY_MASK; // start at BuffSz boundary
      alignDiskPositionToBufferStartIfNeeded();
      int n = this.fillBuffer();
      this.validLastPosition = this.startPosition + (long) n;
    } else {
      // seeking inside current buffer -- no read required
      if (pos < this.currentPosition) {
        // if seeking backwards, flush buffer.
        this.flushBuffer();
      }
    }
    this.currentPosition = pos;
  }

  /**
   * @return current file position
   */
  @Override
  public long getFilePointer() {
    return this.currentPosition;
  }

  /**
   * Returns the length of the file, depending on whether buffer has more data (to be flushed).
   * @return - length of the file (including data yet to be flushed to the file).
   * @throws IOException
   */
  @Override
  public long length() throws IOException {
    return Math.max(this.currentPosition, super.length());
  }

  /**
   * @return whether currentPosition has reached the end of valid buffer.
   */
  private boolean endOfBufferReached() {
    return this.currentPosition >= this.validLastPosition;
  }

  /**
   * Load a new data block.  Returns false, when EOF is reached.
   * @return - whether new data block was loaded or not
   * @throws IOException
   */
  private boolean loadNewBlockToBuffer() throws IOException {
    if (this.isEOF) {
      return false;
    }

    // read next block into buffer
    this.seek(this.currentPosition);

    // if currentPosition is at start, EOF has been reached
    return this.currentPosition != this.validLastPosition;
  }

  /**
   * @return - returns a byte as an integer.
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    if (endOfBufferReached()) {
      if (!loadNewBlockToBuffer()) {
        return -1;
      }
    }
    byte res = this.dataBuffer.array()[(int) (this.currentPosition - this.startPosition)];
    this.currentPosition++;
    return ((int) res) & 0xFF; // convert byte -> int
  }

  /**
   * @param b - byte array into which to read data.
   * @return  - returns number of bytes read.
   * @throws IOException
   */
  @Override
  public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  /**
   * Read specified number of bytes into given array starting at given offset.
   * @param b  - byte array
   * @param off - start offset
   * @param len  - length of bytes to be read
   * @return - number of bytes read.
   * @throws IOException
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (endOfBufferReached()) {
      if (!loadNewBlockToBuffer()) {
        return -1;
      }
    }

    // copy data from buffer
    len = Math.min(len, (int) (this.validLastPosition - this.currentPosition));
    int buffOff = (int) (this.currentPosition - this.startPosition);
    System.arraycopy(this.dataBuffer.array(), buffOff, b, off, len);
    this.currentPosition += len;
    return len;
  }

  /**
   * @return endPosition of the buffer.  For the last file block, this may not be a valid position.
   */
  private long endPosition() {
    return this.startPosition + this.capacity;
  }

  /**
   * @return - whether space is available at the end of the buffer.
   */
  private boolean spaceAvailableInBuffer() {
    return (this.isEOF && (this.validLastPosition < this.endPosition()));
  }

  /**
   * write a byte to the buffer/file.
   * @param v - value to be written
   * @throws IOException
   */
  @Override
  public void write(int v) throws IOException {
    byte [] b = new byte[1];
    b[0] = (byte) v;
    this.write(b, 0, b.length);
  }

  /**
   * write an array of bytes to the buffer/file.
   * @param b - byte array with data to be written
   * @throws IOException
   */
  @Override
  public void write(byte[] b) throws IOException {
    this.write(b, 0, b.length);
  }

  /**
   * Write specified number of bytes into buffer/file, with given starting offset and length.
   * @param b - byte array with data to be written
   * @param off - starting offset.
   * @param len - length of bytes to be written
   * @throws IOException
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // As all data may not fit into the buffer, more than one write would be required.
    while (len > 0) {
      int n = this.writeAtMost(b, off, len);
      off += n;
      len -= n;
      this.isDirty = true;
    }
  }

  /**
   * If space is available at the end of the buffer, start using it. Otherwise,
   * flush the unwritten data into the file and load the buffer corresponding to startPosition.
   * @throws IOException
   */
  private void expandBufferToCapacityIfNeeded() throws IOException {
    if (spaceAvailableInBuffer()) {
      // space available at end of buffer -- adjust validLastPosition
      this.validLastPosition = this.endPosition();
    } else {
      loadNewBlockToBuffer();
      // appending to EOF, adjust validLastPosition.
      if (this.currentPosition == this.validLastPosition) {
        this.validLastPosition = this.endPosition();
      }
    }
  }

  /**
   * Given a byte array, offset in the array and length of bytes to be written,
   * update the buffer/file.
   * @param b - byte array of data to be written
   * @param off - starting offset.
   * @param len - length of bytes to be written
   * @return - number of bytes written
   * @throws IOException
   */
  private int writeAtMost(byte[] b, int off, int len) throws IOException {
    if (endOfBufferReached()) {
      expandBufferToCapacityIfNeeded();
    }

    // copy data to buffer, until all data is copied or to buffer capacity.
    len = Math.min(len, (int) (this.validLastPosition - this.currentPosition));
    int buffOff = (int) (this.currentPosition - this.startPosition);
    System.arraycopy(b, off, this.dataBuffer.array(), buffOff, len);
    this.currentPosition += len;
    return len;
  }
}
