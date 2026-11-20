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

import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;

/**
 * A {@link SeekableBlob} that reads its source in aligned blocks of {@link #BLOCK_BYTES} and keeps the last {@link #BLOCKS}
 * of them: FFmpeg reads a video through JavaCV's fixed 4 KiB I/O buffer, and moves back and forth between the index and the
 * media data, so each of its reads and seeks, otherwise one storage call, is served from memory while it stays inside a kept
 * block. A seek moves nothing in storage until a read needs a block that is not kept. At most {@code BLOCKS × min(BLOCK_BYTES,
 * length)} bytes are held.
 *
 * <p>Failures read exactly as they do without the blocks: when filling a block fails before any of it was read, the read that
 * asked for it is retried against the source for exactly the bytes it asked for, so it fails (or succeeds) where an unbuffered
 * read would; a fill that failed part way keeps the bytes it got. {@link #failures()} counts every failure the source threw.
 */
final class BlockBufferedBlob implements SeekableBlob {

  /** 1 MiB per block. */
  static final int BLOCK_BYTES = 1 << 20;
  /** Two blocks: the one with the index and the one with the media data FFmpeg is reading, for an index at either end. */
  static final int BLOCKS = 2;

  private final SeekableBlob source;
  private final long length;
  private final int blockBytes;
  private final byte[][] data = new byte[BLOCKS][];
  /** Per kept block: its first byte's position (-1: empty), and how many of its bytes are valid. */
  private final long[] starts = new long[BLOCKS];
  private final int[] valid = new int[BLOCKS];
  /** The block filled least recently: the next one replaced. */
  private int oldest;
  private long position;
  private long failures;

  BlockBufferedBlob(SeekableBlob source) {
    this(source, BLOCK_BYTES);
  }

  BlockBufferedBlob(SeekableBlob source, int blockBytes) {
    this.source = source;
    this.length = source.length();
    this.blockBytes = (int) Math.max(1, Math.min(blockBytes, length));
    Arrays.fill(starts, -1);
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0 || pos > length) {
      failures++;
      throw new EOFException("seek to " + pos + " outside the blob's " + length + " bytes");
    }
    position = pos;
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (position >= length) {
      return -1;
    }
    int block = kept(position);
    if (block < 0) {
      block = fill(position);
      if (block < 0) {
        // the fill failed before any byte: this read alone, unbuffered, as it would have been made
        return readDirect(buffer, offset, len);
      }
    }
    int from = (int) (position - starts[block]);
    int n = Math.min(len, valid[block] - from);
    System.arraycopy(data[block], from, buffer, offset, n);
    position += n;
    return n;
  }

  /**
   * The storage failures this blob has thrown (a {@link BlobTruncatedException} is not one), so a caller can tell whether one
   * happened since it last looked.
   */
  long failures() {
    return failures;
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  /** The kept block holding {@code pos} with a valid byte there, or -1. */
  private int kept(long pos) {
    for (int i = 0; i < BLOCKS; i++) {
      if (starts[i] >= 0 && pos >= starts[i] && pos < starts[i] + valid[i]) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Fills the aligned block holding {@code pos}: into the slot that holds part of it already (a fill that failed part way), else
   * into the oldest; the slot, or -1 when the fill read nothing at {@code pos}. An interrupt is thrown as it is (counted), never
   * retried.
   */
  private int fill(long pos) throws IOException {
    long start = pos - pos % blockBytes;
    int slot = slotFor(start);
    int size = (int) Math.min(blockBytes, length - start);
    byte[] block = blockOf(slot);
    starts[slot] = -1;
    valid[slot] = 0;
    int read = 0;
    try {
      if (source.getPos() != start) {
        source.seek(start);
      }
      while (read < size) {
        int n = source.read(block, read, size - read);
        if (n <= 0) {
          // the end, or a source that returned nothing: the caller's read goes to the source unbuffered
          break;
        }
        read += n;
      }
    } catch (InterruptedIOException e) {
      SourceFailures.rethrowJvmFailure(e);
      if (e instanceof SocketTimeoutException) {
        // a timeout, not an interrupt: retried unbuffered below like any other failure
        keep(slot, start, read);
        return start + read > pos ? slot : -1;
      }
      failures++;
      throw e;
    } catch (IOException | RuntimeException e) {
      SourceFailures.rethrowJvmFailure(e);
      if (Thread.currentThread().isInterrupted()) {
        failures++;
        throw e;
      }
      // not counted: the read that asked for this block is retried unbuffered, and fails, or not, as it would have
    }
    keep(slot, start, read);
    // nothing at pos was read (the fill failed, or the source ended, before it): the caller reads unbuffered
    return start + read > pos ? slot : -1;
  }

  /** The slot that holds part of the block at {@code start} already (a fill that failed part way), else the oldest. */
  private int slotFor(long start) {
    int slot = oldest;
    for (int i = 0; i < BLOCKS; i++) {
      if (starts[i] == start) {
        slot = i;
      }
    }
    return slot;
  }

  /** The buffer of {@code slot}, allocated at its first fill. */
  private byte[] blockOf(int slot) {
    if (data[slot] == null) {
      data[slot] = new byte[blockBytes];
    }
    return data[slot];
  }

  /** Records what a fill read; a block refilled in place keeps the other slot's age, so it does not evict the other block. */
  private void keep(int slot, long start, int read) {
    if (read > 0) {
      starts[slot] = start;
      valid[slot] = read;
      if (slot == oldest) {
        oldest = (slot + 1) % BLOCKS;
      }
    }
  }

  private int readDirect(byte[] buffer, int offset, int len) throws IOException {
    try {
      if (source.getPos() != position) {
        source.seek(position);
      }
      int n = source.read(buffer, offset, len);
      if (n > 0) {
        position += n;
      }
      return n;
    } catch (IOException | RuntimeException e) {
      if (!(e instanceof BlobTruncatedException)) {
        // a stored file shorter than its length is the file's data, never a storage failure
        failures++;
      }
      throw e;
    }
  }
}
