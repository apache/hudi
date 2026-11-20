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

package org.apache.hudi.utilities.unstructured.embedding.image;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SeekableBlobImageInputStream}: ImageIO's primitive reads complete across partial blob reads, a storage failure is a
 * {@link BlobReadException} that survives the reader's wrapping, a short stored file is the data's fault, and the header cap bounds
 * the bytes read wherever they lie.
 */
class TestSeekableBlobImageInputStream {

  @Test
  void primitiveReadsCompleteAcrossPartialBlobReads() throws IOException {
    byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(bytes, 2))) {
      assertEquals(0x00010203, in.readInt());
      assertEquals(0x0405060708090a0bL, in.readLong());
      assertEquals(12, in.getStreamPosition());
      assertEquals(0x0c0d0e0f, in.readInt());
      assertThrows(EOFException.class, in::readInt);
    }
  }

  @Test
  void seeksMoveTheBlobAndInvalidRangesReadNothing() throws IOException {
    byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7};
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(bytes, Integer.MAX_VALUE))) {
      assertEquals(8, in.length());
      in.seek(6);
      assertEquals(6, in.read());
      in.seek(2);
      assertEquals(2, in.read());
      byte[] buffer = new byte[4];
      assertThrows(IndexOutOfBoundsException.class, () -> in.read(buffer, 0, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> in.read(buffer, 3, 2));
      assertEquals(0, in.read(buffer, 4, 0));
      assertEquals(3, in.getStreamPosition());
    }
  }

  @Test
  void storageFailureOfAReadOrSeekIsABlobReadException() throws IOException {
    IOException reset = new IOException("connection reset by peer");
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[10], Integer.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw reset;
      }

      @Override
      public void seek(long pos) throws IOException {
        throw reset;
      }
    })) {
      BlobReadException read = assertThrows(BlobReadException.class, in::read);
      assertSame(reset, read.getCause());
      assertTrue(BlobReadException.isStorageFailure(new IOException("a reader's wrapper", read)), "found in the cause chain");
      assertThrows(BlobReadException.class, () -> in.seek(3));
    }
  }

  /** A bare EOFException of the storage client (Hadoop's report of a dropped connection) is a storage failure. */
  @Test
  void bareStorageEndOfFileIsAStorageFailure() throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[10], Integer.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new EOFException("End of File Exception between local host and the name node");
      }
    })) {
      assertThrows(BlobReadException.class, in::read);
    }
  }

  /** A stored file shorter than its recorded range repeats on every read: the data's fault, passed through. */
  @Test
  void truncatedBlobIsTheDataNotTheStorage() throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[10], Integer.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new BlobTruncatedException("stored file shorter than its recorded range");
      }
    })) {
      IOException failed = assertThrows(IOException.class, in::read);
      assertTrue(failed instanceof BlobTruncatedException, failed.toString());
      assertFalse(BlobReadException.isStorageFailure(failed));
    }
  }

  /**
   * The header cap counts every distinct byte read, wherever it lies, and fails the next read past it. A bulk read is cut at
   * the cap; a primitive read the cap would cut (it needs all its bytes in one call) fails with the cap's exception.
   */
  @Test
  void theByteCapBoundsTheBytesRead() throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[100], Integer.MAX_VALUE), 12)) {
      assertEquals(4, in.read(new byte[4], 0, 4));
      in.seek(0);
      assertEquals(4, in.read(new byte[4], 0, 4), "a byte read again is not counted again");
      in.seek(80);
      assertEquals(8, in.read(new byte[16], 0, 16), "the bulk read is cut at the cap");
      ImageHeaderException capped = assertThrows(ImageHeaderException.class, in::read);
      assertEquals("image header not found within 12 bytes read", capped.getMessage());
    }
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[100], Integer.MAX_VALUE), 6)) {
      assertEquals(4, in.read(new byte[4], 0, 4));
      in.seek(90);
      ImageHeaderException capped = assertThrows(ImageHeaderException.class, in::readInt, "a primitive read the cap would cut");
      assertEquals("image header not found within 6 bytes read", capped.getMessage());
    }
  }

  /**
   * Opening a stored input: a storage failure, checked or unchecked with an IOException under it, becomes a BlobReadException; a
   * content failure of the stored input and a programming error pass through.
   */
  @Test
  void openStoredInputClassifiesTheOpen() throws IOException {
    assertThrows(BlobReadException.class,
        () -> SeekableBlobImageInputStream.openStoredInput(() -> {
          throw new IOException("503 Slow Down");
        }));
    assertThrows(BlobReadException.class,
        () -> SeekableBlobImageInputStream.openStoredInput(() -> {
          throw new HoodieIOException("Exception creating input stream", new IOException("connection reset"));
        }));
    assertThrows(BlobReferenceException.class,
        () -> SeekableBlobImageInputStream.openStoredInput(() -> {
          throw new BlobReferenceException("OUT_OF_LINE blob has null reference");
        }));
    assertThrows(IllegalStateException.class,
        () -> SeekableBlobImageInputStream.openStoredInput(() -> {
          throw new IllegalStateException("a bug");
        }));
    try (SeekableBlobImageInputStream in = SeekableBlobImageInputStream.openStoredInput(() -> new BytesBlob(new byte[] {7}, 1))) {
      assertEquals(7, in.read());
    }
  }

  @Test
  void closingTheStreamClosesTheBlob() throws IOException {
    AtomicInteger closes = new AtomicInteger();
    SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new BytesBlob(new byte[4], 4) {
      @Override
      public void close() {
        closes.incrementAndGet();
      }
    });
    in.close();
    assertEquals(1, closes.get());
  }

  /** A blob over {@code bytes} whose reads return at most {@code maxPerRead} bytes, as a storage response boundary may. */
  private static class BytesBlob implements SeekableBlob {
    private final byte[] bytes;
    private final int maxPerRead;
    private int pos;

    BytesBlob(byte[] bytes, int maxPerRead) {
      this.bytes = bytes;
      this.maxPerRead = maxPerRead;
    }

    @Override
    public long length() {
      return bytes.length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      pos = (int) newPos;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (pos >= bytes.length) {
        return -1;
      }
      int n = Math.min(maxPerRead, Math.min(len, bytes.length - pos));
      System.arraycopy(bytes, pos, buffer, offset, n);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      // nothing to release: the bytes are held in memory
    }
  }
}
