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

import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The InputStream JavaCV seeks through: mark/reset/skip are seeks, storage failures are classified and remembered. */
public class TestSeekableBlobInputStream {

  @TempDir
  Path dir;

  @Test
  public void markResetAndSkipAreSeeksOverTheBlob() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE))) {
      assertTrue(in.markSupported(), "a stream without mark support would be buffered whole by JavaCV");
      in.mark(Integer.MAX_VALUE);
      assertEquals(0, in.read());
      // JavaCV's SEEK_SET: reset to the mark, then skip the offset
      in.reset();
      assertEquals(7, in.skip(7));
      assertEquals(7, in.read());
      assertEquals(2, in.available());
      // skip(Long.MAX_VALUE) is how JavaCV finds the size: it stops at the end and reports what it moved
      assertEquals(2, in.skip(Long.MAX_VALUE));
      assertEquals(0, in.skip(5));
      assertEquals(-1, in.read());
      assertEquals(0, in.skip(-3), "a backward skip moves nothing");
      in.reset();
      byte[] buffer = new byte[4];
      assertEquals(4, in.read(buffer, 0, 4));
      assertEquals(3, buffer[3]);
      assertEquals(0, in.read(buffer, 0, 0));
      assertNull(in.storageFailure());
    }
  }

  @Test
  public void storageFailureIsABlobReadExceptionAndIsRemembered() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[10]);
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, 4))) {
      assertEquals(4, in.read(new byte[4], 0, 4));
      IOException failed = assertThrows(IOException.class, () -> in.read(new byte[4], 0, 4));
      assertTrue(failed instanceof BlobReadException, failed.toString());
      assertSame(failed, in.storageFailure(), "the first failure is kept for the frame source");
      assertThrows(BlobReadException.class, () -> in.read(new byte[4], 0, 4));
      assertSame(failed, in.storageFailure());
    }
  }

  @Test
  public void anEndOfFileIsDataNotStorage() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[10]);
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new BlobTruncatedException("stored file shorter than its recorded range");
      }
    })) {
      assertThrows(EOFException.class, () -> in.read());
      assertNull(in.storageFailure(), "a truncated file repeats on every attempt: not a storage failure");
    }
  }

  /** A bare EOFException of the storage client (Hadoop's report of a dropped connection) is a storage failure. */
  @Test
  public void bareStorageEndOfFileIsAStorageFailure() throws IOException {
    Path file = Files.write(dir.resolve("ten-bare.bin"), new byte[10]);
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new EOFException("End of File Exception between local host and the name node");
      }
    })) {
      assertThrows(BlobReadException.class, () -> in.read());
      assertTrue(in.storageFailure() != null, "remembered as the stream's storage failure");
    }
  }

  /**
   * The framework's blob view throws an unchecked SourceReadException for a throttled or reset read; JavaCV's read callback swallows
   * whatever the stream throws, so the stream must remember it as its storage failure, cause chain intact, for the round to fail.
   */
  @Test
  public void uncheckedStorageFailureIsRememberedWithItsCause() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[10]);
    SourceReadException read = new SourceReadException("Failed to read the blob source", new IOException("503 slow down"));
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) {
        throw read;
      }
    })) {
      IOException failed = assertThrows(IOException.class, () -> in.read(new byte[4], 0, 4));
      assertTrue(failed instanceof BlobReadException, failed.toString());
      assertNotNull(in.storageFailure(), "remembered for the frame source");
      assertSame(read, PrepFailures.roundFailure(in.storageFailure()), "the SourceReadException stays in the cause chain");
    }
  }

  @Test
  public void uncheckedStorageFailureOfASeekOrPositionIsRemembered() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[10]);
    SourceReadException read = new SourceReadException("Failed to read the blob source", new IOException("connection reset"));
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public long getPos() {
        throw read;
      }

      @Override
      public void seek(long pos) {
        throw read;
      }
    })) {
      in.mark(0);
      assertSame(read, PrepFailures.roundFailure(in.storageFailure()), "a failed mark is remembered");
      assertThrows(BlobReadException.class, in::reset);
      assertThrows(BlobReadException.class, () -> in.skip(3));
      assertThrows(BlobReadException.class, in::available);
      assertSame(read, PrepFailures.roundFailure(in.storageFailure()), "the first failure is kept");
    }
  }

  /**
   * Closing twice (a grabber that failed to start closes the stream it was given, then the frame source closes it again) closes the
   * blob once.
   */
  @Test
  public void closingTwiceClosesTheBlobOnce() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), new byte[10]);
    AtomicInteger closes = new AtomicInteger();
    SeekableBlobInputStream in = new SeekableBlobInputStream(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public void close() throws IOException {
        closes.incrementAndGet();
        super.close();
      }
    });
    in.close();
    in.close();
    assertEquals(1, closes.get());
  }
}
