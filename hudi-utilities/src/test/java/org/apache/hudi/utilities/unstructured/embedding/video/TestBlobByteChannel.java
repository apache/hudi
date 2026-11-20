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

import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The read-only JCodec channel over a blob: positioned reads, clamped seeks, and storage failures told from the file's end. */
public class TestBlobByteChannel {

  private static final byte[] TEN = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  @TempDir
  Path dir;

  @Test
  public void readsAndSeeksOverTheBlob() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), TEN);
    try (BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, Long.MAX_VALUE))) {
      assertEquals(10, channel.size());
      assertEquals(0, channel.position());
      ByteBuffer heap = ByteBuffer.allocate(4);
      assertEquals(4, channel.read(heap));
      assertArrayEquals(new byte[] {0, 1, 2, 3}, heap.array());
      assertEquals(4, channel.position());

      assertSame(channel, channel.setPosition(6));
      // a direct buffer has no array: the channel copies through its transfer buffer
      ByteBuffer direct = ByteBuffer.allocateDirect(3);
      assertEquals(3, channel.read(direct));
      direct.flip();
      byte[] copied = new byte[3];
      direct.get(copied);
      assertArrayEquals(new byte[] {6, 7, 8}, copied);
      assertEquals(9, channel.position());

      // a read into an offset heap buffer lands at the buffer's position
      channel.setPosition(2);
      ByteBuffer offset = ByteBuffer.allocate(5);
      offset.position(3);
      assertEquals(2, channel.read(offset));
      assertArrayEquals(new byte[] {0, 0, 0, 2, 3}, offset.array());
      assertEquals(0, channel.read(ByteBuffer.allocate(0)), "a full buffer reads nothing");
    }
  }

  @Test
  public void positionPastEitherEndIsClampedToTheBlob() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), TEN);
    try (BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, Long.MAX_VALUE))) {
      // JCodec may position after the last box: the channel stays at the end and reads end-of-file
      channel.setPosition(25);
      assertEquals(10, channel.position());
      assertEquals(-1, channel.read(ByteBuffer.allocate(4)));
      channel.setPosition(-3);
      assertEquals(0, channel.position());
      ByteBuffer one = ByteBuffer.allocate(1);
      assertEquals(1, channel.read(one));
      assertEquals(0, one.get(0));
    }
  }

  @Test
  public void storageFailureIsABlobReadException() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), TEN);
    try (BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, 4))) {
      assertEquals(4, channel.read(ByteBuffer.allocate(4)));
      IOException failed = assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(4)));
      assertTrue(failed instanceof BlobReadException, failed.toString());
      assertTrue(BlobReadException.isStorageFailure(failed));
    }
    try (BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public long getPos() throws IOException {
        throw new IOException("connection reset");
      }

      @Override
      public void seek(long pos) throws IOException {
        throw new IOException("connection reset");
      }
    })) {
      assertThrows(BlobReadException.class, channel::position);
      assertThrows(BlobReadException.class, () -> channel.setPosition(3));
    }
  }

  @Test
  public void truncatedFileIsTheFilesOwnEndNotStorage() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), TEN);
    BlobTruncatedException truncated = new BlobTruncatedException("stored file shorter than its recorded range");
    try (BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw truncated;
      }
    })) {
      IOException failed = assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(4)));
      assertSame(truncated, failed, "a truncation passes as is");
      assertFalse(BlobReadException.isStorageFailure(failed));
    }
  }

  @Test
  public void isReadOnlyAndClosesTheBlobOnce() throws IOException {
    Path file = Files.write(dir.resolve("ten.bin"), TEN);
    AtomicInteger closes = new AtomicInteger();
    BlobByteChannel channel = new BlobByteChannel(new VideoClips.FileBlob(file, Long.MAX_VALUE) {
      @Override
      public void close() throws IOException {
        closes.incrementAndGet();
        super.close();
      }
    });
    assertThrows(NonWritableChannelException.class, () -> channel.write(ByteBuffer.allocate(1)));
    assertThrows(NonWritableChannelException.class, () -> channel.truncate(1));
    assertTrue(channel.isOpen());
    channel.close();
    channel.close();
    assertFalse(channel.isOpen());
    assertEquals(1, closes.get());
  }
}
