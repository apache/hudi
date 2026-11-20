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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The blocks under the FFmpeg frame source's one view of a video: the bytes of any read and seek sequence are the source's,
 * storage is read in whole blocks, a failure happens at the read that would fail unbuffered, and one clip is one open.
 */
class TestBlockBufferedBlob {

  @TempDir
  Path dir;

  /** Random seeks and reads of random sizes return what the file holds there, through 3 KiB blocks. */
  @Test
  void randomReadsReturnTheSourceBytes() throws IOException {
    byte[] content = new byte[20_000];
    new Random(7).nextBytes(content);
    Path file = Files.write(dir.resolve("random.bin"), content);
    Random random = new Random(11);
    try (BlockBufferedBlob blob = new BlockBufferedBlob(new VideoClips.FileBlob(file, Long.MAX_VALUE), 3 * 1024)) {
      assertEquals(content.length, blob.length());
      for (int i = 0; i < 500; i++) {
        long pos = random.nextInt(content.length + 1);
        blob.seek(pos);
        byte[] buffer = new byte[1 + random.nextInt(5000)];
        int n = blob.read(buffer, 0, buffer.length);
        if (pos == content.length) {
          assertEquals(-1, n);
          continue;
        }
        assertTrue(n > 0, "a read inside the blob returns bytes");
        assertArrayEquals(Arrays.copyOfRange(content, (int) pos, (int) pos + n), Arrays.copyOf(buffer, n), "at " + pos);
        assertEquals(pos + n, blob.getPos());
      }
      assertEquals(0, blob.failures());
    }
  }

  /** Sequential 4 KiB reads of a file go to storage as one read per block, never one per 4 KiB. */
  @Test
  void sequentialReadsReachStorageOncePerBlock() throws IOException {
    byte[] content = new byte[100_000];
    new Random(3).nextBytes(content);
    Path file = Files.write(dir.resolve("seq.bin"), content);
    CountingBlob source = new CountingBlob(new VideoClips.FileBlob(file, Long.MAX_VALUE));
    try (BlockBufferedBlob blob = new BlockBufferedBlob(source, 32 * 1024)) {
      byte[] buffer = new byte[4096];
      byte[] all = new byte[content.length];
      int total = 0;
      for (int n = blob.read(buffer, 0, buffer.length); n > 0; n = blob.read(buffer, 0, buffer.length)) {
        System.arraycopy(buffer, 0, all, total, n);
        total += n;
      }
      assertArrayEquals(content, all);
      // 100,000 bytes in 32 KiB blocks: 4 blocks, against 25 reads of 4 KiB unbuffered
      assertEquals(4, source.reads);
    }
  }

  /**
   * Storage that fails at or past a position fails the read that reaches it, as it would unbuffered: reads wholly before it
   * succeed although the block they lie in crosses it, and the first read past it throws and is counted.
   */
  @Test
  void failureHappensAtTheReadThatWouldFailUnbuffered() throws IOException {
    byte[] content = new byte[50_000];
    new Random(5).nextBytes(content);
    Path file = Files.write(dir.resolve("fail.bin"), content);
    try (BlockBufferedBlob blob = new BlockBufferedBlob(new VideoClips.FileBlob(file, 30_000), 16 * 1024)) {
      byte[] buffer = new byte[4096];
      blob.seek(20_000);
      // 20,000..24,096 is before the failure although its block (16,384..32,768) is not
      assertEquals(4096, blob.read(buffer, 0, 4096));
      assertArrayEquals(Arrays.copyOfRange(content, 20_000, 24_096), buffer);
      assertEquals(0, blob.failures());
      blob.seek(28_000);
      assertThrows(IOException.class, () -> blob.read(buffer, 0, 4096), "28,000..32,096 reaches the failure at 30,000");
      assertEquals(1, blob.failures());
      blob.seek(0);
      assertEquals(4096, blob.read(buffer, 0, 4096), "a block before the failure still reads");
      assertArrayEquals(Arrays.copyOfRange(content, 0, 4096), buffer);
    }
  }

  /** A stored file shorter than its length is the file's data: thrown as it is, never counted as a storage failure. */
  @Test
  void truncatedSourceIsNotAStorageFailure() throws IOException {
    SeekableBlob truncated = new SeekableBlob() {
      private long pos;

      @Override
      public long length() {
        return 10_000;
      }

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long position) {
        pos = position;
      }

      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new BlobTruncatedException("stored file shorter than its length");
      }

      @Override
      public void close() {
        // nothing to release: the blob holds no stream
      }
    };
    try (BlockBufferedBlob blob = new BlockBufferedBlob(truncated, 4096)) {
      assertThrows(BlobTruncatedException.class, () -> blob.read(new byte[100], 0, 100));
      assertEquals(0, blob.failures());
      assertThrows(EOFException.class, () -> blob.seek(10_001));
      assertEquals(1, blob.failures(), "a seek outside the blob fails as the source's own would");
    }
  }

  /** A source that returns no bytes ends the fill (no spin); the read is handed to the source as it would be unbuffered. */
  @Test
  void sourceReturningNothingEndsTheFill() throws IOException {
    int[] calls = {0};
    SeekableBlob empty = new FixedBlob(10_000) {
      @Override
      public int read(byte[] buffer, int offset, int len) {
        calls[0]++;
        return 0;
      }
    };
    try (BlockBufferedBlob blob = new BlockBufferedBlob(empty, 4096)) {
      assertEquals(0, blob.read(new byte[100], 0, 100));
      assertEquals(2, calls[0], "one fill attempt, then the read itself");
    }
  }

  /** An interrupt under a fill is thrown at once and counted, never retried unbuffered. */
  @Test
  void interruptUnderAFillIsThrownNotRetried() throws IOException {
    int[] calls = {0};
    SeekableBlob interrupted = new FixedBlob(10_000) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        calls[0]++;
        throw new java.io.InterruptedIOException("interrupted");
      }
    };
    try (BlockBufferedBlob blob = new BlockBufferedBlob(interrupted, 4096)) {
      assertThrows(java.io.InterruptedIOException.class, () -> blob.read(new byte[100], 0, 100));
      assertEquals(1, calls[0]);
      assertEquals(1, blob.failures());
    }
  }

  /** A blob of {@code length} bytes whose reads the test defines. */
  private abstract static class FixedBlob implements SeekableBlob {
    private final long length;
    private long pos;

    FixedBlob(long length) {
      this.length = length;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long position) {
      pos = position;
    }

    @Override
    public void close() {
      // nothing to release: the blob holds no stream
    }
  }

  /** The frame source opens a clip once (one grabber over one view), and its frames are the clip's kept keyframes. */
  @Test
  void clipIsOpenedOnce() throws IOException {
    Path clip = VideoClips.generate(dir.resolve("clip.mp4"), 32, 10, 2, 320, 240, 10, 20);
    int[] opens = {0};
    List<CountingBlob> views = new ArrayList<>();
    SeekableBlobBytesSupplier supplier = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        opens[0]++;
        CountingBlob view = new CountingBlob(new VideoClips.FileBlob(clip, Long.MAX_VALUE));
        views.add(view);
        return view;
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(clip);
      }
    };
    List<Long> timestamps = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> frames = new FfmpegKeyframeVideoFrameSource().frames(supplier,
        new FileMeta("fid-once", "mp4", Files.size(clip)), new TypedProperties())) {
      while (frames.hasNext()) {
        timestamps.add(frames.next().getTimeMs());
      }
    }
    assertEquals(Arrays.asList(0L, 2000L, 4000L, 6000L, 8000L, 10000L, 20000L, 22000L, 24000L, 26000L, 28000L, 30000L),
        timestamps);
    assertEquals(1, opens[0], "one view of the clip");
    // the clip is under one block: storage serves it in one read, the index and media data FFmpeg asks for from memory
    long length = Files.size(clip);
    assertTrue(length < BlockBufferedBlob.BLOCK_BYTES, "the fixture fits one block: " + length);
    assertEquals(1, views.get(0).reads, "one storage read for a clip of " + length + " bytes");
    assertTrue(views.get(0).closed, "the view is closed with the frames");
  }

  /** Counts the storage reads under a blob. */
  private static final class CountingBlob implements SeekableBlob {
    private final SeekableBlob blob;
    int reads;
    boolean closed;

    CountingBlob(SeekableBlob blob) {
      this.blob = blob;
    }

    @Override
    public long length() {
      return blob.length();
    }

    @Override
    public long getPos() throws IOException {
      return blob.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      blob.seek(pos);
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      reads++;
      return blob.read(buffer, offset, len);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      blob.close();
    }
  }
}
