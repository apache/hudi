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

import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data prep's bounded header read ({@link SeekableBlobImageInputStream} with a byte bound): the bound counts the distinct bytes
 * the reader is served, so a header the reader parses twice does not fail a file smaller than the bound; the file's leading bytes
 * are fetched in one request, so a reader that probes, seeks back and re-reads sends one request; and every header, and every
 * refusal, is the one the unbounded stream (compute's, unchanged) reads.
 */
class TestImageHeaderPrefixRead {

  private static final long HEADER_MAX_BYTES = 1024L * 1024;

  /**
   * A JPEG under 1 MiB whose metadata segments before the frame header are ~576 KiB: the JDK reader reads them twice (its
   * tables-only probe, then the header), so it is served more than 1 MiB over fewer distinct bytes.
   */
  @Test
  void headerReadTwiceDoesNotFailAFileSmallerThanTheBound() throws IOException {
    byte[] jpeg = jpegWithMetadataSegments(9, 0xE2);
    assertTrue(jpeg.length < HEADER_MAX_BYTES, "the file is smaller than the bound: " + jpeg.length);
    CountingBlob blob = new CountingBlob(jpeg);
    ImageHeader header;
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(blob, HEADER_MAX_BYTES)) {
      header = ImageHeaderReader.read(in);
    }
    assertEquals("jpeg", header.getFormat());
    assertEquals(64, header.getWidth());
    assertEquals(48, header.getHeight());
    assertEquals(header.toString(), unboundedHeader(jpeg));
  }

  /** The bound still holds: a header that needs more distinct bytes than the bound fails as it always has. */
  @Test
  void headerNeedingMoreDistinctBytesThanTheBoundStillFails() {
    byte[] jpeg = jpegWithMetadataSegments(9, 0xE2);
    long bound = 300L * 1024;
    ImageHeaderException capped = assertThrows(ImageHeaderException.class, () -> {
      try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(jpeg), bound)) {
        ImageHeaderReader.read(in);
      }
    });
    assertTrue(capped.getMessage().contains("image header not found within " + bound + " bytes read"), capped.getMessage());
  }

  /**
   * One storage request per header for files no larger than the prefix: the leading bytes are fetched once and every probe,
   * seek back and re-read is served from memory (a TIFF's directory after its pixel data included).
   */
  @Test
  void headerWithinThePrefixIsOneRequest() throws IOException {
    for (Map.Entry<String, byte[]> image : corpus().entrySet()) {
      byte[] bytes = image.getValue();
      if (bytes.length == 0 || bytes.length > SeekableBlobImageInputStream.PREFIX_MAX_BYTES) {
        continue;
      }
      CountingBlob blob = new CountingBlob(bytes);
      try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(blob, HEADER_MAX_BYTES)) {
        readHeaderOrFailure(in);
      }
      assertEquals(1, blob.requests, image.getKey() + ": one request, not one per seek");
    }
  }

  /** A file larger than the prefix reads its leading bytes in one request, then only what the reader asks for past them. */
  @Test
  void headerPastThePrefixReadsOnlyWhatTheReaderAsks() throws IOException {
    // a TIFF whose directory lies after ~1.4 MiB of pixel data: the reader seeks there and reads a few hundred bytes
    byte[] tiff = ImageFixtures.singleStripTiff(800, 600);
    assertTrue(tiff.length > SeekableBlobImageInputStream.PREFIX_MAX_BYTES, "larger than the prefix: " + tiff.length);
    CountingBlob blob = new CountingBlob(tiff);
    String header;
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(blob, HEADER_MAX_BYTES)) {
      header = readHeaderOrFailure(in);
    }
    assertEquals(unboundedHeader(tiff), header);
    CountingBlob unbounded = new CountingBlob(tiff);
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(unbounded)) {
      readHeaderOrFailure(in);
    }
    assertTrue(blob.requests < unbounded.requests, "the prefix, then the directory: " + blob.requests + " requests, "
        + unbounded.requests + " without the prefix");
    assertTrue(blob.bytesRead < SeekableBlobImageInputStream.PREFIX_MAX_BYTES + 64 * 1024, "bytes read " + blob.bytesRead);
  }

  /** Every header and every refusal of the bounded stream is the unbounded stream's, over formats and malformed inputs. */
  @Test
  void boundedHeadersAndRefusalsMatchTheUnboundedStream() throws IOException {
    for (Map.Entry<String, byte[]> image : corpus().entrySet()) {
      String bounded;
      try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(image.getValue()), HEADER_MAX_BYTES)) {
        bounded = readHeaderOrFailure(in);
      }
      assertEquals(unboundedHeader(image.getValue()), bounded, image.getKey());
    }
  }

  /**
   * A storage failure past the header, while the prefix is fetched, changes nothing: no reader asked for those bytes. The same
   * failure where the reader does read surfaces as before.
   */
  @Test
  void failureWhileFetchingThePrefixPastTheHeaderIsNotTheReaders() throws IOException {
    byte[] png = ImageFixtures.image("png", 300, 200);
    assertTrue(png.length > 8192, "the failure lies past the header: " + png.length);
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new FailingAfterBlob(png, 4096), HEADER_MAX_BYTES)) {
      ImageHeader header = ImageHeaderReader.read(in);
      assertEquals(300, header.getWidth());
      assertEquals(200, header.getHeight());
    }
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new FailingAfterBlob(png, 16), HEADER_MAX_BYTES)) {
      ImageHeaderException failed = assertThrows(ImageHeaderException.class, () -> ImageHeaderReader.read(in));
      assertTrue(BlobReadException.isStorageFailure(failed), "the header's own bytes failing is still the storage's: " + failed);
    }
  }

  /** A bound smaller than the prefix bounds the prefix too. */
  @Test
  void theBoundBoundsThePrefix() throws IOException {
    CountingBlob blob = new CountingBlob(new byte[10_000]);
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(blob, 100)) {
      assertEquals(4, in.read(new byte[4], 0, 4));
    }
    assertEquals(100, blob.bytesRead);
  }

  /** The end of the blob, not the bound, ends a primitive read across it: an EOFException, as before. */
  @Test
  void primitiveReadPastTheEndIsTheEndNotTheBound() throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(new byte[10]), 12)) {
      in.readFully(new byte[9]);
      // 3 bytes of budget left, 1 byte of blob: the blob's end cuts the int, so the reader's EOF, not the bound's exception
      assertThrows(java.io.EOFException.class, in::readInt, "the blob ends inside the int; the bound is not reached");
    }
  }

  /** Re-reads are bounded too: a reader re-reading the same bytes without end stops at the bound's exception. */
  @Test
  void endlessReReadsStopAtTheBound() throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(new byte[100]), 10)) {
      ImageHeaderException capped = assertThrows(ImageHeaderException.class, () -> {
        while (true) {
          in.seek(0);
          in.readFully(new byte[10]);
        }
      });
      assertEquals("image header not found within 10 bytes read", capped.getMessage());
    }
  }

  /**
   * Past the ranges it tracks, a scattered reader is bounded by bytes served, as before distinct bytes were counted: never refused
   * earlier than that, and refused once more bytes are served than the bound.
   */
  @Test
  void scatteredReadsPastTheTrackedRangesAreBoundByBytesServed() throws IOException {
    int ranges = SeekableBlobImageInputStream.MAX_DISTINCT_RANGES + 100;
    byte[] bytes = new byte[2 * ranges + 4];
    long bound = ranges + 10L;
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(bytes), bound)) {
      for (int i = 0; i < ranges; i++) {
        in.seek(2L * i);
        in.read();
      }
      // more separate ranges than tracked, all within the bound in bytes served: accepted; a re-read now counts again
      for (int i = 0; i < 10; i++) {
        in.seek(0);
        in.read();
      }
      ImageHeaderException capped = assertThrows(ImageHeaderException.class, in::read, "one byte past the bound in bytes served");
      assertEquals("image header not found within " + bound + " bytes read", capped.getMessage());
    }
  }

  /** Past the tracked ranges, a bound near Long.MAX_VALUE does not overflow: reads go on as within any bound. */
  @Test
  void boundNearTheLongMaximumDoesNotOverflowPastTheTrackedRanges() throws IOException {
    int ranges = SeekableBlobImageInputStream.MAX_DISTINCT_RANGES + 2;
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(new byte[2 * ranges + 4]), Long.MAX_VALUE - 1)) {
      for (int i = 0; i < ranges; i++) {
        in.seek(2L * i);
        in.read();
      }
      in.seek(0);
      assertEquals(8, in.read(new byte[8], 0, 8), "a read past the tracked ranges is served whole");
    }
  }

  @Test
  void distinctRangesCountEachByteOnce() {
    SeekableBlobImageInputStream.DistinctRanges ranges = new SeekableBlobImageInputStream.DistinctRanges();
    ranges.add(0, 10);
    ranges.add(5, 15);
    assertEquals(15, ranges.total());
    ranges.add(20, 30);
    ranges.add(15, 20);
    assertEquals(30, ranges.total(), "adjacent ranges merge");
    ranges.add(0, 30);
    assertEquals(30, ranges.total(), "a re-read adds nothing");
    assertEquals(40, ranges.allowedEnd(0, 50, 10), "covered bytes are free, the gap uses the budget");
    assertEquals(35, ranges.allowedEnd(10, 50, 5));
    assertEquals(30, ranges.allowedEnd(30, 50, 0), "no budget: nothing past the covered bytes");
    ranges.add(40, 45);
    assertEquals(50, ranges.allowedEnd(0, 50, 15), "the gaps around a later range use the budget in order");
    assertEquals(47, ranges.allowedEnd(0, 50, 12));
  }

  /** The image corpus of the parity checks: every format the header reader knows, and malformed inputs. */
  private static Map<String, byte[]> corpus() {
    Map<String, byte[]> corpus = new LinkedHashMap<>();
    corpus.put("png", ImageFixtures.image("png", 97, 41));
    corpus.put("jpeg", ImageFixtures.image("jpeg", 120, 80));
    corpus.put("progressive jpeg", ImageFixtures.progressiveJpeg(64, 64));
    corpus.put("gif", ImageFixtures.image("gif", 33, 17));
    corpus.put("bmp", ImageFixtures.image("bmp", 40, 30));
    corpus.put("tiff, directory at the end", ImageFixtures.singleStripTiff(400, 300));
    corpus.put("tiled tiff", ImageFixtures.tiledTiff(256, 256, 64));
    corpus.put("alpha png", ImageFixtures.alphaPng(50, 50));
    corpus.put("webp header", ImageFixtures.webpHeader(64, 48));
    corpus.put("jpeg with large metadata", jpegWithMetadataSegments(9, 0xE2));
    corpus.put("jpeg with skipped metadata", jpegWithMetadataSegments(9, 0xE1));
    corpus.put("truncated png", ImageFixtures.truncatedPng());
    corpus.put("corrupt png header", ImageFixtures.headerCorrupt("png"));
    corpus.put("corrupt jpeg header", ImageFixtures.headerCorrupt("jpeg"));
    corpus.put("garbage", ImageFixtures.garbage());
    corpus.put("empty", new byte[0]);
    return corpus;
  }

  /** The header the unbounded stream reads (compute's path, whose code this change leaves as it was), or its refusal. */
  private static String unboundedHeader(byte[] bytes) throws IOException {
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(new CountingBlob(bytes))) {
      return readHeaderOrFailure(in);
    }
  }

  private static String readHeaderOrFailure(SeekableBlobImageInputStream in) {
    try {
      return ImageHeaderReader.read(in).toString();
    } catch (ImageHeaderException e) {
      return "refused: " + e.getMessage();
    }
  }

  /**
   * A small JPEG with {@code segments} 64 KiB metadata segments of marker {@code marker} before its frame header. The JDK
   * reader keeps an APP2 segment (an ICC profile), reading it, and skips an APP1 one by seeking.
   */
  static byte[] jpegWithMetadataSegments(int segments, int marker) {
    byte[] jpeg = ImageFixtures.image("jpeg", 64, 48);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(jpeg, 0, 2);
    Random random = new Random(1);
    int payload = 65533;
    for (int i = 0; i < segments; i++) {
      out.write(0xFF);
      out.write(marker);
      out.write((payload + 2) >> 8);
      out.write((payload + 2) & 0xFF);
      byte[] data = new byte[payload];
      random.nextBytes(data);
      for (int b = 0; b < data.length; b++) {
        if (data[b] == (byte) 0xFF) {
          data[b] = 0;
        }
      }
      out.write(data, 0, data.length);
    }
    out.write(jpeg, 2, jpeg.length - 2);
    return out.toByteArray();
  }

  /**
   * A blob over bytes that counts its storage requests the way a ranged object store serves them: each read that does not
   * continue the previous one (the first read, or one after a seek elsewhere) is a new request.
   */
  static class CountingBlob implements SeekableBlob {
    final byte[] bytes;
    int pos;
    int requests;
    long bytesRead;
    private int nextContiguous = -1;

    CountingBlob(byte[] bytes) {
      this.bytes = bytes;
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
    public void seek(long newPos) {
      pos = (int) newPos;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (pos >= bytes.length) {
        return -1;
      }
      if (pos != nextContiguous) {
        requests++;
      }
      int n = Math.min(len, bytes.length - pos);
      System.arraycopy(bytes, pos, buffer, offset, n);
      pos += n;
      bytesRead += n;
      nextContiguous = pos;
      return n;
    }

    @Override
    public void close() {
      // in-memory bytes: nothing to release
    }
  }

  /** A blob whose reads of any byte at or past {@code failAt} fail as a reset connection does. */
  private static final class FailingAfterBlob extends CountingBlob {
    private final int failAt;

    FailingAfterBlob(byte[] bytes, int failAt) {
      super(bytes);
      this.failAt = failAt;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (pos >= failAt) {
        throw new IOException("connection reset by peer");
      }
      return super.read(buffer, offset, (int) Math.min(len, failAt - pos));
    }
  }
}
