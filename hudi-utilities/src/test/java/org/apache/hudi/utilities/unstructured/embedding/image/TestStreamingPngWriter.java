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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The streaming PNG writer the large-image tests generate their fixtures with. */
public class TestStreamingPngWriter {

  private static final byte[] SIGNATURE = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};

  /**
   * Stored mode, one stored block (37x23: 3,427 raw bytes), several blocks (300x200: 240,200), raw bytes exactly one and two
   * full blocks (64-pixel rows are 257 bytes, 255 rows = 65,535 = one block), each with IDAT chunks smaller than the stream.
   */
  @ParameterizedTest
  @CsvSource({"37,23,64", "300,200,4096", "64,255,1000", "64,510,65536"})
  public void storedPngIsWellFormedDecodesToTheGeneratedPixelsAndHasThePredictedSize(int width, int height, int idatChunkBytes)
      throws IOException, DataFormatException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long written = StreamingPngWriter.writeGradient(out, width, height, false, idatChunkBytes);
    byte[] png = out.toByteArray();
    assertEquals(png.length, written);
    assertEquals(StreamingPngWriter.storedSize(width, height, idatChunkBytes), written);
    assertWellFormed(png, width, height, idatChunkBytes);
    assertGradient(ImageFixtures.decode(png), width, height);
  }

  @Test
  public void compressedPngIsWellFormedDecodesToTheGeneratedPixelsAndIsSmallerThanTheRaster()
      throws IOException, DataFormatException {
    int width = 300;
    int height = 200;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long written = StreamingPngWriter.writeGradient(out, width, height, true, 1024);
    byte[] png = out.toByteArray();
    assertWellFormed(png, width, height, 1024);
    assertGradient(ImageFixtures.decode(png), width, height);
    assertTrue(written < (long) width * height * 4, "compressed " + written);
  }

  @Test
  public void invalidDimensionsAreRefused() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThrows(IllegalArgumentException.class, () -> StreamingPngWriter.writeGradient(out, 0, 10, false, 64));
    assertThrows(IllegalArgumentException.class, () -> StreamingPngWriter.writeGradient(out, 10, 0, false, 64));
    assertThrows(IllegalArgumentException.class, () -> StreamingPngWriter.writeGradient(out, 10, 10, false, 0));
    assertThrows(IllegalArgumentException.class, () -> StreamingPngWriter.storedSize(Integer.MAX_VALUE / 4 + 1, 1, 64));
    assertEquals(0, out.size(), "nothing written before the refusal");
  }

  private static void assertGradient(BufferedImage decoded, int width, int height) {
    assertEquals(width, decoded.getWidth());
    assertEquals(height, decoded.getHeight());
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        assertEquals(StreamingPngWriter.argb(x, y, width, height), decoded.getRGB(x, y), "pixel " + x + "," + y);
      }
    }
  }

  /**
   * Checks the encoding without an image decoder: signature, IHDR, every chunk's CRC-32 over type and data, IDAT chunks of
   * {@code idatChunkBytes} but the last, IEND last; the concatenated IDAT payload inflates (zlib, Adler-32 verified by the
   * inflater) to exactly height x (1 + 4 x width) bytes of filter-0 scanlines and nothing follows it.
   */
  private static void assertWellFormed(byte[] png, int width, int height, int idatChunkBytes) throws DataFormatException {
    assertArrayEquals(SIGNATURE, Arrays.copyOf(png, SIGNATURE.length));
    ByteBuffer buffer = ByteBuffer.wrap(png, SIGNATURE.length, png.length - SIGNATURE.length);
    ByteArrayOutputStream idat = new ByteArrayOutputStream();
    String last = null;
    int idatChunks = 0;
    int lastIdatLength = -1;
    while (buffer.hasRemaining()) {
      int length = buffer.getInt();
      byte[] typeAndData = new byte[4 + length];
      buffer.get(typeAndData);
      CRC32 crc = new CRC32();
      crc.update(typeAndData);
      assertEquals((int) crc.getValue(), buffer.getInt(), "CRC of a chunk");
      String type = new String(typeAndData, 0, 4, StandardCharsets.US_ASCII);
      if (last == null) {
        assertEquals("IHDR", type);
        ByteBuffer header = ByteBuffer.wrap(typeAndData, 4, length);
        assertEquals(width, header.getInt());
        assertEquals(height, header.getInt());
        assertEquals(8, header.get());
        assertEquals(6, header.get());
      }
      if ("IDAT".equals(type)) {
        if (lastIdatLength >= 0) {
          assertEquals(idatChunkBytes, lastIdatLength, "every IDAT chunk but the last is full");
        }
        idat.write(typeAndData, 4, length);
        lastIdatLength = length;
        idatChunks++;
      }
      last = type;
    }
    assertEquals("IEND", last);
    assertTrue(idatChunks >= 1);
    Inflater inflater = new Inflater();
    try {
      inflater.setInput(idat.toByteArray());
      byte[] raw = new byte[height * (1 + 4 * width) + 1];
      int inflated = 0;
      while (!inflater.finished() && inflated < raw.length) {
        int n = inflater.inflate(raw, inflated, raw.length - inflated);
        assertTrue(n > 0 || inflater.finished(), "the zlib stream ends early");
        inflated += n;
      }
      assertTrue(inflater.finished(), "zlib stream complete, Adler-32 verified");
      assertEquals(height * (1 + 4 * width), inflated);
      assertEquals(0, inflater.getRemaining(), "nothing after the zlib stream");
      for (int y = 0; y < height; y++) {
        assertEquals(0, raw[y * (1 + 4 * width)], "filter type of row " + y);
      }
    } finally {
      inflater.end();
    }
  }
}
