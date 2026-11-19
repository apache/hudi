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

package org.apache.hudi.utilities.unstructured.memory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Images of the image memory cells that the shared image fixtures do not write, each streamed row by row to a file (one row in
 * memory, never the image), so a file larger than any test heap is generated in bounded memory. The pixels are seeded noise
 * ({@link #noise}), a deterministic function of the position and the seed, so the content does not compress and a regeneration
 * is byte-identical:
 * <ul>
 *   <li>{@link #writeStoredPng16}: a 16-bit RGBA PNG whose zlib stream is stored (uncompressed) blocks, so the file is about
 *   8 bytes per pixel: at 83.9 MP, under the default 100 MP pixel guards, it is larger than a 576 MiB heap;</li>
 *   <li>{@link #writeBmp24}: an uncompressed bottom-up 24-bit BMP (BI_RGB), which the JDK reader decodes row by row (no decoder
 *   intermediates);</li>
 *   <li>{@link #writeStripTiff}: an uncompressed RGB TIFF of short strips, so the strip the JDK reader decodes at full resolution
 *   before subsampling is small.</li>
 * </ul>
 */
public final class ImageCellFixtures {

  private static final byte[] PNG_SIGNATURE = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
  private static final int PNG_COLOR_TYPE_RGBA = 6;
  private static final int PNG_IDAT_CHUNK_BYTES = 1 << 20;
  private static final int BMP_HEADERS_BYTES = 14 + 40;
  private static final short TIFF_SHORT = 3;
  private static final short TIFF_LONG = 4;
  private static final int TIFF_ENTRIES = 10;
  private static final int WRITE_BUFFER_BYTES = 1 << 20;

  private ImageCellFixtures() {
  }

  /** The noise pixel at ({@code x}, {@code y}) of {@code seed}, as 24 bits of RGB. */
  static int noise(int x, int y, long seed) {
    long h = seed * 0x9E3779B97F4A7C15L + x * 0xC2B2AE3D27D4EB4FL + y * 0x165667B19E3779F9L;
    h ^= h >>> 33;
    h *= 0xFF51AFD7ED558CCDL;
    h ^= h >>> 33;
    return (int) h & 0xffffff;
  }

  /**
   * Writes a 16-bit RGBA PNG of noise, non-interlaced, filter None, its zlib stream in stored blocks, in IDAT chunks of 1 MiB.
   *
   * @return the file size
   */
  public static long writeStoredPng16(Path path, int width, int height, long seed) throws IOException {
    try (OutputStream file = new BufferedOutputStream(Files.newOutputStream(path), WRITE_BUFFER_BYTES)) {
      file.write(PNG_SIGNATURE);
      ByteBuffer header = ByteBuffer.allocate(13);
      header.putInt(width).putInt(height).put((byte) 16).put((byte) PNG_COLOR_TYPE_RGBA).put((byte) 0).put((byte) 0).put((byte) 0);
      writeChunk(file, "IHDR", header.array(), 13);
      Deflater deflater = new Deflater(Deflater.NO_COMPRESSION);
      try (OutputStream zlib = new DeflaterOutputStream(new IdatChunks(file), deflater, 1 << 16)) {
        byte[] row = new byte[1 + width * 8];
        for (int y = 0; y < height; y++) {
          for (int x = 0; x < width; x++) {
            int rgb = noise(x, y, seed);
            int at = 1 + x * 8;
            // 16-bit samples, big-endian: red, green, blue from the noise, alpha opaque
            row[at] = (byte) (rgb >>> 16);
            row[at + 1] = (byte) (rgb >>> 8);
            row[at + 2] = (byte) (rgb >>> 8);
            row[at + 3] = (byte) rgb;
            row[at + 4] = (byte) rgb;
            row[at + 5] = (byte) (rgb >>> 16);
            row[at + 6] = (byte) 0xff;
            row[at + 7] = (byte) 0xff;
          }
          zlib.write(row);
        }
      } finally {
        deflater.end();
      }
      writeChunk(file, "IEND", new byte[0], 0);
    }
    return Files.size(path);
  }

  /**
   * Writes an uncompressed 24-bit BMP of noise (BITMAPINFOHEADER, BI_RGB, bottom-up rows padded to 4 bytes).
   *
   * @return the file size
   */
  public static long writeBmp24(Path path, int width, int height, long seed) throws IOException {
    int rowBytes = (width * 3 + 3) & ~3;
    long bitmap = (long) rowBytes * height;
    try (OutputStream file = new BufferedOutputStream(Files.newOutputStream(path), WRITE_BUFFER_BYTES)) {
      ByteBuffer header = ByteBuffer.allocate(BMP_HEADERS_BYTES).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) 'B').put((byte) 'M').putInt((int) (BMP_HEADERS_BYTES + bitmap)).putInt(0).putInt(BMP_HEADERS_BYTES);
      header.putInt(40).putInt(width).putInt(height).putShort((short) 1).putShort((short) 24).putInt(0).putInt((int) bitmap)
          .putInt(2835).putInt(2835).putInt(0).putInt(0);
      file.write(header.array());
      byte[] row = new byte[rowBytes];
      for (int y = height - 1; y >= 0; y--) {
        for (int x = 0; x < width; x++) {
          int rgb = noise(x, y, seed);
          row[x * 3] = (byte) rgb;
          row[x * 3 + 1] = (byte) (rgb >>> 8);
          row[x * 3 + 2] = (byte) (rgb >>> 16);
        }
        file.write(row);
      }
    }
    return Files.size(path);
  }

  /**
   * Writes an uncompressed little-endian RGB TIFF of noise in strips of {@code rowsPerStrip} rows: the header, then the one IFD
   * and its arrays, then the strips (the IFD first, so a header read stops early).
   *
   * @return the file size
   */
  public static long writeStripTiff(Path path, int width, int height, int rowsPerStrip, long seed) throws IOException {
    int strips = (height + rowsPerStrip - 1) / rowsPerStrip;
    long rowBytes = (long) width * 3;
    int ifdOffset = 8;
    int ifdBytes = 2 + TIFF_ENTRIES * 12 + 4;
    int bitsOffset = ifdOffset + ifdBytes;
    int stripOffsetsOffset = bitsOffset + 6;
    int stripCountsOffset = stripOffsetsOffset + 4 * strips;
    long dataOffset = stripCountsOffset + 4L * strips;
    ByteBuffer head = ByteBuffer.allocate((int) dataOffset).order(ByteOrder.LITTLE_ENDIAN);
    head.put((byte) 'I').put((byte) 'I').putShort((short) 42).putInt(ifdOffset);
    head.putShort((short) TIFF_ENTRIES);
    entry(head, 256, TIFF_LONG, 1, width);
    entry(head, 257, TIFF_LONG, 1, height);
    entry(head, 258, TIFF_SHORT, 3, bitsOffset);
    entry(head, 259, TIFF_SHORT, 1, 1);
    entry(head, 262, TIFF_SHORT, 1, 2);
    entry(head, 273, TIFF_LONG, strips, stripOffsetsOffset);
    entry(head, 277, TIFF_SHORT, 1, 3);
    entry(head, 278, TIFF_LONG, 1, rowsPerStrip);
    entry(head, 279, TIFF_LONG, strips, stripCountsOffset);
    entry(head, 284, TIFF_SHORT, 1, 1);
    head.putInt(0);
    head.putShort((short) 8).putShort((short) 8).putShort((short) 8);
    for (int s = 0; s < strips; s++) {
      head.putInt((int) (dataOffset + s * rowsPerStrip * rowBytes));
    }
    for (int s = 0; s < strips; s++) {
      int rows = Math.min(rowsPerStrip, height - s * rowsPerStrip);
      head.putInt((int) (rows * rowBytes));
    }
    try (OutputStream file = new BufferedOutputStream(Files.newOutputStream(path), WRITE_BUFFER_BYTES)) {
      file.write(head.array());
      byte[] row = new byte[(int) rowBytes];
      for (int y = 0; y < height; y++) {
        for (int x = 0; x < width; x++) {
          int rgb = noise(x, y, seed);
          row[x * 3] = (byte) (rgb >>> 16);
          row[x * 3 + 1] = (byte) (rgb >>> 8);
          row[x * 3 + 2] = (byte) rgb;
        }
        file.write(row);
      }
    }
    return Files.size(path);
  }

  /** One IFD entry; a SHORT value that fits is left-justified in the value field, as TIFF requires. */
  private static void entry(ByteBuffer ifd, int tag, short type, int count, int value) {
    ifd.putShort((short) tag).putShort(type).putInt(count);
    if (type == TIFF_SHORT && count == 1) {
      ifd.putShort((short) value).putShort((short) 0);
    } else {
      ifd.putInt(value);
    }
  }

  private static void writeChunk(OutputStream out, String type, byte[] data, int length) throws IOException {
    DataOutputStream chunk = new DataOutputStream(out);
    byte[] typeBytes = type.getBytes(StandardCharsets.US_ASCII);
    chunk.writeInt(length);
    chunk.write(typeBytes);
    chunk.write(data, 0, length);
    CRC32 crc = new CRC32();
    crc.update(typeBytes);
    crc.update(data, 0, length);
    chunk.writeInt((int) crc.getValue());
    chunk.flush();
  }

  /** Cuts the zlib stream into IDAT chunks of {@link #PNG_IDAT_CHUNK_BYTES}; closing writes the last one, not the file. */
  private static final class IdatChunks extends OutputStream {
    private final OutputStream file;
    private final byte[] buffer = new byte[PNG_IDAT_CHUNK_BYTES];
    private int filled;

    IdatChunks(OutputStream file) {
      this.file = file;
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      while (length > 0) {
        int n = Math.min(length, buffer.length - filled);
        System.arraycopy(bytes, offset, buffer, filled, n);
        filled += n;
        offset += n;
        length -= n;
        if (filled == buffer.length) {
          flushChunk();
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (filled > 0) {
        flushChunk();
      }
    }

    private void flushChunk() throws IOException {
      writeChunk(file, "IDAT", buffer, filled);
      filled = 0;
    }
  }
}
