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

import org.apache.hudi.common.util.ValidationUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;

/**
 * Writes an 8-bit RGBA PNG of any size row by row, holding one scanline, one IDAT chunk buffer and one 64 KiB stored-block
 * buffer (or, compressed, the deflater's state): a fixture far larger than the test JVM's heap is generated straight into
 * a storage stream. The pixels are a deterministic gradient
 * ({@link #argb}). Stored mode writes the zlib stream as uncompressed (stored) deflate blocks, so the file size is exactly
 * {@link #storedSize} and independent of the content; compressed mode uses the default deflater.
 */
public final class StreamingPngWriter {

  private static final byte[] SIGNATURE = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
  private static final int BYTES_PER_PIXEL = 4;
  private static final int COLOR_TYPE_RGBA = 6;
  /** The largest payload of one stored deflate block. */
  private static final int STORED_BLOCK_MAX = 65535;
  /** Per chunk: length, type and CRC. */
  private static final int CHUNK_OVERHEAD = 12;

  private StreamingPngWriter() {
  }

  /** The pixel at ({@code x}, {@code y}) as opaque ARGB: red follows x, green follows y, blue is (x + y) mod 256. */
  public static int argb(int x, int y, int width, int height) {
    int red = (int) ((long) x * 255 / Math.max(1, width - 1));
    int green = (int) ((long) y * 255 / Math.max(1, height - 1));
    int blue = (x + y) & 0xff;
    return 0xff000000 | (red << 16) | (green << 8) | blue;
  }

  /** The exact size in bytes of a stored-mode PNG of {@code width} x {@code height} with IDAT chunks of {@code idatChunkBytes}. */
  public static long storedSize(int width, int height, int idatChunkBytes) {
    checkDimensions(width, height, idatChunkBytes);
    long raw = (long) height * (1 + (long) width * BYTES_PER_PIXEL);
    long blocks = (raw + STORED_BLOCK_MAX - 1) / STORED_BLOCK_MAX;
    long zlib = 2 + raw + 5 * blocks + 4;
    long idatChunks = (zlib + idatChunkBytes - 1) / idatChunkBytes;
    return SIGNATURE.length + (CHUNK_OVERHEAD + 13) + zlib + idatChunks * CHUNK_OVERHEAD + CHUNK_OVERHEAD;
  }

  /** The pixels of an image, as opaque or translucent ARGB. */
  @FunctionalInterface
  public interface PixelSource {
    int argb(int x, int y);
  }

  /** Writes the {@link #argb} gradient PNG to {@code out} (not closed); see {@link #write}. */
  public static long writeGradient(OutputStream out, int width, int height, boolean compressed, int idatChunkBytes)
      throws IOException {
    return write(out, width, height, compressed, idatChunkBytes, (x, y) -> argb(x, y, width, height));
  }

  /**
   * Writes a PNG of {@code pixels} to {@code out} (not closed).
   *
   * @param compressed     false: stored deflate blocks (size {@link #storedSize}); true: the default deflater
   * @param idatChunkBytes the payload of each IDAT chunk but the last
   * @return the bytes written
   */
  public static long write(OutputStream out, int width, int height, boolean compressed, int idatChunkBytes,
                           PixelSource pixels) throws IOException {
    checkDimensions(width, height, idatChunkBytes);
    CountingOutputStream counted = new CountingOutputStream(out);
    counted.write(SIGNATURE);
    DataOutputStream header = new DataOutputStream(new ChunkBuffer(counted, "IHDR", 13));
    header.writeInt(width);
    header.writeInt(height);
    header.writeByte(8);
    header.writeByte(COLOR_TYPE_RGBA);
    header.writeByte(0);
    header.writeByte(0);
    header.writeByte(0);
    header.close();
    ChunkBuffer idat = new ChunkBuffer(counted, "IDAT", idatChunkBytes);
    OutputStream zlib = compressed ? new DeflaterOutputStream(idat, true) : new StoredZlibOutputStream(idat);
    byte[] row = new byte[1 + width * BYTES_PER_PIXEL];
    for (int y = 0; y < height; y++) {
      // filter type 0 (None) then RGBA
      for (int x = 0; x < width; x++) {
        int pixel = pixels.argb(x, y);
        int at = 1 + x * BYTES_PER_PIXEL;
        row[at] = (byte) (pixel >>> 16);
        row[at + 1] = (byte) (pixel >>> 8);
        row[at + 2] = (byte) pixel;
        row[at + 3] = (byte) (pixel >>> 24);
      }
      zlib.write(row);
    }
    zlib.close();
    new ChunkBuffer(counted, "IEND", 0).close();
    counted.flush();
    return counted.count;
  }

  private static void checkDimensions(int width, int height, int idatChunkBytes) {
    ValidationUtils.checkArgument(width >= 1 && width <= (Integer.MAX_VALUE - 1) / BYTES_PER_PIXEL,
        "width must be in [1, (2^31 - 2) / 4]: " + width);
    ValidationUtils.checkArgument(height >= 1, "height must be positive: " + height);
    ValidationUtils.checkArgument(idatChunkBytes >= 1, "IDAT chunk bytes must be positive: " + idatChunkBytes);
  }

  /** Buffers up to {@code capacity} bytes and writes each full buffer as one chunk of {@code type}; close writes the rest. */
  private static final class ChunkBuffer extends OutputStream {
    private final OutputStream out;
    private final byte[] type;
    private final byte[] buffer;
    private int size;
    private boolean anyWritten;

    private ChunkBuffer(OutputStream out, String type, int capacity) {
      this.out = out;
      this.type = type.getBytes(StandardCharsets.US_ASCII);
      this.buffer = new byte[capacity];
    }

    @Override
    public void write(int b) throws IOException {
      if (size == buffer.length) {
        emit();
      }
      buffer[size++] = (byte) b;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      int done = 0;
      while (done < length) {
        if (size == buffer.length) {
          emit();
        }
        int n = Math.min(length - done, buffer.length - size);
        System.arraycopy(bytes, offset + done, buffer, size, n);
        size += n;
        done += n;
      }
    }

    @Override
    public void close() throws IOException {
      if (size > 0 || !anyWritten) {
        emit();
      }
    }

    private void emit() throws IOException {
      DataOutputStream data = new DataOutputStream(out);
      data.writeInt(size);
      data.write(type);
      data.write(buffer, 0, size);
      CRC32 crc = new CRC32();
      crc.update(type);
      crc.update(buffer, 0, size);
      data.writeInt((int) crc.getValue());
      anyWritten = true;
      size = 0;
    }
  }

  /**
   * A zlib stream of stored (uncompressed) deflate blocks: header, blocks of at most {@link #STORED_BLOCK_MAX} bytes, the
   * last one marked final, then the Adler-32 of the data. A full block is written only when more data follows it.
   */
  private static final class StoredZlibOutputStream extends OutputStream {
    private final OutputStream out;
    private final byte[] block = new byte[STORED_BLOCK_MAX];
    private final Adler32 adler = new Adler32();
    private int size;

    private StoredZlibOutputStream(OutputStream out) throws IOException {
      this.out = out;
      // CMF 0x78 (deflate, 32K window), FLG 0x01 (fastest; header checksum (0x7801 % 31) == 0)
      out.write(0x78);
      out.write(0x01);
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      adler.update(bytes, offset, length);
      int done = 0;
      while (done < length) {
        if (size == block.length) {
          emit(false);
        }
        int n = Math.min(length - done, block.length - size);
        System.arraycopy(bytes, offset + done, block, size, n);
        size += n;
        done += n;
      }
    }

    @Override
    public void close() throws IOException {
      emit(true);
      long checksum = adler.getValue();
      out.write((int) (checksum >>> 24) & 0xff);
      out.write((int) (checksum >>> 16) & 0xff);
      out.write((int) (checksum >>> 8) & 0xff);
      out.write((int) checksum & 0xff);
      out.close();
    }

    private void emit(boolean last) throws IOException {
      out.write(last ? 1 : 0);
      out.write(size & 0xff);
      out.write((size >>> 8) & 0xff);
      out.write(~size & 0xff);
      out.write((~size >>> 8) & 0xff);
      out.write(block, 0, size);
      size = 0;
    }
  }

  private static final class CountingOutputStream extends OutputStream {
    private final OutputStream out;
    private long count;

    private CountingOutputStream(OutputStream out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      count++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      out.write(bytes, offset, length);
      count += length;
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }
  }
}
