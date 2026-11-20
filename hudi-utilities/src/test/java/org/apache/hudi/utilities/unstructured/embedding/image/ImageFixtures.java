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

import javax.imageio.ImageIO;
import javax.imageio.stream.MemoryCacheImageInputStream;

import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;

/**
 * Small images built in memory for the image tests: real encodings written by the JDK's own ImageIO writers, a hand-built
 * WebP header (the JDK has no WebP writer) and corrupt variants.
 */
public final class ImageFixtures {

  private ImageFixtures() {
  }

  /** A {@code width}x{@code height} RGB gradient encoded as {@code format} (png, jpeg, gif, bmp, tiff). */
  public static byte[] image(String format, int width, int height) {
    return encode(gradient(width, height, BufferedImage.TYPE_INT_RGB), format);
  }

  /** A gradient with a varying alpha channel, encoded as PNG. */
  public static byte[] alphaPng(int width, int height) {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int alpha = (x * 255) / Math.max(1, width - 1);
        image.setRGB(x, y, (alpha << 24) | ((y * 255 / Math.max(1, height - 1)) << 8) | 0x40);
      }
    }
    return encode(image, "png");
  }

  /** An 8-bit grayscale PNG. */
  public static byte[] grayPng(int width, int height) {
    return encode(gradient(width, height, BufferedImage.TYPE_BYTE_GRAY), "png");
  }

  /** A 4-colour palette PNG. */
  public static byte[] palettePng(int width, int height) {
    byte[] r = {0, (byte) 255, 0, 0};
    byte[] g = {0, 0, (byte) 255, 0};
    byte[] b = {0, 0, 0, (byte) 255};
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_INDEXED, new IndexColorModel(2, 4, r, g, b));
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        image.getRaster().setSample(x, y, 0, (x + y) % 4);
      }
    }
    return encode(image, "png");
  }

  /**
   * A minimal WebP header (RIFF container, extended {@code VP8X} chunk) declaring a {@code width}x{@code height} canvas;
   * no pixel data follows, which is enough for a header-only reader.
   */
  public static byte[] webpHeader(int width, int height) {
    byte[] bytes = new byte[30];
    put(bytes, 0, "RIFF");
    putLe32(bytes, 4, bytes.length - 8);
    put(bytes, 8, "WEBP");
    put(bytes, 12, "VP8X");
    putLe32(bytes, 16, 10);
    // bytes 20..23: flags + reserved; 24..26 canvas width - 1; 27..29 canvas height - 1 (24-bit little endian)
    putLe24(bytes, 24, width - 1);
    putLe24(bytes, 27, height - 1);
    return bytes;
  }

  /**
   * An uncompressed RGB TIFF with ONE strip holding every row, the IFD written AFTER the pixel data (as libtiff writers place
   * it), built by hand so the layout is exact: 8-byte header, strip, IFD, BitsPerSample array.
   */
  public static byte[] singleStripTiff(int width, int height) {
    int stripBytes = width * height * 3;
    int[][] tags = {{256, 4, width}, {257, 4, height}, {258, 3, -1}, {259, 3, 1}, {262, 3, 2}, {273, 4, 8}, {277, 3, 3},
        {278, 4, height}, {279, 4, stripBytes}};
    int ifdOffset = 8 + stripBytes;
    int ifdSize = 2 + tags.length * 12 + 4;
    java.nio.ByteBuffer out = java.nio.ByteBuffer.allocate(ifdOffset + ifdSize + 6).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    out.put((byte) 'I').put((byte) 'I').putShort((short) 42).putInt(ifdOffset);
    BufferedImage image = gradient(width, height, BufferedImage.TYPE_INT_RGB);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int rgb = image.getRGB(x, y);
        out.put((byte) (rgb >> 16)).put((byte) (rgb >> 8)).put((byte) rgb);
      }
    }
    out.putShort((short) tags.length);
    for (int[] tag : tags) {
      out.putShort((short) tag[0]).putShort((short) tag[1]);
      if (tag[0] == 258) {
        out.putInt(3).putInt(ifdOffset + ifdSize);
      } else if (tag[1] == 3) {
        out.putInt(1).putShort((short) tag[2]).putShort((short) 0);
      } else {
        out.putInt(1).putInt(tag[2]);
      }
    }
    out.putInt(0);
    out.putShort((short) 8).putShort((short) 8).putShort((short) 8);
    return out.array();
  }

  /** A TIFF written by the JDK writer in {@code tile}x{@code tile} tiles. */
  public static byte[] tiledTiff(int width, int height, int tile) {
    return write(gradient(width, height, BufferedImage.TYPE_INT_RGB), "tiff", param -> {
      param.setTilingMode(javax.imageio.ImageWriteParam.MODE_EXPLICIT);
      param.setTiling(tile, tile, 0, 0);
    });
  }

  /** A progressive (SOF2) JPEG written by the JDK writer, 4:2:0. */
  public static byte[] progressiveJpeg(int width, int height) {
    return write(gradient(width, height, BufferedImage.TYPE_INT_RGB), "jpeg",
        param -> param.setProgressiveMode(javax.imageio.ImageWriteParam.MODE_DEFAULT));
  }

  /** A gradient TIFF whose strips are compressed with {@code compression} (a JDK TIFF writer type, e.g. "Deflate", "LZW"). */
  public static byte[] compressedTiff(int width, int height, String compression) {
    return write(gradient(width, height, BufferedImage.TYPE_INT_RGB), "tiff", param -> {
      param.setCompressionMode(javax.imageio.ImageWriteParam.MODE_EXPLICIT);
      param.setCompressionType(compression);
    });
  }

  private static byte[] write(BufferedImage image, String format,
                              java.util.function.Consumer<javax.imageio.ImageWriteParam> configure) {
    javax.imageio.ImageWriter writer = ImageIO.getImageWritersByFormatName(format).next();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (javax.imageio.stream.ImageOutputStream out = ImageIO.createImageOutputStream(bytes)) {
      writer.setOutput(out);
      javax.imageio.ImageWriteParam param = writer.getDefaultWriteParam();
      configure.accept(param);
      writer.write(null, new javax.imageio.IIOImage(image, null, null), param);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      writer.dispose();
    }
    return bytes.toByteArray();
  }

  /**
   * A valid non-interlaced PNG of {@code width} x {@code height} whose every sample is 0, written row by row in small pieces so a
   * very wide row is never held whole: compressed, a row of zeros costs about a thousandth of its size. A palette image
   * (colorType 3) gets a two-entry PLTE.
   *
   * @param colorType the IHDR color type (0 gray, 2 RGB, 3 palette, 4 gray + alpha, 6 RGBA)
   */
  public static byte[] zeroPng(int width, int height, int bitDepth, int colorType) {
    int samples = colorType == 2 ? 3 : colorType == 4 ? 2 : colorType == 6 ? 4 : 1;
    long rowBytes = ((long) width * samples * bitDepth + 7) / 8;
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      out.write(new byte[] {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'});
      ByteArrayOutputStream ihdr = new ByteArrayOutputStream();
      DataOutputStream header = new DataOutputStream(ihdr);
      header.writeInt(width);
      header.writeInt(height);
      header.writeByte(bitDepth);
      header.writeByte(colorType);
      header.writeByte(0);
      header.writeByte(0);
      header.writeByte(0);
      pngChunk(out, "IHDR", ihdr.toByteArray());
      if (colorType == 3) {
        pngChunk(out, "PLTE", new byte[] {0, 0, 0, (byte) 255, (byte) 255, (byte) 255});
      }
      ByteArrayOutputStream zlib = new ByteArrayOutputStream();
      try (DeflaterOutputStream deflater = new DeflaterOutputStream(zlib)) {
        byte[] piece = new byte[65536];
        for (int y = 0; y < height; y++) {
          // filter type 0 (None), then the row's zero samples
          deflater.write(0);
          for (long left = rowBytes; left > 0; left -= piece.length) {
            deflater.write(piece, 0, (int) Math.min(piece.length, left));
          }
        }
      }
      pngChunk(out, "IDAT", zlib.toByteArray());
      pngChunk(out, "IEND", new byte[0]);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void pngChunk(DataOutputStream out, String type, byte[] data) throws IOException {
    byte[] name = type.getBytes(StandardCharsets.US_ASCII);
    CRC32 crc = new CRC32();
    crc.update(name);
    crc.update(data);
    out.writeInt(data.length);
    out.write(name);
    out.write(data);
    out.writeInt((int) crc.getValue());
  }

  /**
   * An uncompressed (BI_RGB) BMP of {@code width} x {@code height} at {@code bitsPerPixel} (1, 4, 8, 16, 24 or 32) with a
   * BITMAPINFOHEADER, every pixel 0; an indexed image gets a palette of 2^bitsPerPixel black entries.
   */
  public static byte[] uncompressedBmp(int width, int height, int bitsPerPixel) {
    int rowBytes = (int) (((long) width * bitsPerPixel + 31) / 32 * 4);
    int palette = bitsPerPixel <= 8 ? 4 << bitsPerPixel : 0;
    int offset = 14 + 40 + palette;
    byte[] bmp = new byte[offset + rowBytes * height];
    put(bmp, 0, "BM");
    putLe32(bmp, 2, bmp.length);
    putLe32(bmp, 10, offset);
    putLe32(bmp, 14, 40);
    putLe32(bmp, 18, width);
    putLe32(bmp, 22, height);
    bmp[26] = 1;
    bmp[28] = (byte) bitsPerPixel;
    putLe32(bmp, 34, rowBytes * height);
    return bmp;
  }

  /** A PNG cut inside its IHDR chunk: the signature is valid, the header is not. */
  public static byte[] truncatedPng() {
    return Arrays.copyOf(image("png", 40, 30), 14);
  }

  /**
   * A 40x30 image of {@code format} (png, jpeg, gif, bmp or tiff) cut inside its header: the signature is valid, so the format's
   * reader takes it, but the header it needs for the dimensions is incomplete (png: inside IHDR; jpeg: after APP0, before any
   * frame header; gif: inside the logical screen descriptor; bmp: before the info header; tiff: inside the first directory).
   */
  public static byte[] headerCorrupt(String format) {
    byte[] whole = image(format, 40, 30);
    switch (format) {
      case "png":
        return Arrays.copyOf(whole, 14);
      case "jpeg":
        return Arrays.copyOf(whole, 20);
      case "gif":
        return Arrays.copyOf(whole, 8);
      case "bmp":
        return Arrays.copyOf(whole, 10);
      case "tiff":
        return Arrays.copyOf(whole, 10);
      default:
        throw new IllegalArgumentException(format);
    }
  }

  /**
   * A {@code format} file (gif or tiff) holding two images of {@code width} x {@code height}: image 0 is {@link #image}'s gradient,
   * image 1 the same gradient mirrored left to right, so the two decode to different pixels (an animated GIF, a multi-page TIFF).
   */
  public static byte[] twoFrames(String format, int width, int height) {
    BufferedImage first = gradient(width, height, BufferedImage.TYPE_INT_RGB);
    BufferedImage second = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        second.setRGB(x, y, first.getRGB(width - 1 - x, y));
      }
    }
    javax.imageio.ImageWriter writer = ImageIO.getImageWritersByFormatName(format).next();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (javax.imageio.stream.ImageOutputStream out = ImageIO.createImageOutputStream(bytes)) {
      writer.setOutput(out);
      writer.prepareWriteSequence(null);
      writer.writeToSequence(new javax.imageio.IIOImage(first, null, null), null);
      writer.writeToSequence(new javax.imageio.IIOImage(second, null, null), null);
      writer.endWriteSequence();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      writer.dispose();
    }
    return bytes.toByteArray();
  }

  /** Image {@code index} of a multi-image file written by {@link #twoFrames}, re-encoded alone in {@code format}. */
  public static byte[] frame(byte[] multi, int index, String format) {
    try (javax.imageio.stream.ImageInputStream in = new MemoryCacheImageInputStream(new ByteArrayInputStream(multi))) {
      javax.imageio.ImageReader reader = ImageIO.getImageReaders(in).next();
      try {
        reader.setInput(in);
        return encode(reader.read(index), format);
      } finally {
        reader.dispose();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Bytes no image reader recognises. */
  public static byte[] garbage() {
    return "this is not an image, only text pretending to be one".getBytes(StandardCharsets.UTF_8);
  }

  /** A source over in-memory bytes (tests only: production compute opens a seekable blob stream). */
  public static ImageSource source(byte[] bytes) {
    return () -> new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes));
  }

  public static BufferedImage decode(byte[] encoded) {
    try {
      return ImageIO.read(new ByteArrayInputStream(encoded));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static BufferedImage gradient(int width, int height, int type) {
    BufferedImage image = new BufferedImage(width, height, type);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int red = (x * 255) / Math.max(1, width - 1);
        int green = (y * 255) / Math.max(1, height - 1);
        image.setRGB(x, y, (red << 16) | (green << 8) | ((x ^ y) & 0xff));
      }
    }
    return image;
  }

  private static byte[] encode(BufferedImage image, String format) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      if (!ImageIO.write(image, format, out)) {
        throw new IllegalStateException("no ImageIO writer for " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  private static void put(byte[] bytes, int offset, String ascii) {
    byte[] chars = ascii.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(chars, 0, bytes, offset, chars.length);
  }

  private static void putLe32(byte[] bytes, int offset, int value) {
    putLe24(bytes, offset, value);
    bytes[offset + 3] = (byte) (value >>> 24);
  }

  private static void putLe24(byte[] bytes, int offset, int value) {
    bytes[offset] = (byte) value;
    bytes[offset + 1] = (byte) (value >>> 8);
    bytes[offset + 2] = (byte) (value >>> 16);
  }
}
