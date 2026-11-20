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

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.IndexColorModel;
import java.awt.image.WritableRaster;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.zip.DeflaterOutputStream;

/**
 * Generated images of every stored image type (png, jpeg, gif, bmp, tiff): per type one image near (under) the default pixel
 * guard ({@code hoodie.unstructured.embedding.prep.image.max.pixels}) and one small image, plus the images whose decoder
 * intermediates image compute's intermediate budget refuses ({@link Fixture#refusedBeforeDecode()}: a BMP RLE bitmap read whole, a compressed
 * TIFF strip or tile beside the decoded one, StripByteCounts far above the strip, inside the file) and small RLE4, LZW and PackBits images it
 * admits. Every image is a deterministic function of
 * its {@link Fixture} (no randomness), so a regeneration on the same JDK (its ImageIO writers and zlib) is byte-identical and its
 * sha256 can be checked against a manifest, which records {@code java.version}.
 * The near-bound images hold up to 100 MP and are written by the JDK's ImageIO writers from an in-memory image, so generating
 * them needs a heap of a few GiB (they are written once, in their own JVM, and never generated inside a measured run).
 */
public final class ImageFormatFixtures {

  /** One generated image: its file name, the content type it decodes as, its encoding and its dimensions. */
  public static final class Fixture {
    private final String name;
    private final String format;
    private final String encoding;
    private final int width;
    private final int height;
    private final boolean refusedBeforeDecode;

    Fixture(String name, String format, String encoding, int width, int height) {
      this(name, format, encoding, width, height, false);
    }

    Fixture(String name, String format, String encoding, int width, int height, boolean refusedBeforeDecode) {
      this.name = name;
      this.format = format;
      this.encoding = encoding;
      this.width = width;
      this.height = height;
      this.refusedBeforeDecode = refusedBeforeDecode;
    }

    /** The file name, with the extension the blob table admits for the format. */
    public String name() {
      return name;
    }

    /** The format the JDK reader reports, normalized as {@link ImageHeaderReader#normalize} does. */
    public String format() {
      return format;
    }

    public String encoding() {
      return encoding;
    }

    public int width() {
      return width;
    }

    public int height() {
      return height;
    }

    public boolean isNearBound() {
      return name.startsWith(NEAR_PREFIX);
    }

    /**
     * Whether image compute refuses this image before decoding under its default budgets: its decoder intermediates (a compressed
     * TIFF strip or tile beside the decoded one, a BMP RLE bitmap read whole) are above the default intermediate budget.
     */
    public boolean refusedBeforeDecode() {
      return refusedBeforeDecode;
    }

    @Override
    public String toString() {
      return name + " " + format + " " + width + "x" + height + " " + encoding;
    }
  }

  public static final String NEAR_PREFIX = "near-";
  public static final String SMALL_PREFIX = "small-";
  /** Under the default pixel guard of 100,000,000. */
  static final int NEAR_LONG = 10000;
  static final int NEAR_SHORT = 9999;
  /**
   * A 4:2:0 progressive JPEG whose DCT coefficients (3 bytes per pixel, as image compute estimates them) are just under the
   * default intermediate budget of 67,108,864 B: 5456 x 4096 gives 67,043,328 B.
   */
  static final int PROGRESSIVE_WIDTH = 5456;
  static final int PROGRESSIVE_HEIGHT = 4096;
  /** RGB strips of 2237 rows of 9999 pixels: 67,103,289 B per strip, just under the default intermediate budget. */
  static final int TIFF_ROWS_PER_STRIP = 2237;
  /**
   * Height of the near-bound strip TIFF: four FULL strips, so wherever a probe's gate falls in the pixel data it is inside a strip of
   * {@link #TIFF_ROWS_PER_STRIP} rows (with a short last strip, a gate at 90 % of the file lands in that short strip).
   */
  static final int TIFF_STRIPS_HEIGHT = 4 * TIFF_ROWS_PER_STRIP;
  /** RGB tiles of 4720 x 4736 pixels: 67,061,760 B per tile, just under the default intermediate budget. */
  static final int TIFF_TILE_WIDTH = 4720;
  static final int TIFF_TILE_HEIGHT = 4736;
  /**
   * Sides of the BMP of length-1 encoded runs: 2 bytes per pixel plus an end of line per row, 72,012,002 B of bitmap, whose
   * whole-bitmap read (2 x the bitmap) is above the default intermediate budget, at 36 MP.
   */
  static final int UNIT_RUNS_SIDE = 6000;
  /** Sides and rows per strip of the TIFF whose StripByteCounts each run to the end of {@link #OVERSIZED_COUNTS_TRAILING_BYTES}. */
  static final int OVERSIZED_COUNTS_SIDE = 2000;
  static final int OVERSIZED_COUNTS_ROWS_PER_STRIP = 256;
  /**
   * Bytes written after the strips of the oversized-count TIFF; every strip's declared byte count runs from its offset to their
   * end, so each count stays inside the file (the JDK reader refuses a segment past the stream's end at the header) and is far
   * above the strip's compressed size and the default intermediate budget.
   */
  static final long OVERSIZED_COUNTS_TRAILING_BYTES = 96L * 1024 * 1024;
  static final int SECOND_FRAME_SIDE = 64;
  static final int SMALL_WIDTH = 640;
  static final int SMALL_HEIGHT = 480;
  private static final int PNG_IDAT_CHUNK_BYTES = 1024 * 1024;
  private static final int BMP_FILE_HEADER_BYTES = 14;
  private static final int BMP_INFO_HEADER_BYTES = 40;
  private static final int BMP_BI_RLE8 = 1;
  private static final float JPEG_QUALITY = 0.85f;

  private static final List<Fixture> CATALOG = Collections.unmodifiableList(Arrays.asList(
      new Fixture(NEAR_PREFIX + "png.png", "png", "8-bit RGBA, deflate (default level), non-interlaced", NEAR_LONG, NEAR_SHORT),
      new Fixture(NEAR_PREFIX + "jpeg-progressive.jpg", "jpeg", "progressive (SOF2), YCbCr 4:2:0, quality 0.85",
          PROGRESSIVE_WIDTH, PROGRESSIVE_HEIGHT),
      new Fixture(NEAR_PREFIX + "jpeg-baseline.jpg", "jpeg", "baseline (SOF0), YCbCr 4:2:0, quality 0.85", NEAR_LONG, NEAR_SHORT),
      new Fixture(NEAR_PREFIX + "gif-two-frames.gif", "gif", "8-bit palette, LZW, 2 frames (2nd 64x64)", NEAR_LONG, NEAR_SHORT),
      new Fixture(NEAR_PREFIX + "bmp-rle8.bmp", "bmp", "8-bit palette, BI_RLE8, dense content (absolute runs)",
          NEAR_LONG, NEAR_SHORT, true),
      new Fixture(NEAR_PREFIX + "bmp-rle8-unit-runs.bmp", "bmp", "8-bit palette, BI_RLE8, every pixel a length-1 encoded run",
          UNIT_RUNS_SIDE, UNIT_RUNS_SIDE, true),
      new Fixture(NEAR_PREFIX + "tiff-strips.tif", "tiff", "RGB 8-bit, Deflate, RowsPerStrip " + TIFF_ROWS_PER_STRIP
          + " (4 full strips)", NEAR_SHORT, TIFF_STRIPS_HEIGHT, true),
      new Fixture(NEAR_PREFIX + "tiff-tiles.tif", "tiff", "RGB 8-bit, Deflate, tiles " + TIFF_TILE_WIDTH + "x" + TIFF_TILE_HEIGHT,
          NEAR_SHORT, NEAR_LONG, true),
      new Fixture(NEAR_PREFIX + "tiff-oversized-counts.tif", "tiff", "RGB 8-bit, Deflate, RowsPerStrip "
          + OVERSIZED_COUNTS_ROWS_PER_STRIP + ", StripByteCounts each running over " + OVERSIZED_COUNTS_TRAILING_BYTES + " trailing bytes",
          OVERSIZED_COUNTS_SIDE, OVERSIZED_COUNTS_SIDE, true),
      new Fixture(SMALL_PREFIX + "png.png", "png", "8-bit RGBA, deflate (default level), non-interlaced",
          SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "jpeg.jpg", "jpeg", "baseline (SOF0), YCbCr 4:2:0, quality 0.85", SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "gif.gif", "gif", "8-bit palette, LZW, 1 frame", SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "bmp-rle8.bmp", "bmp", "8-bit palette, BI_RLE8, dense content (absolute runs)",
          SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "bmp-rle4.bmp", "bmp", "4-bit palette, BI_RLE4, dense content", SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "tiff.tif", "tiff", "RGB 8-bit, Deflate, RowsPerStrip " + TIFF_ROWS_PER_STRIP,
          SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "tiff-lzw.tif", "tiff", "RGB 8-bit, LZW, the JDK writer's strips", SMALL_WIDTH, SMALL_HEIGHT),
      new Fixture(SMALL_PREFIX + "tiff-packbits.tif", "tiff", "RGB 8-bit, PackBits, the JDK writer's strips",
          SMALL_WIDTH, SMALL_HEIGHT)));

  private ImageFormatFixtures() {
  }

  /** Every fixture, near-bound first. */
  public static List<Fixture> catalog() {
    return CATALOG;
  }

  /** The fixture named {@code name}. */
  public static Fixture named(String name) {
    return CATALOG.stream().filter(f -> f.name().equals(name)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no fixture " + name));
  }

  /** Writes {@code fixture} to {@code dir}/{@link Fixture#name()} and returns the file. */
  public static Path write(Fixture fixture, Path dir) throws IOException {
    Path file = dir.resolve(fixture.name());
    String kind = fixture.name().substring(fixture.isNearBound() ? NEAR_PREFIX.length() : SMALL_PREFIX.length());
    if (kind.startsWith("png")) {
      try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
        StreamingPngWriter.write(out, fixture.width(), fixture.height(), true, PNG_IDAT_CHUNK_BYTES,
            (x, y) -> rgb(x, y) | 0xff000000);
      }
    } else if (kind.startsWith("jpeg")) {
      writeJpeg(fixture, file, kind.contains("progressive"));
    } else if (kind.startsWith("gif")) {
      writeGif(fixture, file, kind.contains("two-frames"));
    } else if (kind.equals("bmp-rle8-unit-runs.bmp")) {
      writeBmpRle8UnitRuns(file, fixture.width(), fixture.height(), true);
    } else if (kind.startsWith("bmp-rle4")) {
      writeBmpRle4(file, fixture.width(), fixture.height());
    } else if (kind.startsWith("bmp")) {
      writeBmpRle8(fixture, file);
    } else if (kind.equals("tiff-oversized-counts.tif")) {
      writeStripTiff(file, fixture.width(), fixture.height(), OVERSIZED_COUNTS_ROWS_PER_STRIP, OVERSIZED_COUNTS_TRAILING_BYTES);
    } else if (kind.equals("tiff-lzw.tif")) {
      writeJdkTiff(file, fixture.width(), fixture.height(), "LZW");
    } else if (kind.equals("tiff-packbits.tif")) {
      writeJdkTiff(file, fixture.width(), fixture.height(), "PackBits");
    } else if (kind.startsWith("tiff")) {
      writeTiff(fixture, file, kind.contains("tiles"));
    } else {
      throw new IllegalArgumentException("unknown fixture kind " + kind);
    }
    return file;
  }

  /** One manifest line of {@code file}: name, format, dimensions, size, sha256 and encoding. */
  public static String manifestLine(Fixture fixture, Path file) throws IOException {
    return String.join("\t", fixture.name(), fixture.format(), fixture.width() + "x" + fixture.height(),
        String.valueOf(Files.size(file)), sha256(file), fixture.encoding());
  }

  public static String sha256(Path file) throws IOException {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    byte[] buffer = new byte[1 << 16];
    try (InputStream in = Files.newInputStream(file)) {
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
        digest.update(buffer, 0, n);
      }
    }
    StringBuilder hex = new StringBuilder();
    for (byte b : digest.digest()) {
      hex.append(String.format(Locale.ROOT, "%02x", b));
    }
    return hex.toString();
  }

  // ------------------------------------------------------------------ content

  /** Opaque RGB with gradients and a fine texture, so the lossy and deflate encoders produce realistic sizes. */
  static int rgb(int x, int y) {
    int texture = ((x * 7) ^ (y * 13)) & 0x1f;
    int red = (x * 255 / NEAR_LONG + texture) & 0xff;
    int green = (y * 255 / NEAR_LONG + texture) & 0xff;
    int blue = ((x + y) / 40 + texture) & 0xff;
    return (red << 16) | (green << 8) | blue;
  }

  /**
   * A hashed palette index: neighbours are rarely equal (a hash mod 256), so BI_RLE8 mostly uses absolute runs (about one byte
   * per pixel).
   */
  static int denseIndex(int x, int y) {
    int mixed = x * 0x9E3779B1 ^ y * 0x85EBCA77;
    mixed ^= mixed >>> 15;
    mixed *= 0x2C1B3C6D;
    mixed ^= mixed >>> 12;
    return mixed & 0xff;
  }

  private static BufferedImage rgbImage(int width, int height) {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
    byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
    int at = 0;
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int pixel = rgb(x, y);
        data[at++] = (byte) pixel;
        data[at++] = (byte) (pixel >>> 8);
        data[at++] = (byte) (pixel >>> 16);
      }
    }
    return image;
  }

  /** An 8-bit image with the JDK's default 256-entry palette; {@code dense} picks {@link #denseIndex}, else a smooth index. */
  private static BufferedImage indexedImage(int width, int height, boolean dense) {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_INDEXED);
    byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
    int at = 0;
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        data[at++] = (byte) (dense ? denseIndex(x, y) : ((x / 64 + y / 64) % 216));
      }
    }
    return image;
  }

  // ------------------------------------------------------------------ writers

  private static void writeJpeg(Fixture fixture, Path file, boolean progressive) throws IOException {
    ImageWriter writer = writer("jpeg");
    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionQuality(JPEG_QUALITY);
    if (progressive) {
      param.setProgressiveMode(ImageWriteParam.MODE_DEFAULT);
    }
    write(writer, file, new IIOImage(rgbImage(fixture.width(), fixture.height()), null, null), param);
  }

  private static void writeGif(Fixture fixture, Path file, boolean twoFrames) throws IOException {
    ImageWriter writer = writer("gif");
    Files.deleteIfExists(file);
    try (ImageOutputStream out = ImageIO.createImageOutputStream(file.toFile())) {
      writer.setOutput(out);
      writer.prepareWriteSequence(null);
      writer.writeToSequence(new IIOImage(indexedImage(fixture.width(), fixture.height(), false), null, null), null);
      if (twoFrames) {
        writer.writeToSequence(new IIOImage(indexedImage(SECOND_FRAME_SIDE, SECOND_FRAME_SIDE, false), null, null), null);
      }
      writer.endWriteSequence();
    } finally {
      writer.dispose();
    }
  }

  private static void writeBmpRle8(Fixture fixture, Path file) throws IOException {
    writeBmpRle8(file, fixture.width(), fixture.height());
  }

  /** An 8-bit palette BMP, BI_RLE8, through the JDK writer, of {@link #denseIndex} (mostly absolute runs). */
  static void writeBmpRle8(Path file, int width, int height) throws IOException {
    ImageWriter writer = writer("bmp");
    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionType("BI_RLE8");
    write(writer, file, new IIOImage(indexedImage(width, height, true), null, null), param);
  }

  /**
   * Deflate-compressed RGB TIFF: in tiles of {@link #TIFF_TILE_WIDTH} x {@link #TIFF_TILE_HEIGHT} through the JDK writer, or in
   * strips of {@link #TIFF_ROWS_PER_STRIP} rows through {@link #writeStripTiff} (the JDK writer picks its own RowsPerStrip and
   * ignores one given in the image metadata).
   */
  private static void writeTiff(Fixture fixture, Path file, boolean tiles) throws IOException {
    if (!tiles) {
      writeStripTiff(file, fixture.width(), fixture.height(), TIFF_ROWS_PER_STRIP);
      return;
    }
    writeTiledTiff(file, fixture.width(), fixture.height(), TIFF_TILE_WIDTH, TIFF_TILE_HEIGHT, "Deflate");
  }

  /** RGB 8-bit TIFF in tiles of {@code tileWidth} x {@code tileHeight} through the JDK writer with {@code compression}. */
  static void writeTiledTiff(Path file, int width, int height, int tileWidth, int tileHeight, String compression) throws IOException {
    ImageWriter writer = writer("tiff");
    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionType(compression);
    param.setTilingMode(ImageWriteParam.MODE_EXPLICIT);
    param.setTiling(tileWidth, tileHeight, 0, 0);
    write(writer, file, new IIOImage(rgbImage(width, height), null, null), param);
  }

  /**
   * A baseline little-endian TIFF, RGB 8-bit chunky, compression 8 (zlib Deflate), {@code rowsPerStrip} rows per strip, written
   * strip by strip (one uncompressed strip in memory): header, the strips, then the IFD and its out-of-line values.
   */
  static long[] writeStripTiff(Path file, int width, int height, int rowsPerStrip) throws IOException {
    return writeStripTiff(file, width, height, rowsPerStrip, 0);
  }

  /**
   * {@link #writeStripTiff(Path, int, int, int)} with {@code trailingBytes} zero bytes after the strips; when it is positive every
   * strip's StripByteCounts entry declares the range from the strip's offset to the end of those bytes (inside the file, above the
   * strip's compressed size). Returns the declared byte counts.
   */
  static long[] writeStripTiff(Path file, int width, int height, int rowsPerStrip, long trailingBytes) throws IOException {
    int strips = (height + rowsPerStrip - 1) / rowsPerStrip;
    long[] offsets = new long[strips];
    long[] counts = new long[strips];
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING)) {
      ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) 'I').put((byte) 'I').putShort((short) 42).putInt(0);
      header.flip();
      channel.write(header);
      long position = 8;
      for (int strip = 0; strip < strips; strip++) {
        int firstRow = strip * rowsPerStrip;
        int rows = Math.min(rowsPerStrip, height - firstRow);
        byte[] raw = new byte[rows * width * 3];
        int at = 0;
        for (int y = firstRow; y < firstRow + rows; y++) {
          for (int x = 0; x < width; x++) {
            int pixel = rgb(x, y);
            raw[at++] = (byte) (pixel >>> 16);
            raw[at++] = (byte) (pixel >>> 8);
            raw[at++] = (byte) pixel;
          }
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (DeflaterOutputStream zlib = new DeflaterOutputStream(compressed)) {
          zlib.write(raw);
        }
        offsets[strip] = position;
        counts[strip] = compressed.size();
        position += channel.write(ByteBuffer.wrap(compressed.toByteArray()));
      }
      if (trailingBytes > 0) {
        byte[] zeros = new byte[1 << 20];
        for (long left = trailingBytes; left > 0; left -= zeros.length) {
          position += channel.write(ByteBuffer.wrap(zeros, 0, (int) Math.min(zeros.length, left)));
        }
        for (int strip = 0; strip < strips; strip++) {
          counts[strip] = position - offsets[strip];
        }
      }
      if ((position & 1) != 0) {
        position += channel.write(ByteBuffer.wrap(new byte[1]));
      }
      long ifd = position;
      int entries = 10;
      long values = ifd + 2 + 12L * entries + 4;
      long bitsAt = values;
      long offsetsAt = bitsAt + 6;
      long countsAt = offsetsAt + 4L * strips;
      ByteBuffer dir = ByteBuffer.allocate((int) (countsAt + 4L * strips - ifd)).order(ByteOrder.LITTLE_ENDIAN);
      dir.putShort((short) entries);
      entry(dir, 256, 4, 1, width);
      entry(dir, 257, 4, 1, height);
      entry(dir, 258, 3, 3, bitsAt);
      entry(dir, 259, 3, 1, 8);
      entry(dir, 262, 3, 1, 2);
      entry(dir, 273, 4, strips, strips == 1 ? offsets[0] : offsetsAt);
      entry(dir, 277, 3, 1, 3);
      entry(dir, 278, 4, 1, rowsPerStrip);
      entry(dir, 279, 4, strips, strips == 1 ? counts[0] : countsAt);
      entry(dir, 284, 3, 1, 1);
      dir.putInt(0);
      dir.putShort((short) 8).putShort((short) 8).putShort((short) 8);
      for (long offset : offsets) {
        dir.putInt((int) offset);
      }
      for (long count : counts) {
        dir.putInt((int) count);
      }
      dir.flip();
      channel.write(dir);
      ByteBuffer first = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) ifd);
      first.flip();
      channel.write(first, 4);
    }
    return counts;
  }

  /**
   * RGB 8-bit TIFF in strips through the JDK writer with {@code compression} (LZW, PackBits, Deflate, ...), or uncompressed when
   * {@code compression} is null.
   */
  static void writeJdkTiff(Path file, int width, int height, String compression) throws IOException {
    ImageWriter writer = writer("tiff");
    ImageWriteParam param = writer.getDefaultWriteParam();
    if (compression == null) {
      param.setCompressionMode(ImageWriteParam.MODE_DISABLED);
    } else {
      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
      param.setCompressionType(compression);
    }
    write(writer, file, new IIOImage(rgbImage(width, height), null, null), param);
  }

  /** A 4-bit palette BMP, BI_RLE4, through the JDK writer, of {@link #denseIndex} mod 16. */
  static void writeBmpRle4(Path file, int width, int height) throws IOException {
    byte[] levels = new byte[16];
    for (int i = 0; i < levels.length; i++) {
      levels[i] = (byte) (i * 17);
    }
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY,
        new IndexColorModel(4, levels.length, levels, levels, levels));
    WritableRaster raster = image.getRaster();
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        raster.setSample(x, y, 0, denseIndex(x, y) & 0xf);
      }
    }
    ImageWriter writer = writer("bmp");
    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionType("BI_RLE4");
    write(writer, file, new IIOImage(image, null, null), param);
  }

  /** Bytes of the BI_RLE8 bitmap {@link #writeBmpRle8UnitRuns} writes: 2 per pixel, 2 per end of line, 2 for the end of bitmap. */
  static long unitRunsBitmapBytes(int width, int height) {
    return (long) height * (2L * width + 2) + 2;
  }

  /**
   * A bottom-up 8-bit BMP whose BI_RLE8 bitmap encodes every pixel as a run of length 1 (count 1, then the index of
   * {@link #denseIndex}): 2 bytes per pixel, twice the 8-bit raster it decodes to. Written row by row (no image in memory). With
   * {@code declareImageSize} false the info header's imageSize is 0, so a reader takes the bitmap size from bfSize - bfOffBits.
   */
  static void writeBmpRle8UnitRuns(Path file, int width, int height, boolean declareImageSize) throws IOException {
    int paletteEntries = 256;
    long bitmapOffset = BMP_FILE_HEADER_BYTES + BMP_INFO_HEADER_BYTES + 4L * paletteEntries;
    long imageSize = unitRunsBitmapBytes(width, height);
    ByteBuffer header = ByteBuffer.allocate((int) bitmapOffset).order(ByteOrder.LITTLE_ENDIAN);
    header.put((byte) 'B').put((byte) 'M').putInt((int) (bitmapOffset + imageSize)).putInt(0).putInt((int) bitmapOffset);
    header.putInt(BMP_INFO_HEADER_BYTES).putInt(width).putInt(height).putShort((short) 1).putShort((short) 8)
        .putInt(BMP_BI_RLE8).putInt(declareImageSize ? (int) imageSize : 0).putInt(2835).putInt(2835).putInt(paletteEntries).putInt(0);
    for (int i = 0; i < paletteEntries; i++) {
      header.put((byte) i).put((byte) (255 - i)).put((byte) ((i * 7) & 0xff)).put((byte) 0);
    }
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), 1 << 16)) {
      out.write(header.array());
      byte[] row = new byte[2 * width + 2];
      for (int y = height - 1; y >= 0; y--) {
        for (int x = 0; x < width; x++) {
          row[2 * x] = 1;
          row[2 * x + 1] = (byte) denseIndex(x, y);
        }
        // end of line (0, 0) closes the row
        row[2 * width] = 0;
        row[2 * width + 1] = 0;
        out.write(row);
      }
      // end of bitmap (0, 1)
      out.write(new byte[] {0, 1});
    }
  }

  /** One IFD entry; a SHORT value that fits is left-justified in the value field, as TIFF requires. */
  private static void entry(ByteBuffer dir, int tag, int type, long count, long value) {
    dir.putShort((short) tag).putShort((short) type).putInt((int) count);
    if (type == 3 && count == 1) {
      dir.putShort((short) value).putShort((short) 0);
    } else {
      dir.putInt((int) value);
    }
  }

  /**
   * The JDK's built-in writer of {@code format}: the test classpath also carries jai-imageio (through Tika), whose writers ImageIO
   * may list first, and the fixtures are defined as the JDK writers' output.
   */
  private static ImageWriter writer(String format) {
    Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(format);
    while (writers.hasNext()) {
      ImageWriter writer = writers.next();
      if (writer.getOriginatingProvider() != null
          && writer.getOriginatingProvider().getClass().getName().startsWith(ImageHeaderReader.JDK_PLUGIN_PACKAGE)) {
        return writer;
      }
      writer.dispose();
    }
    throw new IllegalStateException("no JDK ImageIO writer for " + format);
  }

  private static void write(ImageWriter writer, Path file, IIOImage image, ImageWriteParam param) throws IOException {
    File target = file.toFile();
    Files.deleteIfExists(file);
    try (ImageOutputStream out = ImageIO.createImageOutputStream(target)) {
      writer.setOutput(out);
      writer.write(null, image, param);
    } finally {
      writer.dispose();
    }
  }
}
