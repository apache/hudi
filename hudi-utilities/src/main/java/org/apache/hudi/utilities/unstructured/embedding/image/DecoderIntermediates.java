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

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.tiff.BaselineTIFFTagSet;
import javax.imageio.plugins.tiff.TIFFDirectory;
import javax.imageio.plugins.tiff.TIFFField;
import javax.imageio.stream.ImageInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

/**
 * Bytes a JDK reader needs BEFORE source subsampling applies, estimated from the image's header fields without decoding a pixel
 * (measured on 50 MP fixtures: the strip of a single-strip TIFF lands on the heap and a progressive JPEG's
 * coefficients in native memory, each estimated at about 143 MiB, while the subsampled destination raster is 0.59 MB). Sizes
 * are the ones the header DECLARES: Deflate allocates the declared byte count before reading a byte, and the other readers
 * read up to it (at or above one 1,024,000-byte unit, the staggered read of LZW, PackBits, CCITT and BMP RLE allocates its units
 * as the data arrives, so a declared size beyond the data costs only what is read). For the terms below the estimate is an upper
 * bound of the reader's own arrays; it does not count JPEG-in-TIFF tables, and a BMP whose bitmap is an embedded JPEG or PNG
 * (BI_JPEG/BI_PNG), which a reader decodes at full size, is refused instead (the JDK readers report no image type for it):
 * <ul>
 *   <li>TIFF: the reader decodes a whole strip (or tile) at full resolution, then subsamples it: one strip/tile =
 *       {@code width x rowsPerStrip} (or {@code tileWidth x tileLength}) x bits per pixel / 8; a compressed strip/tile is
 *       read whole beside it, so the largest TileByteCounts (else StripByteCounts) entry is added, twice for the compressions
 *       whose decompressor reads it in units and then copies them into one array (LZW, PackBits, CCITT; below one unit the
 *       read is a single array, so 2 x over-counts by at most 1,024,000 B), once for the others (Deflate; JPEG and old JPEG,
 *       whose tables, typically under 1 KiB, are not counted);</li>
 *   <li>BMP BI_RLE8 / BI_RLE4: the reader reads the whole compressed bitmap in units and copies them into one array before
 *       decoding a row: 2 x the bitmap size, the info header's imageSize or, when that is 0, bfSize - bfOffBits, plus a
 *       conservative {@code 4 x width} allowance for the decoded source row;</li>
 *   <li>BMP BI_RGB / BI_BITFIELDS / BI_ALPHABITFIELDS (uncompressed): the reader reads one full-width row, padded to 4 bytes,
 *       before subsampling it: {@code ceil(width x bitsPerPixel / 32) x 4} for modern headers; legacy or other BMP headers
 *       retain the conservative {@code 4 x width} source-row allowance;</li>
 *   <li>PNG: the reader holds the current and the prior scanline and a one-row raster, all at full width, before subsampling
 *       (measured on JDK 11 and 17: 3.00 x the row for every non-interlaced color type and depth): 3 x
 *       {@code ceil(width x samplesPerPixel x bitDepth / 8)}; an interlaced image decodes its passes one after another, each
 *       pass's rows released before the next, and its widest pass is the full width;</li>
 *   <li>progressive JPEG (SOF2): libjpeg keeps every DCT coefficient of the image: per component
 *       {@code ceil(componentWidth / 8) x ceil(componentHeight / 8)} blocks of 64 two-byte coefficients (native memory);</li>
 *   <li>GIF: one byte per source-row pixel;</li>
 *   <li>anything else: 0 (baseline JPEG has no whole-image coefficient buffer).</li>
 * </ul>
 */
final class DecoderIntermediates {

  static final String TIFF_METADATA_FORMAT = "javax_imageio_tiff_image_1.0";
  static final String JPEG_METADATA_FORMAT = "javax_imageio_jpeg_image_1.0";
  /** SOF process of a progressive JPEG in the JDK's JPEG metadata. */
  private static final String PROGRESSIVE_PROCESS = "2";
  private static final int BLOCK = 8;
  private static final long BYTES_PER_BLOCK = 64L * 2;
  /** BMP header bytes read: the file header (14) and the info header up to its imageSize field. */
  private static final int BMP_HEADER_BYTES = 38;
  private static final int BMP_BITS_PER_PIXEL_OFFSET = 28;
  private static final int BMP_FILE_SIZE_OFFSET = 2;
  private static final int BMP_BITMAP_OFFSET_OFFSET = 10;
  private static final int BMP_INFO_HEADER_SIZE_OFFSET = 14;
  private static final int BMP_COMPRESSION_OFFSET = 30;
  private static final int BMP_IMAGE_SIZE_OFFSET = 34;
  /** Info header of OS/2 1.x / Windows 2.x: no compression field, so never RLE. */
  private static final long BMP_CORE_HEADER_SIZE = 12;
  private static final long BMP_BI_RGB = 0;
  private static final long BMP_BI_RLE8 = 1;
  private static final long BMP_BI_RLE4 = 2;
  private static final long BMP_BI_BITFIELDS = 3;
  /** A BMP whose bitmap is an embedded JPEG stream. */
  private static final long BMP_BI_JPEG = 4;
  /** A BMP whose bitmap is an embedded PNG stream. */
  private static final long BMP_BI_PNG = 5;
  private static final long BMP_BI_ALPHABITFIELDS = 6;
  /** The RLE bitmap is held twice while it is read: the units, then the one array they are copied into. */
  private static final long BMP_RLE_COPIES = 2;
  /** PNG bytes read: the signature (8), the IHDR length and type (8), width, height, bit depth and color type. */
  private static final int PNG_HEADER_BYTES = 26;
  private static final int PNG_BIT_DEPTH_OFFSET = 24;
  private static final int PNG_COLOR_TYPE_OFFSET = 25;
  /** The PNG reader's full-width rows: the current scanline, the prior scanline and the one-row raster. */
  private static final long PNG_ROW_COPIES = 3;
  private static final int PNG_COLOR_RGB = 2;
  private static final int PNG_COLOR_GRAY_ALPHA = 4;
  private static final int PNG_COLOR_RGBA = 6;
  /** The header bytes the estimate reads ({@link #headerPrefix}): the longer of the BMP and PNG prefixes. */
  private static final int HEADER_PREFIX_BYTES = Math.max(BMP_HEADER_BYTES, PNG_HEADER_BYTES);

  private DecoderIntermediates() {
  }

  /**
   * The next {@link #HEADER_PREFIX_BYTES} bytes of {@code in} from its current position (fewer when the stream is shorter), read
   * under mark/reset so the stream is left where it was; called on a stream at the start of the file, before a reader takes it:
   * the PNG reader flushes the stream behind its header, so the header bytes cannot be read again afterwards.
   *
   * @throws IOException when the read fails in storage (a {@link BlobReadException} of compute's stored-input stream, retried by
   *     the caller)
   */
  static byte[] headerPrefix(ImageInputStream in) throws IOException {
    byte[] prefix = new byte[HEADER_PREFIX_BYTES];
    int read = 0;
    in.mark();
    try {
      while (read < prefix.length) {
        int n = in.read(prefix, read, prefix.length - read);
        if (n < 0) {
          break;
        }
        read += n;
      }
    } finally {
      in.reset();
    }
    return read == prefix.length ? prefix : Arrays.copyOf(prefix, read);
  }

  /**
   * @param reader a JDK reader whose input is set (an {@link ImageInputStream}), positioned on image 0's header
   * @param prefix the stream's first bytes ({@link #headerPrefix}), read before the reader took the stream
   * @return the estimated bytes, 0 when the format decodes straight into the subsampled destination
   * @throws IOException when the metadata cannot be read (a corrupt header)
   */
  static long estimate(ImageReader reader, ImageHeader header, byte[] prefix) throws IOException {
    if ("tiff".equals(header.getFormat())) {
      return tiffStripOrTile(reader.getImageMetadata(0), header);
    }
    if ("jpeg".equals(header.getFormat())) {
      return progressiveJpegCoefficients(reader.getImageMetadata(0), header);
    }
    if ("bmp".equals(header.getFormat())) {
      return bmpBitmapOrRow(prefix, header);
    }
    if ("png".equals(header.getFormat())) {
      return pngRows(prefix, header);
    }
    if ("gif".equals(header.getFormat())) {
      return header.getWidth();
    }
    return 0;
  }

  /**
   * Long.MAX_VALUE when the product overflows a long: the tile dimensions are 32-bit header values that the image's checked width
   * and height do not bound, and a wrapped product must reach the budget refusal, not slip under it.
   */
  private static long tiffStripOrTile(IIOMetadata metadata, ImageHeader header) throws IOException {
    TIFFDirectory directory = TIFFDirectory.createFromMetadata(metadata);
    long bitsPerPixel = bitsPerPixel(directory);
    TIFFField tileWidth = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_WIDTH);
    TIFFField tileLength = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_LENGTH);
    try {
      long pixels;
      if (tileWidth != null && tileLength != null) {
        pixels = Math.multiplyExact(tileWidth.getAsLong(0), tileLength.getAsLong(0));
      } else {
        TIFFField rowsPerStrip = directory.getTIFFField(BaselineTIFFTagSet.TAG_ROWS_PER_STRIP);
        long rows = rowsPerStrip == null ? header.getHeight() : Math.min(rowsPerStrip.getAsLong(0), header.getHeight());
        pixels = header.getWidth() * rows;
      }
      long decoded = Math.addExact(Math.multiplyExact(pixels, bitsPerPixel), 7) / 8;
      return Math.addExact(decoded, Math.multiplyExact(compressedCopies(directory), largestByteCount(directory, decoded)));
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * How many times the decompressor holds a compressed strip/tile at once: 0 uncompressed (read straight into the decoded
   * buffer), 2 for the readers of the whole strip in units then one copy ({@code ReaderUtil.staggeredReadByteStream}: LZW,
   * PackBits, CCITT RLE / T.4 / T.6), 1 for every other compression (Deflate, JPEG, old JPEG and any other).
   */
  private static long compressedCopies(TIFFDirectory directory) {
    TIFFField field = directory.getTIFFField(BaselineTIFFTagSet.TAG_COMPRESSION);
    int compression = field == null ? BaselineTIFFTagSet.COMPRESSION_NONE : field.getAsInt(0);
    switch (compression) {
      case BaselineTIFFTagSet.COMPRESSION_NONE:
        return 0;
      case BaselineTIFFTagSet.COMPRESSION_LZW:
      case BaselineTIFFTagSet.COMPRESSION_PACKBITS:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_RLE:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_T_4:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_T_6:
        return 2;
      default:
        return 1;
    }
  }

  /**
   * The largest TileByteCounts entry, else the largest StripByteCounts entry (the field the reader takes each strip/tile's
   * byte count from), as declared; {@code decoded} when the directory has neither (the reader then reads the uncompressed size).
   */
  private static long largestByteCount(TIFFDirectory directory, long decoded) {
    TIFFField counts = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_BYTE_COUNTS);
    if (counts == null) {
      counts = directory.getTIFFField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS);
    }
    if (counts == null) {
      return decoded;
    }
    long largest = 0;
    for (int i = 0; i < counts.getCount(); i++) {
      largest = Math.max(largest, counts.getAsLong(i));
    }
    return largest;
  }

  /**
   * 2 x the bitmap of a BI_RLE8 / BI_RLE4 BMP, plus a conservative full-width decoded row. The bitmap uses the info header's
   * imageSize, or bfSize - bfOffBits when it is 0. A modern uncompressed BMP uses its precise padded row; legacy or other BMP
   * headers retain the conservative source-row allowance. Embedded JPEG/PNG decoder buffers are not counted. The fields are
   * read from {@code header}, the file's first bytes ({@code BMPMetadata} does not carry imageSize).
   */
  private static long bmpBitmapOrRow(byte[] header, ImageHeader image) throws ImageRefusedException {
    long sourceRow = 4L * image.getWidth();
    // shorter than a BITMAPINFOHEADER (an OS/2 1.x header or a truncated file), or an OS/2 1.x / Windows 2.x header: a 16-bit
    // width, never RLE
    if (header.length < BMP_HEADER_BYTES || le32(header, BMP_INFO_HEADER_SIZE_OFFSET) == BMP_CORE_HEADER_SIZE) {
      return sourceRow;
    }
    long compression = le32(header, BMP_COMPRESSION_OFFSET);
    if (compression == BMP_BI_JPEG || compression == BMP_BI_PNG) {
      throw new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + image + " is a BMP with an embedded JPEG or "
          + "PNG bitmap (compression " + compression + "), which is decoded at full size outside the decoder-intermediate budget");
    }
    if (compression == BMP_BI_RGB || compression == BMP_BI_BITFIELDS || compression == BMP_BI_ALPHABITFIELDS) {
      long bitsPerPixel = (header[BMP_BITS_PER_PIXEL_OFFSET] & 0xffL) | (header[BMP_BITS_PER_PIXEL_OFFSET + 1] & 0xffL) << 8;
      return (image.getWidth() * bitsPerPixel + 31) / 32 * 4;
    }
    if (compression != BMP_BI_RLE8 && compression != BMP_BI_RLE4) {
      return sourceRow;
    }
    long imageSize = le32(header, BMP_IMAGE_SIZE_OFFSET);
    long bitmap = imageSize == 0 ? le32(header, BMP_FILE_SIZE_OFFSET) - le32(header, BMP_BITMAP_OFFSET_OFFSET) : imageSize;
    return BMP_RLE_COPIES * Math.max(0, bitmap) + sourceRow;
  }

  /**
   * 3 x one full-width row of a PNG, {@code ceil(width x samplesPerPixel x bitDepth / 8)}, from the IHDR's bit depth and color
   * type (the reader has already read and checked the IHDR, which a PNG holds at a fixed offset), taken from {@code ihdr}, the
   * file's first bytes.
   */
  private static long pngRows(byte[] ihdr, ImageHeader header) throws EOFException {
    if (ihdr.length < PNG_HEADER_BYTES) {
      // the reader already parsed this IHDR, so the file is never shorter
      throw new EOFException("PNG shorter than its IHDR");
    }
    long bitDepth = ihdr[PNG_BIT_DEPTH_OFFSET] & 0xff;
    long row = (bitDepth * header.getWidth() * pngSamplesPerPixel(ihdr[PNG_COLOR_TYPE_OFFSET] & 0xff) + 7) / 8;
    return PNG_ROW_COPIES * row;
  }

  /** Samples per pixel of a PNG color type: gray 1, RGB 3, palette 1 (an index), gray + alpha 2, RGBA 4. */
  private static long pngSamplesPerPixel(int colorType) {
    switch (colorType) {
      case PNG_COLOR_RGB:
        return 3;
      case PNG_COLOR_GRAY_ALPHA:
        return 2;
      case PNG_COLOR_RGBA:
        return 4;
      default:
        return 1;
    }
  }

  /** The unsigned little-endian 32-bit value at {@code offset}. */
  private static long le32(byte[] bytes, int offset) {
    return (bytes[offset] & 0xffL) | (bytes[offset + 1] & 0xffL) << 8 | (bytes[offset + 2] & 0xffL) << 16
        | (bytes[offset + 3] & 0xffL) << 24;
  }

  private static long bitsPerPixel(TIFFDirectory directory) {
    TIFFField bits = directory.getTIFFField(BaselineTIFFTagSet.TAG_BITS_PER_SAMPLE);
    TIFFField samples = directory.getTIFFField(BaselineTIFFTagSet.TAG_SAMPLES_PER_PIXEL);
    long samplesPerPixel = samples == null ? 1 : samples.getAsLong(0);
    if (bits == null) {
      return samplesPerPixel;
    }
    if (bits.getCount() >= samplesPerPixel) {
      long sum = 0;
      for (int i = 0; i < samplesPerPixel; i++) {
        sum += bits.getAsLong(i);
      }
      return sum;
    }
    return bits.getAsLong(0) * samplesPerPixel;
  }

  private static long progressiveJpegCoefficients(IIOMetadata metadata, ImageHeader header) {
    Node sof = find(metadata.getAsTree(JPEG_METADATA_FORMAT), "sof");
    if (sof == null || !PROGRESSIVE_PROCESS.equals(attribute(sof, "process"))) {
      return 0;
    }
    int maxH = 1;
    int maxV = 1;
    for (Node component = sof.getFirstChild(); component != null; component = component.getNextSibling()) {
      maxH = Math.max(maxH, Integer.parseInt(attribute(component, "HsamplingFactor")));
      maxV = Math.max(maxV, Integer.parseInt(attribute(component, "VsamplingFactor")));
    }
    long blocks = 0;
    for (Node component = sof.getFirstChild(); component != null; component = component.getNextSibling()) {
      long width = ceilDiv((long) header.getWidth() * Integer.parseInt(attribute(component, "HsamplingFactor")), maxH);
      long height = ceilDiv((long) header.getHeight() * Integer.parseInt(attribute(component, "VsamplingFactor")), maxV);
      blocks += ceilDiv(width, BLOCK) * ceilDiv(height, BLOCK);
    }
    return blocks * BYTES_PER_BLOCK;
  }

  private static Node find(Node node, String name) {
    if (name.equals(node.getNodeName())) {
      return node;
    }
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
      Node found = find(child, name);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static String attribute(Node node, String name) {
    NamedNodeMap attributes = node.getAttributes();
    Node value = attributes == null ? null : attributes.getNamedItem(name);
    return value == null ? null : value.getNodeValue();
  }

  private static long ceilDiv(long value, long divisor) {
    return (value + divisor - 1) / divisor;
  }
}
