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

import org.apache.hudi.utilities.unstructured.SourceCauses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.spi.ImageReaderWriterSpi;
import javax.imageio.stream.ImageInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Reads an image's header without decoding a pixel (the dimensions are known before any
 * decode). The format is recognised by content, not by file extension: the first ImageIO reader whose
 * {@code canDecodeInput} accepts the bytes, else a WebP RIFF container (the JDK has no WebP reader; its header is parsed
 * here so data prep can validate it).
 */
public final class ImageHeaderReader {

  private static final Logger LOG = LoggerFactory.getLogger(ImageHeaderReader.class);

  /** Format name of a WebP image, which this JVM can read the header of but cannot decode. */
  public static final String WEBP = "webp";
  /** Package of the JDK's built-in ImageIO reader and writer providers (png, jpeg, gif, bmp, wbmp, tiff). */
  static final String JDK_PLUGIN_PACKAGE = "com.sun.imageio.plugins.";
  /** Bytes of a WebP container read to find the first chunk's dimensions. */
  private static final int WEBP_HEADER_BYTES = 30;

  private ImageHeaderReader() {
  }

  /**
   * @param in positioned at the image's first byte; left at an unspecified position
   * @throws ImageHeaderException when no reader recognises the content, the header is truncated or corrupt, or a dimension
   *     is not positive
   */
  public static ImageHeader read(ImageInputStream in) throws ImageHeaderException {
    ImageReader reader = firstReader(in);
    if (reader == null) {
      return readWebp(in);
    }
    try {
      reader.setInput(in, true, true);
      return headerOf(reader);
    } finally {
      reader.dispose();
    }
  }

  /**
   * The header of image 0 of a reader whose input is set.
   *
   * @throws ImageHeaderException when the header is truncated or corrupt, or a dimension is not positive
   */
  static ImageHeader headerOf(ImageReader reader) throws ImageHeaderException {
    String format = formatOf(reader);
    try {
      return checked(new ImageHeader(format, reader.getWidth(0), reader.getHeight(0)));
    } catch (IOException | RuntimeException e) {
      // a reader signals a corrupt header with IIOException, or with unchecked exceptions (index, state) on malformed data
      throw new ImageHeaderException("corrupt " + format + " image header: " + describe(e), e);
    }
  }

  /** {@code <class>: <message>}, followed by the root cause's when the reader wrapped one (e.g. a read cap). */
  static String describe(Throwable e) {
    Throwable root = SourceCauses.getRootCause(e);
    String text = e.getClass().getSimpleName() + ": " + e.getMessage();
    return root == e ? text : text + " (caused by " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
  }

  /**
   * The first JDK built-in ImageIO reader that recognises the content at the stream's position, or null. Readers from any
   * other plugin on the classpath (JAI, TwelveMonkeys, ...) are ignored: what prep accepts and how compute decodes an image are
   * defined over the JDK readers, so a plugin that happens to be registered first must not change either. The probe leaves the
   * stream where it found it (ImageIO marks and resets around each {@code canDecodeInput}); readers not returned are disposed.
   * A provider that fails during the walk is skipped; the chosen reader is disposed too only when anything else fails the walk
   * (a JVM Error, say), as {@code firstJdk} describes.
   */
  static ImageReader firstReader(ImageInputStream in) {
    return firstJdk(ImageIO.getImageReaders(in), ImageHeaderReader::isJdkReader, ImageReader::dispose);
  }

  /**
   * The first of {@code candidates} that {@code isJdk} accepts, or null; every other candidate is disposed. A provider that
   * fails to create its candidate is skipped ({@link #nextCandidate}). Any other failure of the walk (a JVM Error out of the
   * iterator, say): the chosen candidate is then disposed too, never stranded, and the failure propagates, any failure of that
   * dispose suppressed.
   */
  static <T> T firstJdk(Iterator<T> candidates, Predicate<T> isJdk, Consumer<T> dispose) {
    T chosen = null;
    try {
      while (candidates.hasNext()) {
        T candidate = nextCandidate(candidates);
        if (candidate == null) {
          continue;
        }
        if (chosen == null && isJdk.test(candidate)) {
          chosen = candidate;
        } else {
          dispose.accept(candidate);
        }
      }
      return chosen;
      // by design: on any failure of the walk, an Error included, the chosen candidate is disposed and the failure rethrown
    } catch (RuntimeException | Error e) {
      if (chosen != null) {
        try {
          dispose.accept(chosen);
          // by design: a failure of that dispose, an Error included, is kept as suppressed on the walk's failure
        } catch (RuntimeException | Error suppressed) {
          e.addSuppressed(suppressed);
        }
      }
      throw e;
    }
  }

  /**
   * The provider walk's next candidate, or null when its provider failed to create it. ImageIO yields null for a provider whose
   * {@code create*Instance} throws IOException (and deregisters that provider), and lets an unchecked failure of that method,
   * or of a later provider's {@code canDecodeInput}, out of {@code next} after it moved past the failing provider. Such a
   * provider (a third-party plugin missing a native or a licence, say) offers nothing to choose or dispose, and must not hide
   * the JDK's reader or writer, so the walk goes on past it.
   */
  private static <T> T nextCandidate(Iterator<T> candidates) {
    try {
      return candidates.next();
    } catch (RuntimeException e) {
      LOG.warn("An ImageIO provider failed during the walk; skipping it", e);
      return null;
    }
  }

  /** Whether {@code reader} comes from a JDK built-in ImageIO plugin. */
  static boolean isJdkReader(ImageReader reader) {
    return isJdkProvider(reader.getOriginatingProvider());
  }

  /**
   * The first JDK built-in ImageIO writer of {@code format}, or null. Writers from any other plugin are ignored for the reason
   * {@link #firstReader} ignores their readers: a plugin that happens to be registered first must not change the bytes an image
   * is encoded to. Writers not returned are disposed. A provider that fails during the walk is skipped; the chosen writer is
   * disposed too only when anything else fails the walk (a JVM Error, say), as {@code firstJdk} describes.
   */
  public static ImageWriter firstJdkWriter(String format) {
    return firstJdk(ImageIO.getImageWritersByFormatName(format), writer -> isJdkProvider(writer.getOriginatingProvider()),
        ImageWriter::dispose);
  }

  private static boolean isJdkProvider(ImageReaderWriterSpi provider) {
    return provider != null && provider.getClass().getName().startsWith(JDK_PLUGIN_PACKAGE);
  }

  /** The reader's format name, {@link #normalize normalized}; {@code unknown} when the reader cannot name it. */
  static String formatOf(ImageReader reader) {
    try {
      return normalize(reader.getFormatName());
    } catch (IOException e) {
      return "unknown";
    }
  }

  /** {@code jpg} → {@code jpeg}, {@code tif} → {@code tiff}, otherwise the reader's name in lower case. */
  static String normalize(String formatName) {
    String format = formatName.toLowerCase(Locale.ROOT);
    switch (format) {
      case "jpg":
        return "jpeg";
      case "tif":
        return "tiff";
      default:
        return format;
    }
  }

  /** WebP: RIFF container, then a VP8X (extended), VP8L (lossless) or "VP8 " (lossy) first chunk. */
  private static ImageHeader readWebp(ImageInputStream in) throws ImageHeaderException {
    byte[] header = new byte[WEBP_HEADER_BYTES];
    int read;
    try {
      in.seek(0);
      read = readUpTo(in, header);
    } catch (IOException e) {
      throw new ImageHeaderException("cannot read the image header: " + e.getMessage(), e);
    }
    if (read < WEBP_HEADER_BYTES || !ascii(header, 0, "RIFF") || !ascii(header, 8, "WEBP")) {
      throw new ImageHeaderException("no image reader recognises the content");
    }
    if (ascii(header, 12, "VP8X")) {
      return checked(new ImageHeader(WEBP, 1 + le24(header, 24), 1 + le24(header, 27)));
    }
    if (ascii(header, 12, "VP8L")) {
      if ((header[20] & 0xff) != 0x2f) {
        throw new ImageHeaderException("corrupt webp image header: bad VP8L signature");
      }
      int bits = le24(header, 21) | ((header[24] & 0xff) << 24);
      return checked(new ImageHeader(WEBP, 1 + (bits & 0x3fff), 1 + ((bits >>> 14) & 0x3fff)));
    }
    if (ascii(header, 12, "VP8 ")) {
      if ((header[23] & 0xff) != 0x9d || (header[24] & 0xff) != 0x01 || (header[25] & 0xff) != 0x2a) {
        throw new ImageHeaderException("corrupt webp image header: bad VP8 start code");
      }
      return checked(new ImageHeader(WEBP, le16(header, 26) & 0x3fff, le16(header, 28) & 0x3fff));
    }
    throw new ImageHeaderException("corrupt webp image header: unknown first chunk");
  }

  private static ImageHeader checked(ImageHeader header) throws ImageHeaderException {
    if (header.getWidth() <= 0 || header.getHeight() <= 0) {
      throw new ImageHeaderException("image header declares a non-positive dimension: " + header);
    }
    return header;
  }

  private static int readUpTo(ImageInputStream in, byte[] buffer) throws IOException {
    int total = 0;
    while (total < buffer.length) {
      int n = in.read(buffer, total, buffer.length - total);
      if (n < 0) {
        break;
      }
      total += n;
    }
    return total;
  }

  private static boolean ascii(byte[] bytes, int offset, String expected) {
    return new String(bytes, offset, expected.length(), StandardCharsets.US_ASCII).equals(expected);
  }

  private static int le16(byte[] bytes, int offset) {
    return (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8);
  }

  private static int le24(byte[] bytes, int offset) {
    return le16(bytes, offset) | ((bytes[offset + 2] & 0xff) << 16);
  }
}
