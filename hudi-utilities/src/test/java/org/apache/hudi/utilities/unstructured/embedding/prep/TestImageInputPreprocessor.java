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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data prep of image files: one header-validated row per image whose payload is the image's source bytes, unchanged
 * (no decode, no resize), typed by the format the header declares; every refusal fails the file, and a storage failure that is
 * not the file's own is an interruption of the round.
 */
public class TestImageInputPreprocessor {

  @ParameterizedTest
  @CsvSource({"png,png,png,image/png", "jpg,jpeg,jpeg,image/jpeg", "jpeg,jpeg,jpeg,image/jpeg", "gif,gif,gif,image/gif",
      "bmp,bmp,bmp,image/bmp", "tif,tiff,tiff,image/tiff", "tiff,tiff,tiff,image/tiff"})
  public void oneRowPerImageCarryingItsSourceBytes(String extension, String writeFormat, String expectedFormat,
                                                    String expectedContentType) throws IOException {
    byte[] source = ImageFixtures.image(writeFormat, 97, 41);
    List<Chunk> chunks = prep(source, extension, new TypedProperties());
    assertEquals(1, chunks.size());
    Chunk chunk = chunks.get(0);
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, chunk.getModality());
    assertNull(chunk.getText());
    Map<String, String> expectedAux = new HashMap<>();
    expectedAux.put(ImageInputPreprocessor.AUX_WIDTH, "97");
    expectedAux.put(ImageInputPreprocessor.AUX_HEIGHT, "41");
    expectedAux.put(ImageInputPreprocessor.AUX_FORMAT, expectedFormat);
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, "97");
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, "41");
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, ImageInputPreprocessor.INPUT_LOGIC);
    assertEquals(expectedAux, chunk.getAux());
    // the payload is the source, byte for byte: no decode, no resize
    assertTrue(chunk.isPayloadSource(), "the framework stores the source row's own blob value");
    assertEquals(expectedContentType, chunk.getPayloadContentType());
    assertEquals(source.length, chunk.getPayloadSizeBytes());
    try (InputStream in = chunk.getPayload().open()) {
      assertArrayEquals(source, FileIOUtils.readAsByteArray(in));
    }
    assertTrue(ImageContentTypes.stored().contains(chunk.getPayloadContentType()));
  }

  @Test
  public void declaresThatItStoresInputs() {
    assertTrue(new ImageInputPreprocessor().storesInputs());
    assertEquals("image-input-v1", new ImageInputPreprocessor().name());
    assertEquals(SourceKind.IMAGE, new ImageInputPreprocessor().sourceKind());
  }

  @Test
  public void webpHeaderIsReadButNotStored() {
    // the JVM has no WebP reader: the refusal is recorded once at prep, not once per model at compute
    PreprocessingException refused = assertThrows(PreprocessingException.class,
        () -> prep(ImageFixtures.webpHeader(640, 480), "webp", new TypedProperties()));
    assertEquals("image webp 640x480 is not stored: the stored image types are [bmp, gif, jpeg, png, tiff]", refused.getMessage());
  }

  @Test
  public void imageAboveTheByteGuardFailsBeforeAnyRead() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "100");
    byte[] png = ImageFixtures.image("png", 20, 10);
    assertTrue(png.length > 100);
    FileMeta meta = new FileMeta("fid-1", "png", png.length);
    BlobBytesSupplier neverRead = () -> {
      throw new AssertionError("the byte guard reads nothing");
    };
    PreprocessingException e = assertThrows(PreprocessingException.class,
        () -> new ImageInputPreprocessor().open(neverRead, meta, null, props));
    assertEquals("image of " + png.length + " bytes is above " + EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key()
        + " = 100; its source bytes are not read", e.getMessage());
    // the neighbour at the guard: exactly max.bytes is stored
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), String.valueOf(png.length));
    assertDoesNotThrow(() -> prep(png, "png", props));
  }

  /**
   * The row stores the source's own blob value, never a copy prep reads, so by default no byte size refuses an image: one above
   * what one array holds is one chunk whose payload is its whole file, read only at its header here.
   */
  @Test
  public void anImageOfAnySizeIsStoredByDefault() throws IOException {
    byte[] png = ImageFixtures.image("png", 20, 10);
    long fileSize = 3L * 1024 * 1024 * 1024;
    FileMeta meta = new FileMeta("fid-1", "png", fileSize);
    List<Chunk> chunks = new ArrayList<>();
    try (ClosableIterator<Chunk> it = new ImageInputPreprocessor().open(seekable(png), meta, null, new TypedProperties())) {
      it.forEachRemaining(chunks::add);
    }
    assertEquals(1, chunks.size());
    assertEquals(fileSize, chunks.get(0).getPayloadSizeBytes());
    assertEquals(ImageContentTypes.PNG, chunks.get(0).getPayloadContentType());
  }

  @Test
  public void byteGuardConfigIsValidated() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "0");
    assertThrows(IllegalArgumentException.class, () -> new ImageInputPreprocessor().validateConfig(props, null));
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "-1");
    assertThrows(IllegalArgumentException.class, () -> new ImageInputPreprocessor().validateConfig(props, null));
    // prep reads no payload, so a limit beyond what one array holds is accepted
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), String.valueOf(8L * Integer.MAX_VALUE));
    assertDoesNotThrow(() -> new ImageInputPreprocessor().validateConfig(props, null));
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "1");
    assertDoesNotThrow(() -> new ImageInputPreprocessor().validateConfig(props, null));
  }

  /**
   * a storage failure opening or reading the source that is not its absence (a throttled or reset read) is a
   * {@link SourceReadException}, an interruption of the round, never the file's FAILED row; a missing source, a short source and
   * a corrupt header stay content failures of the file (the neighbours).
   */
  @Test
  public void storageFailuresOfTheSourceAreInterruptions() {
    FileMeta meta = new FileMeta("fid-503", "png", 1234);
    SourceReadException atOpen = assertThrows(SourceReadException.class, () -> new ImageInputPreprocessor().open(
        failingOpen(new IOException("503 Slow Down")), meta, null, new TypedProperties()));
    assertEquals("503 Slow Down", rootCause(atOpen).getMessage());
    SourceReadException hoodieAtOpen = assertThrows(SourceReadException.class, () -> new ImageInputPreprocessor().open(
        failingOpenUnchecked(new HoodieIOException("Exception creating input stream", new IOException("connection reset"))), meta,
        null, new TypedProperties()));
    assertEquals("connection reset", rootCause(hoodieAtOpen).getMessage());
    byte[] png = ImageFixtures.image("png", 20, 10);
    SourceReadException atRead = assertThrows(SourceReadException.class, () -> new ImageInputPreprocessor().open(
        failingRead(png, new IOException("connection reset by peer")), new FileMeta("fid-rst", "png", png.length), null,
        new TypedProperties()));
    assertEquals("connection reset by peer", rootCause(atRead).getMessage());

    // neighbours: the source is gone (at open, wrapped by the blob reader, or at a read), short, or corrupt: content failures
    HoodieIOException gone = assertThrows(HoodieIOException.class, () -> new ImageInputPreprocessor().open(
        failingOpenUnchecked(new HoodieIOException("Exception creating input stream", new FileNotFoundException("gone.png"))),
        meta, null, new TypedProperties()));
    assertFalse(gone instanceof SourceReadException, gone.toString());
    IOException goneAtRead = assertThrows(IOException.class, () -> new ImageInputPreprocessor().open(
        failingRead(png, new FileNotFoundException("gone.png")), new FileMeta("fid-gone", "png", png.length), null,
        new TypedProperties()));
    assertTrue(goneAtRead.getCause() == null || !(goneAtRead.getCause() instanceof SourceReadException), goneAtRead.toString());
    assertThrows(ImageHeaderException.class, () -> new ImageInputPreprocessor().open(seekable(Arrays.copyOf(png, 12)),
        new FileMeta("fid-short", "png", 12), null, new TypedProperties()));
    assertThrows(ImageHeaderException.class, () -> new ImageInputPreprocessor().open(seekable(ImageFixtures.garbage()),
        new FileMeta("fid-garbage", "png", 64), null, new TypedProperties()));
  }

  /**
   * A malformed blob row (BlobReferenceException from the blob reader) and a source that ends at open (an
   * EOFException, e.g. an offset beyond the file) are content failures of the file, never SourceReadException interruptions that
   * every rerun would hit again.
   */
  @Test
  public void malformedRowsAndEofAtOpenAreContentFailures() {
    FileMeta meta = new FileMeta("fid-bad", "png", 1234);
    IOException malformed = assertThrows(IOException.class, () -> new ImageInputPreprocessor().open(
        failingOpen(new BlobReferenceException("OUT_OF_LINE blob has null reference")), meta, null, new TypedProperties()));
    assertTrue(malformed instanceof BlobReferenceException, malformed.toString());
    IOException eofAtOpen = assertThrows(IOException.class, () -> new ImageInputPreprocessor().open(
        failingOpen(new BlobTruncatedException("seek beyond EOF")), meta, null, new TypedProperties()));
    assertTrue(eofAtOpen instanceof EOFException, eofAtOpen.toString());
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root;
  }

  private static SeekableBlobBytesSupplier failingOpen(IOException failure) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() throws IOException {
        throw failure;
      }

      @Override
      public SeekableBlob openSeekable() throws IOException {
        throw failure;
      }
    };
  }

  private static SeekableBlobBytesSupplier failingOpenUnchecked(RuntimeException failure) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() {
        throw failure;
      }

      @Override
      public SeekableBlob openSeekable() {
        throw failure;
      }
    };
  }

  /** Opens fine; its first read past byte 8 fails with {@code failure}, as a storage read does mid-file. */
  private static SeekableBlobBytesSupplier failingRead(byte[] bytes, IOException failure) {
    SeekableBlobBytesSupplier healthy = seekable(bytes);
    return new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() {
        return new ByteArrayInputStream(bytes);
      }

      @Override
      public SeekableBlob openSeekable() throws IOException {
        SeekableBlob blob = healthy.openSeekable();
        return new SeekableBlob() {
          @Override
          public long length() {
            return blob.length();
          }

          @Override
          public long getPos() throws IOException {
            return blob.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            blob.seek(newPos);
          }

          @Override
          public int read(byte[] buffer, int offset, int len) throws IOException {
            if (blob.getPos() >= 8) {
              throw failure;
            }
            return blob.read(buffer, offset, (int) Math.min(len, 8 - blob.getPos()));
          }

          @Override
          public void close() throws IOException {
            blob.close();
          }
        };
      }
    };
  }

  @Test
  public void missingSourceFailsAtTheHeaderRead() {
    // a source that cannot be opened fails its header read inside open(), a content failure of the file
    FileMeta meta = new FileMeta("fid-gone", "png", 1234);
    SeekableBlobBytesSupplier gone = new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() throws IOException {
        throw new FileNotFoundException("file:/landing/gone.png");
      }

      @Override
      public SeekableBlob openSeekable() throws IOException {
        throw new FileNotFoundException("file:/landing/gone.png");
      }
    };
    assertThrows(FileNotFoundException.class, () -> new ImageInputPreprocessor().open(gone, meta, null, new TypedProperties()));
  }

  @Test
  public void corruptHeaderFailsTheFile() {
    ImageHeaderException truncated = assertThrows(ImageHeaderException.class,
        () -> prep(ImageFixtures.truncatedPng(), "png", new TypedProperties()));
    assertTrue(truncated.getMessage().startsWith("corrupt png image header"), truncated.getMessage());
    assertThrows(ImageHeaderException.class, () -> prep(ImageFixtures.garbage(), "jpg", new TypedProperties()));
  }

  /** A header-corrupt file of each stored type and extension fails the file at prep. */
  @ParameterizedTest
  @CsvSource({"png, png", "jpeg, jpg", "jpeg, jpeg", "gif, gif", "bmp, bmp", "tiff, tif", "tiff, tiff"})
  public void headerCorruptFileOfEachClaimedTypeFailsTheFile(String format, String extension) {
    ImageHeaderException e = assertThrows(ImageHeaderException.class,
        () -> prep(ImageFixtures.headerCorrupt(format), extension, new TypedProperties()));
    assertTrue(e.getMessage().startsWith("corrupt " + format + " image header: "), e.getMessage());
  }

  @Test
  public void imageAbovePrepPixelGuardFailsTheFile() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key(), "100");
    PreprocessingException e = assertThrows(PreprocessingException.class,
        () -> prep(ImageFixtures.image("png", 20, 10), "png", props));
    assertTrue(e.getMessage().contains("200 pixels"), e.getMessage());
    assertTrue(e.getMessage().contains(EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key()), e.getMessage());
  }

  @Test
  public void headerBeyondTheCapFailsTheFile() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), "10");
    // the PNG reader wraps the cap's exception in its own IIOException; the cause is named in the message
    IOException e = assertThrows(IOException.class, () -> prep(ImageFixtures.image("png", 20, 10), "png", props));
    assertTrue(e.getMessage().contains("image header not found within 10 bytes"), e.getMessage());
  }

  @Test
  public void tiffWithItsDirectoryAfterTheCapIsReadThroughTheSeekableSupplier() throws IOException {
    // libtiff writers put the IFD after the pixel data; a byte-offset cap fails such files, a seekable read does not
    byte[] tiff = ImageFixtures.singleStripTiff(400, 300);   // IFD at offset 360008
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), String.valueOf(64 * 1024));
    FileMeta meta = new FileMeta("fid-tif", "tif", tiff.length);
    List<Chunk> chunks = new ArrayList<>();
    try (ClosableIterator<Chunk> it = new ImageInputPreprocessor().open(seekable(tiff), meta, null, props)) {
      it.forEachRemaining(chunks::add);
    }
    assertEquals(1, chunks.size());
    assertEquals("400", chunks.get(0).getAux().get(ImageInputPreprocessor.AUX_WIDTH));
    assertEquals("tiff", chunks.get(0).getAux().get(ImageInputPreprocessor.AUX_FORMAT));
    assertEquals(tiff.length, chunks.get(0).getPayloadSizeBytes());
    // neighbour: a plain stream supplier still has only the capped, memory-cached read, and refuses the same file
    IOException capped = assertThrows(IOException.class, () -> prep(tiff, "tif", props));
    assertTrue(capped.getMessage().contains("image header not found within 65536 bytes"), capped.getMessage());
  }

  @Test
  public void seekableHeaderReadIsBoundedByBytesRead() throws IOException {
    // blob-table rows always come with random access, so the header budget must bound the bytes READ there too
    byte[] tiff = ImageFixtures.singleStripTiff(400, 300);
    TypedProperties tight = new TypedProperties();
    tight.setProperty(EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), "16");
    FileMeta meta = new FileMeta("fid-tif", "tif", tiff.length);
    IOException refused = assertThrows(IOException.class,
        () -> new ImageInputPreprocessor().open(seekable(tiff), meta, null, tight).close());
    assertTrue(refused.getMessage().contains("image header not found within 16 bytes"), refused.getMessage());
  }

  /** A supplier over {@code bytes} that also offers random access, as the prep framework hands one to a preprocessor. */
  private static SeekableBlobBytesSupplier seekable(byte[] bytes) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() {
        return new ByteArrayInputStream(bytes);
      }

      @Override
      public SeekableBlob openSeekable() {
        return new SeekableBlob() {
          private int pos;

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
          public int read(byte[] buffer, int offset, int len) {
            if (pos >= bytes.length) {
              return -1;
            }
            int n = Math.min(len, bytes.length - pos);
            System.arraycopy(bytes, pos, buffer, offset, n);
            pos += n;
            return n;
          }

          @Override
          public void close() {
          }
        };
      }
    };
  }

  @Test
  public void registryMapsTheSevenImageExtensionsToTheInputPreprocessor() {
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    for (String extension : new String[] {"png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff", "PNG"}) {
      assertTrue(registry.forExtension(extension).get() instanceof ImageInputPreprocessor, extension);
    }
    assertEquals(ImageInputPreprocessor.NAME, registry.forExtension("tiff").get().name());
    // pdf keeps its own default, the PDF chunker
    assertTrue(registry.forExtension("pdf").get() instanceof PdfFilePreprocessor);
  }

  /** A class configured for an image extension replaces its default; the other image extensions keep theirs. */
  @Test
  public void configuredClassReplacesTheImageDefault() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "tif" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        TextPipePreprocessor.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    assertTrue(registry.forExtension("tif").get() instanceof TextPipePreprocessor);
    assertTrue(registry.forExtension("tiff").get() instanceof ImageInputPreprocessor);
    assertEquals(Arrays.asList("png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff"), PreprocessorRegistry.IMAGE_EXTENSIONS);
  }

  /**
   * WebP has no default preprocessor (the image preprocessor does not store it): a webp source row is prep's no_preprocessor
   * case.
   */
  @Test
  public void webpHasNoDefaultPreprocessor() {
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    for (String extension : new String[] {"webp", "WEBP"}) {
      assertFalse(registry.forExtension(extension).isPresent(), extension);
    }
    assertFalse(PreprocessorRegistry.IMAGE_EXTENSIONS.contains("webp"), PreprocessorRegistry.IMAGE_EXTENSIONS.toString());
  }

  private static List<Chunk> prep(byte[] bytes, String extension, TypedProperties props) throws IOException {
    FileMeta meta = new FileMeta("fid-1", extension, bytes.length);
    List<Chunk> chunks = new ArrayList<>();
    try (ClosableIterator<Chunk> it = new ImageInputPreprocessor().open(() -> new ByteArrayInputStream(bytes), meta, null, props)) {
      it.forEachRemaining(chunks::add);
    }
    return chunks;
  }
}
