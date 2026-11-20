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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeader;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderReader;
import org.apache.hudi.utilities.unstructured.embedding.image.SeekableBlobImageInputStream;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Data prep of a standalone image file, {@value #NAME} ("image -> image (no resize)"): it validates the header WITHOUT decoding a
 * pixel and emits exactly ONE chunk, {@code modality=image}, no text, whose payload is the file's SOURCE bytes, unchanged
 * ({@link Chunk.Builder#sourcePayload}): the framework writes the source row's own blob value into the row's
 * {@code prepared_image} (its inline bytes, or its reference to the object), and compute resizes the image in memory before the
 * model call. Aux records what the header declares, {@value #AUX_WIDTH}, {@value #AUX_HEIGHT} and {@value #AUX_FORMAT} (the
 * format recognised from the content, not the extension), and the stored input's {@link EmbeddingTableSchema#AUX_INPUT_WIDTH} /
 * {@link EmbeddingTableSchema#AUX_INPUT_HEIGHT} (the source's: nothing is resized) and
 * {@link EmbeddingTableSchema#AUX_INPUT_LOGIC} = {@value #INPUT_LOGIC}.
 *
 * <p>Every refusal is a quarantine entry of the file (the framework's per-file isolation), raised before any row is emitted: a
 * {@code file_size} above {@link EmbeddingTableConfig#PREP_IMAGE_MAX_BYTES} when an operator set one (nothing read), no reader
 * recognises the content, a truncated or corrupt header, a dimension that is not positive, a header declaring more than
 * {@link EmbeddingTableConfig#PREP_IMAGE_MAX_PIXELS} pixels, a header not found within
 * {@link EmbeddingTableConfig#PREP_IMAGE_HEADER_MAX_BYTES} distinct bytes read, or a format that is not stored (WebP: the stored types are
 * {@link ImageContentTypes#stored}). A source that is gone or short, or a blob row that cannot name a readable range, fails its
 * header read, so it is a quarantine entry as well, never a round that fails again on every rerun; any other storage failure
 * opening or reading it (a throttled or reset read) is a {@link SourceReadException}, an interruption of the round, so a source a
 * later run can read is never marked FAILED.
 *
 * <p>Memory: the header read is bounded by {@link EmbeddingTableConfig#PREP_IMAGE_HEADER_MAX_BYTES} (random access over the
 * source row: the file's leading bytes up to that bound, fetched in one request, then only the bytes the reader asks for past
 * them); the payload is never read in prep, since the row stores the source's own blob
 * value, so an image of any size is prepared in bounded memory.
 */
public class ImageInputPreprocessor implements FilePreprocessor {

  public static final String NAME = "image-input-v1";
  /** {@link EmbeddingTableSchema#AUX_INPUT_LOGIC} of a stored standalone image: its source bytes, no decode, no resize. */
  public static final String INPUT_LOGIC = NAME;
  public static final String AUX_WIDTH = "width";
  public static final String AUX_HEIGHT = "height";
  public static final String AUX_FORMAT = "format";
  /** Content type a stored image of each header format gets: the five stored types. */
  private static final Map<String, String> CONTENT_TYPES;

  static {
    Map<String, String> types = new HashMap<>();
    types.put("png", ImageContentTypes.PNG);
    types.put("jpeg", ImageContentTypes.JPEG);
    types.put("gif", ImageContentTypes.GIF);
    types.put("bmp", ImageContentTypes.BMP);
    types.put("tiff", ImageContentTypes.TIFF);
    CONTENT_TYPES = Collections.unmodifiableMap(types);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.IMAGE;
  }

  /** An image row stores its source bytes as the model input, in {@code prepared_image}. */
  @Override
  public boolean storesInputs() {
    return true;
  }

  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    maxBytes(props);
    positive(props, EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key(), EmbeddingTableConfig.DEFAULT_PREP_IMAGE_MAX_PIXELS);
    positive(props, EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), EmbeddingTableConfig.DEFAULT_PREP_IMAGE_HEADER_MAX_BYTES);
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
      throws IOException {
    long maxBytes = maxBytes(props);
    if (file.getFileSize() > maxBytes) {
      throw new PreprocessingException("image of " + file.getFileSize() + " bytes is above "
          + EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key() + " = " + maxBytes + "; its source bytes are not read");
    }
    long maxPixels = positive(props, EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key(), EmbeddingTableConfig.DEFAULT_PREP_IMAGE_MAX_PIXELS);
    long headerMaxBytes = positive(props, EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(),
        EmbeddingTableConfig.DEFAULT_PREP_IMAGE_HEADER_MAX_BYTES);
    ImageHeader header = readHeader(bytes, headerMaxBytes);
    if (header.pixels() > maxPixels) {
      throw new PreprocessingException("image " + header + " declares " + header.pixels() + " pixels, above "
          + EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key() + " = " + maxPixels);
    }
    String contentType = CONTENT_TYPES.get(header.getFormat());
    if (contentType == null) {
      throw new PreprocessingException("image " + header + " is not stored: the stored image types are "
          + new TreeSet<>(CONTENT_TYPES.keySet()));
    }
    Map<String, String> aux = new HashMap<>();
    aux.put(AUX_WIDTH, String.valueOf(header.getWidth()));
    aux.put(AUX_HEIGHT, String.valueOf(header.getHeight()));
    aux.put(AUX_FORMAT, header.getFormat());
    aux.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, String.valueOf(header.getWidth()));
    aux.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, String.valueOf(header.getHeight()));
    aux.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, INPUT_LOGIC);
    // the payload is the source itself, unchanged: the framework stores the source row's blob value, without reading it
    Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).aux(aux)
        .sourcePayload(bytes, file.getFileSize(), contentType).build();
    return ClosableIterator.wrap(Collections.singletonList(chunk).iterator());
  }

  /**
   * The header of the image, read without decoding a pixel and without reading past {@code headerMaxBytes}.
   *
   * @throws SourceReadException when opening or reading the source fails in storage for a reason other than the source's absence
   * or shortness or a malformed blob row (an interruption): the blob reader's failure at open, or a
   *     {@link BlobReadException} the image reader wrapped, whose causes are no content failure ({@link SourceFailures}: a
   *     confirmed absence, a {@code BlobTruncatedException} or a {@link BlobReferenceException}; an access denial never is)
   * @throws IOException           a content failure of the file: the source is missing or short, its blob row malformed, or its
   *     header unreadable
   */
  private static ImageHeader readHeader(BlobBytesSupplier bytes, long headerMaxBytes) throws IOException {
    if (bytes instanceof SeekableBlobBytesSupplier) {
      SeekableBlob blob;
      try {
        blob = ((SeekableBlobBytesSupplier) bytes).openSeekable();
      } catch (IOException | HoodieIOException e) {
        if (SourceFailures.isContentFailure(e)) {
          throw e;
        }
        throw new SourceReadException("Failed to open the image source", e);
      }
      // random access: the leading bytes in one request, then only the bytes the reader asks for, wherever the header lies (a TIFF
      // IFD may be at the end)
      try (ImageInputStream in = new SeekableBlobImageInputStream(blob, headerMaxBytes)) {
        return ImageHeaderReader.read(in);
      } catch (ImageHeaderException e) {
        BlobReadException storage = blobReadFailure(e);
        if (storage == null || SourceFailures.isContentFailure(storage)) {
          throw e;
        }
        throw new SourceReadException("Failed to read the image source", storage);
      }
    }
    try (InputStream raw = bytes.open();
         ImageInputStream in = new MemoryCacheImageInputStream(new CappedInputStream(raw, headerMaxBytes))) {
      return ImageHeaderReader.read(in);
    }
  }

  /** The storage failure the image reader wrapped while reading the header, or null when the header itself is at fault. */
  private static BlobReadException blobReadFailure(Throwable failure) {
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (cause instanceof BlobReadException) {
        return (BlobReadException) cause;
      }
    }
    return null;
  }

  /** {@link EmbeddingTableConfig#PREP_IMAGE_MAX_BYTES}: positive; no limit by default. */
  private static long maxBytes(TypedProperties props) {
    return positive(props, EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), EmbeddingTableConfig.DEFAULT_PREP_IMAGE_MAX_BYTES);
  }

  /** {@link EmbeddingTableConfig#PREP_IMAGE_HEADER_MAX_BYTES}, positive, read as this preprocessor reads it. */
  static long headerMaxBytes(TypedProperties props) {
    return positive(props, EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), EmbeddingTableConfig.DEFAULT_PREP_IMAGE_HEADER_MAX_BYTES);
  }

  private static long positive(TypedProperties props, String key, long defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), String.valueOf(defaultValue));
    long value;
    try {
      value = Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + key + " must be an integer, got '" + raw + "'", e);
    }
    if (value <= 0) {
      throw new IllegalArgumentException("Config " + key + " must be positive, got " + value);
    }
    return value;
  }

  /** Fails a read that would go past {@code cap} bytes: the image's header was not found within the cap. */
  private static final class CappedInputStream extends FilterInputStream {
    private final long cap;
    private long read;

    CappedInputStream(InputStream in, long cap) {
      super(in);
      this.cap = cap;
    }

    @Override
    public int read() throws IOException {
      checkCap(1);
      int b = super.read();
      if (b >= 0) {
        read++;
      }
      return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      int allowed = (int) Math.min(len, cap - read);
      if (allowed <= 0) {
        checkCap(len);
      }
      int n = super.read(buffer, offset, allowed);
      if (n > 0) {
        read += n;
      }
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      checkCap(n);
      long skipped = super.skip(n);
      read += skipped;
      return skipped;
    }

    private void checkCap(long wanted) throws ImageHeaderException {
      if (wanted > 0 && read + wanted > cap) {
        throw new ImageHeaderException("image header not found within " + cap + " bytes ("
            + EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key() + ")");
      }
    }
  }
}
