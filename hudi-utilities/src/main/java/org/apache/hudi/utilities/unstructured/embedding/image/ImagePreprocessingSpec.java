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

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import java.io.Serializable;

/**
 * How the compute flow turns an image into the model input ("image -> resized -> embedding"), read from the compute keys
 * {@code hoodie.unstructured.embedding.compute.image.*} ({@link EmbeddingTableConfig#imagePreprocessing()}):
 * <ul>
 *   <li>{@link #longSide()}: the resized image's long side in pixels; an image is never upscaled;</li>
 *   <li>{@link #rasterBudgetBytes()}: the most bytes the decoded (subsampled) raster may take, which picks the subsampling;</li>
 *   <li>{@link #maxPixels()}: images whose header declares more pixels are refused before decode;</li>
 *   <li>{@link #intermediateBudgetBytes()}: the most bytes a reader may need BEFORE subsampling applies (a TIFF strip or tile
 *       decoded at full resolution, the whole-image DCT coefficient buffers libjpeg keeps for a progressive JPEG, in native
 *       memory), estimated from the image metadata by {@link DecoderIntermediates}; above it the image is refused before decode;</li>
 *   <li>{@link #decoderClass()}: the {@link ImageDecoder}, which must run {@link ImageResizeLogicV1};</li>
 *   <li>{@link #encoding()} and {@link #jpegQuality()}: the encoding of the resized model input, JPEG at a quality or lossless
 *       PNG.</li>
 * </ul>
 */
public final class ImagePreprocessingSpec implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_LONG_SIDE = 512;
  public static final long DEFAULT_RASTER_BUDGET_BYTES = 16L * 1024 * 1024;
  public static final long DEFAULT_MAX_PIXELS = 100_000_000L;
  /**
   * Default {@link #intermediateBudgetBytes()}: 64 MiB. A 50 MP (8660x5774) RGB single-strip TIFF is one 150,008,520-byte strip
   * and a 50 MP 4:2:0 progressive JPEG holds 150,176,000 bytes of coefficients (both about 143 MiB); both are refused at this
   * default.
   */
  public static final long DEFAULT_INTERMEDIATE_BUDGET_BYTES = 64L * 1024 * 1024;
  /** {@link #encoding()} value: JPEG at {@link #jpegQuality()}. */
  public static final String ENCODING_JPEG = "jpeg";
  /** {@link #encoding()} value: lossless PNG. */
  public static final String ENCODING_PNG = "png";
  /** Default {@link #encoding()}: JPEG. */
  public static final String DEFAULT_ENCODING = ENCODING_JPEG;
  /** Default {@link #jpegQuality()}: 90. */
  public static final int DEFAULT_JPEG_QUALITY = 90;

  private final int longSide;
  private final long rasterBudgetBytes;
  private final long maxPixels;
  private final long intermediateBudgetBytes;
  private final String decoderClass;
  private final String encoding;
  private final int jpegQuality;

  /** A spec with the default {@link #DEFAULT_INTERMEDIATE_BUDGET_BYTES} and the default encoding (JPEG, quality 90). */
  public ImagePreprocessingSpec(int longSide, long rasterBudgetBytes, long maxPixels, String decoderClass) {
    this(longSide, rasterBudgetBytes, maxPixels, DEFAULT_INTERMEDIATE_BUDGET_BYTES, decoderClass);
  }

  /** A spec with the default encoding (JPEG, quality 90). */
  public ImagePreprocessingSpec(int longSide, long rasterBudgetBytes, long maxPixels, long intermediateBudgetBytes,
                                String decoderClass) {
    this(longSide, rasterBudgetBytes, maxPixels, intermediateBudgetBytes, decoderClass, DEFAULT_ENCODING, DEFAULT_JPEG_QUALITY);
  }

  /**
   * @param encoding    {@value #ENCODING_JPEG} or {@value #ENCODING_PNG}
   * @param jpegQuality JPEG quality in [1, 100]; ignored for PNG
   * @throws IllegalArgumentException for another encoding or a quality outside [1, 100]
   */
  public ImagePreprocessingSpec(int longSide, long rasterBudgetBytes, long maxPixels, long intermediateBudgetBytes,
                                String decoderClass, String encoding, int jpegQuality) {
    if (!ENCODING_JPEG.equals(encoding) && !ENCODING_PNG.equals(encoding)) {
      throw new IllegalArgumentException("Image encoding must be " + ENCODING_JPEG + " or " + ENCODING_PNG + ", got " + encoding);
    }
    if (jpegQuality < 1 || jpegQuality > 100) {
      throw new IllegalArgumentException("JPEG quality must be in [1, 100], got " + jpegQuality);
    }
    this.longSide = longSide;
    this.rasterBudgetBytes = rasterBudgetBytes;
    this.maxPixels = maxPixels;
    this.intermediateBudgetBytes = intermediateBudgetBytes;
    this.decoderClass = decoderClass;
    this.encoding = encoding;
    this.jpegQuality = jpegQuality;
  }

  /**
   * A new instance of the configured decoder.
   *
   * @throws IllegalArgumentException when the class does not load or is not an {@link ImageDecoder}
   * @throws ImageDecoderLogicMismatchException when it runs another logic version than {@link ImageResizeLogicV1}
   */
  public ImageDecoder newDecoder() {
    Object instance;
    try {
      instance = ReflectionUtils.loadClass(decoderClass);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Image decoder class " + decoderClass + " cannot be instantiated", e);
    }
    if (!(instance instanceof ImageDecoder)) {
      throw new IllegalArgumentException("Image decoder class " + decoderClass + " is not an " + ImageDecoder.class.getName());
    }
    ImageDecoder decoder = (ImageDecoder) instance;
    if (!ImageResizeLogicV1.LOGIC_VERSION.equals(decoder.logicVersion())) {
      throw new ImageDecoderLogicMismatchException("Image decoder " + decoderClass + " runs decoding logic "
          + decoder.logicVersion() + ", but this code resizes images with " + ImageResizeLogicV1.LOGIC_VERSION
          + "; a decoder may move (thread, process) but must run the same logic");
    }
    return decoder;
  }

  /**
   * The compute-side input transform this spec applies before a model call, for a VECTOR column's identity: the decoding logic's
   * pixel-transform version ({@link ImageResizeLogicV1#PIXEL_TRANSFORM_VERSION}, not its full logic version, which also changes
   * when only the refused images change) and every setting that changes the pixels or bytes sent (long side, raster budget,
   * which sets the decode subsampling, encoding and, for JPEG, the quality). The pixel cap and the intermediate budget only
   * refuse an image.
   */
  public String inputTransformVersion() {
    return ImageResizeLogicV1.PIXEL_TRANSFORM_VERSION + ";long.side=" + longSide() + ";raster.budget.bytes=" + rasterBudgetBytes()
        + ";encoding=" + encoding() + (ENCODING_JPEG.equals(encoding()) ? ";jpeg.quality=" + jpegQuality() : "");
  }

  public int longSide() {
    return longSide;
  }

  public long rasterBudgetBytes() {
    return rasterBudgetBytes;
  }

  public long maxPixels() {
    return maxPixels;
  }

  /** The most bytes a reader may need before subsampling applies ({@link DecoderIntermediates}). */
  public long intermediateBudgetBytes() {
    return intermediateBudgetBytes;
  }

  public String decoderClass() {
    return decoderClass;
  }

  /** {@value #ENCODING_JPEG} or {@value #ENCODING_PNG}: how the resized model input is encoded. */
  public String encoding() {
    return encoding;
  }

  /** JPEG quality in [1, 100] of a {@value #ENCODING_JPEG} model input. */
  public int jpegQuality() {
    return jpegQuality;
  }

  @Override
  public String toString() {
    return "long_side=" + longSide + ", raster_budget=" + rasterBudgetBytes + ", encoding=" + encoding
        + (ENCODING_JPEG.equals(encoding) ? ", quality=" + jpegQuality : "") + ", max_pixels=" + maxPixels
        + ", intermediate_budget=" + intermediateBudgetBytes + ", decoder=" + decoderClass;
  }
}
