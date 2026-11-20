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

import javax.imageio.IIOException;
import javax.imageio.IIOImage;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Decoding logic version {@value #LOGIC_VERSION}: decode image 0 with the source subsampling of the {@link RasterPlan}
 * (JDK ImageIO; the reader allocates only the subsampled destination), assert the raster
 * it allocated is within the plan, scale bilinearly so the long side is at most the target (never up), flatten any alpha on
 * white into 8-bit RGB, and encode the spec's {@link ImagePreprocessingSpec#encoding()}: JPEG at its quality or
 * lossless PNG. Any change to what this does to the pixels before the encoding needs a new logic version.
 */
public final class ImageResizeLogicV1 {

  /** The decoding-logic version of this class, owned by the code; every {@link ImageDecoder} must run it. */
  public static final String LOGIC_VERSION = "image-resize-v2";
  /** Versions the pixels and bytes of accepted images independently of decode refusals. */
  public static final String PIXEL_TRANSFORM_VERSION = "image-pixels-v1";
  /** Content type of a {@link ImagePreprocessingSpec#ENCODING_PNG} model input. */
  public static final String PNG_MIME_TYPE = "image/png";
  /** Content type of a {@link ImagePreprocessingSpec#ENCODING_JPEG} model input. */
  public static final String JPEG_MIME_TYPE = "image/jpeg";

  private ImageResizeLogicV1() {
  }

  /**
   * @throws ImageRefusedException when no reader in this JVM decodes the content
   * @throws IIOException          when the pixel data is corrupt
   * @throws IllegalStateException when the reader allocated more than the plan predicted (the budget assertion)
   */
  public static PreparedImage run(ImageSource source, RasterPlan plan, ImagePreprocessingSpec spec) throws IOException {
    BufferedImage decoded;
    try (ImageInputStream in = source.open()) {
      ImageReader reader = ImageHeaderReader.firstReader(in);
      if (reader == null) {
        throw new ImageRefusedException(ImageRefusedException.NO_DECODER + "no image reader in this JVM decodes the content");
      }
      try {
        reader.setInput(in, true, true);
        ImageReadParam param = reader.getDefaultReadParam();
        param.setSourceSubsampling(plan.subsampling(), plan.subsampling(), 0, 0);
        // the type RasterPlan sized the raster with: the reader's first image type
        param.setDestinationType(reader.getImageTypes(0).next());
        decoded = reader.read(0, param);
      } catch (RuntimeException e) {
        // readers report some malformed data with unchecked exceptions; to the caller it is corrupt pixel data
        throw new IIOException("corrupt image data: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
      } finally {
        reader.dispose();
      }
    }
    checkRaster(decoded, plan);
    BufferedImage resized = resize(decoded, spec.longSide());
    boolean jpeg = ImagePreprocessingSpec.ENCODING_JPEG.equals(spec.encoding());
    byte[] encoded = jpeg ? encode(resized, ImagePreprocessingSpec.ENCODING_JPEG, spec.jpegQuality())
        : encode(resized, ImagePreprocessingSpec.ENCODING_PNG, 0);
    return new PreparedImage(encoded, jpeg ? JPEG_MIME_TYPE : PNG_MIME_TYPE, resized.getWidth(), resized.getHeight(),
        plan.subsampling());
  }

  private static void checkRaster(BufferedImage decoded, RasterPlan plan) {
    DataBuffer buffer = decoded.getRaster().getDataBuffer();
    long allocated = (long) buffer.getSize() * buffer.getNumBanks() * (DataBuffer.getDataTypeSize(buffer.getDataType()) / 8);
    if (allocated > plan.predictedBytes()) {
      throw new IllegalStateException("Image raster budget assertion failed: the decode allocated " + allocated
          + " bytes, the plan " + plan + " predicted at most " + plan.predictedBytes());
    }
  }

  private static BufferedImage resize(BufferedImage decoded, int targetLongSide) {
    int width = decoded.getWidth();
    int height = decoded.getHeight();
    int longSide = Math.max(width, height);
    int targetWidth = width;
    int targetHeight = height;
    if (longSide > targetLongSide) {
      double scale = (double) targetLongSide / longSide;
      targetWidth = Math.max(1, (int) Math.round(width * scale));
      targetHeight = Math.max(1, (int) Math.round(height * scale));
    }
    BufferedImage resized = new BufferedImage(targetWidth, targetHeight, BufferedImage.TYPE_3BYTE_BGR);
    Graphics2D graphics = resized.createGraphics();
    try {
      graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
      graphics.setColor(Color.WHITE);
      graphics.fillRect(0, 0, targetWidth, targetHeight);
      graphics.drawImage(decoded, 0, 0, targetWidth, targetHeight, null);
    } finally {
      graphics.dispose();
    }
    return resized;
  }

  /**
   * {@code format} (jpeg or png) through an explicit writer on a memory-cached output stream: no temp file, no global ImageIO cache
   * setting. JPEG is written at {@code jpegQuality} / 100 with the writer's explicit compression mode; the image is 8-bit RGB.
   */
  private static byte[] encode(BufferedImage image, String format, int jpegQuality) throws IOException {
    // the JDK's writer, as the JDK's reader decodes (ImageHeaderReader): a plugin registered first must not change the bytes
    ImageWriter writer = ImageHeaderReader.firstJdkWriter(format);
    if (writer == null) {
      throw new IIOException("no JDK ImageIO writer for " + format);
    }
    ImageWriteParam param = null;
    if (ImagePreprocessingSpec.ENCODING_JPEG.equals(format)) {
      param = writer.getDefaultWriteParam();
      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
      param.setCompressionQuality(jpegQuality / 100f);
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ImageOutputStream out = new MemoryCacheImageOutputStream(bytes)) {
      writer.setOutput(out);
      writer.write(null, new IIOImage(image, null, null), param);
    } finally {
      writer.dispose();
    }
    return bytes.toByteArray();
  }
}
