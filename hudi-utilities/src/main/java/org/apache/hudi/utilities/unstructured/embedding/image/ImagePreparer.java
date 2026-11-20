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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.stream.ImageInputStream;

import java.io.IOException;
import java.util.Iterator;

/**
 * Turns one image into a model input for one model, in the compute flow (image -> resized -> embedding).
 * Everything that bounds memory happens BEFORE the decoder is called: the header is read,
 * the pixel count asserted against {@link ImagePreprocessingSpec#maxPixels()}, the reader's own intermediates
 * ({@link DecoderIntermediates}) asserted against {@link ImagePreprocessingSpec#intermediateBudgetBytes()}, the decoder's
 * destination type looked up and the {@link RasterPlan} sized to the raster budget; a refusal throws {@link ImageRefusedException} and no pixel is decoded.
 * Only then does the configured {@link ImageDecoder} run. One instance serves the images of one Spark task, sequentially.
 */
public class ImagePreparer {

  private final ImagePreprocessingSpec spec;
  private final ImageDecoder decoder;

  public ImagePreparer(ImagePreprocessingSpec spec) {
    this.spec = spec;
    this.decoder = spec.newDecoder();
  }

  public ImagePreprocessingSpec spec() {
    return spec;
  }

  /**
   * Plans the decode, then runs the decoder. The source is opened twice, once for the plan and once by the decoder: a reader
   * may flush its stream behind the header (PNG), so one stream cannot be rewound from the plan to the decode, and opening a
   * fresh seekable stream is a seek, not a re-read of the file. For an OUT_OF_LINE blob without a recorded length each open
   * also asks storage for the file length.
   *
   * @throws ImageHeaderException  unreadable header
   * @throws ImageRefusedException refused before decode (pixel guard, intermediate budget, raster budget, no decoder)
   * @throws IOException           storage failure or corrupt pixel data ({@link javax.imageio.IIOException})
   */
  public PreparedImage prepare(ImageSource source) throws IOException {
    return decoder.decode(source, plan(source), spec);
  }

  /**
   * Reads the header and sizes the decode, without decoding a pixel; one reader session over one stream (a reader may
   * flush the stream behind its header, so the stream is never rewound to the decode). The intermediate estimate's header bytes
   * are read first, under mark/reset, before the reader takes the stream.
   */
  public RasterPlan plan(ImageSource source) throws IOException {
    try (ImageInputStream in = source.open()) {
      ImageReader reader = ImageHeaderReader.firstReader(in);
      if (reader == null) {
        // no decoder in this JVM: still validate the header (WebP), so a bad one reads as such
        ImageHeader header = ImageHeaderReader.read(in);
        checkPixels(header);
        throw new ImageRefusedException(ImageRefusedException.NO_DECODER + "no image reader in this JVM decodes "
            + header.getFormat() + " (" + header + ")");
      }
      try {
        // the header bytes the intermediate estimate needs, read before the reader takes the stream (a PNG reader flushes it)
        byte[] prefix = DecoderIntermediates.headerPrefix(in);
        reader.setInput(in, true, true);
        ImageHeader header = ImageHeaderReader.headerOf(reader);
        checkPixels(header);
        long intermediate = DecoderIntermediates.estimate(reader, header, prefix);
        if (intermediate > spec.intermediateBudgetBytes()) {
          throw new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + header + " needs about " + intermediate
              + " bytes of decoder intermediates before subsampling (a full-resolution TIFF strip/tile with its compressed "
              + "bytes, source-width PNG/BMP/GIF rows, a BMP RLE bitmap read whole, or progressive JPEG coefficients), "
              + "above the configured budget ("
              + EmbeddingTableConfig.COMPUTE_IMAGE_INTERMEDIATE_BUDGET_BYTES.key() + ") of "
              + spec.intermediateBudgetBytes());
        }
        Iterator<ImageTypeSpecifier> types = reader.getImageTypes(0);
        if (!types.hasNext()) {
          throw new ImageRefusedException(ImageRefusedException.NO_DECODER + "the " + header.getFormat()
              + " reader offers no destination type for " + header);
        }
        return RasterPlan.of(header, types.next(), spec);
      } catch (RuntimeException e) {
        throw new ImageHeaderException("corrupt image header: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
      } finally {
        reader.dispose();
      }
    }
  }

  private void checkPixels(ImageHeader header) throws ImageRefusedException {
    if (header.pixels() > spec.maxPixels()) {
      throw new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + header + " declares "
          + header.pixels() + " pixels, above the configured maximum (" + EmbeddingTableConfig.COMPUTE_IMAGE_MAX_PIXELS.key()
          + ") of " + spec.maxPixels());
    }
  }
}
