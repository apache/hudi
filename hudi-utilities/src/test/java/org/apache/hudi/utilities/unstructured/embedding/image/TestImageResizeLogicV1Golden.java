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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins what decoding logic {@value ImageResizeLogicV1#LOGIC_VERSION} produces: the SHA-256 of the prepared image's RGB pixels
 * for fixed inputs. A change to the subsampling, interpolation, alpha flattening or output that alters these pixels MUST come
 * with a new {@link ImageResizeLogicV1#LOGIC_VERSION}; when this test fails, bump the version (a new logic) instead of updating
 * the digests. Pixels, not PNG bytes, are pinned (through the lossless PNG encoding), so an encoder's compression choices do
 * not matter; the JPEG encoding is chosen by its own settings (encoding, quality).
 */
class TestImageResizeLogicV1Golden {

  /** The default spec with the lossless PNG encoding, so the decoded output IS the prepared pixels (JPEG would alter them). */
  private static final ImagePreprocessingSpec DEFAULTS = new ImagePreprocessingSpec(
      ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
      ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES,
      InProcessImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY);

  @ParameterizedTest
  @CsvSource({
      "jpeg-1500x1000, 512x341, cbf891f3b29886eca0266df88e94718aa6d70206c1a0e23f7036424fba48b6b9",
      "alpha-png-900x600, 512x341, 36a411273f64cedf08b26fbfab0ece5b791d22fc86cb005d7390a41b2f7e954d",
      "gif-300x200, 300x200, 04786933241a06447b5d585f0b45d0e7df47c81fcb4a34020e9baa7c0472cc5c",
      "bmp-600x26, 512x22, a3352e526e46d0c8598853fdea2e44954da09d840b2e4e8e33984f250d3671cd",
      "tiff-520x30, 512x30, 9101a27c067aa5fa1195f258fbaf115ff85e1a78d5f98840e6c39b03dd0981fc"})
  void preparedPixelsArePinnedForThisLogicVersion(String fixture, String size, String digest) throws IOException {
    assertEquals("image-resize-v2", ImageResizeLogicV1.LOGIC_VERSION, "a new logic version needs new pinned digests");
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(fixture(fixture)));
    BufferedImage pixels = ImageFixtures.decode(prepared.getEncoded());
    assertEquals(size, pixels.getWidth() + "x" + pixels.getHeight());
    assertEquals(digest, rgbDigest(pixels), fixture);
  }

  /**
   * The encoded bytes do not depend on the writers registered: the JDK's JPEG and PNG writers encode the prepared image even
   * when another writer of the format is registered ahead of them, and the bytes are those they write alone.
   */
  @ParameterizedTest
  @CsvSource({"jpeg", "png"})
  void theJdksWriterEncodesEvenWhenAnotherIsRegisteredFirst(String encoding) throws Exception {
    ImagePreprocessingSpec spec = new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE,
        ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
        ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES, InProcessImageDecoder.class.getName(), encoding,
        ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY);
    byte[] source = ImageFixtures.image("jpeg", 300, 200);
    byte[] alone = new ImagePreparer(spec).prepare(ImageFixtures.source(source)).getEncoded();
    byte[] encoded = ForeignImageWriterSpi.registeredFirst(encoding, () -> {
      ImageWriter first = ImageIO.getImageWritersByFormatName(encoding).next();
      assertTrue(ForeignImageWriterSpi.isForeign(first), "the foreign writer is listed first");
      first.dispose();
      return new ImagePreparer(spec).prepare(ImageFixtures.source(source)).getEncoded();
    });
    assertArrayEquals(alone, encoded, encoding);
  }

  /**
   * An animated GIF and a multi-page TIFF are embedded from their first image only. The prepared
   * pixels of the two-image file equal those of its image 0 written alone, and differ from those of its image 1 alone.
   */
  @ParameterizedTest
  @CsvSource({"gif", "tiff"})
  void multiImageFileIsPreparedFromItsFirstImageOnly(String format) throws IOException {
    byte[] multi = ImageFixtures.twoFrames(format, 300, 200);
    String prepared = preparedDigest(multi);
    assertEquals(preparedDigest(ImageFixtures.frame(multi, 0, format)), prepared, "image 0 of the " + format);
    assertNotEquals(preparedDigest(ImageFixtures.frame(multi, 1, format)), prepared, "image 1 of the " + format);
  }

  private static String preparedDigest(byte[] source) throws IOException {
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(source));
    return rgbDigest(ImageFixtures.decode(prepared.getEncoded()));
  }

  private static byte[] fixture(String name) {
    switch (name) {
      case "jpeg-1500x1000":
        return ImageFixtures.image("jpeg", 1500, 1000);
      case "alpha-png-900x600":
        return ImageFixtures.alphaPng(900, 600);
      case "gif-300x200":
        return ImageFixtures.image("gif", 300, 200);
      case "bmp-600x26":
        return ImageFixtures.image("bmp", 600, 26);
      case "tiff-520x30":
        return ImageFixtures.image("tiff", 520, 30);
      default:
        throw new IllegalArgumentException(name);
    }
  }

  private static String rgbDigest(BufferedImage image) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      ByteBuffer row = ByteBuffer.allocate(image.getWidth() * 4);
      for (int y = 0; y < image.getHeight(); y++) {
        row.clear();
        for (int x = 0; x < image.getWidth(); x++) {
          row.putInt(image.getRGB(x, y));
        }
        sha.update(row.array());
      }
      StringBuilder hex = new StringBuilder();
      for (byte b : sha.digest()) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
