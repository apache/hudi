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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A source-width row must fit before BMP decoding, including RLE and legacy CORE headers. */
public class TestBmpSourceRowBudget {
  private static final int WIDTH = 20_000;

  @BeforeEach
  void resetCalls() {
    CountingImageDecoder.CALLS.set(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 8})
  void sourceRowBeyondBudgetIsRefusedBeforeDecode(int bits) throws IOException {
    byte[] input = rleBmp(bits);
    long budget = WIDTH / 2;
    assertTrue(2L * input.length < budget, "the compressed file fits; the decoded source row does not");
    ImageRefusedException refusal = assertThrows(ImageRefusedException.class,
        () -> new ImagePreparer(spec(budget)).prepare(ImageFixtures.source(input)));
    assertTrue(refusal.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE));
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 8})
  void sameValidImageDecodesWithRoomForItsSourceRow(int bits) throws IOException {
    byte[] input = rleBmp(bits);
    PreparedImage image = new ImagePreparer(spec(4L * WIDTH + 2L * input.length))
        .prepare(ImageFixtures.source(input));
    assertEquals(512, image.getWidth());
    assertEquals(1, image.getHeight());
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  @Test
  void legacyCoreSourceRowBeyondBudgetIsRefusedBeforeDecode() throws IOException {
    byte[] input = coreBmp();
    ImageRefusedException refusal = assertThrows(ImageRefusedException.class,
        () -> new ImagePreparer(spec(WIDTH / 2)).prepare(ImageFixtures.source(input)));
    assertTrue(refusal.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE));
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @Test
  void sameLegacyCoreImageDecodesWithRoomForItsSourceRow() throws IOException {
    PreparedImage image = new ImagePreparer(spec(1L << 20)).prepare(ImageFixtures.source(coreBmp()));
    assertEquals(512, image.getWidth());
    assertEquals(1, image.getHeight());
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  /** A valid 24-bit OS/2 CORE header retaining the modern BMP fixture pixel rows. */
  private static byte[] coreBmp() throws IOException {
    byte[] modern = ImageFixtures.uncompressedBmp(WIDTH, 1, 24);
    int pixels = ByteBuffer.wrap(modern).order(ByteOrder.LITTLE_ENDIAN).getInt(10);
    byte[] core = new byte[26 + modern.length - pixels];
    ByteBuffer output = ByteBuffer.wrap(core).order(ByteOrder.LITTLE_ENDIAN);
    output.putShort((short) 0x4d42).putInt(core.length).putInt(0).putInt(26);
    output.putInt(12).putShort((short) WIDTH).putShort((short) 1).putShort((short) 1).putShort((short) 24);
    output.put(modern, pixels, modern.length - pixels);
    return core;
  }

  private static ImagePreprocessingSpec spec(long budget) {
    return new ImagePreprocessingSpec(512, 16L << 20, 100_000_000L, budget,
        CountingImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, 90);
  }

  /** A single valid row written by the JDK, with the requested BMP RLE encoding. */
  private static byte[] rleBmp(int bits) throws IOException {
    int colors = 1 << bits;
    byte[] gray = new byte[colors];
    for (int i = 0; i < colors; i++) {
      gray[i] = (byte) (i * 255 / (colors - 1));
    }
    IndexColorModel palette = new IndexColorModel(bits, colors, gray, gray, gray);
    BufferedImage image = new BufferedImage(WIDTH, 1,
        bits == 4 ? BufferedImage.TYPE_BYTE_BINARY : BufferedImage.TYPE_BYTE_INDEXED, palette);
    ImageWriter writer = ImageIO.getImageWritersByFormatName("bmp").next();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ImageOutputStream output = ImageIO.createImageOutputStream(bytes)) {
      ImageWriteParam param = writer.getDefaultWriteParam();
      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
      param.setCompressionType(bits == 4 ? "BI_RLE4" : "BI_RLE8");
      writer.setOutput(output);
      writer.write(null, new IIOImage(image, null, null), param);
    } finally {
      writer.dispose();
    }
    return bytes.toByteArray();
  }
}
