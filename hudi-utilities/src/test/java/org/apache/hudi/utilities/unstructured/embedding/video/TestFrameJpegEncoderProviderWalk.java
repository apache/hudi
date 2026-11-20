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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriter;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A third-party JPEG writer provider that fails to create its writer, ahead of the JDK's or behind it, does not fail the video
 * frame encoder: one whose create method throws IOException (ImageIO then yields a null candidate) and one that throws
 * unchecked. The encoder still gets the JDK's JPEG writer and encodes a frame.
 */
class TestFrameJpegEncoderProviderWalk {

  private final IIORegistry registry = IIORegistry.getDefaultInstance();

  @ParameterizedTest(name = "checked={0}, foreignFirst={1}")
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void failingForeignJpegWriterIsSkipped(boolean checked, boolean foreignFirst) throws IOException {
    FailingJpegWriterSpi foreign = new FailingJpegWriterSpi(checked);
    registry.registerServiceProvider(foreign, ImageWriterSpi.class);
    try {
      List<ImageWriterSpi> providers = new ArrayList<>();
      registry.getServiceProviders(ImageWriterSpi.class, false).forEachRemaining(providers::add);
      for (ImageWriterSpi provider : providers) {
        if (provider != foreign && Arrays.asList(provider.getFormatNames()).contains("jpeg")) {
          if (foreignFirst) {
            registry.setOrdering(ImageWriterSpi.class, foreign, provider);
          } else {
            registry.setOrdering(ImageWriterSpi.class, provider, foreign);
          }
        }
      }
      byte[] jpeg;
      try (FrameJpegEncoder encoder = new FrameJpegEncoder()) {
        jpeg = encoder.encode(new BufferedImage(16, 8, BufferedImage.TYPE_3BYTE_BGR), new FileMeta("fid-walk", "mp4", 1));
      }
      assertTrue(foreign.attempts > 0, "the walk met the failing provider");
      BufferedImage decoded;
      try (ByteArrayInputStream in = new ByteArrayInputStream(jpeg)) {
        decoded = ImageIO.read(in);
      }
      assertNotNull(decoded, "the frame was encoded as a JPEG");
      assertEquals(16, decoded.getWidth());
      assertEquals(8, decoded.getHeight());
    } finally {
      registry.deregisterServiceProvider(foreign, ImageWriterSpi.class);
    }
  }

  /** A foreign JPEG writer provider whose writer creation fails: IOException when {@code checked}, else unchecked. */
  private static final class FailingJpegWriterSpi extends ImageWriterSpi {
    private final boolean checked;
    private int attempts;

    FailingJpegWriterSpi(boolean checked) {
      super("hudi-test-foreign", "1", new String[] {"jpeg"}, null, null, ImageWriter.class.getName(),
          new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
      this.checked = checked;
    }

    @Override
    public boolean canEncodeImage(ImageTypeSpecifier type) {
      return true;
    }

    @Override
    public ImageWriter createWriterInstance(Object extension) throws IOException {
      attempts++;
      if (checked) {
        throw new IOException("test: a foreign ImageIO plugin failed to create its writer");
      }
      throw new IllegalStateException("test: a foreign ImageIO plugin failed to create its writer");
    }

    @Override
    public String getDescription(Locale locale) {
      return "hudi test foreign JPEG writer that fails";
    }
  }
}
