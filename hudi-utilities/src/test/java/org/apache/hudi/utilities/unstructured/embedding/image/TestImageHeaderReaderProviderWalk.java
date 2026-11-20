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
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriter;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.spi.ImageReaderWriterSpi;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A third-party ImageIO provider that fails to create its reader or writer is skipped by the JDK provider walk, ahead of the
 * JDK's provider or behind it: one whose create method throws IOException (ImageIO then yields a null candidate) and one that
 * throws unchecked (out of the iterator's next). The JDK reader or writer is still found.
 */
public class TestImageHeaderReaderProviderWalk {

  private static final byte[] PNG_MAGIC = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};

  private final IIORegistry registry = IIORegistry.getDefaultInstance();

  @ParameterizedTest(name = "checked={0}, foreignFirst={1}")
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void failingForeignReaderIsSkipped(boolean checked, boolean foreignFirst) throws IOException {
    byte[] png = png(7, 5);
    FailingReaderSpi foreign = new FailingReaderSpi(checked);
    register(ImageReaderSpi.class, foreign, "png", foreignFirst);
    try (ImageInputStream in = ImageIO.createImageInputStream(new ByteArrayInputStream(png))) {
      ImageHeader header = ImageHeaderReader.read(in);
      assertEquals("png", header.getFormat());
      assertEquals(7, header.getWidth());
      assertEquals(5, header.getHeight());
      assertTrue(foreign.attempts > 0, "the walk met the failing provider");
    } finally {
      registry.deregisterServiceProvider(foreign, ImageReaderSpi.class);
    }
  }

  @ParameterizedTest(name = "checked={0}, foreignFirst={1}")
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void failingForeignWriterIsSkipped(boolean checked, boolean foreignFirst) {
    FailingWriterSpi foreign = new FailingWriterSpi(checked);
    register(ImageWriterSpi.class, foreign, "jpeg", foreignFirst);
    try {
      ImageWriter writer = ImageHeaderReader.firstJdkWriter("jpeg");
      assertNotNull(writer, "the JDK's JPEG writer is found");
      try {
        assertTrue(writer.getOriginatingProvider().getClass().getName().startsWith(ImageHeaderReader.JDK_PLUGIN_PACKAGE));
      } finally {
        writer.dispose();
      }
      assertTrue(foreign.attempts > 0, "the walk met the failing provider");
    } finally {
      registry.deregisterServiceProvider(foreign, ImageWriterSpi.class);
    }
  }

  /** Registers {@code foreign} and orders it before (or after) every JDK provider of {@code format}. */
  private <T extends ImageReaderWriterSpi> void register(Class<T> category, T foreign, String format, boolean foreignFirst) {
    registry.registerServiceProvider(foreign, category);
    List<T> providers = new ArrayList<>();
    registry.getServiceProviders(category, false).forEachRemaining(providers::add);
    for (T provider : providers) {
      if (provider != foreign && Arrays.asList(provider.getFormatNames()).contains(format)) {
        if (foreignFirst) {
          registry.setOrdering(category, foreign, provider);
        } else {
          registry.setOrdering(category, provider, foreign);
        }
      }
    }
  }

  private static byte[] png(int width, int height) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertTrue(ImageIO.write(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB), "png", out));
    return out.toByteArray();
  }

  private static boolean startsWithPngMagic(Object source) throws IOException {
    if (!(source instanceof ImageInputStream)) {
      return false;
    }
    ImageInputStream in = (ImageInputStream) source;
    byte[] head = new byte[PNG_MAGIC.length];
    in.mark();
    try {
      in.readFully(head);
    } finally {
      in.reset();
    }
    return Arrays.equals(PNG_MAGIC, head);
  }

  /** A foreign PNG reader provider whose reader creation fails: IOException when {@code checked}, else unchecked. */
  private static final class FailingReaderSpi extends ImageReaderSpi {
    private final boolean checked;
    private int attempts;

    FailingReaderSpi(boolean checked) {
      super("hudi-test-foreign", "1", new String[] {"png"}, null, null, ImageReader.class.getName(),
          new Class<?>[] {ImageInputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
      this.checked = checked;
    }

    @Override
    public boolean canDecodeInput(Object source) throws IOException {
      return startsWithPngMagic(source);
    }

    @Override
    public ImageReader createReaderInstance(Object extension) throws IOException {
      attempts++;
      if (checked) {
        throw new IOException("test: a foreign ImageIO plugin failed to create its reader");
      }
      throw new IllegalStateException("test: a foreign ImageIO plugin failed to create its reader");
    }

    @Override
    public String getDescription(Locale locale) {
      return "hudi test foreign reader that fails";
    }
  }

  /** A foreign JPEG writer provider whose writer creation fails: IOException when {@code checked}, else unchecked. */
  private static final class FailingWriterSpi extends ImageWriterSpi {
    private final boolean checked;
    private int attempts;

    FailingWriterSpi(boolean checked) {
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
      return "hudi test foreign writer that fails";
    }
  }
}
