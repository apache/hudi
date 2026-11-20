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

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

/**
 * Test-only third-party ImageIO plugin that claims PNG content and reports every image as 1x1, standing for a jar such as a
 * JAI or TwelveMonkeys plugin on an executor's classpath. {@link #installFirst()} registers it AHEAD of the JDK PNG reader.
 */
public class ForeignPngReaderSpi extends ImageReaderSpi {

  private static final byte[] PNG_SIGNATURE = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};

  public ForeignPngReaderSpi() {
    super("test-vendor", "1.0", new String[] {"png"}, new String[] {"png"}, new String[] {"image/png"},
        ForeignReader.class.getName(), new Class<?>[] {ImageInputStream.class}, null, false, null, null, null, null, false,
        null, null, null, null);
  }

  /** Registers an instance ordered before every other PNG reader provider; returns it for {@link #uninstall}. */
  public static ForeignPngReaderSpi installFirst() {
    ForeignPngReaderSpi spi = new ForeignPngReaderSpi();
    IIORegistry registry = IIORegistry.getDefaultInstance();
    registry.registerServiceProvider(spi, ImageReaderSpi.class);
    Iterator<ImageReaderSpi> others = registry.getServiceProviders(ImageReaderSpi.class, true);
    while (others.hasNext()) {
      ImageReaderSpi other = others.next();
      if (other != spi) {
        registry.setOrdering(ImageReaderSpi.class, spi, other);
      }
    }
    return spi;
  }

  public static void uninstall(ForeignPngReaderSpi spi) {
    IIORegistry.getDefaultInstance().deregisterServiceProvider(spi, ImageReaderSpi.class);
  }

  @Override
  public boolean canDecodeInput(Object source) throws IOException {
    if (!(source instanceof ImageInputStream)) {
      return false;
    }
    ImageInputStream in = (ImageInputStream) source;
    byte[] head = new byte[PNG_SIGNATURE.length];
    in.mark();
    try {
      in.readFully(head);
    } finally {
      in.reset();
    }
    for (int i = 0; i < head.length; i++) {
      if (head[i] != PNG_SIGNATURE[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ImageReader createReaderInstance(Object extension) {
    return new ForeignReader(this);
  }

  @Override
  public String getDescription(Locale locale) {
    return "test-only foreign PNG reader";
  }

  /** Reports a 1x1 image whatever the bytes say. */
  public static class ForeignReader extends ImageReader {
    ForeignReader(ImageReaderSpi spi) {
      super(spi);
    }

    @Override
    public int getNumImages(boolean allowSearch) {
      return 1;
    }

    @Override
    public int getWidth(int imageIndex) {
      return 1;
    }

    @Override
    public int getHeight(int imageIndex) {
      return 1;
    }

    @Override
    public Iterator<ImageTypeSpecifier> getImageTypes(int imageIndex) {
      return Collections.singletonList(ImageTypeSpecifier.createFromBufferedImageType(BufferedImage.TYPE_3BYTE_BGR)).iterator();
    }

    @Override
    public IIOMetadata getStreamMetadata() {
      return null;
    }

    @Override
    public IIOMetadata getImageMetadata(int imageIndex) {
      return null;
    }

    @Override
    public BufferedImage read(int imageIndex, ImageReadParam param) {
      return new BufferedImage(1, 1, BufferedImage.TYPE_3BYTE_BGR);
    }
  }
}
