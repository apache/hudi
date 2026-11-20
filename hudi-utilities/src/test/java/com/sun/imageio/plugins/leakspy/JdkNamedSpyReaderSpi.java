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

package com.sun.imageio.plugins.leakspy;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TEST ONLY. An image reader provider whose class name lies under the JDK's built-in plugin package
 * ({@code com.sun.imageio.plugins.}), so the embedding image code treats it as a JDK reader and may choose it; it recognises
 * only content starting with {@link #MAGIC} and records every reader it creates and whether that reader was disposed. It lets a
 * test observe the release of the reader the production code chose, which a real JDK reader does not expose.
 */
public class JdkNamedSpyReaderSpi extends ImageReaderSpi {

  /** The first bytes of the content this provider recognises. */
  public static final byte[] MAGIC = "LEAKSPY!".getBytes(StandardCharsets.US_ASCII);

  private final List<SpyReader> created = new CopyOnWriteArrayList<>();

  public JdkNamedSpyReaderSpi() {
    super("hudi-test", "1", new String[] {"leakspy"}, new String[] {"leakspy"}, new String[] {"image/x-leakspy"},
        SpyReader.class.getName(), new Class<?>[] {ImageInputStream.class}, null, false, null, null, null, null, false, null,
        null, null, null);
  }

  /** The readers this provider created, in order. */
  public List<SpyReader> created() {
    return Collections.unmodifiableList(created);
  }

  @Override
  public boolean canDecodeInput(Object source) throws IOException {
    return startsWithMagic(source);
  }

  /** Whether {@code source}, an image input stream, starts with {@link #MAGIC}; the caller marks and resets the stream. */
  public static boolean startsWithMagic(Object source) throws IOException {
    if (!(source instanceof ImageInputStream)) {
      return false;
    }
    byte[] head = new byte[MAGIC.length];
    ((ImageInputStream) source).readFully(head);
    return Arrays.equals(head, MAGIC);
  }

  @Override
  public ImageReader createReaderInstance(Object extension) {
    SpyReader reader = new SpyReader(this);
    created.add(reader);
    return reader;
  }

  @Override
  public String getDescription(Locale locale) {
    return "hudi test spy reader";
  }

  /** A reader that only records whether it was disposed; it decodes nothing. */
  public static final class SpyReader extends ImageReader {
    private volatile boolean disposed;

    SpyReader(ImageReaderSpi provider) {
      super(provider);
    }

    public boolean disposed() {
      return disposed;
    }

    @Override
    public void dispose() {
      disposed = true;
      super.dispose();
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
