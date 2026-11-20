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

import javax.imageio.IIOImage;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageOutputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 * An ImageIO writer provider of no JDK plugin, registered ahead of every other writer of one format while a test body runs (a
 * plugin jar on the classpath that ImageIO happens to list first). Its writer writes {@link #MARKER}, never an image.
 */
public final class ForeignImageWriterSpi extends ImageWriterSpi {

  /** The bytes the foreign writer writes in place of an image. */
  public static final byte[] MARKER = "FOREIGN-WRITER".getBytes(StandardCharsets.US_ASCII);

  private ForeignImageWriterSpi(String format) {
    super("test", "1.0", new String[] {format, format.toUpperCase(Locale.ROOT)}, null, null, ForeignWriter.class.getName(),
        new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
  }

  /** Runs {@code body} with a foreign writer of {@code format} registered ahead of every other writer of it, then removes it. */
  public static <T> T registeredFirst(String format, Callable<T> body) throws Exception {
    IIORegistry registry = IIORegistry.getDefaultInstance();
    ForeignImageWriterSpi foreign = new ForeignImageWriterSpi(format);
    registry.registerServiceProvider(foreign, ImageWriterSpi.class);
    try {
      Iterator<ImageWriterSpi> providers = registry.getServiceProviders(ImageWriterSpi.class, true);
      while (providers.hasNext()) {
        ImageWriterSpi other = providers.next();
        if (other != foreign && writes(other, format)) {
          registry.setOrdering(ImageWriterSpi.class, foreign, other);
        }
      }
      return body.call();
    } finally {
      registry.deregisterServiceProvider(foreign, ImageWriterSpi.class);
    }
  }

  /** Whether {@code writer} is a foreign writer's. */
  public static boolean isForeign(ImageWriter writer) {
    return writer.getOriginatingProvider() instanceof ForeignImageWriterSpi;
  }

  private static boolean writes(ImageWriterSpi provider, String format) {
    for (String name : provider.getFormatNames()) {
      if (name.equalsIgnoreCase(format)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean canEncodeImage(ImageTypeSpecifier type) {
    return true;
  }

  @Override
  public ImageWriter createWriterInstance(Object extension) {
    return new ForeignWriter(this);
  }

  @Override
  public String getDescription(Locale locale) {
    return "test writer of no JDK plugin";
  }

  /** Writes the marker; accepts the JPEG parameters the encoders set. */
  private static final class ForeignWriter extends ImageWriter {

    private ForeignWriter(ImageWriterSpi provider) {
      super(provider);
    }

    @Override
    public ImageWriteParam getDefaultWriteParam() {
      return new JPEGImageWriteParam(null);
    }

    @Override
    public IIOMetadata getDefaultStreamMetadata(ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata getDefaultImageMetadata(ImageTypeSpecifier type, ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata convertStreamMetadata(IIOMetadata in, ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata convertImageMetadata(IIOMetadata in, ImageTypeSpecifier type, ImageWriteParam param) {
      return null;
    }

    @Override
    public void write(IIOMetadata streamMetadata, IIOImage image, ImageWriteParam param) throws IOException {
      ((ImageOutputStream) getOutput()).write(MARKER);
    }
  }
}
