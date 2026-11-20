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

import com.sun.imageio.plugins.leakspy.JdkNamedSpyReaderSpi;
import com.sun.imageio.plugins.leakspy.JdkNamedSpyWriterSpi;

import javax.imageio.IIOImage;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registers, for the duration of one test, a JDK-named spy provider ({@link JdkNamedSpyReaderSpi} / {@link JdkNamedSpyWriterSpi})
 * that the image code chooses, ordered before a foreign provider of the same content whose reader or writer creation throws an
 * unchecked exception (a misbehaving third-party ImageIO plugin): the failure escapes the provider walk after the spy's reader or
 * writer was created, so a test can check that the chosen one is disposed.
 */
public final class ImageProviderLeakSpies implements AutoCloseable {

  /** The message of the foreign provider's failure. */
  public static final String FOREIGN_FAILURE = "a foreign ImageIO plugin failed to create its instance";

  private final IIORegistry registry = IIORegistry.getDefaultInstance();
  private final JdkNamedSpyReaderSpi spyReader = new JdkNamedSpyReaderSpi();
  private final JdkNamedSpyWriterSpi spyWriter;
  private final ImageReaderSpi foreignReader;
  private final ImageWriterSpi foreignWriter;

  private ImageProviderLeakSpies(JdkNamedSpyWriterSpi spyWriter, ImageReaderSpi foreignReader, ImageWriterSpi foreignWriter,
                                 boolean foreignFirst) {
    this.spyWriter = spyWriter;
    this.foreignReader = foreignReader;
    this.foreignWriter = foreignWriter;
    registry.registerServiceProvider(spyReader, ImageReaderSpi.class);
    registry.registerServiceProvider(spyWriter, ImageWriterSpi.class);
    // ahead of every other writer of its format already registered (for jpeg: the JDK's own), so the walk meets it first
    String format = spyWriter.getFormatNames()[0];
    List<ImageWriterSpi> others = new ArrayList<>();
    registry.getServiceProviders(ImageWriterSpi.class, false).forEachRemaining(others::add);
    for (ImageWriterSpi other : others) {
      if (other != spyWriter && Arrays.asList(other.getFormatNames()).contains(format)) {
        registry.setOrdering(ImageWriterSpi.class, spyWriter, other);
      }
    }
    if (foreignReader != null) {
      registry.registerServiceProvider(foreignReader, ImageReaderSpi.class);
      if (foreignFirst) {
        registry.setOrdering(ImageReaderSpi.class, foreignReader, spyReader);
      } else {
        registry.setOrdering(ImageReaderSpi.class, spyReader, foreignReader);
      }
    }
    if (foreignWriter != null) {
      registry.registerServiceProvider(foreignWriter, ImageWriterSpi.class);
      if (foreignFirst) {
        registry.setOrdering(ImageWriterSpi.class, foreignWriter, spyWriter);
      } else {
        registry.setOrdering(ImageWriterSpi.class, spyWriter, foreignWriter);
      }
    }
  }

  /** The spies, each ordered before a foreign provider of the same content whose instance creation throws. */
  public static ImageProviderLeakSpies install() {
    return new ImageProviderLeakSpies(new JdkNamedSpyWriterSpi(), new ThrowingReaderSpi(), new ThrowingWriterSpi(JdkNamedSpyWriterSpi.FORMAT), false);
  }

  /**
   * The spies alone, no foreign provider: the control that the image code treats the spies as JDK providers, so that the leak
   * tests reach the path where the spy is the chosen reader or writer.
   */
  public static ImageProviderLeakSpies installAlone() {
    return new ImageProviderLeakSpies(new JdkNamedSpyWriterSpi(), null, null, false);
  }

  /** The spy writer, ordered after a foreign writer of the same format that creates writers which record their disposal. */
  public static ImageProviderLeakSpies installBehindAForeignWriter() {
    return new ImageProviderLeakSpies(new JdkNamedSpyWriterSpi(), null, new ForeignWriterSpi(), true);
  }

  /**
   * A JDK-named spy JPEG writer ordered ahead of the JDK's own JPEG writer, so a JPEG writer walk that takes the first JDK
   * writer chooses it. With {@code laterProviderFails}, a foreign JPEG writer provider whose writer creation throws is ordered after the
   * spy; with {@code disposeFails}, the spy writer's dispose throws {@link JdkNamedSpyWriterSpi#DISPOSE_FAILURE}. While installed,
   * every JPEG write of the JVM that picks the first JDK writer gets the spy, which writes nothing.
   */
  public static ImageProviderLeakSpies installJpeg(boolean laterProviderFails, boolean disposeFails) {
    return new ImageProviderLeakSpies(new JdkNamedSpyWriterSpi("jpeg", disposeFails), null,
        laterProviderFails ? new ThrowingWriterSpi("jpeg") : null, false);
  }

  /** The foreign writer provider of {@link #installBehindAForeignWriter}; null for the other installations. */
  public ForeignWriterSpi foreignWriter() {
    return foreignWriter instanceof ForeignWriterSpi ? (ForeignWriterSpi) foreignWriter : null;
  }

  public JdkNamedSpyReaderSpi spyReader() {
    return spyReader;
  }

  public JdkNamedSpyWriterSpi spyWriter() {
    return spyWriter;
  }

  /** A call into the provider walk whose outcome is not under test. */
  @FunctionalInterface
  public interface WalkCall {
    void run() throws Exception;
  }

  /**
   * Runs {@code call} and ignores how it ends: a walk that rethrows the foreign provider's failure and one that skips that
   * provider and returns normally are both correct; only the release of what the walk chose is under test.
   */
  public static void runEitherOutcome(WalkCall call) {
    try {
      call.run();
    } catch (Throwable ignored) {
      // either outcome is acceptable
    }
  }

  /** Content the spy reader and the throwing reader both recognise. */
  public static byte[] spyContent() {
    byte[] content = new byte[64];
    System.arraycopy(JdkNamedSpyReaderSpi.MAGIC, 0, content, 0, JdkNamedSpyReaderSpi.MAGIC.length);
    return content;
  }

  @Override
  public void close() {
    if (foreignReader != null) {
      registry.deregisterServiceProvider(foreignReader, ImageReaderSpi.class);
    }
    registry.deregisterServiceProvider(spyReader, ImageReaderSpi.class);
    if (foreignWriter != null) {
      registry.deregisterServiceProvider(foreignWriter, ImageWriterSpi.class);
    }
    registry.deregisterServiceProvider(spyWriter, ImageWriterSpi.class);
  }

  /** A foreign reader provider of the spy's content whose reader creation throws unchecked. */
  private static final class ThrowingReaderSpi extends ImageReaderSpi {
    ThrowingReaderSpi() {
      super("hudi-test-foreign", "1", new String[] {"leakspy-foreign"}, null, null, ImageReader.class.getName(),
          new Class<?>[] {ImageInputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
    }

    @Override
    public boolean canDecodeInput(Object source) throws IOException {
      return JdkNamedSpyReaderSpi.startsWithMagic(source);
    }

    @Override
    public ImageReader createReaderInstance(Object extension) {
      throw new IllegalStateException(FOREIGN_FAILURE);
    }

    @Override
    public String getDescription(Locale locale) {
      return "hudi test foreign reader that fails";
    }
  }

  /** A foreign writer provider of the spy's format whose writer creation throws unchecked. */
  private static final class ThrowingWriterSpi extends ImageWriterSpi {
    ThrowingWriterSpi(String format) {
      super("hudi-test-foreign", "1", new String[] {format}, null, null, ImageWriter.class.getName(),
          new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
    }

    @Override
    public boolean canEncodeImage(ImageTypeSpecifier type) {
      return true;
    }

    @Override
    public ImageWriter createWriterInstance(Object extension) {
      throw new IllegalStateException(FOREIGN_FAILURE);
    }

    @Override
    public String getDescription(Locale locale) {
      return "hudi test foreign writer that fails";
    }
  }

  /** A foreign (not JDK-named) writer provider of the spy's format; it records every writer it creates and its disposal. */
  public static final class ForeignWriterSpi extends ImageWriterSpi {
    private final List<RecordingWriter> created = new CopyOnWriteArrayList<>();

    ForeignWriterSpi() {
      super("hudi-test-foreign", "1", new String[] {JdkNamedSpyWriterSpi.FORMAT}, null, null, RecordingWriter.class.getName(),
          new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null, false, null, null, null, null);
    }

    /** The writers this provider created, in order. */
    public List<RecordingWriter> created() {
      return Collections.unmodifiableList(created);
    }

    @Override
    public boolean canEncodeImage(ImageTypeSpecifier type) {
      return true;
    }

    @Override
    public ImageWriter createWriterInstance(Object extension) {
      RecordingWriter writer = new RecordingWriter(this);
      created.add(writer);
      return writer;
    }

    @Override
    public String getDescription(Locale locale) {
      return "hudi test foreign writer that records its disposal";
    }
  }

  /** A foreign writer that only records whether it was disposed; it writes nothing. */
  public static final class RecordingWriter extends ImageWriter {
    private volatile boolean disposed;

    RecordingWriter(ImageWriterSpi provider) {
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
    public IIOMetadata getDefaultStreamMetadata(ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata getDefaultImageMetadata(ImageTypeSpecifier imageType, ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata convertStreamMetadata(IIOMetadata inData, ImageWriteParam param) {
      return null;
    }

    @Override
    public IIOMetadata convertImageMetadata(IIOMetadata inData, ImageTypeSpecifier imageType, ImageWriteParam param) {
      return null;
    }

    @Override
    public void write(IIOMetadata streamMetadata, IIOImage image, ImageWriteParam param) {
    }
  }
}
