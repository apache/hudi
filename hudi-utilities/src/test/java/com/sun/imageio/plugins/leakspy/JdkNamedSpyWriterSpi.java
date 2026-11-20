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

import javax.imageio.IIOImage;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageOutputStream;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TEST ONLY. An image writer provider of format {@link #FORMAT} (or of the format it is built for) whose class name lies under the JDK's built-in plugin package, so
 * the embedding image code treats it as a JDK writer and may choose it; it records every writer it creates and whether that
 * writer was disposed.
 */
public class JdkNamedSpyWriterSpi extends ImageWriterSpi {

  /** The format name this provider writes. */
  public static final String FORMAT = "leakspy";

  /** The message of the failure a writer whose dispose fails throws. */
  public static final String DISPOSE_FAILURE = "test: the spy ImageWriter failed to dispose";

  private final List<SpyWriter> created = new CopyOnWriteArrayList<>();
  private final boolean disposeFails;

  public JdkNamedSpyWriterSpi() {
    this(FORMAT, false);
  }

  /**
   * A provider of {@code format}; when {@code disposeFails}, each writer it creates records its disposal and then throws an
   * unchecked {@link #DISPOSE_FAILURE} from {@code dispose}.
   */
  public JdkNamedSpyWriterSpi(String format, boolean disposeFails) {
    super("hudi-test", "1", new String[] {format}, new String[] {format}, new String[] {"image/x-" + format},
        SpyWriter.class.getName(), new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null, false, null,
        null, null, null);
    this.disposeFails = disposeFails;
  }

  /** The writers this provider created, in order. */
  public List<SpyWriter> created() {
    return Collections.unmodifiableList(created);
  }

  @Override
  public boolean canEncodeImage(ImageTypeSpecifier type) {
    return true;
  }

  @Override
  public ImageWriter createWriterInstance(Object extension) {
    SpyWriter writer = new SpyWriter(this, disposeFails);
    created.add(writer);
    return writer;
  }

  @Override
  public String getDescription(Locale locale) {
    return "hudi test spy writer";
  }

  /** A writer that only records whether it was disposed; it writes nothing. */
  public static final class SpyWriter extends ImageWriter {
    private final boolean disposeFails;
    private volatile boolean disposed;

    SpyWriter(ImageWriterSpi provider, boolean disposeFails) {
      super(provider);
      this.disposeFails = disposeFails;
    }

    /** Whether dispose was called (also when it then failed). */
    public boolean disposed() {
      return disposed;
    }

    @Override
    public void dispose() {
      disposed = true;
      super.dispose();
      if (disposeFails) {
        throw new IllegalStateException(DISPOSE_FAILURE);
      }
    }

    /** A JPEG-style parameter (explicit compression supported), so the spy can stand in for a JPEG writer. */
    @Override
    public ImageWriteParam getDefaultWriteParam() {
      return new JPEGImageWriteParam(getLocale());
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
      // writes nothing: the spy only records whether it was disposed
    }
  }
}
