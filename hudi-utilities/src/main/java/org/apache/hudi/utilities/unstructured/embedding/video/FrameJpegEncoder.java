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

import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderReader;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import javax.imageio.IIOImage;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * The shared frame representation of every frame strategy: a kept frame encoded JPEG at quality
 * {@value #JPEG_QUALITY} at its NATIVE size, in memory, by the JDK's own JPEG writer only (a writer another plugin registers,
 * even ahead of it, is never used: the frames' bytes must not depend on the classpath). One instance per video;
 * {@link #close()} releases the writer. A JVM without the JDK's JPEG writer is a {@link PreprocessorConfigurationException},
 * an in-memory encode that fails a {@link PrepPlatformException}: the executor's, never the video's (class P); a frame larger
 * than a JPEG can hold is the video's bad input ({@link VideoDecodeException}).
 */
final class FrameJpegEncoder implements AutoCloseable {

  /** JPEG quality of an encoded frame. */
  static final int JPEG_QUALITY = 90;
  /** The largest width or height a baseline JPEG can hold (the JDK writer refuses more): 65500 pixels. */
  static final int MAX_JPEG_SIDE = 65500;

  private final ImageWriter writer;
  private final ImageWriteParam param;

  /** The encoder over the JDK's JPEG writer: a plugin registered ahead of it must not change the frames' bytes. */
  FrameJpegEncoder() {
    this(jdkJpegWriter());
  }

  /** The encoder over the first of {@code writers}; a test seam (production passes the JDK's JPEG writer alone, or none). */
  FrameJpegEncoder(Iterator<ImageWriter> writers) {
    if (!writers.hasNext()) {
      throw new PreprocessorConfigurationException("no JPEG ImageWriter in this JVM: video frames cannot be encoded", null);
    }
    writer = writers.next();
    param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionQuality(JPEG_QUALITY / 100f);
  }

  /**
   * {@code image} (a 3-byte BGR raster) as a JPEG at quality JPEG_QUALITY, at its native size.
   *
   * @throws VideoDecodeException for a frame wider or taller than {@link #MAX_JPEG_SIDE}: the video's own frame no JPEG can hold,
   * its bad input (class B), never a platform failure that would fail every run on it
   * @throws PrepPlatformException when the in-memory encode fails
   */
  byte[] encode(BufferedImage image, FileMeta video) {
    if (image.getWidth() > MAX_JPEG_SIDE || image.getHeight() > MAX_JPEG_SIDE) {
      throw new VideoDecodeException("video " + video + " has a " + image.getWidth() + "x" + image.getHeight() + " frame; a JPEG "
          + "frame holds at most " + MAX_JPEG_SIDE + " pixels per side");
    }
    int rowBytes = image.getWidth() * 3;
    // a q90 JPEG is well below its raster; the buffer starts at a tenth of it and grows as needed
    ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(1024, rowBytes * image.getHeight() / 10));
    // in memory, never ImageIO's file cache: a full or unwritable temp directory must not fail a readable video
    try (ImageOutputStream stream = new MemoryCacheImageOutputStream(out)) {
      writer.setOutput(stream);
      writer.write(null, new IIOImage(image, null, null), param);
    } catch (IOException e) {
      // an in-memory encode does not touch storage, and a decoded 3-byte BGR raster is no input the writer may refuse
      throw new PrepPlatformException("JPEG encoding of a frame of video " + video + " failed", e);
    } finally {
      writer.setOutput(null);
    }
    return out.toByteArray();
  }

  @Override
  public void close() {
    writer.dispose();
  }

  /**
   * The JDK's JPEG writer, or none: the image code's walk ({@link ImageHeaderReader#firstJdkWriter}), which disposes the writers
   * not chosen, skips a provider that fails to create its writer, and disposes the chosen writer when anything else fails it.
   */
  private static Iterator<ImageWriter> jdkJpegWriter() {
    ImageWriter writer = ImageHeaderReader.firstJdkWriter("jpeg");
    return writer == null ? Collections.emptyIterator() : Collections.singletonList(writer).iterator();
  }
}
