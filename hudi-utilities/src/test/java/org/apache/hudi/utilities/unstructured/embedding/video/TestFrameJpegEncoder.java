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
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import org.junit.jupiter.api.Test;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The frame JPEG encoder's failures are the platform's, never the video's (class P): no writer, or an in-memory encode that fails. */
public class TestFrameJpegEncoder {

  private static final FileMeta VIDEO = new FileMeta("fid-1", "mp4", 1L);

  @Test
  public void jvmWithoutAJpegWriterIsAConfigurationError() {
    assertThrows(PreprocessorConfigurationException.class, () -> new FrameJpegEncoder(Collections.emptyIterator()));
  }

  @Test
  public void anInMemoryEncodeFailureIsAPlatformFailure() throws IOException {
    ImageWriter real = ImageIO.getImageWritersByFormatName("jpeg").next();
    ImageWriteParam param = real.getDefaultWriteParam();
    ImageWriter failing = mock(ImageWriter.class);
    when(failing.getDefaultWriteParam()).thenReturn(param);
    doThrow(new IOException("encoder state broken")).when(failing).write(any(), any(IIOImage.class), any());
    try (FrameJpegEncoder encoder = new FrameJpegEncoder(Collections.singletonList(failing).iterator())) {
      PrepPlatformException e = assertThrows(PrepPlatformException.class,
          () -> encoder.encode(new BufferedImage(16, 16, BufferedImage.TYPE_3BYTE_BGR), VIDEO));
      assertTrue(e.getCause() instanceof IOException, String.valueOf(e.getCause()));
    } finally {
      real.dispose();
    }
  }

  /** A frame wider than a JPEG can hold is the video's bad input, never a platform failure failing every run. */
  @Test
  public void frameWiderThanAJpegCanHoldIsTheVideosBadInput() {
    try (FrameJpegEncoder encoder = new FrameJpegEncoder()) {
      VideoDecodeException e = assertThrows(VideoDecodeException.class,
          () -> encoder.encode(new BufferedImage(FrameJpegEncoder.MAX_JPEG_SIDE + 1, 1, BufferedImage.TYPE_3BYTE_BGR), VIDEO));
      assertTrue(e.getMessage().contains(String.valueOf(FrameJpegEncoder.MAX_JPEG_SIDE)), e.getMessage());
      assertTrue(encoder.encode(new BufferedImage(FrameJpegEncoder.MAX_JPEG_SIDE, 1, BufferedImage.TYPE_3BYTE_BGR), VIDEO).length > 0,
          "the largest side a JPEG holds still encodes");
    }
  }

  @Test
  public void theJvmsWriterEncodesAFrame() {
    try (FrameJpegEncoder encoder = new FrameJpegEncoder()) {
      assertTrue(encoder.encode(new BufferedImage(16, 16, BufferedImage.TYPE_3BYTE_BGR), VIDEO).length > 0);
    }
  }

  /**
   * The encoded bytes are part of the frame identity: the JDK's JPEG writer encodes a frame even when another JPEG writer is
   * registered ahead of it, and the bytes are those it writes alone.
   */
  @Test
  public void theJdksWriterEncodesEvenWhenAnotherIsRegisteredFirst() throws Exception {
    BufferedImage image = new BufferedImage(16, 16, BufferedImage.TYPE_3BYTE_BGR);
    image.setRGB(3, 5, 0x336699);
    byte[] alone;
    try (FrameJpegEncoder encoder = new FrameJpegEncoder()) {
      alone = encoder.encode(image, VIDEO);
    }
    IIORegistry registry = IIORegistry.getDefaultInstance();
    ImageWriterSpi jdk = ImageIO.getImageWritersByFormatName("jpeg").next().getOriginatingProvider();
    ForeignJpegWriterSpi foreign = new ForeignJpegWriterSpi();
    registry.registerServiceProvider(foreign, ImageWriterSpi.class);
    byte[] encoded;
    try {
      registry.setOrdering(ImageWriterSpi.class, foreign, jdk);
      ImageWriter first = ImageIO.getImageWritersByFormatName("jpeg").next();
      assertTrue(first.getOriginatingProvider() instanceof ForeignJpegWriterSpi, "the foreign writer is listed first");
      first.dispose();
      try (FrameJpegEncoder encoder = new FrameJpegEncoder()) {
        encoded = encoder.encode(image, VIDEO);
      }
    } finally {
      registry.deregisterServiceProvider(foreign, ImageWriterSpi.class);
    }
    assertArrayEquals(alone, encoded);
  }

  /** A JPEG writer outside the JDK's plugins whose output is never a JPEG: its bytes must never become a frame's. */
  private static final class ForeignJpegWriterSpi extends ImageWriterSpi {
    private ForeignJpegWriterSpi() {
      super("test", "1", new String[] {"jpeg", "JPEG"}, new String[] {"jpg"}, new String[] {"image/jpeg"},
          ForeignJpegWriter.class.getName(), new Class<?>[] {ImageOutputStream.class}, null, false, null, null, null, null,
          false, null, null, null, null);
    }

    @Override
    public boolean canEncodeImage(ImageTypeSpecifier type) {
      return true;
    }

    @Override
    public ImageWriter createWriterInstance(Object extension) {
      return new ForeignJpegWriter(this);
    }

    @Override
    public String getDescription(Locale locale) {
      return "foreign test JPEG writer";
    }
  }

  private static final class ForeignJpegWriter extends ImageWriter {
    private ForeignJpegWriter(ImageWriterSpi provider) {
      super(provider);
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
    public void write(IIOMetadata streamMetadata, IIOImage image, ImageWriteParam param) throws IOException {
      ((ImageOutputStream) getOutput()).write(new byte[] {'n', 'o', 't', 'j', 'p', 'g'});
    }
  }
}
