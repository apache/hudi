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

import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;

import com.sun.imageio.plugins.leakspy.JdkNamedSpyReaderSpi;
import com.sun.imageio.plugins.leakspy.JdkNamedSpyWriterSpi;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageInputStream;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handle release of the compute-side image preparation ({@link ImagePreparer}, {@link ImageResizeLogicV1},
 * {@link ImageHeaderReader}): every stream an {@link ImageSource} opens is closed, closing the seekable blob under it, and every
 * ImageIO reader or writer the code chooses is disposed, on success and on each failure.
 */
class TestImagePreparerHandleRelease {

  private static final ImagePreprocessingSpec SPEC = new ImagePreprocessingSpec(
      ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
      ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES,
      InProcessImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY);

  private final AtomicInteger opened = new AtomicInteger();
  private final AtomicInteger closed = new AtomicInteger();

  @Test
  void preparedPngClosesBothStreams() throws IOException {
    PreparedImage prepared = new ImagePreparer(SPEC).prepare(source(ImageFixtures.image("png", 600, 300)));
    assertEquals(512, prepared.getWidth());
    assertEquals(2, opened.get(), "the plan and the decode each open the source");
    assertAllClosed("after a prepared image");
  }

  @Test
  void preparedJpegClosesBothStreams() throws IOException {
    ImagePreprocessingSpec jpeg = new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE,
        ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
        InProcessImageDecoder.class.getName());
    new ImagePreparer(jpeg).prepare(source(ImageFixtures.image("jpeg", 300, 200)));
    assertEquals(2, opened.get());
    assertAllClosed("after a prepared JPEG");
  }

  @Test
  void refusalBeforeDecodeClosesTheStream() {
    ImagePreprocessingSpec tiny = new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE,
        ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, 100, InProcessImageDecoder.class.getName());
    assertThrows(ImageRefusedException.class, () -> new ImagePreparer(tiny).prepare(source(ImageFixtures.image("png", 50, 30))));
    assertEquals(1, opened.get());
    assertAllClosed("after a refusal before decode");
  }

  @Test
  void unreadableHeaderClosesTheStream() {
    assertThrows(ImageHeaderException.class, () -> new ImagePreparer(SPEC).prepare(source(ImageFixtures.garbage())));
    assertAllClosed("after an unreadable header");
  }

  @Test
  void webpWithoutADecoderClosesTheStream() {
    assertThrows(ImageRefusedException.class, () -> new ImagePreparer(SPEC).prepare(source(ImageFixtures.webpHeader(40, 20))));
    assertAllClosed("after a WebP refusal");
  }

  @Test
  void corruptPixelDataClosesBothStreams() {
    assertThrows(IOException.class, () -> new ImagePreparer(SPEC).prepare(source(ImageFixtures.truncatedPng())));
    assertAllClosed("after corrupt pixel data");
  }

  @Test
  void storageFailureMidReadClosesTheStream() {
    byte[] png = ImageFixtures.image("png", 50, 30);
    ImageSource failing = () -> new SeekableBlobImageInputStream(new CountingBlob(png, 40));
    assertThrows(IOException.class, () -> new ImagePreparer(SPEC).prepare(failing));
    assertAllClosed("after a storage failure mid-read");
  }

  @Test
  void openStoredInputClosesTheBlobItOpened() throws IOException {
    byte[] png = ImageFixtures.image("png", 50, 30);
    new ImagePreparer(SPEC).prepare(() -> SeekableBlobImageInputStream.openStoredInput(() -> new CountingBlob(png, -1)));
    assertEquals(2, opened.get());
    assertAllClosed("after a stored input was prepared");
  }

  // ------------------------------------------------------------------ chosen ImageIO reader / writer

  @Test
  void chosenReaderIsDisposedWhenALaterProviderFailsInRead() throws IOException {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.install();
         MemoryCacheImageInputStream in = new MemoryCacheImageInputStream(new ByteArrayInputStream(ImageProviderLeakSpies.spyContent()))) {
      ImageProviderLeakSpies.runEitherOutcome(() -> ImageHeaderReader.read(in));
      assertChosenReaderDisposed(spies.spyReader(), "ImageHeaderReader.read");
    }
  }

  @Test
  void chosenReaderIsDisposedWhenALaterProviderFailsInPlan() {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.install()) {
      ImageProviderLeakSpies.runEitherOutcome(() -> new ImagePreparer(SPEC).plan(source(ImageProviderLeakSpies.spyContent())));
      assertAllClosed("after the provider walk failed in plan");
      assertChosenReaderDisposed(spies.spyReader(), "ImagePreparer.plan");
    }
  }

  @Test
  void chosenWriterIsDisposedWhenALaterProviderFails() {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.install()) {
      ImageWriter[] returned = new ImageWriter[1];
      ImageProviderLeakSpies.runEitherOutcome(() -> returned[0] = ImageHeaderReader.firstJdkWriter(JdkNamedSpyWriterSpi.FORMAT));
      assertEquals(1, spies.spyWriter().created().size(), "the spy writer was chosen");
      JdkNamedSpyWriterSpi.SpyWriter chosen = spies.spyWriter().created().get(0);
      if (returned[0] == chosen) {
        // the walk skipped the failing provider and handed the chosen writer to its caller, who owns it now
        chosen.dispose();
      }
      assertTrue(chosen.disposed(), "ImageHeaderReader.firstJdkWriter: the chosen ImageWriter " + chosen
          + " was neither returned nor disposed when a later provider's createWriterInstance threw");
    }
  }

  /** Control for the leak tests above: the spies are classified as JDK providers, so the walk chooses them. */
  @Test
  void spyProvidersAreChosenAsJdkProviders() throws IOException {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installAlone();
         MemoryCacheImageInputStream in = new MemoryCacheImageInputStream(new ByteArrayInputStream(ImageProviderLeakSpies.spyContent()))) {
      ImageReader reader = ImageHeaderReader.firstReader(in);
      try {
        assertEquals(1, spies.spyReader().created().size(), "the spy reader was created");
        assertSame(spies.spyReader().created().get(0), reader, "ImageHeaderReader.firstReader chose the spy reader");
        assertFalse(spies.spyReader().created().get(0).disposed(), "the chosen reader is returned, not disposed");
      } finally {
        if (reader != null) {
          reader.dispose();
        }
      }
      ImageWriter writer = ImageHeaderReader.firstJdkWriter(JdkNamedSpyWriterSpi.FORMAT);
      try {
        assertEquals(1, spies.spyWriter().created().size(), "the spy writer was created");
        assertSame(spies.spyWriter().created().get(0), writer, "ImageHeaderReader.firstJdkWriter chose the spy writer");
      } finally {
        if (writer != null) {
          writer.dispose();
        }
      }
    }
  }

  @Test
  void chosenWriterIsReturnedAndOthersDisposedOnSuccess() {
    // control for the writer walk: a foreign writer of the format ordered first is skipped and disposed, the JDK one comes back
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installBehindAForeignWriter()) {
      ImageWriter writer = ImageHeaderReader.firstJdkWriter(JdkNamedSpyWriterSpi.FORMAT);
      try {
        assertEquals(1, spies.spyWriter().created().size(), "the JDK-named spy writer was created");
        assertSame(spies.spyWriter().created().get(0), writer, "the JDK-named writer is the one returned");
        assertFalse(spies.spyWriter().created().get(0).disposed(), "the returned writer is the caller's, not disposed");
        assertEquals(1, spies.foreignWriter().created().size(), "the foreign writer ordered first was created");
        ImageProviderLeakSpies.RecordingWriter foreign = spies.foreignWriter().created().get(0);
        assertTrue(foreign.disposed(), "ImageHeaderReader.firstJdkWriter: the writer not chosen " + foreign + " was not disposed");
      } finally {
        if (writer != null) {
          writer.dispose();
        }
      }
    }
  }

  @Test
  void firstJdkWriterReturnsTheJdkPngWriter() {
    ImageWriter writer = ImageHeaderReader.firstJdkWriter("png");
    try {
      assertNotNull(writer);
      assertTrue(writer.getOriginatingProvider().getClass().getName().startsWith("com.sun.imageio.plugins."),
          "a JDK writer: " + writer.getOriginatingProvider());
    } finally {
      if (writer != null) {
        writer.dispose();
      }
    }
  }

  // ------------------------------------------------------------------ helpers

  private static void assertChosenReaderDisposed(JdkNamedSpyReaderSpi spi, String where) {
    assertEquals(1, spi.created().size(), where + ": the spy reader was chosen");
    JdkNamedSpyReaderSpi.SpyReader chosen = spi.created().get(0);
    assertTrue(chosen.disposed(), where + ": the chosen ImageReader " + chosen
        + " was not disposed when a later provider's createReaderInstance threw");
  }

  private ImageSource source(byte[] bytes) {
    return () -> new SeekableBlobImageInputStream(new CountingBlob(bytes, -1));
  }

  private void assertAllClosed(String when) {
    assertTrue(opened.get() > 0, when + ": the source was opened");
    assertEquals(opened.get(), closed.get(), when + ": unclosed SeekableBlob(s): opened=" + opened + " closed=" + closed);
  }

  /** An in-memory blob counted open at construction and closed once; a read past {@code failAt} (>= 0) fails as storage does. */
  private final class CountingBlob implements SeekableBlob {
    private final byte[] bytes;
    private final int failAt;
    private int pos;
    private boolean done;

    CountingBlob(byte[] bytes, int failAt) {
      this.bytes = Arrays.copyOf(bytes, bytes.length);
      this.failAt = failAt;
      opened.incrementAndGet();
    }

    @Override
    public long length() {
      return bytes.length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long position) throws IOException {
      if (position < 0 || position > bytes.length) {
        throw new EOFException("seek to " + position);
      }
      pos = (int) position;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (failAt >= 0 && pos + len > failAt) {
        throw new IOException("connection reset");
      }
      if (pos >= bytes.length) {
        return -1;
      }
      int n = Math.min(len, bytes.length - pos);
      System.arraycopy(bytes, pos, buffer, offset, n);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      if (!done) {
        done = true;
        closed.incrementAndGet();
      }
    }
  }
}
