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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageProviderLeakSpies;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import com.sun.imageio.plugins.leakspy.JdkNamedSpyReaderSpi;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handle release of {@link ImageInputPreprocessor}: the header read closes the seekable blob or the stream it opened, on a stored
 * image and on every refusal, and disposes the ImageIO reader it chose.
 */
class TestImageInputPreprocessorHandleRelease {

  private final AtomicInteger opened = new AtomicInteger();
  private final AtomicInteger closed = new AtomicInteger();

  @Test
  void storedImageClosesTheSeekableBlob() throws IOException {
    byte[] png = ImageFixtures.image("png", 40, 20);
    try (ClosableIterator<Chunk> chunks = open(seekable(png, -1), png.length, new TypedProperties())) {
      assertTrue(chunks.hasNext());
      chunks.next();
      // the header read is done when open returns: nothing stays open while the chunk waits to be pulled
      assertAllClosed("after open of a stored image");
    }
    assertEquals(1, opened.get(), "only the header read opens the source; the payload is the source's own blob value");
    assertAllClosed("after close");
  }

  @Test
  void storedImageClosesTheStreamOfAPlainSupplier() throws IOException {
    byte[] jpeg = ImageFixtures.image("jpeg", 40, 20);
    try (ClosableIterator<Chunk> chunks = open(stream(jpeg), jpeg.length, new TypedProperties())) {
      assertTrue(chunks.hasNext());
    }
    assertAllClosed("after a stored image read through a plain stream");
  }

  @Test
  void unrecognisedContentClosesTheSource() {
    assertThrows(ImageHeaderException.class, () -> open(seekable(ImageFixtures.garbage(), -1), 64, new TypedProperties()));
    assertThrows(ImageHeaderException.class, () -> open(stream(ImageFixtures.garbage()), 64, new TypedProperties()));
    assertEquals(2, opened.get());
    assertAllClosed("after unrecognised content");
  }

  @Test
  void headerCapClosesTheSource() {
    byte[] png = ImageFixtures.image("png", 40, 20);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), "8");
    assertThrows(IOException.class, () -> open(seekable(png, -1), png.length, props));
    assertThrows(IOException.class, () -> open(stream(png), png.length, props));
    assertEquals(2, opened.get());
    assertAllClosed("after the header cap");
  }

  @Test
  void pixelRefusalClosesTheSource() {
    byte[] png = ImageFixtures.image("png", 40, 20);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key(), "10");
    SeekableBlobBytesSupplier intact = seekable(png, -1);
    assertThrows(PreprocessingException.class, () -> open(intact, png.length, props));
    assertAllClosed("after a pixel refusal");
  }

  @Test
  void storageFailureMidReadClosesTheSource() {
    byte[] png = ImageFixtures.image("png", 40, 20);
    SeekableBlobBytesSupplier failing = seekable(png, 8);
    TypedProperties none = new TypedProperties();
    assertThrows(RuntimeException.class, () -> open(failing, png.length, none));
    assertAllClosed("after a storage failure mid header read");
  }

  @Test
  void chosenReaderIsDisposedWhenALaterProviderFails() {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.install()) {
      byte[] content = ImageProviderLeakSpies.spyContent();
      ImageProviderLeakSpies.runEitherOutcome(() -> {
        try (ClosableIterator<Chunk> chunks = open(seekable(content, -1), content.length, new TypedProperties())) {
          chunks.forEachRemaining(chunk -> { });
        }
      });
      assertAllClosed("after the provider walk failed");
      assertEquals(1, spies.spyReader().created().size(), "the spy reader was chosen");
      JdkNamedSpyReaderSpi.SpyReader chosen = spies.spyReader().created().get(0);
      assertTrue(chosen.disposed(), "ImageInputPreprocessor.open: the chosen ImageReader " + chosen
          + " was not disposed when a later provider's createReaderInstance threw");
    }
  }

  // ------------------------------------------------------------------ helpers

  private static ClosableIterator<Chunk> open(BlobBytesSupplier bytes, long size, TypedProperties props) throws IOException {
    return new ImageInputPreprocessor().open(bytes, new FileMeta("sha-1", "png", size), null, props);
  }

  private void assertAllClosed(String when) {
    assertTrue(opened.get() > 0, when + ": the source was opened");
    assertEquals(opened.get(), closed.get(), when + ": unclosed source handle(s): opened=" + opened + " closed=" + closed);
  }

  /** A plain (non-seekable) supplier whose streams are counted. */
  private BlobBytesSupplier stream(byte[] bytes) {
    return () -> {
      opened.incrementAndGet();
      return new FilterInputStream(new ByteArrayInputStream(bytes)) {
        private boolean done;

        @Override
        public void close() throws IOException {
          if (!done) {
            done = true;
            closed.incrementAndGet();
          }
          super.close();
        }
      };
    };
  }

  /** A seekable supplier whose blobs are counted; a blob read past {@code failAt} (>= 0) fails as a reset storage read does. */
  private SeekableBlobBytesSupplier seekable(byte[] bytes, int failAt) {
    BlobBytesSupplier plain = stream(bytes);
    return new SeekableBlobBytesSupplier() {
      @Override
      public InputStream open() throws IOException {
        return plain.open();
      }

      @Override
      public SeekableBlob openSeekable() {
        opened.incrementAndGet();
        return new CountingBlob(bytes, failAt);
      }
    };
  }

  private final class CountingBlob implements SeekableBlob {
    private final byte[] bytes;
    private final int failAt;
    private int pos;
    private boolean done;

    CountingBlob(byte[] bytes, int failAt) {
      this.bytes = Arrays.copyOf(bytes, bytes.length);
      this.failAt = failAt;
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
