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

package org.apache.hudi.utilities.unstructured.embedding.prep.video;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The video frame value type and the frame-extraction seam: the value invariants, and that {@link FrameExtractionStrategy} can be
 * implemented as stated (data prep's video preprocessor drives it).
 */
class TestVideoInterfaces {

  @Test
  void extractedFrameValidatesItsFields() {
    BlobBytesSupplier image = () -> new ByteArrayInputStream(new byte[] {1, 2, 3});
    ExtractedFrame frame = new ExtractedFrame(2000, 48, true, "00ff", new ExtractedFrame.EncodedImage(640, 360, "image/jpeg", 3, image));
    assertEquals(2000, frame.getTimeMs());
    assertEquals(48, frame.getFrameIndex());
    assertTrue(frame.isKeyframe());
    assertEquals(640, frame.getWidth());
    assertEquals(360, frame.getHeight());
    assertEquals("image/jpeg", frame.getMimeType());
    assertEquals(3, frame.getSizeBytes());
    assertEquals("00ff", frame.getDedupKey());
    assertSame(image, frame.getImage());

    ExtractedFrame.EncodedImage valid = new ExtractedFrame.EncodedImage(1, 1, "image/jpeg", 3, image);
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame(-1, 0, true, "k", valid));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame(0, -1, true, "k", valid));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame(0, 0, true, "", valid));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame(0, 0, true, "k", null));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame.EncodedImage(0, 1, "image/jpeg", 3, image));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame.EncodedImage(1, 1, "", 3, image));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame.EncodedImage(1, 1, "image/jpeg", -1, image));
    assertThrows(IllegalArgumentException.class, () -> new ExtractedFrame.EncodedImage(1, 1, "image/jpeg", 3, null));
  }

  /** The interface compiles and can be implemented as stated: a strategy over a fixed list, drained as a caller would. */
  @Test
  void interfacesCanBeImplementedAsStated() throws IOException {
    BlobBytesSupplier image = () -> new ByteArrayInputStream(new byte[0]);
    ExtractedFrame.EncodedImage encoded = new ExtractedFrame.EncodedImage(2, 2, "image/jpeg", 0, image);
    List<ExtractedFrame> frames = Arrays.asList(new ExtractedFrame(0, 0, true, "a", encoded), new ExtractedFrame(2000, 1, true, "b", encoded));
    FrameExtractionStrategy strategy = new FrameExtractionStrategy() {
      @Override
      public String name() {
        return "fixed-list-v0";
      }

      @Override
      public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) {
        return over(frames);
      }
    };
    FileMeta video = new FileMeta("fid0", "mp4", 10);
    assertEquals("fixed-list-v0", strategy.name());
    strategy.validateConfig(new TypedProperties());
    assertEquals(frames, drain(strategy.frames(image, video, new TypedProperties())));
  }

  private static <T> ClosableIterator<T> over(List<T> items) {
    Iterator<T> it = items.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // nothing to release: the iterator is over an in-memory list
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public T next() {
        return it.next();
      }
    };
  }

  private static <T> List<T> drain(ClosableIterator<T> iterator) {
    List<T> out = new ArrayList<>();
    try (ClosableIterator<T> it = iterator) {
      it.forEachRemaining(out::add);
    }
    return out;
  }
}
