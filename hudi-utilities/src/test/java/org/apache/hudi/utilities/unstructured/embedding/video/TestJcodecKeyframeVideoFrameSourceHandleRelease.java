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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageProviderLeakSpies;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import com.sun.imageio.plugins.leakspy.JdkNamedSpyWriterSpi;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one blob channel the JCodec keyframe source opens is closed on every way out. */
public class TestJcodecKeyframeVideoFrameSourceHandleRelease {

  private static final String MATRIX = "/video/codec-matrix/";

  @Test
  public void drainedVideoClosesItsBlob() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01);
    int frames = 0;
    try (ClosableIterator<ExtractedFrame> it = new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mp4"),
        new TypedProperties())) {
      while (it.hasNext()) {
        it.next();
        frames++;
      }
    }
    assertTrue(frames > 0, "c01 yields frames");
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a drained video");
  }

  @Test
  public void abandonedAfterTheFirstFrameClosesItsBlob() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01);
    try (ClosableIterator<ExtractedFrame> it = new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mp4"),
        new TypedProperties())) {
      assertTrue(it.hasNext());
      it.next();
    }
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the consumer stopped early");
  }

  @Test
  public void refusalWhileOpeningClosesItsBlob() throws Exception {
    Path hevc = resource("c03-hevc-aac.mp4");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(hevc);
    assertThrows(VideoDecodeException.class, () -> new JcodecKeyframeVideoFrameSource().frames(bytes, meta(hevc, "mp4"),
        new TypedProperties()));
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a refusal while opening");
  }

  @Test
  public void anotherExtensionOpensNothing() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01);
    assertThrows(VideoDecodeException.class, () -> new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mov"),
        new TypedProperties()));
    assertEquals(0, bytes.opened(), "refused before any read");
  }

  @Test
  public void storageFailurePartWayClosesItsBlob() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    for (long failAt : new long[] {Files.size(c01) / 4, Files.size(c01) / 2, Files.size(c01) * 3 / 4}) {
      TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01, failAt, n -> null);
      try {
        try (ClosableIterator<ExtractedFrame> it = new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mp4"),
            new TypedProperties())) {
          while (it.hasNext()) {
            it.next();
          }
        }
      } catch (IOException | RuntimeException expected) {
        // a storage failure or a refusal: the outcome under test is the release
      }
      assertEquals(1, bytes.opened());
      assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a read failure past byte " + failAt);
    }
  }

  /**
   * The JPEG writer of the frame encoder fails to dispose (an unchecked throw): the blob channel, closed after it, still closes,
   * and the dispose failure is the one close() throws.
   */
  @Test
  public void writerThatFailsToDisposeStillClosesTheBlob() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01);
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installJpeg(false, true)) {
      ClosableIterator<ExtractedFrame> it = new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mp4"),
          new TypedProperties());
      assertEquals(1, spies.spyWriter().created().size(), "the frame encoder chose the spy JPEG writer");
      RuntimeException thrown = assertThrows(RuntimeException.class, it::close);
      assertEquals(JdkNamedSpyWriterSpi.DISPOSE_FAILURE, thrown.getMessage(), "close() throws the writer's dispose failure");
      assertTrue(spies.spyWriter().created().get(0).disposed(), "the encoder's close tried to dispose its writer");
    }
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the JPEG writer's dispose threw");
  }

  /**
   * The JPEG writer fails to dispose and the blob then fails to close as well (both unchecked): the writer's failure, the first,
   * is the one close() throws, and the blob's is suppressed into it.
   */
  @Test
  public void writerDisposeFailureStaysTheOneThrownWhenTheBlobAlsoFailsToClose() throws Exception {
    Path c01 = resource("c01-h264-aac.mp4");
    RuntimeException blobFailure = new IllegalStateException("test: the blob failed to close");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(c01, Long.MAX_VALUE, n -> blobFailure);
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installJpeg(false, true)) {
      ClosableIterator<ExtractedFrame> it = new JcodecKeyframeVideoFrameSource().frames(bytes, meta(c01, "mp4"),
          new TypedProperties());
      RuntimeException thrown = assertThrows(RuntimeException.class, it::close);
      assertEquals(JdkNamedSpyWriterSpi.DISPOSE_FAILURE, thrown.getMessage(), "close() throws the writer's dispose failure");
      assertEquals(Collections.singletonList(blobFailure), Arrays.asList(thrown.getSuppressed()),
          "the blob's close failure is suppressed into the writer's");
    }
  }

  /**
   * Opening fails (a refused codec) and the blob then fails to close (an unchecked throw): the open failure is the one thrown,
   * the close failure suppressed into it, so how the video is sorted does not depend on the cleanup.
   */
  @Test
  public void openFailureStaysTheOneThrownWhenTheBlobAlsoFailsToClose() throws Exception {
    Path hevc = resource("c03-hevc-aac.mp4");
    RuntimeException blobFailure = new IllegalStateException("test: the blob failed to close");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(hevc, Long.MAX_VALUE, n -> blobFailure);
    VideoDecodeException thrown = assertThrows(VideoDecodeException.class,
        () -> new JcodecKeyframeVideoFrameSource().frames(bytes, meta(hevc, "mp4"), new TypedProperties()));
    assertEquals(1, thrown.getSuppressed().length, "the close failure is suppressed into the open failure");
    assertSame(blobFailure, thrown.getSuppressed()[0]);
  }

  private static Path resource(String name) throws URISyntaxException {
    return Paths.get(Objects.requireNonNull(TestJcodecKeyframeVideoFrameSourceHandleRelease.class.getResource(MATRIX + name)).toURI());
  }

  private static FileMeta meta(Path file, String extension) throws IOException {
    return new FileMeta("fid-release", extension, Files.size(file));
  }
}
