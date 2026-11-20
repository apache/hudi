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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageProviderLeakSpies;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one blob the FFmpeg keyframe source opens per video (its grabber's) is closed on every way out. */
class TestFfmpegKeyframeVideoFrameSourceHandleRelease {

  @TempDir
  static Path dir;
  /** 8 s at 160x120, a keyframe every 2 s. */
  private static Path clip;

  @BeforeAll
  static void generate() throws IOException {
    clip = VideoClips.generate(dir.resolve("clip.mp4"), 8, 10, 2, 160, 120, -1, -1);
  }

  @Test
  void drainedVideoClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
    int frames = 0;
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(bytes, meta(), new TypedProperties())) {
      while (it.hasNext()) {
        it.next();
        frames++;
      }
    }
    assertTrue(frames > 0, "the clip yields frames");
    assertEquals(1, bytes.opened(), "one blob per video: the scan and the decodes share it");
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a drained video");
  }

  @Test
  void abandonedAfterTheFirstFrameClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(bytes, meta(), new TypedProperties())) {
      assertTrue(it.hasNext());
      it.next();
    }
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the consumer stopped early");
  }

  @Test
  void refusalWhileOpeningClosesTheBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), "100");
    FfmpegKeyframeVideoFrameSource source = new FfmpegKeyframeVideoFrameSource();
    FileMeta meta = meta();
    assertThrows(RuntimeException.class, () -> source.frames(bytes, meta, props));
    assertEquals(1, bytes.opened(), "refused after the probe");
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a refusal while opening");
  }

  @Test
  void storageFailurePartWayClosesTheBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip, Files.size(clip) * 3 / 4, n -> null);
    FileMeta file = meta();
    assertThrows(RuntimeException.class, () -> drain(bytes, file));
    assertTrue(bytes.opened() >= 1);
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a storage failure part way");
  }

  /**
   * The grabber's close fails with an Error (thrown by its blob's close, as a native free or an exhausted heap would): the
   * frame encoder's JPEG writer, closed after it, must still be released, and that Error is the one close() throws.
   */
  @Test
  void errorClosingTheGrabberStillDisposesTheEncoder() throws IOException {
    Error closeFailure = new OutOfMemoryError("test: the video blob failed to close");
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip, Long.MAX_VALUE, n -> n == 1 ? closeFailure : null);
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installJpeg(false, false)) {
      ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(bytes, meta(), new TypedProperties());
      assertEquals(1, spies.spyWriter().created().size(), "the frame encoder chose the spy JPEG writer");
      assertTrue(it.hasNext());
      it.next();
      Error thrown = assertThrows(Error.class, it::close);
      assertSame(closeFailure, thrown, "close() rethrows the grabber's close failure");
      assertTrue(spies.spyWriter().created().get(0).disposed(), "the encoder's JPEG writer was not disposed after the grabber's "
          + "close threw an Error");
    }
    assertEquals(1, bytes.opened(), "one blob per video");
  }

  private static FileMeta meta() throws IOException {
    return new FileMeta("fid-release", "mp4", Files.size(clip));
  }

  /** Reads every frame of {@code file}, closing the iterator. */
  private static void drain(TrackedBlobSupplier bytes, FileMeta file) throws IOException {
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(bytes, file, new TypedProperties())) {
      while (it.hasNext()) {
        it.next();
      }
    }
  }
}
