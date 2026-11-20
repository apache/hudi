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

package org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.video.TrackedBlobSupplier;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The blob and FFmpeg grabber the audio segment source opens are closed on every way out, the seek resume included. */
class TestFfmpegAudioSegmentSourceHandleRelease {

  /** Tone 2 s, silence 1 s, tone 1.5 s, silence 2 s, tone 3 s: three speech segments. */
  private static final long[] LAYOUT = {2000, 1000, 1500, 2000, 3000};

  @TempDir
  static Path dir;
  private static Path aac;
  private static Path silentVideo;

  @BeforeAll
  static void generate() throws IOException {
    aac = AudioClips.generate(dir.resolve("aac.mp4"), AudioClips.Codec.AAC, 44100, 2, LAYOUT);
    silentVideo = VideoClips.generate(dir.resolve("video-only.mp4"), 2, 10, 2, 160, 120, -1, -1);
  }

  @Test
  void drainedTrackClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(aac);
    int segments = 0;
    try (FfmpegAudioSegmentSource.Segments it = open(bytes, aac, 0, 0, new TypedProperties())) {
      while (it.hasNext()) {
        AudioSegment segment = it.next();
        try (InputStream in = segment.openPayload()) {
          assertTrue(in.read() >= 0);
        }
        segments++;
      }
    }
    assertTrue(segments > 0, "the clip has speech");
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a drained track");
  }

  @Test
  void abandonedAfterTheFirstSegmentClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(aac);
    try (FfmpegAudioSegmentSource.Segments it = open(bytes, aac, 0, 0, new TypedProperties())) {
      assertTrue(it.hasNext());
      it.next();
    }
    assertTrue(bytes.opened() > 0, "the track was opened");
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the consumer stopped early");
  }

  @Test
  void refusalWhileOpeningClosesItsBlob() {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(aac);
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.MAX_DURATION_MS.key(), "1000");
    assertThrows(VideoDecodeException.class, () -> open(bytes, aac, 0, 0, props));
    assertEquals(1, bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a refusal while opening");
  }

  @Test
  void videoWithoutAudioClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(silentVideo);
    try (FfmpegAudioSegmentSource.Segments it = open(bytes, silentVideo, 0, 0, new TypedProperties())) {
      assertFalse(it.hasNext());
    }
    assertTrue(bytes.opened() > 0, "the video was opened");
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a video with no audio stream");
  }

  /** A resume part way seeks, and may reopen the grabber from the start: every blob it opened is closed. */
  @Test
  void resumePartWayClosesEveryBlobItOpened() throws IOException {
    for (long startMs : new long[] {200, 3000, 6500}) {
      TrackedBlobSupplier bytes = new TrackedBlobSupplier(aac);
      try (FfmpegAudioSegmentSource.Segments it = open(bytes, aac, 1, startMs, new TypedProperties())) {
        while (it.hasNext()) {
          it.next();
        }
      }
      assertTrue(bytes.opened() >= 1);
      assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a resume at " + startMs + " ms");
    }
  }

  @Test
  void storageFailurePartWayClosesItsBlob() throws IOException {
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(aac, Files.size(aac) / 2, n -> null);
    assertThrows(RuntimeException.class, () -> readAll(bytes));
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a storage failure part way");
  }

  private static void readAll(TrackedBlobSupplier bytes) throws IOException {
    try (FfmpegAudioSegmentSource.Segments it = open(bytes, aac, 0, 0, new TypedProperties())) {
      while (it.hasNext()) {
        it.next();
      }
    }
  }

  private static FfmpegAudioSegmentSource.Segments open(TrackedBlobSupplier bytes, Path clip, long startSegment, long startMs,
                                                        TypedProperties props) throws IOException {
    return new FfmpegAudioSegmentSource().segments(AudioClips.input("fid-release", clip, bytes), startSegment, startMs, props);
  }
}
