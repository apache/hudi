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
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The large-frames fixture ({@code /video/large-frames/large-frames-720p.mp4}, made by {@code generate-large-frames.sh}): a
 * small clip whose kept frames, as the FFmpeg frame source writes them with the default video config, are each a JPEG above
 * 80 KiB, the largest binary value Spark 3.5 keeps inline.
 */
@Tag("functional")
public class TestLargeFramesFixture {

  static final String FIXTURE = "/video/large-frames/large-frames-720p.mp4";
  /** Spark 3.5's inline cap for a binary value. */
  private static final int INLINE_CAP_BYTES = 80 * 1024;
  private static final long MAX_FIXTURE_BYTES = 1024 * 1024;

  @TempDir
  Path dir;

  @Test
  public void everyKeptFrameIsAJpegAboveTheInlineCap() throws IOException {
    Path clip = dir.resolve("large-frames-720p.mp4");
    try (InputStream in = TestLargeFramesFixture.class.getResourceAsStream(FIXTURE)) {
      assertNotNull(in, FIXTURE);
      Files.copy(in, clip);
    }
    long size = Files.size(clip);
    assertTrue(size <= MAX_FIXTURE_BYTES, "the fixture stays small: " + size + " bytes");

    List<ExtractedFrame> frames = new ArrayList<>();
    List<byte[]> images = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(clip),
        new FileMeta("fid-large-frames", "mp4", size), new TypedProperties())) {
      while (it.hasNext()) {
        ExtractedFrame frame = it.next();
        frames.add(frame);
        try (InputStream in = frame.getImage().open()) {
          images.add(in.readAllBytes());
        }
      }
    }
    System.out.println("LARGE_FRAMES fixture " + size + " bytes, " + frames.size() + " kept frames");
    for (int k = 0; k < frames.size(); k++) {
      ExtractedFrame frame = frames.get(k);
      System.out.println("LARGE_FRAMES frame " + k + " t=" + frame.getTimeMs() + "ms " + frame.getWidth() + "x"
          + frame.getHeight() + " " + frame.getMimeType() + " " + images.get(k).length + " bytes");
    }
    assertTrue(frames.size() >= 2, "the fixture keeps several frames: " + frames.size());
    for (int k = 0; k < frames.size(); k++) {
      ExtractedFrame frame = frames.get(k);
      assertEquals("image/jpeg", frame.getMimeType());
      assertEquals(images.get(k).length, frame.getSizeBytes(), "frame " + k + " size");
      assertTrue(images.get(k).length > INLINE_CAP_BYTES,
          "frame " + k + " is above the 80 KiB inline cap: " + images.get(k).length + " bytes");
    }
  }
}
