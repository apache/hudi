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
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frames whose BGR rows the decoder pads to a 64-pixel width: JavaCV derives a frame's channel count as its row stride over
 * its width, so a frame 129 to 144 pixels wide reads as 4 channels (65 to 96 wide as 4, 32 wide as 6). Such a clip yields its
 * frames at their native size, and their pixels are the clip's own, under both dedup methods.
 */
class TestFfmpegPaddedFrameRows {

  /** Mean absolute luma difference, per mosaic cell, between a stored frame and the picture the clip was drawn from. */
  private static final double MAX_MEAN_LUMA_ERROR = 16;

  @TempDir
  Path dir;

  @ParameterizedTest
  @CsvSource({"136,96,dhash", "136,96,sha256", "80,64,dhash", "32,24,dhash", "320,240,dhash"})
  void paddedRowsYieldTheClipsOwnFrames(int width, int height, String method) throws IOException {
    // keyframes every 2 s of a 6 s clip whose picture changes every second: 0, 2 and 4 s are kept
    Path clip = VideoClips.generate(dir.resolve("clip-" + width + "x" + height + ".mp4"), 6, 10, 2, width, height, -1, -1);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), method);
    List<Long> timestamps = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> frames = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(clip),
        new FileMeta("fid-" + width, "mp4", Files.size(clip)), props)) {
      while (frames.hasNext()) {
        ExtractedFrame frame = frames.next();
        timestamps.add(frame.getTimeMs());
        assertEquals(width + "x" + height, frame.getWidth() + "x" + frame.getHeight());
        BufferedImage stored;
        try (InputStream in = frame.getImage().open()) {
          stored = ImageFixtures.decode(readAll(in));
        }
        assertEquals(width + "x" + height, stored.getWidth() + "x" + stored.getHeight(), "native size");
        double error = meanLumaError(stored, (int) (frame.getTimeMs() / 1000));
        assertTrue(error <= MAX_MEAN_LUMA_ERROR, "frame at " + frame.getTimeMs() + " ms is the clip's picture: mean luma error "
            + error + " over the mosaic cells");
      }
    }
    assertEquals(Arrays.asList(0L, 2000L, 4000L), timestamps);
  }

  /**
   * The mean, over the cells of the second's mosaic that are at least 2 pixels inside, of |stored luma - drawn luma| at the cell's
   * centre ({@link VideoClips}: a seeded 16 x 12 grid of colours per second).
   */
  private static double meanLumaError(BufferedImage stored, int second) {
    int width = stored.getWidth();
    int height = stored.getHeight();
    Random random = new Random(1000L + second);
    double total = 0;
    int cells = 0;
    for (int row = 0; row < 12; row++) {
      for (int column = 0; column < 16; column++) {
        int level = random.nextInt(256);
        int x0 = column * width / 16;
        int x1 = (column + 1) * width / 16;
        int y0 = row * height / 12;
        int y1 = (row + 1) * height / 12;
        if (x1 - x0 < 5 || y1 - y0 < 5) {
          continue;
        }
        int rgb = stored.getRGB((x0 + x1) / 2, (y0 + y1) / 2);
        double drawn = luma(level, (level * 7) % 256, 255 - level);
        double actual = luma((rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff);
        total += Math.abs(drawn - actual);
        cells++;
      }
    }
    if (cells == 0) {
      // cells too small to sample (a 32 px wide clip): the size and the timestamps carry the check
      return 0;
    }
    return total / cells;
  }

  private static double luma(int red, int green, int blue) {
    return 0.299 * red + 0.587 * green + 0.114 * blue;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
      bytes.write(buffer, 0, n);
    }
    return bytes.toByteArray();
  }
}
