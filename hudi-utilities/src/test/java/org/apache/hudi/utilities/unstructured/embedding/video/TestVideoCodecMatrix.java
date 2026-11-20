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
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Codec and container support matrix of the FFmpeg frame source: each clip of {@code /video/codec-matrix/} (made by
 * {@code generate-codec-matrix.sh} with the bytedeco GPL ffmpeg, expectations from ffprobe) runs through
 * {@link FfmpegKeyframeVideoFrameSource} with the real FFmpeg natives. Each cell records its frames (count and times), or the
 * failure that would be the file's FAILED row; the matrix is written to {@code target/codec-matrix/matrix.txt}.
 */
class TestVideoCodecMatrix {

  private static final String RESOURCES = "/video/codec-matrix/";
  private static final Map<String, String> MATRIX = new TreeMap<>();
  /**
   * The MPEG-TS cells (no index: FFmpeg's backward seek is not keyframe-exact): each is a FAILED row after the frames before its
   * failing seek — {frames kept before the failure, how the seek landed}. The H.264 clip's seeks land on time until its last
   * keyframe, whose seek reaches the end of the stream; the HEVC clip's second seek lands on a B-frame inside the GOP.
   */
  private static final Map<String, String[]> SEEK_FAILURES = new HashMap<>();

  static {
    SEEK_FAILURES.put("c16", new String[] {"5", "seek to the keyframe at 6600000us landed on the end of the stream"});
    SEEK_FAILURES.put("c17", new String[] {"2", "seek to the keyframe at 3600000us landed on 3700000us, keyframe=false"});
  }

  @TempDir
  static Path dir;

  static List<String[]> cells() throws IOException {
    List<String[]> cells = new ArrayList<>();
    try (InputStream in = TestVideoCodecMatrix.class.getResourceAsStream(RESOURCES + "codec-matrix-expected.txt")) {
      assertNotNull(in, "the expectations file");
      BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (!line.startsWith("#") && !line.trim().isEmpty()) {
          cells.add(line.split("\\|", -1));
        }
      }
    }
    return cells;
  }

  @BeforeAll
  static void copyClips() throws IOException {
    for (String[] cell : cells()) {
      try (InputStream in = TestVideoCodecMatrix.class.getResourceAsStream(RESOURCES + cell[0])) {
        assertNotNull(in, cell[0]);
        Files.copy(in, dir.resolve(cell[0]));
      }
    }
  }

  @AfterAll
  static void writeMatrix() throws IOException {
    Path out = Paths.get("target", "codec-matrix", "matrix.txt");
    Files.createDirectories(out.getParent());
    List<String> lines = new ArrayList<>();
    lines.add("# cell|file|container|video|audio|outcome|frames|frame ms|failure");
    lines.addAll(MATRIX.values());
    Files.write(out, lines, StandardCharsets.UTF_8);
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("cells")
  void cellDecodesThroughTheFrameSource(String file, String cell, String container, String video, String audio,
                                               String startMs, String keyframesMs) throws IOException {
    Path clip = dir.resolve(file);
    TypedProperties props = new TypedProperties();
    // every kept keyframe is a frame: the matrix measures decoding, not the dedup of a test pattern
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    List<Long> frameMs = new ArrayList<>();
    String failure = "";
    try (ClosableIterator<ExtractedFrame> frames = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(clip),
        new FileMeta("fid-" + cell, container, Files.size(clip)), props)) {
      while (frames.hasNext()) {
        frameMs.add(frames.next().getTimeMs());
      }
    } catch (Exception e) {
      failure = e.getClass().getSimpleName() + ": " + e.getMessage();
    }
    String line = String.join("|", cell, file, container, video, audio, failure.isEmpty() ? "DECODES" : "FAILED",
        String.valueOf(frameMs.size()), frameMs.stream().map(String::valueOf).collect(Collectors.joining(" ")),
        failure.replace('\n', ' '));
    MATRIX.put(cell, line);
    System.out.println("MATRIX " + line + " | expected keyframes " + keyframesMs + " start " + startMs);
    List<String> expectedFrames = Arrays.asList(keyframesMs.split(" "));
    List<String> frames = frameMs.stream().map(String::valueOf).collect(Collectors.toList());
    if (SEEK_FAILURES.containsKey(cell)) {
      // the frame source refuses a seek that does not land on the chosen keyframe: the file's FAILED row
      int kept = Integer.parseInt(SEEK_FAILURES.get(cell)[0]);
      assertTrue(failure.startsWith("VideoDecodeException") && failure.contains(SEEK_FAILURES.get(cell)[1]), line);
      // the reason names the video by its source_path_sha, so the matrix is the same on every run
      assertTrue(failure.startsWith("VideoDecodeException: video source_path_sha fid-" + cell + " (" + container + ", "), line);
      assertTrue(kept < expectedFrames.size(), line);
      assertEquals(expectedFrames.subList(0, kept), frames, "the frames before the failure, on time: " + line);
    } else {
      assertEquals("", failure, line);
      // demuxer PTS minus the container start_time, clamped at 0, DISCARD keyframes skipped (ffprobe's oracle)
      assertEquals(expectedFrames, frames, line);
    }
  }
}
