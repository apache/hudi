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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct audio-source coverage for the codec and container support matrix. */
class TestAudioCodecMatrix {

  private static final String RESOURCES = "/video/codec-matrix/";
  @TempDir
  static Path dir;

  static List<String[]> cells() throws IOException {
    List<String[]> cells = new ArrayList<>();
    try (InputStream in = TestAudioCodecMatrix.class.getResourceAsStream(RESOURCES + "codec-matrix-expected.txt")) {
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
      try (InputStream in = TestAudioCodecMatrix.class.getResourceAsStream(RESOURCES + cell[0])) {
        assertNotNull(in, cell[0]);
        Files.copy(in, dir.resolve(cell[0]));
      }
    }
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("cells")
  void cellAudioSegments(String file, String cell, String container, String video, String audio, String startMs,
                         String keyframesMs) {
    String spans = segments(dir.resolve(file), cell, container, new TypedProperties());
    if (audio.startsWith("none")) {
      assertEquals(" (" + FfmpegAudioSegmentSource.REASON_NO_AUDIO_STREAM + ")", spans, cell);
    } else {
      assertTrue(spans.startsWith("0-") && !spans.contains("FAILED") && !spans.contains("("), cell + " " + spans);
    }
  }

  /** The audio source's segments of the clip run alone ("start-end" ms each), its end reason, or its failure. */
  private static String segments(Path clip, String cell, String extension, TypedProperties props) {
    AudioInput input = new AudioInput() {
      @Override
      public String sourcePathSha() {
        return "fid-" + cell;
      }

      @Override
      public String sourcePath() {
        return clip.toString();
      }

      @Override
      public String sourcePartitionPath() {
        return null;
      }

      @Override
      public long sizeBytes() {
        try {
          return Files.size(clip);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }

      @Override
      public String extension() {
        return extension;
      }

      @Override
      public BlobBytesSupplier bytes() {
        return VideoClips.supplier(clip);
      }
    };
    List<String> spans = new ArrayList<>();
    try (FfmpegAudioSegmentSource.Segments segments = new FfmpegAudioSegmentSource().segments(input, 0, 0, props)) {
      while (segments.hasNext()) {
        AudioSegment segment = segments.next();
        spans.add(segment.startMs() + "-" + segment.endMs());
      }
      String reason = segments.endReason();
      return String.join(" ", spans) + (reason == null ? "" : " (" + reason + ")");
    } catch (Exception e) {
      return String.join(" ", spans) + " FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }
  }
}
