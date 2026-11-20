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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The speech columns of the codec and container support matrix: each clip of {@code /video/codec-matrix/} (cells listed in
 * {@code codec-matrix-expected.txt}) runs through {@link VideoFramePreprocessor} with audio on, the real FFmpeg frame extraction
 * and the real FFmpeg audio segment source, as a prep task does, and through the audio segment source alone. A cell with audio
 * has speech rows from 0 ms and no failed segment, whatever its frames do (an MPEG-TS cell fails on its frames after its speech);
 * a cell without audio has no speech row and its segments end with {@link FfmpegAudioSegmentSource#REASON_NO_AUDIO_STREAM}. The
 * frame columns are checked by the video codec matrix.
 */
class TestAudioPrepCodecMatrix {

  private static final String RESOURCES = "/video/codec-matrix/";
  /**
   * The MPEG-TS cells (no index: FFmpeg's backward seek is not keyframe-exact): each fails its frames at a seek, after the speech
   * is prepared; the text names how the seek landed. Every other cell finishes with no failure.
   */
  private static final Map<String, String> SEEK_FAILURES = new HashMap<>();

  static {
    SEEK_FAILURES.put("c16", "seek to the keyframe at 6600000us landed on the end of the stream");
    SEEK_FAILURES.put("c17", "seek to the keyframe at 3600000us landed on 3700000us, keyframe=false");
  }

  @TempDir
  static Path dir;

  static List<String[]> cells() throws IOException {
    List<String[]> cells = new ArrayList<>();
    try (InputStream in = TestAudioPrepCodecMatrix.class.getResourceAsStream(RESOURCES + "codec-matrix-expected.txt")) {
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
      try (InputStream in = TestAudioPrepCodecMatrix.class.getResourceAsStream(RESOURCES + cell[0])) {
        assertNotNull(in, cell[0]);
        Files.copy(in, dir.resolve(cell[0]));
      }
    }
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("cells")
  void cellSpeechThroughPrep(String file, String cell, String container, String video, String audio, String startMs,
                             String keyframesMs) throws IOException {
    Path clip = dir.resolve(file);
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), FfmpegAudioSegmentSource.class.getName());
    // the matrix measures demuxing and decoding: the stub transcriber, not the default whisper (which needs a model)
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    Tokenizer tokenizer = TokenizerFactory.create(props);
    List<String> speechStarts = new ArrayList<>();
    String failure = "";
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      preprocessor.validateConfig(props, tokenizer);
      try (ClosableIterator<Chunk> chunks = preprocessor.open(VideoClips.supplier(clip),
          new FileMeta("fid-" + cell, container, Files.size(clip)), tokenizer, props)) {
        while (chunks.hasNext()) {
          Chunk chunk = chunks.next();
          if (chunk.getFrameIndex() == null) {
            speechStarts.add(chunk.getAux().get(AudioRows.AUX_START_MS));
          }
        }
      }
    } catch (Exception e) {
      // a frame failure (the MPEG-TS cells, SEEK_FAILURES) comes after the speech; checked below
      failure = e.getClass().getSimpleName() + ": " + e.getMessage();
    }
    String segments = segments(clip, cell, container, props);
    String line = String.join("|", cell, file, container, video, audio, String.valueOf(speechStarts.size()),
        String.join(" ", speechStarts), segments, failure.replace('\n', ' '));
    System.out.println("AUDIO MATRIX " + line);
    if (SEEK_FAILURES.containsKey(cell)) {
      assertTrue(failure.startsWith("VideoDecodeException") && failure.contains(SEEK_FAILURES.get(cell)), line);
    } else {
      assertEquals("", failure, "the cell prepares with audio on and no failure: " + line);
    }
    if (audio.startsWith("none")) {
      assertTrue(speechStarts.isEmpty() && segments.endsWith("(" + FfmpegAudioSegmentSource.REASON_NO_AUDIO_STREAM + ")"), line);
    } else {
      // the speech is prepared whatever the frames do, measured from the same origin: the first tone at 0
      assertFalse(speechStarts.isEmpty(), line);
      assertEquals("0", speechStarts.get(0), line);
      assertTrue(segments.startsWith("0-") && !segments.contains("FAILED") && !segments.contains("("), line);
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
