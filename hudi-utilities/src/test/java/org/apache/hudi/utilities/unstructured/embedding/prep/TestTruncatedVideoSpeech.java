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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A video cut mid-stream with audio on: the speech of what is left is still prepared, and the file then fails on its frames'
 * truncation (a {@link VideoDecodeException}, the file's failure). The real FFmpeg frames and audio segment source, the stub
 * transcriber, over the truncation fixtures of the codec matrix.
 */
class TestTruncatedVideoSpeech {

  @TempDir
  Path dir;

  /** With speech on, a cut mkv's speech rows are still prepared and the file then fails on its frames' truncation. */
  @Test
  void speechIsPreparedBeforeTheTruncationFailsTheFile() throws IOException {
    byte[] whole;
    try (InputStream in = TestTruncatedVideoSpeech.class.getResourceAsStream("/video/codec-matrix/c08-h264-mp3.mkv")) {
      assertNotNull(in, "the clip");
      whole = in.readAllBytes();
    }
    Path cut = Files.write(dir.resolve("cut.mkv"), Arrays.copyOf(whole, whole.length / 2));
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), FfmpegAudioSegmentSource.class.getName());
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    List<Chunk> chunks = new ArrayList<>();
    VideoDecodeException failure = assertThrows(VideoDecodeException.class, () -> prepare(cut, "mkv", props, chunks));
    assertTrue(failure.getMessage().contains("the file is truncated"), failure.getMessage());
    assertTrue(chunks.stream().anyMatch(c -> c.getFrameIndex() != null), chunks.toString());
    assertTrue(chunks.stream().anyMatch(c -> c.getFrameIndex() == null), "speech rows");
  }

  private static void prepare(Path video, String extension, TypedProperties props, List<Chunk> out) throws IOException {
    // every kept keyframe is a row
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    Tokenizer tokenizer = TokenizerFactory.create(props);
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      preprocessor.validateConfig(props, tokenizer);
      try (ClosableIterator<Chunk> chunks = preprocessor.open(VideoClips.supplier(video),
          new FileMeta("fid-" + extension, extension, Files.size(video)), tokenizer, props)) {
        while (chunks.hasNext()) {
          out.add(chunks.next());
        }
      }
    }
    assertFalse(out.isEmpty(), video.toString());
  }
}
