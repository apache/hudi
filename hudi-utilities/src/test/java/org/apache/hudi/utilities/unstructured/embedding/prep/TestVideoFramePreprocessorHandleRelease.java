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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubTranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.TrackedBlobSupplier;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The video preprocessor's main path with FFmpeg frames and FFmpeg speech segments (stub transcriber and chunker): every blob the
 * frames and the speech open is closed when the file's chunks are drained, abandoned, or fail part way.
 */
class TestVideoFramePreprocessorHandleRelease {

  /** 12 s of speech over H.264 video. */
  private static final String SPEECH_CLIP = "/video/h264-aac-speech-12s.mp4";

  @Test
  void framesAndSpeechDrainedCloseEveryBlob() throws Exception {
    Path clip = clip();
    TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
    List<String> modalities = new ArrayList<>();
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      try (ClosableIterator<Chunk> chunks = preprocessor.open(bytes, file(clip), new WordCountTokenizer(), audioOn())) {
        chunks.forEachRemaining(chunk -> modalities.add(chunk.getModality()));
      }
    }
    assertTrue(modalities.contains(EmbeddingTableSchema.MODALITY_IMAGE), modalities.toString());
    assertTrue(modalities.contains(EmbeddingTableSchema.MODALITY_TEXT), modalities.toString());
    assertTrue(bytes.opened() >= 2, "the frames' grabber and the speech's grabber: " + bytes.opened());
    assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after a drained video with speech");
  }

  @Test
  void abandonedAfterTheFirstChunkClosesEveryBlob() throws Exception {
    Path clip = clip();
    for (TypedProperties props : new TypedProperties[] {audioOn(), new TypedProperties()}) {
      TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
      try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
        try (ClosableIterator<Chunk> chunks = preprocessor.open(bytes, file(clip), new WordCountTokenizer(), props)) {
          assertTrue(chunks.hasNext());
          chunks.next();
        }
      }
      assertTrue(bytes.opened() > 0, "the video was opened");
      assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the consumer stopped early, audio "
          + props.getProperty(AudioConfigKeys.ENABLED.key(), "off"));
    }
  }

  /** The frames fail part way (held behind the speech with audio on, thrown at once with audio off). */
  @Test
  void framesThatFailPartWayCloseEveryBlob() throws Exception {
    Path clip = clip();
    for (TypedProperties props : new TypedProperties[] {audioOn(), new TypedProperties()}) {
      TrackedBlobSupplier bytes = new TrackedBlobSupplier(clip);
      VideoDecodeException corrupt = new VideoDecodeException("test: frame 1 cannot be decoded");
      FileMeta file = file(clip);
      try (VideoFramePreprocessor preprocessor = new FailingFrames(corrupt)) {
        assertThrows(VideoDecodeException.class, () -> readEveryChunk(preprocessor, bytes, file, props));
      }
      assertTrue(bytes.opened() > 0, "the video was opened");
      assertEquals(Collections.emptyList(), bytes.unclosed(), "unclosed handles after the frames failed part way, audio "
          + props.getProperty(AudioConfigKeys.ENABLED.key(), "off"));
    }
  }

  /** The real FFmpeg frames, failing in hasNext() after the first frame. */
  private static final class FailingFrames extends VideoFramePreprocessor {
    private FailingFrames(RuntimeException failure) {
      super(new FrameExtractionStrategy() {
        private final FfmpegKeyframeVideoFrameSource real = new FfmpegKeyframeVideoFrameSource();

        @Override
        public String name() {
          return real.name();
        }

        @Override
        public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props)
            throws IOException {
          ClosableIterator<ExtractedFrame> all = real.frames(video, file, props);
          return new ClosableIterator<ExtractedFrame>() {
            private int taken;

            @Override
            public boolean hasNext() {
              if (taken == 1) {
                throw failure;
              }
              return all.hasNext();
            }

            @Override
            public ExtractedFrame next() {
              taken++;
              return all.next();
            }

            @Override
            public void close() {
              all.close();
            }
          };
        }
      });
    }
  }

  private static void readEveryChunk(VideoFramePreprocessor preprocessor, BlobBytesSupplier bytes, FileMeta file, TypedProperties props)
      throws IOException {
    try (ClosableIterator<Chunk> chunks = preprocessor.open(bytes, file, new WordCountTokenizer(), props)) {
      chunks.forEachRemaining(chunk -> { });
    }
  }

  private static TypedProperties audioOn() {
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), StubTranscriptChunker.class.getName());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    return props;
  }

  private static Path clip() throws URISyntaxException {
    return Paths.get(Objects.requireNonNull(TestVideoFramePreprocessorHandleRelease.class.getResource(SPEECH_CLIP)).toURI());
  }

  private static FileMeta file(Path clip) throws IOException {
    return new FileMeta("fid-release", "mp4", Files.size(clip));
  }
}
