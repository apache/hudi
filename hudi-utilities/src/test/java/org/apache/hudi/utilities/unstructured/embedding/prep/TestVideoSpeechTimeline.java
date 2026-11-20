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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioStubs;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.AudioClips;
import org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The timeline of a video's frames and speech, and its speech when no frame is kept: the default video frame preprocessor with
 * audio on, the default FFmpeg audio segment source, the stub transcriber, which says one fixed six-word line per segment, and
 * the stub chunker; {@code chunk.max.tokens} = 16 packs two lines per chunk, so every expected speech chunk follows from the
 * segments the source cuts when run alone (the oracle).
 */
public class TestVideoSpeechTimeline {

  private static final String SPEECH = "/video/h264-aac-speech-12s.mp4";
  private static final String DELAYED = "/video/h264-aac-speech-video-delayed-1500ms.mp4";
  private static final String NOISE = "/video/h264-noise-aac-speech.mp4";
  /** The speech clip's frame interval: 120 frames over 12 s. */
  private static final long FRAME_INTERVAL_MS = 100;

  @TempDir
  Path dir;

  /**
   * Frames and speech share one timeline from the container start. With the video track delayed 1.5 s (the audio untouched),
   * the first frame moves to 1500 ms and the speech stays where the audio is: each agrees with the undelayed clip within one
   * frame interval.
   */
  @Test
  public void framesAndSpeechAreTimedFromTheContainerStart() throws IOException {
    List<Chunk> original = prepare(clip(SPEECH, "original.mp4"), "original");
    List<Chunk> delayed = prepare(clip(DELAYED, "delayed.mp4"), "delayed");
    long originalFrame = first(original, EmbeddingTableSchema.MODALITY_IMAGE, VideoFramePreprocessor.AUX_TIMESTAMP_MS);
    long delayedFrame = first(delayed, EmbeddingTableSchema.MODALITY_IMAGE, VideoFramePreprocessor.AUX_TIMESTAMP_MS);
    long originalSpeech = first(original, EmbeddingTableSchema.MODALITY_TEXT, AudioRows.AUX_START_MS);
    long delayedSpeech = first(delayed, EmbeddingTableSchema.MODALITY_TEXT, AudioRows.AUX_START_MS);
    assertTrue(originalFrame >= 0 && originalFrame < FRAME_INTERVAL_MS, "original first frame " + originalFrame);
    assertTrue(Math.abs(delayedFrame - 1500) < FRAME_INTERVAL_MS, "delayed first frame " + delayedFrame);
    assertTrue(Math.abs(delayedSpeech - originalSpeech) < FRAME_INTERVAL_MS, "speech " + originalSpeech + " vs " + delayedSpeech);
    // one timeline: the delayed clip's speech starts before its first frame when the audio starts within the delay
    assertTrue(delayedSpeech < delayedFrame || originalSpeech >= 1500, "speech " + delayedSpeech + ", frame " + delayedFrame);
  }

  /**
   * The audio track is transcribed whatever the frames do: a file whose video packets are noise with good audio keeps no frame,
   * which is no failure. It has its speech chunks alone, every one its audio makes, and no frame chunk; the file ends without a
   * failure.
   */
  @Test
  public void speechIsWrittenWhenNoFrameIsKept() throws IOException {
    Path noise = clip(NOISE, "noise.mp4");
    // prepare() fails the test on any failure of the file
    List<Chunk> chunks = prepare(noise, "noise");
    assertEquals(0, chunks.stream().filter(c -> EmbeddingTableSchema.MODALITY_IMAGE.equals(c.getModality())).count(),
        "noise: no frame kept");
    List<List<Long>> segments = segmentsOf(noise, "noise");
    assertTrue(segments.size() >= 2, "segments " + segments);
    assertSpeechChunks(chunks, segments);
  }

  // ---------------------------------------------------------------- oracle

  /** [start_ms, end_ms] of each segment the real FFmpeg source cuts from {@code clip}'s audio, run alone. */
  private static List<List<Long>> segmentsOf(Path clip, String fid) throws IOException {
    TypedProperties props = props();
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    source.validate(props);
    List<List<Long>> spans = new ArrayList<>();
    try (ClosableIterator<AudioSegment> segments = source.segments(AudioClips.input(fid, clip, VideoClips.supplier(clip)), 0, 0,
        props)) {
      while (segments.hasNext()) {
        AudioSegment segment = segments.next();
        spans.add(Arrays.asList(segment.startMs(), segment.endMs()));
      }
    }
    return spans;
  }

  /**
   * The video's speech chunks are one per pair of expected segments (two stub lines per chunk), in order, with the stub's text,
   * the first segment's start and the last segment's end.
   */
  private static void assertSpeechChunks(List<Chunk> chunks, List<List<Long>> segments) {
    List<Chunk> speech = chunks.stream().filter(c -> EmbeddingTableSchema.MODALITY_TEXT.equals(c.getModality()))
        .collect(Collectors.toList());
    String described = segments + " -> " + speech.stream().map(c -> c.getAux().get(AudioRows.AUX_FIRST_SEGMENT) + "-"
        + c.getAux().get(AudioRows.AUX_LAST_SEGMENT) + " " + c.getAux().get(AudioRows.AUX_START_MS) + "-"
        + c.getAux().get(AudioRows.AUX_END_MS) + " " + c.getText()).collect(Collectors.toList());
    assertEquals((segments.size() + 1) / 2, speech.size(), described);
    for (int k = 0; k < speech.size(); k++) {
      Chunk chunk = speech.get(k);
      int first = 2 * k;
      int last = Math.min(first + 1, segments.size() - 1);
      String text = "[speaker-1] " + StubSegmentTranscriber.utterance(first)
          + (last > first ? "\n[speaker-1] " + StubSegmentTranscriber.utterance(last) : "");
      assertEquals(text, chunk.getText());
      assertNull(chunk.getFrameIndex(), "speech is a plain text chunk the framework numbers");
      assertNull(chunk.getPayload(), "a speech row stores no input");
      Map<String, String> aux = chunk.getAux();
      assertEquals(String.valueOf(segments.get(first).get(0)), aux.get(AudioRows.AUX_START_MS), "chunk " + k);
      assertEquals(String.valueOf(segments.get(last).get(1)), aux.get(AudioRows.AUX_END_MS), "chunk " + k);
      assertEquals(String.valueOf(first), aux.get(AudioRows.AUX_FIRST_SEGMENT));
      assertEquals(String.valueOf(last), aux.get(AudioRows.AUX_LAST_SEGMENT));
      assertTrue(aux.get(AudioRows.AUX_AUDIO_LOGIC).endsWith(";speech=prep-v1"), aux.toString());
    }
  }

  // ---------------------------------------------------------------- helpers

  private static TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    // the default segment source (FFmpeg); the stub transcriber, not the default whisper (which needs a model), and the stub
    // chunker, which packs one [speaker] line per utterance
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    AudioStubs.stubChunker(props);
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    // two stub utterance lines ("[speaker-1] Utterance of audio segment k.", 6 words each) per chunk
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16");
    props.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    props.setProperty(AudioConfigKeys.SEGMENT_MAX_MS.key(), "3000");
    return props;
  }

  /** Every chunk the default video preprocessor makes of {@code clip}; any failure of the file fails the test. */
  private static List<Chunk> prepare(Path clip, String fid) throws IOException {
    TypedProperties props = props();
    Tokenizer tokenizer = TokenizerFactory.create(props);
    List<Chunk> out = new ArrayList<>();
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      preprocessor.validateConfig(props, tokenizer);
      try (ClosableIterator<Chunk> chunks = preprocessor.open(VideoClips.supplier(clip),
          new FileMeta(fid, "mp4", Files.size(clip)), tokenizer, props)) {
        chunks.forEachRemaining(out::add);
      }
    }
    return out;
  }

  private static long first(List<Chunk> chunks, String modality, String auxKey) {
    List<Chunk> ofModality = chunks.stream().filter(c -> modality.equals(c.getModality())).collect(Collectors.toList());
    assertFalse(ofModality.isEmpty(), "no " + modality + " chunk");
    return Long.parseLong(ofModality.get(0).getAux().get(auxKey));
  }

  private Path clip(String resource, String name) throws IOException {
    try (InputStream in = TestVideoSpeechTimeline.class.getResourceAsStream(resource)) {
      assertNotNull(in, resource);
      Path out = dir.resolve(name);
      Files.copy(in, out);
      return out;
    }
  }
}
