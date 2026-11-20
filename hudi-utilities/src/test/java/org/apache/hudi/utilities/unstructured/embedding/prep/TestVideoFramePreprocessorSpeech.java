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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioStubs;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubTranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunk;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The speech of the video frame preprocessor ({@code video-frames-v1}) with audio on: the frames, then the video's speech
 * chunks; a content failure of the frames is thrown after the speech, and one naming both when the speech fails too; a failure
 * that fails the round is never held behind the speech; a storage failure reading the audio is an interruption; the task's audio
 * implementations are built once and closed with the preprocessor. Real FFmpeg frames over a generated clip; the stub audio
 * implementations.
 */
class TestVideoFramePreprocessorSpeech {

  @TempDir
  static Path dir;
  /** 6 s at 160x120, keyframes every 2 s: kept frames at 0, 2 and 4 s. */
  private static Path clip;

  @BeforeAll
  static void generate() throws IOException {
    clip = VideoClips.generate(dir.resolve("clip.mp4"), 6, 10, 2, 160, 120, -1, -1);
  }

  /** One chunk per kept frame, then the speech chunks the stub source, transcriber and chunker make; audio off: frames only. */
  @Test
  void framesThenTheirSpeechWithAudioOnAndFramesOnlyWithAudioOff() throws IOException {
    TypedProperties props = AudioStubs.named(new TypedProperties());
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    Tokenizer tokenizer = TokenizerFactory.create(props);
    VideoFramePreprocessor preprocessor = new VideoFramePreprocessor();
    assertEquals(new HashSet<>(Arrays.asList(EmbeddingTableSchema.MODALITY_IMAGE, EmbeddingTableSchema.MODALITY_TEXT)),
        preprocessor.modalities(props));
    List<Chunk> chunks = drain(preprocessor.open(VideoClips.supplier(clip), file(Files.size(clip)), tokenizer, props));
    assertEquals(4, chunks.size(), chunks.toString());
    for (int k = 0; k < 3; k++) {
      Chunk frame = chunks.get(k);
      assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, frame.getModality());
      assertEquals(Long.valueOf(k), frame.getFrameIndex(), "the frame index rides on the chunk");
      assertEquals(ImageContentTypes.JPEG, frame.getPayloadContentType());
      assertEquals(String.valueOf(k), frame.getAux().get(EmbeddingTableSchema.AUX_FRAME_INDEX));
      assertEquals(String.valueOf(2000L * k), frame.getAux().get(VideoFramePreprocessor.AUX_TIMESTAMP_MS));
      assertEquals(FfmpegKeyframeVideoFrameSource.LOGIC_VERSION, frame.getAux().get(VideoFramePreprocessor.AUX_VIDEO_LOGIC));
    }
    // the stub source: one 30 s segment over the 6 s clip; the stub transcriber: one utterance per segment
    Chunk speech = chunks.get(3);
    assertEquals(EmbeddingTableSchema.MODALITY_TEXT, speech.getModality());
    assertNull(speech.getPayload());
    assertNull(speech.getFrameIndex(), "speech is a plain text chunk the framework numbers");
    assertEquals("[speaker-1] Utterance of audio segment 0.", speech.getText());
    assertEquals("0", speech.getAux().get(AudioRows.AUX_START_MS));
    assertEquals("6000", speech.getAux().get(AudioRows.AUX_END_MS));
    assertEquals("audio=audio-stub-v0;asr=stub/fixed-utterance-v0;pack=transcript-stub-pack-v0;tokenizer=" + tokenizer.id()
        + ";speech=prep-v1", speech.getAux().get(AudioRows.AUX_AUDIO_LOGIC));
    // audio off: the frames only
    assertEquals(Collections.singleton(EmbeddingTableSchema.MODALITY_IMAGE), preprocessor.modalities(new TypedProperties()));
    assertEquals(3, drain(new VideoFramePreprocessor().open(VideoClips.supplier(clip), file(Files.size(clip)), tokenizer,
        new TypedProperties())).size());
  }

  /**
   * with audio on, a content failure of the frames — at the open (no video stream) or part way — keeps the
   * frames before it, the speech is still prepared, and the frames' failure is thrown after it (the file's FAILED marker).
   */
  @Test
  void framesThatFailStillLeaveTheirSpeechBeforeTheFailure() throws IOException {
    TypedProperties props = audioOn();
    VideoDecodeException noVideo = new VideoDecodeException("video v has no video stream");
    List<Chunk> before = new ArrayList<>();
    ClosableIterator<Chunk> noFrames = new FailingFramesPreprocessor(0, noVideo, FailingFramesPreprocessor.AT_OPEN)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    VideoDecodeException atOpen = assertThrows(VideoDecodeException.class, () -> drainInto(noFrames, before));
    assertSame(noVideo, atOpen);
    assertEquals(Collections.singletonList(EmbeddingTableSchema.MODALITY_TEXT), modalities(before), "the speech only");

    VideoDecodeException corrupt = new VideoDecodeException("video v cannot be decoded at 4000000us");
    before.clear();
    ClosableIterator<Chunk> failingInHasNext = new FailingFramesPreprocessor(2, corrupt, FailingFramesPreprocessor.IN_HAS_NEXT)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    assertSame(corrupt, assertThrows(VideoDecodeException.class, () -> drainInto(failingInHasNext, before)));
    assertEquals(Arrays.asList(EmbeddingTableSchema.MODALITY_IMAGE, EmbeddingTableSchema.MODALITY_IMAGE,
        EmbeddingTableSchema.MODALITY_TEXT), modalities(before), "the frames before the failure, then the speech");

    // a content failure met in the strategy's next(), or when its frames are closed after a clean end, is held too
    before.clear();
    ClosableIterator<Chunk> failingInNext = new FailingFramesPreprocessor(2, corrupt, FailingFramesPreprocessor.IN_NEXT)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    assertSame(corrupt, assertThrows(VideoDecodeException.class, () -> drainInto(failingInNext, before)));
    assertEquals(Arrays.asList(EmbeddingTableSchema.MODALITY_IMAGE, EmbeddingTableSchema.MODALITY_IMAGE,
        EmbeddingTableSchema.MODALITY_TEXT), modalities(before), "a failure in next(): the frames before it, then the speech");
    before.clear();
    ClosableIterator<Chunk> failingAtClose = new FailingFramesPreprocessor(-1, corrupt, FailingFramesPreprocessor.AT_CLOSE)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    assertSame(corrupt, assertThrows(VideoDecodeException.class, () -> drainInto(failingAtClose, before)));
    assertEquals(Arrays.asList(EmbeddingTableSchema.MODALITY_IMAGE, EmbeddingTableSchema.MODALITY_IMAGE,
        EmbeddingTableSchema.MODALITY_IMAGE, EmbeddingTableSchema.MODALITY_TEXT), modalities(before),
        "a failure closing the frames: every frame, then the speech");

    // audio off: the frames' failure ends the file at once
    ClosableIterator<Chunk> audioOff = new FailingFramesPreprocessor(2, corrupt, FailingFramesPreprocessor.IN_HAS_NEXT)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), new TypedProperties());
    assertThrows(VideoDecodeException.class, () -> drain(audioOff));
  }

  /**
   * With audio on, a failure of the frames that fails the round (a platform failure, here a codec missing from the executor)
   * is never held behind the speech: it leaves at once, so no speech row of the video is prepared and no quarantine entry is built.
   */
  @Test
  void framesThatFailTheRoundAreNotHeldBehindTheSpeech() throws IOException {
    TypedProperties props = audioOn();
    PrepPlatformException platform = new PrepPlatformException("no JPEG writer", null);
    int[] places = {FailingFramesPreprocessor.AT_OPEN, FailingFramesPreprocessor.IN_HAS_NEXT, FailingFramesPreprocessor.IN_NEXT,
        FailingFramesPreprocessor.AT_CLOSE};
    for (int where : places) {
      List<Chunk> before = new ArrayList<>();
      RuntimeException failure = assertThrows(RuntimeException.class, () -> openAndDrainInto(where, platform, props, before));
      assertSame(platform, PrepFailures.roundFailure(failure), where + ": " + failure);
      assertFalse(modalities(before).contains(EmbeddingTableSchema.MODALITY_TEXT), where + ": no speech row: " + modalities(before));
    }
    // wrapped by the strategy, it still fails the round at once
    List<Chunk> before = new ArrayList<>();
    ClosableIterator<Chunk> wrappedFailure = new FailingFramesPreprocessor(1,
        new VideoDecodeException("video v frame 1 cannot be encoded", platform), FailingFramesPreprocessor.IN_HAS_NEXT)
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    RuntimeException wrapped = assertThrows(RuntimeException.class, () -> drainInto(wrappedFailure, before));
    assertTrue(PrepFailures.roundFailure(wrapped) instanceof PrepPlatformException, wrapped.toString());
    assertFalse(modalities(before).contains(EmbeddingTableSchema.MODALITY_TEXT), modalities(before).toString());
  }

  @Test
  void framesAndSpeechThatBothFailMakeOneFailureNamingBoth() throws IOException {
    TypedProperties props = audioOn();
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), RefusingTranscriber.class.getName());
    VideoFramePreprocessor failingFrames = new FailingFramesPreprocessor(1, new VideoDecodeException("video v cannot be decoded"),
        FailingFramesPreprocessor.IN_HAS_NEXT);
    ClosableIterator<Chunk> bothFailing = failingFrames.open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    PreprocessingException both = assertThrows(PreprocessingException.class, () -> drain(bothFailing));
    assertEquals(VideoFramePreprocessor.FRAMES_PREFIX + "VideoDecodeException: video v cannot be decoded; "
        + VideoSpeechChunks.SPEECH_PREFIX + "IllegalStateException: the engine refused segment 0", both.getMessage());
    // the speech alone failing: its own failure, after the frames
    List<Chunk> before = new ArrayList<>();
    ClosableIterator<Chunk> speechFailing = new VideoFramePreprocessor()
        .open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(), props);
    PreprocessingException speech = assertThrows(PreprocessingException.class, () -> drainInto(speechFailing, before));
    assertEquals(VideoSpeechChunks.SPEECH_PREFIX + "IllegalStateException: the engine refused segment 0", speech.getMessage());
    assertEquals(3, before.size(), "the frames stay");
  }

  /** The size guard reads nothing, speech included. */
  @Test
  void videoAboveTheSizeGuardWithAudioOnReadsNothing() {
    BlobBytesSupplier never = () -> {
      throw new AssertionError("an oversized video is never read");
    };
    TypedProperties props = audioOn();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "4999");
    VideoFramePreprocessor preprocessor = new VideoFramePreprocessor();
    FileMeta oversized = file(5000L);
    WordCountTokenizer tokenizer = new WordCountTokenizer();
    assertThrows(PreprocessingException.class, () -> preprocessor.open(never, oversized, tokenizer, props));
  }

  /** A storage failure while the audio is read, after the frames: an interruption of the round, never a FAILED row. */
  @Test
  void storageFailureReadingTheAudioIsAnInterruption() throws IOException {
    TypedProperties props = audioOn();
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), UnreadableAudioSource.class.getName());
    ClosableIterator<Chunk> unreadableAudio = new VideoFramePreprocessor().open(VideoClips.supplier(clip),
        file(Files.size(clip)), new WordCountTokenizer(), props);
    assertThrows(SourceReadException.class, () -> drain(unreadableAudio));
  }

  /** One instance of each audio implementation per task, validated once, closed once with the preprocessor (lifecycle). */
  @Test
  void theTasksAudioImplementationsAreBuiltOnceAndClosedWithThePreprocessor() throws IOException {
    TypedProperties props = audioOn();
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), CountingTranscriber.class.getName());
    CountingTranscriber.CREATED.set(0);
    CountingTranscriber.VALIDATED.set(0);
    CountingTranscriber.CLOSED.set(0);
    VideoFramePreprocessor preprocessor = new VideoFramePreprocessor();
    for (int video = 0; video < 3; video++) {
      assertEquals(4, drain(preprocessor.open(VideoClips.supplier(clip), file(Files.size(clip)), new WordCountTokenizer(),
          props)).size());
    }
    assertEquals(1, CountingTranscriber.CREATED.get());
    assertEquals(1, CountingTranscriber.VALIDATED.get());
    assertEquals(0, CountingTranscriber.CLOSED.get(), "open while the task runs");
    preprocessor.close();
    preprocessor.close();
    assertEquals(1, CountingTranscriber.CLOSED.get(), "closed once");
  }

  /**
   * The default FFmpeg segment source links FFmpeg lazily, so a missing class can surface when its segments are opened, read or
   * advanced: the guard turns each into the configuration error naming the FFmpeg jars setting, and keeps the source's logic
   * version.
   */
  @Test
  void theFfmpegLinkageGuardTurnsEveryLinkageErrorIntoTheConfigurationError() {
    for (int where = 0; where < 3; where++) {
      int failAt = where;
      AudioSegmentSource unlinkable = new StubAudioSegmentSource() {
        @Override
        public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props) {
          if (failAt == 0) {
            throw new NoClassDefFoundError("org/bytedeco/javacv/FFmpegFrameGrabber");
          }
          return new ClosableIterator<AudioSegment>() {
            @Override
            public boolean hasNext() {
              if (failAt == 1) {
                throw new NoClassDefFoundError("org/bytedeco/javacv/FrameGrabber");
              }
              return true;
            }

            @Override
            public AudioSegment next() {
              throw new UnsatisfiedLinkError("no jniavcodec in java.library.path");
            }

            @Override
            public void close() {
              // the stub iterator holds no resource
            }
          };
        }
      };
      VideoFramePreprocessor.FfmpegLinkageGuard guard = new VideoFramePreprocessor.FfmpegLinkageGuard(unlinkable);
      PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class, () -> readFirstSegment(guard),
          "linkage error at step " + failAt);
      assertTrue(refused.getMessage().contains(VideoFramePreprocessor.FFMPEG_JARS_SETTING), refused.getMessage());
      assertTrue(refused.getMessage().contains(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key()), refused.getMessage());
      assertTrue(refused.getCause() instanceof LinkageError, String.valueOf(refused.getCause()));
    }
    // a source that links passes through unchanged
    VideoFramePreprocessor.FfmpegLinkageGuard passing = new VideoFramePreprocessor.FfmpegLinkageGuard(new StubAudioSegmentSource());
    assertEquals(new StubAudioSegmentSource().logicVersion(), passing.logicVersion());
  }

  @Test
  public void audioPartsCloseOnTranscriberValidationError() throws IOException {
    TypedProperties props = audioOn();
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), SetupSource.class.getName());
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), SetupTranscriber.class.getName());
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), SetupChunker.class.getName());
    SetupSource.CLOSED.set(0);
    SetupTranscriber.CLOSED.set(0);
    SetupChunker.CLOSED.set(0);
    FileMeta metadata = file(Files.size(clip));
    WordCountTokenizer tokenizer = new WordCountTokenizer();

    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      assertEquals(4, drain(preprocessor.open(VideoClips.supplier(clip), metadata, tokenizer, props)).size());
      assertSetupCloseCount(0);
    }
    assertSetupCloseCount(1);

    props.setProperty(SetupTranscriber.FAIL_VALIDATION, "true");
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      AssertionError failure = assertThrows(AssertionError.class,
          () -> preprocessor.open(VideoClips.supplier(clip), metadata, tokenizer, props));
      assertSame(SetupTranscriber.FAILURE, failure);
      assertSetupCloseCount(2);
    }
    assertSetupCloseCount(2);
  }

  private static void assertSetupCloseCount(int expected) {
    assertEquals(expected, SetupSource.CLOSED.get(), "segment source");
    assertEquals(expected, SetupTranscriber.CLOSED.get(), "transcriber");
    assertEquals(expected, SetupChunker.CLOSED.get(), "chunker");
  }

  public static final class SetupSource extends StubAudioSegmentSource implements AutoCloseable {
    private static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  public static final class SetupTranscriber extends StubSegmentTranscriber implements AutoCloseable {
    private static final String FAIL_VALIDATION = "test.speech.fail.transcriber.validation";
    private static final AssertionError FAILURE = new AssertionError("transcriber validation ended early");
    private static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void validate(TypedProperties props) {
      super.validate(props);
      if (Boolean.parseBoolean(props.getProperty(FAIL_VALIDATION))) {
        throw FAILURE;
      }
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  public static final class SetupChunker extends StubTranscriptChunker implements AutoCloseable {
    private static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  @Test
  public void speechNextFailureRetainsTheEarlierFrameFailure() throws IOException {
    TypedProperties props = audioOn();
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), OffsetOrdinalChunker.class.getName());
    FileMeta metadata = file(Files.size(clip));
    WordCountTokenizer tokenizer = new WordCountTokenizer();
    List<Chunk> frames = new ArrayList<>();
    PreprocessingException speechOnly;
    try (VideoFramePreprocessor preprocessor = new VideoFramePreprocessor()) {
      speechOnly = assertThrows(PreprocessingException.class, () -> drainInto(
          preprocessor.open(VideoClips.supplier(clip), metadata, tokenizer, props), frames));
    }
    assertEquals(Collections.nCopies(3, EmbeddingTableSchema.MODALITY_IMAGE), modalities(frames));
    assertTrue(speechOnly.getMessage().startsWith(VideoSpeechChunks.SPEECH_PREFIX), speechOnly.getMessage());
    assertTrue(speechOnly.getMessage().contains("returned ordinal 1 where 0 was due"), speechOnly.getMessage());

    VideoDecodeException frameFailure = new VideoDecodeException("frame stream unavailable");
    try (VideoFramePreprocessor preprocessor = new FailingFramesPreprocessor(0, frameFailure, FailingFramesPreprocessor.AT_OPEN)) {
      PreprocessingException both = assertThrows(PreprocessingException.class,
          () -> drain(preprocessor.open(VideoClips.supplier(clip), metadata, tokenizer, props)));
      assertTrue(both.getMessage().contains(frameFailure.getMessage()), both.getMessage());
      assertTrue(both.getMessage().contains(speechOnly.getMessage()), both.getMessage());
    }
  }

  public static final class OffsetOrdinalChunker extends StubTranscriptChunker {
    @Override
    public ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal,
                                                    Tokenizer tokenizer, TypedProperties props) {
      return super.chunks(utterances, firstOrdinal + 1, tokenizer, props);
    }
  }

  private static TypedProperties audioOn() {
    TypedProperties props = AudioStubs.named(new TypedProperties());
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    return props;
  }

  private static List<String> modalities(List<Chunk> chunks) {
    List<String> modalities = new ArrayList<>();
    chunks.forEach(chunk -> modalities.add(chunk.getModality()));
    return modalities;
  }

  private static void openAndDrainInto(int where, RuntimeException failure, TypedProperties props, List<Chunk> chunks) throws IOException {
    drainInto(new FailingFramesPreprocessor(where == FailingFramesPreprocessor.AT_CLOSE ? -1 : 1, failure, where).open(VideoClips.supplier(clip),
        file(Files.size(clip)), new WordCountTokenizer(), props), chunks);
  }

  private static void readFirstSegment(VideoFramePreprocessor.FfmpegLinkageGuard guard) throws IOException {
    try (ClosableIterator<AudioSegment> segments = guard.segments(null, 0, 0, new TypedProperties())) {
      if (segments.hasNext()) {
        segments.next();
      }
    }
  }

  private static void drainInto(ClosableIterator<Chunk> iterator, List<Chunk> chunks) {
    try {
      iterator.forEachRemaining(chunks::add);
    } finally {
      iterator.close();
    }
  }

  /**
   * The real FFmpeg frames of the clip, failing with {@code failure} at the open, in {@code hasNext()} or {@code next()} after
   * {@code frames} frames, or when the frames are closed.
   */
  private static final class FailingFramesPreprocessor extends VideoFramePreprocessor {
    static final int AT_OPEN = 0;
    static final int IN_HAS_NEXT = 1;
    static final int IN_NEXT = 2;
    static final int AT_CLOSE = 3;

    private FailingFramesPreprocessor(int frames, RuntimeException failure, int where) {
      super(new FrameExtractionStrategy() {
        private final FfmpegKeyframeVideoFrameSource real = new FfmpegKeyframeVideoFrameSource();

        @Override
        public String name() {
          return real.name();
        }

        @Override
        public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props)
            throws IOException {
          if (where == AT_OPEN) {
            throw failure;
          }
          ClosableIterator<ExtractedFrame> all = real.frames(video, file, props);
          return new ClosableIterator<ExtractedFrame>() {
            private int taken;

            @Override
            public boolean hasNext() {
              if (where == IN_HAS_NEXT && taken == frames) {
                throw failure;
              }
              return all.hasNext();
            }

            @Override
            public ExtractedFrame next() {
              if (where == IN_NEXT && taken == frames) {
                throw failure;
              }
              taken++;
              return all.next();
            }

            @Override
            public void close() {
              all.close();
              if (where == AT_CLOSE) {
                throw failure;
              }
            }
          };
        }
      });
    }
  }

  /** A transcriber that refuses every segment: a transcription error (PERMANENT for the video). */
  public static class RefusingTranscriber extends StubSegmentTranscriber {
    @Override
    public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) {
      throw new IllegalStateException("the engine refused segment " + segment.index());
    }
  }

  /** A segment source whose read fails in storage: an interruption. */
  public static class UnreadableAudioSource extends StubAudioSegmentSource {
    @Override
    public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props) {
      throw new UncheckedIOException("audio read", new BlobReadException(new IOException("connection reset")));
    }
  }

  /** The stub transcriber, counting its instances, validations and closes. */
  public static class CountingTranscriber extends StubSegmentTranscriber implements AutoCloseable {
    static final AtomicInteger CREATED = new AtomicInteger();
    static final AtomicInteger VALIDATED = new AtomicInteger();
    static final AtomicInteger CLOSED = new AtomicInteger();

    public CountingTranscriber() {
      CREATED.incrementAndGet();
    }

    @Override
    public void validate(TypedProperties props) {
      VALIDATED.incrementAndGet();
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  private static FileMeta file(long size) {
    return new FileMeta("fid-1", "mp4", size);
  }

  private static List<Chunk> drain(ClosableIterator<Chunk> iterator) {
    List<Chunk> chunks = new ArrayList<>();
    try {
      iterator.forEachRemaining(chunks::add);
    } finally {
      iterator.close();
    }
    return chunks;
  }
}
