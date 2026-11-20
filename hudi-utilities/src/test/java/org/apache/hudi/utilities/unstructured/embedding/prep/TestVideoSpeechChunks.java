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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.SpeakerLabels;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubTranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunk;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptPacker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerException;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The speech phase of a video in prep ({@link VideoSpeechChunks}): its rows, its lazy one-segment-at-a-time
 * pull, the segment guard, the utterance guard, the failure classes (content failure of the video, interruption of the
 * round, configuration error of the task) and that every way out, an {@link Error} included, closes the open utterance iterator
 * and the segments, and the real packer's chunk iterator exactly once.
 */
public class TestVideoSpeechChunks {

  private static final String LOGIC = "audio=fake;asr=fake;pack=" + StubTranscriptChunker.LOGIC_VERSION + ";tokenizer=word-count";

  private TypedProperties props;
  private FakeSource source;
  private FakeTranscriber transcriber;
  private TranscriptChunker chunker;

  @BeforeEach
  public void setUp() {
    props = new TypedProperties();
    // one utterance line ("[speaker-1] words of segment k", 5 words) per chunk
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "6");
    props.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    source = new FakeSource();
    transcriber = new FakeTranscriber();
    chunker = new StubTranscriptChunker();
  }

  @Test
  public void eachTranscriptChunkIsATextChunkWithItsOrdinalAndTranscriptAux() {
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1500, 2500), segment(2, 4000, 5000));
    List<Chunk> chunks = drain(speech());
    assertEquals(3, chunks.size(), chunks.toString());
    for (int k = 0; k < 3; k++) {
      Chunk chunk = chunks.get(k);
      assertEquals(EmbeddingTableSchema.MODALITY_TEXT, chunk.getModality());
      assertNull(chunk.getFrameIndex(), "a plain text chunk: the framework numbers it");
      assertEquals("[speaker-1] words of segment " + k, chunk.getText());
      assertEquals(Integer.valueOf(5), chunk.getTokenCount());
      assertNull(chunk.getPayload());
      Map<String, String> aux = chunk.getAux();
      AudioSegment segment = source.segments.get(k);
      assertEquals(String.valueOf(segment.startMs()), aux.get(AudioRows.AUX_START_MS));
      assertEquals(String.valueOf(segment.endMs()), aux.get(AudioRows.AUX_END_MS));
      assertEquals(SpeakerLabels.FILE_SPEAKER, aux.get(AudioRows.AUX_SPEAKERS));
      assertEquals(SpeakerLabels.SCOPE_FILE, aux.get(AudioRows.AUX_SPEAKER_SCOPE));
      assertEquals(String.valueOf(k), aux.get(AudioRows.AUX_FIRST_SEGMENT));
      assertEquals(String.valueOf(k), aux.get(AudioRows.AUX_LAST_SEGMENT));
      assertEquals(LOGIC, aux.get(AudioRows.AUX_AUDIO_LOGIC));
    }
    assertEquals(1, source.closes, "the segments are closed once the video is packed");
    assertEquals(transcriber.opened, transcriber.closed, "every utterance iterator is closed");
  }

  /** Nothing sorts or buffers the segments; the source runs at most one segment ahead of what is emitted. */
  @Test
  public void segmentsArePulledOneAtATimeWithOneUtteranceIteratorOpen() {
    List<AudioSegment> many = new ArrayList<>();
    for (int k = 0; k < 50; k++) {
      many.add(segment(k, 1000L * k, 1000L * k + 800));
    }
    source.segments = many;
    VideoSpeechChunks speech = speech();
    long ordinal = 0;
    while (speech.hasNext()) {
      Chunk chunk = speech.next();
      // the chunker looks at the next utterance to end a chunk: at most that one segment beyond the emitted one
      assertTrue(source.produced <= ordinal + 2, "segments produced " + source.produced + " at chunk " + ordinal);
      ordinal++;
    }
    assertEquals(50, ordinal);
    assertEquals(1, transcriber.maxOpen, "one segment's utterance iterator open at a time");
  }

  @Test
  public void silenceGapsAreAllowedAndASegmentWithoutUtterancesWritesNothing() {
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 60_000, 61_000), segment(2, 61_000, 62_000));
    transcriber.silent = 1;
    List<Chunk> chunks = drain(speech());
    assertEquals(Arrays.asList("[speaker-1] words of segment 0", "[speaker-1] words of segment 2"), texts(chunks));
    source.segments = Collections.emptyList();
    assertEquals(Collections.emptyList(), drain(speech()), "no speech, no row");
  }

  /** The segment guard: each breach is the video's content failure, after the rows before it. */
  @Test
  public void segmentsOutOfOrderRepeatedGappedOrOverlappingFailTheVideo() {
    assertGuard("index out of order", Arrays.asList(segment(0, 0, 1000), segment(2, 2000, 3000)), "segment 2 where 1 was due");
    assertGuard("index repeated", Arrays.asList(segment(0, 0, 1000), segment(0, 2000, 3000)), "segment 0 where 1 was due");
    assertGuard("index not from 0", Collections.singletonList(segment(1, 0, 1000)), "segment 1 where 0 was due");
    assertGuard("time overlap", Arrays.asList(segment(0, 0, 1000), segment(1, 900, 2000)), "starting at 900 ms, before the end");
    assertGuard("time backwards", Arrays.asList(segment(0, 5000, 6000), segment(1, 1000, 2000)), "starting at 1000 ms");
    assertGuard("empty span", Collections.singletonList(segment(0, 1000, 1000)), "spanning [1000, 1000)");
    assertGuard("negative start", Collections.singletonList(segment(0, -5, 1000)), "spanning [-5, 1000)");
  }

  /** The utterance guard: a start outside its segment's span or before the previous start fails the video. */
  @Test
  public void utterancesStartingOutOfTheirSegmentOrDecreasingFailTheVideo() {
    assertUtteranceGuard("decreasing in a segment", Arrays.asList(utterance(500, 900), utterance(400, 900)),
        "starting at 400 ms, before the previous utterance's start at 500 ms");
    assertUtteranceGuard("before the segment", Collections.singletonList(utterance(1500, 1900)),
        "starting at 1500 ms, outside the segment's span [2000, 3000) ms");
    assertUtteranceGuard("at the segment end", Collections.singletonList(utterance(3000, 3000)),
        "starting at 3000 ms, outside the segment's span [2000, 3000) ms");
    assertUtteranceGuard("after the segment", Collections.singletonList(utterance(4500, 4600)),
        "starting at 4500 ms, outside the segment's span [2000, 3000) ms");
    // equal starts and a start at the span's first ms are fine
    source = new FakeSource();
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 2000, 3000));
    transcriber = new FakeTranscriber();
    transcriber.scripted.put(1L, Arrays.asList(utterance(2000, 2500), utterance(2000, 2600)));
    assertEquals(2, drain(speech()).size(), "segment 1's two utterances pack into the rows after segment 0's");
  }

  @Test
  public void utteranceEndMustFitItsSourceSegment() {
    props = new TypedProperties();
    chunker = new TranscriptPacker();
    source.segments = Collections.singletonList(segment(0, 0, 1000));
    transcriber.scripted.put(0L, Collections.singletonList(utterance(100, 1000)));
    List<Chunk> atBoundary = drain(speech());
    assertEquals(1, atBoundary.size());
    assertEquals("1000", atBoundary.get(0).getAux().get(AudioRows.AUX_END_MS));

    source = new FakeSource();
    source.segments = Collections.singletonList(segment(0, 0, 1000));
    transcriber = new FakeTranscriber();
    transcriber.scripted.put(0L, Collections.singletonList(utterance(100, 1001)));
    List<Chunk> emitted = new ArrayList<>();
    try (VideoSpeechChunks chunks = speech()) {
      PreprocessingException failure = assertThrows(PreprocessingException.class,
          () -> chunks.forEachRemaining(emitted::add));
      assertTrue(failure.getMessage().startsWith(VideoSpeechChunks.SPEECH_PREFIX), failure.getMessage());
    }
    assertTrue(emitted.isEmpty());
    assertEquals(1, source.closes);
    assertEquals(transcriber.opened, transcriber.closed);
  }

  private void assertUtteranceGuard(String name, List<TranscriptSegment> segment1, String expected) {
    source = new FakeSource();
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 2000, 3000));
    transcriber = new FakeTranscriber();
    if (name.startsWith("decreasing")) {
      source.segments = Collections.singletonList(segment(0, 0, 1000));
      transcriber.scripted.put(0L, segment1);
    } else {
      transcriber.scripted.put(1L, segment1);
    }
    VideoSpeechChunks speech = speech();
    List<Chunk> before = new ArrayList<>();
    PreprocessingException failure = assertThrows(PreprocessingException.class, () -> speech.forEachRemaining(before::add), name);
    assertTrue(failure.getMessage().startsWith(VideoSpeechChunks.SPEECH_PREFIX + "IllegalStateException: transcriber fake returned "),
        name + ": " + failure.getMessage());
    assertTrue(failure.getMessage().contains(expected), name + ": " + failure.getMessage());
    assertEquals(1, source.closes, name + ": the segments are closed");
    assertEquals(transcriber.opened, transcriber.closed, name + ": the utterance iterator is closed");
    assertFalse(speech.hasNext(), name);
  }

  /**
   * the real packer's chunk iterator is closed exactly once on each way out — exhaustion, a content failure, an interruption
   * of the round, an Error mid-track, and an early close (twice).
   */
  @Test
  public void theRealPackersIteratorIsClosedExactlyOnceOnEachWayOut() {
    List<AudioSegment> three = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000), segment(2, 2000, 3000));
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16");
    // exhaustion
    CountingChunker counting = useCountingPacker(three);
    assertEquals(1, drain(speech()).size(), "three short utterances pack into one chunk");
    assertEquals(1, counting.closes, "closed once at the end");
    // a content failure mid-track
    counting = useCountingPacker(three);
    transcriber.failAt = 1;
    transcriber.failure = new IllegalStateException("engine refused the audio");
    assertThrows(PreprocessingException.class, () -> drain(speech()));
    assertEquals(1, counting.closes, "closed once on a content failure");
    // an interruption of the round (a retryable transcriber failure)
    counting = useCountingPacker(three);
    transcriber.failAt = 1;
    transcriber.failure = new IOException("engine unavailable");
    assertThrows(SourceReadException.class, () -> drain(speech()));
    assertEquals(1, counting.closes, "closed once on an interruption");
    // an Error mid-track
    counting = useCountingPacker(three);
    UnsatisfiedLinkError error = new UnsatisfiedLinkError("native engine");
    transcriber.failAt = 2;
    transcriber.failInIterator = error;
    VideoSpeechChunks failing = speech();
    assertSame(error, assertThrows(UnsatisfiedLinkError.class, () -> failing.forEachRemaining(chunk -> { })));
    failing.close();
    assertEquals(1, counting.closes, "closed once on an Error, and not again by close()");
    // close() before the end, twice
    List<AudioSegment> many = new ArrayList<>();
    for (int k = 0; k < 20; k++) {
      many.add(segment(k, 1000L * k, 1000L * k + 900));
    }
    counting = useCountingPacker(many);
    VideoSpeechChunks early = speech();
    assertTrue(early.hasNext());
    early.next();
    early.close();
    early.close();
    assertEquals(1, counting.closes, "closed once by an early close");
  }

  /** An invalid chunk setting the packer reads is a configuration error of the task, never a FAILED row of each video. */
  @Test
  public void invalidPackerSettingsFailTheTaskNotTheVideo() {
    useCountingPacker(Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000)));
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16");
    props.setProperty(EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "10");
    PreprocessorConfigurationException config = assertThrows(PreprocessorConfigurationException.class, () -> drain(speech()));
    assertTrue(config.getCause() instanceof IllegalArgumentException, String.valueOf(config.getCause()));
    assertTrue(config.getCause().getMessage().contains(EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key()), config.getCause().getMessage());
    assertEquals(1, source.closes, "the segments are closed");
  }

  private CountingChunker useCountingPacker(List<AudioSegment> segments) {
    source = new FakeSource();
    source.segments = segments;
    transcriber = new FakeTranscriber();
    CountingChunker counting = new CountingChunker(new TranscriptPacker());
    chunker = counting;
    return counting;
  }

  private void assertGuard(String name, List<AudioSegment> segments, String expected) {
    source = new FakeSource();
    source.segments = segments;
    VideoSpeechChunks speech = speech();
    List<Chunk> before = new ArrayList<>();
    PreprocessingException failure = assertThrows(PreprocessingException.class, () -> speech.forEachRemaining(before::add), name);
    assertTrue(failure.getMessage().startsWith(VideoSpeechChunks.SPEECH_PREFIX + "IllegalStateException: segment source fake-v0"
        + " returned "), name + ": " + failure.getMessage());
    assertTrue(failure.getMessage().contains(expected), name + ": " + failure.getMessage());
    // the rows of the segments before the breach may stay (the framework keeps them and adds the FAILED marker)
    assertTrue(before.size() < segments.size(), name + ": rows before " + texts(before));
    assertEquals(1, source.closes, name + ": the segments are closed");
    assertFalse(speech.hasNext(), name);
  }

  /** The fault semantics: what fails the video, what interrupts the round, what fails the task. */
  @Test
  public void failuresAreTheVideosOrTheRoundsOrTheTasks() {
    // the source: a storage failure a later run may not see interrupts; the file's own fault or undecodable audio fails it
    assertSourceFailure(new UncheckedIOException("read", new BlobReadException(new IOException("connection reset"))),
        SourceReadException.class);
    assertSourceFailure(new UncheckedIOException("spill", new IOException("local disk")), SourceReadException.class);
    assertSourceFailure(new UncheckedIOException("read", new BlobReadException(new FileNotFoundException("gone.mp4"))),
        PreprocessingException.class);
    assertSourceFailure(new VideoDecodeException("video v cannot be decoded at 2000 ms"), PreprocessingException.class);
    // the transcriber: a retryable failure interrupts, a transcription error fails the video
    assertTranscriberFailure(new IOException("engine unavailable"), SourceReadException.class);
    assertTranscriberFailure(new UncheckedIOException(new IOException("payload")), SourceReadException.class);
    assertTranscriberFailure(new IllegalStateException("engine refused the audio"), PreprocessingException.class);
    // a configuration error from the call itself fails the task, never the video
    assertTranscriberFailure(new IllegalArgumentException("model file missing"), PreprocessorConfigurationException.class);
    source = new FakeSource();
    source.openFailure = new IllegalArgumentException("bad sample rate");
    PreprocessorConfigurationException config = assertThrows(PreprocessorConfigurationException.class, () -> drain(speech()));
    assertSame(source.openFailure, config.getCause());

    source = new FakeSource();
    source.openFailure = new VideoDecodeException("video v cannot be opened");
    PreprocessingException undecodable = assertThrows(PreprocessingException.class, () -> drain(speech()));
    assertEquals(VideoSpeechChunks.SPEECH_PREFIX + "VideoDecodeException: video v cannot be opened", undecodable.getMessage());
  }

  /**
   * a failure that fails the round — a platform failure, a storage failure a later run may not see, a tokenizer runtime
   * failure after its bounded retry — met anywhere under the speech is never turned into the video's quarantine entry.
   */
  @Test
  public void roundFailuresUnderTheSpeechFailTheRoundNotTheVideo() {
    assertTranscriberFailure(new PrepPlatformException("whisper full() returned -7", null), PrepPlatformException.class);
    assertSourceFailure(new SourceReadException("Failed to read the blob source", new IOException("503 slow down")),
        SourceReadException.class);

    source = new FakeSource();
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000));
    transcriber = new FakeTranscriber();
    Tokenizer failing = new WordCountTokenizer() {
      @Override
      public int countTokens(String text) {
        throw new TokenizerException(TokenizerException.Kind.RETRYABLE, "tokenizer runtime failed", null);
      }
    };
    // the real packer under its default chunk keys (this class's 6-token cap is for the stub chunker)
    VideoSpeechChunks speech = new VideoSpeechChunks(new VideoSpeechChunks.Engines(source, transcriber, new TranscriptPacker()),
        input(), failing, new TypedProperties(), LOGIC, SpeakerLabels.SCOPE_FILE);
    RuntimeException failure = assertThrows(RuntimeException.class, () -> drain(speech));
    assertTrue(failure instanceof TokenizerException, failure.toString());
    assertEquals(TokenizerException.Kind.RETRYABLE, ((TokenizerException) failure).kind());
    assertEquals(1, source.closes, "the segments are closed");
  }

  /** an Error mid-track closes the open utterance iterator and the segments, then propagates as it is. */
  @Test
  public void anErrorMidTrackClosesTheUtterancesAndTheSegments() {
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000), segment(2, 2000, 3000));
    UnsatisfiedLinkError error = new UnsatisfiedLinkError("native engine");
    transcriber.failAt = 1;
    transcriber.failInIterator = error;
    VideoSpeechChunks speech = speech();
    List<Chunk> rows = new ArrayList<>();
    assertSame(error, assertThrows(UnsatisfiedLinkError.class, () -> speech.forEachRemaining(rows::add)));
    assertEquals(1, source.closes, "the segments are closed");
    assertEquals(transcriber.opened, transcriber.closed, "the open utterance iterator is closed");
    assertTrue(rows.size() <= 1, rows.toString());
  }

  @Test
  public void closeBeforeTheEndClosesWhatIsOpen() {
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000), segment(2, 2000, 3000));
    VideoSpeechChunks speech = speech();
    speech.next();
    speech.close();
    speech.close();
    assertEquals(1, source.closes);
    assertEquals(transcriber.opened, transcriber.closed);
    assertFalse(speech.hasNext());
  }

  // ---------------------------------------------------------------- helpers

  private void assertSourceFailure(RuntimeException thrown, Class<? extends RuntimeException> expected) {
    source = new FakeSource();
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000));
    source.failAfter = 1;
    source.failure = thrown;
    RuntimeException failure = assertThrows(expected, () -> drain(speech()), thrown.toString());
    assertEquals(expected, failure.getClass(), thrown.toString());
    assertEquals(1, source.closes, thrown + ": the segments are closed");
  }

  private void assertTranscriberFailure(Exception thrown, Class<? extends RuntimeException> expected) {
    source = new FakeSource();
    source.segments = Arrays.asList(segment(0, 0, 1000), segment(1, 1000, 2000));
    transcriber = new FakeTranscriber();
    transcriber.failAt = 1;
    transcriber.failure = thrown;
    RuntimeException failure = assertThrows(expected, () -> drain(speech()), thrown.toString());
    assertEquals(expected, failure.getClass(), thrown.toString());
    if (failure instanceof PreprocessingException) {
      assertTrue(failure.getMessage().startsWith(VideoSpeechChunks.SPEECH_PREFIX + thrown.getClass().getSimpleName()),
          failure.getMessage());
    }
    assertEquals(1, source.closes, thrown + ": the segments are closed");
  }

  private VideoSpeechChunks speech() {
    return new VideoSpeechChunks(new VideoSpeechChunks.Engines(source, transcriber, chunker), input(), new WordCountTokenizer(), props,
        LOGIC, SpeakerLabels.SCOPE_FILE);
  }

  private static List<Chunk> drain(VideoSpeechChunks speech) {
    List<Chunk> chunks = new ArrayList<>();
    try {
      speech.forEachRemaining(chunks::add);
    } finally {
      speech.close();
    }
    return chunks;
  }

  private static List<String> texts(List<Chunk> chunks) {
    List<String> texts = new ArrayList<>();
    chunks.forEach(chunk -> texts.add(chunk.getText()));
    return texts;
  }

  private static AudioInput input() {
    return new AudioInput() {
      @Override
      public String sourcePathSha() {
        return "v";
      }

      @Override
      public String sourcePath() {
        return null;
      }

      @Override
      public String sourcePartitionPath() {
        return "p";
      }

      @Override
      public long sizeBytes() {
        return 10;
      }

      @Override
      public String extension() {
        return "mp4";
      }

      @Override
      public BlobBytesSupplier bytes() {
        return () -> new ByteArrayInputStream(new byte[10]);
      }
    };
  }

  private static TranscriptSegment utterance(long startMs, long endMs) {
    return new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, "scripted words", startMs, endMs, null);
  }

  /** Wraps a chunker, counting the close() calls on the iterators it returns. */
  private static final class CountingChunker implements TranscriptChunker {
    private final TranscriptChunker delegate;
    private int closes;

    private CountingChunker(TranscriptChunker delegate) {
      this.delegate = delegate;
    }

    @Override
    public String logicVersion() {
      return delegate.logicVersion();
    }

    @Override
    public ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal,
                                                    Tokenizer tokenizer,
                                                    TypedProperties props) {
      ClosableIterator<TranscriptChunk> chunks = delegate.chunks(utterances, firstOrdinal, tokenizer, props);
      return new ClosableIterator<TranscriptChunk>() {
        @Override
        public boolean hasNext() {
          return chunks.hasNext();
        }

        @Override
        public TranscriptChunk next() {
          return chunks.next();
        }

        @Override
        public void close() {
          closes++;
          chunks.close();
        }
      };
    }
  }

  private static AudioSegment segment(long index, long startMs, long endMs) {
    return new AudioSegment() {
      @Override
      public long index() {
        return index;
      }

      @Override
      public long startMs() {
        return startMs;
      }

      @Override
      public long endMs() {
        return endMs;
      }

      @Override
      public int sampleRate() {
        return 16000;
      }

      @Override
      public int channels() {
        return 1;
      }

      @Override
      public String encoding() {
        return "none";
      }

      @Override
      public long payloadBytes() {
        return 0;
      }

      @Override
      public InputStream openPayload() {
        return new ByteArrayInputStream(new byte[0]);
      }

      @Override
      public String toString() {
        return "segment " + index + " [" + startMs + ", " + endMs + ")";
      }
    };
  }

  /** Yields its scripted segments, counting how many it produced and its closes; can fail at the open or after n segments. */
  private static final class FakeSource implements AudioSegmentSource {
    private List<AudioSegment> segments = Collections.emptyList();
    private RuntimeException openFailure;
    private int failAfter = -1;
    private RuntimeException failure;
    private int produced;
    private int closes;

    @Override
    public String logicVersion() {
      return "fake-v0";
    }

    @Override
    public void validate(TypedProperties props) {
    }

    @Override
    public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props) {
      assertEquals(0, startSegment, "prep reads a video whole");
      assertEquals(0, startMs);
      if (openFailure != null) {
        throw openFailure;
      }
      return new ClosableIterator<AudioSegment>() {
        @Override
        public boolean hasNext() {
          if (produced == failAfter) {
            throw failure;
          }
          return produced < segments.size();
        }

        @Override
        public AudioSegment next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          return segments.get(produced++);
        }

        @Override
        public void close() {
          closes++;
        }
      };
    }
  }

  /**
   * One utterance per segment, "words of segment k" over the segment's span; segment {@code silent} has none; at segment
   * {@code failAt} it throws {@code failure} from transcribe() or {@code failInIterator} from its iterator.
   */
  private static final class FakeTranscriber implements SegmentTranscriber {
    private long silent = -1;
    private long failAt = -1;
    /** Utterances of a segment, in place of the one default utterance. */
    private final Map<Long, List<TranscriptSegment>> scripted = new HashMap<>();
    private Exception failure;
    private Error failInIterator;
    private int opened;
    private int closed;
    private int maxOpen;

    @Override
    public String id() {
      return "fake";
    }

    @Override
    public boolean fileScopedSpeakers() {
      return true;
    }

    @Override
    public void validate(TypedProperties props) {
    }

    @Override
    public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) throws IOException {
      if (segment.index() == failAt && failure != null) {
        if (failure instanceof IOException) {
          throw (IOException) failure;
        }
        throw (RuntimeException) failure;
      }
      List<TranscriptSegment> utterances = scripted.containsKey(segment.index()) ? scripted.get(segment.index())
          : segment.index() == silent ? Collections.emptyList()
          : Collections.singletonList(new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, "words of segment " + segment.index(),
              segment.startMs(), segment.endMs(), null));
      boolean failing = segment.index() == failAt && failInIterator != null;
      opened++;
      maxOpen = Math.max(maxOpen, opened - closed);
      return new ClosableIterator<TranscriptSegment>() {
        private int next;
        private boolean done;

        @Override
        public boolean hasNext() {
          if (failing) {
            throw failInIterator;
          }
          return next < utterances.size();
        }

        @Override
        public TranscriptSegment next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          return utterances.get(next++);
        }

        @Override
        public void close() {
          if (!done) {
            done = true;
            closed++;
          }
        }
      };
    }
  }
}
