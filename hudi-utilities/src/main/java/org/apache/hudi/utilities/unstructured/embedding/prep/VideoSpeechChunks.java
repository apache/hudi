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
import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunk;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The speech of ONE video as prep chunks: the configured {@link AudioSegmentSource} yields the video's
 * speech segments one at a time, the {@link SegmentTranscriber} transcribes a segment only when the {@link TranscriptChunker} asks
 * for more text, and every {@link TranscriptChunk} becomes one {@code text} chunk, in ordinal order (the framework numbers it like
 * any text chunk: chunk_id dense from 0 per (source_path_sha, text)), carrying the transcript aux ({@link AudioRows#AUX_START_MS},
 * {@link AudioRows#AUX_END_MS}, {@link AudioRows#AUX_SPEAKERS}, {@link AudioRows#AUX_SPEAKER_SCOPE},
 * {@link AudioRows#AUX_FIRST_SEGMENT}, {@link AudioRows#AUX_LAST_SEGMENT}, {@link AudioRows#AUX_AUDIO_LOGIC}). The rows are
 * committed with the video's frames, in the prep run that reads the video, under that run's checkpoint (no checkpoint of their own).
 *
 * <p>Bounded: nothing sorts or buffers the video's segments; one segment, one utterance iterator and the chunker's
 * bounded working set (for the packer: pending text below two windows before a pull, the overlap and one utterance read ahead)
 * are alive. The segment source is asked for the next segment only once the
 * current segment's utterances are used up.
 *
 * <p>Segment guard: a segment whose index is not the next dense index (out of order, repeated or gapped), that starts
 * before the previous segment's end (out of order or overlapping in time), or whose span is not {@code 0 <= startMs < endMs}
 * fails the video. A silence gap between segments is normal: the voice activity step cuts it.
 *
 * <p>Utterance guard: an utterance whose start is outside its segment's span {@code [startMs, endMs)}, or before the
 * previous utterance's start (in this segment or an earlier one), fails the video instead of corrupting the chunks' times;
 * it holds for any configured transcriber, whatever it promises.
 *
 * <p>Failures:
 * <ul>
 *   <li>an interruption ({@link SourceReadException}, the prep run fails and commits nothing): a storage failure under the source's
 *       read that is not the file's own fault (a {@link BlobReadException} in the cause chain, or the source's own
 *       {@link UncheckedIOException}), and a transcriber's retryable failure ({@link IOException} or {@link UncheckedIOException});</li>
 *   <li>a configuration error ({@link PreprocessorConfigurationException}, the task fails): an {@link IllegalArgumentException}
 *       thrown by the {@code segments()}, {@code transcribe()} or {@code chunks()} call itself;</li>
 *   <li>any failure whose cause chain holds one that fails the round ({@link PrepFailures#roundFailure}: a platform failure such
 *       as a failed whisper run or a corrupt segment of our own source, a {@link SourceReadException}, a configuration error, a
 *       RETRYABLE tokenizer failure after the tokenizer's retries) is thrown as it was met, never as the video's;</li>
 *   <li>a content failure of the video (a {@link PreprocessingException} whose message starts {@value #SPEECH_PREFIX}; the
 *       framework writes the file's quarantine entry after the rows already yielded): anything else — an undecodable track, a
 *       transcription error, a segment-guard or utterance-guard breach, a chunker or tokenizer failure, a chunk out of order;</li>
 *   <li>an {@link Error} propagates untouched.</li>
 * </ul>
 * Every way out (exhaustion, failure, {@link Error}, {@link #close()}) closes the open utterance iterator and the segments.
 */
public final class VideoSpeechChunks implements ClosableIterator<Chunk> {

  /** Prefix of the {@code prep_error} text of a speech content failure. */
  public static final String SPEECH_PREFIX = "speech: ";

  private static final Logger LOG = LoggerFactory.getLogger(VideoSpeechChunks.class);
  private static final String STARTING_AT = " starting at ";
  private static final String SEGMENT = "segment ";

  private final AudioSegmentSource source;
  private final SegmentTranscriber transcriber;
  private final TranscriptChunker chunker;
  private final AudioInput input;
  private final Tokenizer tokenizer;
  private final TypedProperties props;
  private final String logicLine;
  private final String speakerScope;
  private Utterances utterances;
  private ClosableIterator<TranscriptChunk> chunks;
  private long nextOrdinal;
  private boolean done;

  public VideoSpeechChunks(Engines engines, AudioInput input, Tokenizer tokenizer, TypedProperties props, String logicLine,
                           String speakerScope) {
    this.source = engines.source;
    this.transcriber = engines.transcriber;
    this.chunker = engines.chunker;
    this.input = input;
    this.tokenizer = tokenizer;
    this.props = props;
    this.logicLine = logicLine;
    this.speakerScope = speakerScope;
  }

  /** The segment source, the transcriber and the transcript chunker of a video's speech. */
  public static final class Engines {
    private final AudioSegmentSource source;
    private final SegmentTranscriber transcriber;
    private final TranscriptChunker chunker;

    public Engines(AudioSegmentSource source, SegmentTranscriber transcriber, TranscriptChunker chunker) {
      this.source = source;
      this.transcriber = transcriber;
      this.chunker = chunker;
    }
  }

  @Override
  public boolean hasNext() {
    if (done) {
      return false;
    }
    // an Error (e.g. an exhausted heap) closes what is open before it propagates: the body neither returned nor failed
    boolean settled = false;
    try {
      if (chunks == null) {
        open();
      }
      if (chunks.hasNext()) {
        settled = true;
        return true;
      }
      // the chunker must pack every utterance: what it left would never be written
      if (utterances.hasNext()) {
        throw new IllegalStateException("chunker " + chunker.logicVersion() + " left utterances unpacked");
      }
      finish();
      settled = true;
      return false;
    } catch (RuntimeException e) {
      settled = true;
      throw failed(e);
    } finally {
      if (!settled) {
        closeQuietly();
      }
    }
  }

  @Override
  public Chunk next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    // as in hasNext: an Error closes what is open before it propagates
    boolean settled = false;
    try {
      Chunk chunk = chunk(chunks.next());
      settled = true;
      return chunk;
    } catch (RuntimeException e) {
      settled = true;
      throw failed(e);
    } finally {
      if (!settled) {
        closeQuietly();
      }
    }
  }

  @Override
  public void close() {
    done = true;
    closeQuietly();
  }

  private void open() {
    ClosableIterator<AudioSegment> segments;
    try {
      segments = source.segments(input, 0, 0, props);
    } catch (IllegalArgumentException e) {
      throw new ConfigurationFailure(e);
    } catch (IOException | RuntimeException e) {
      throw new SourceFailure(e);
    }
    utterances = new Utterances(segments);
    try {
      chunks = chunker.chunks(utterances, 0, tokenizer, props);
    } catch (IllegalArgumentException e) {
      // the chunker's settings (the table's chunk keys) are refused: the same for every video, so the task fails
      throw new ConfigurationFailure(e);
    }
  }

  /** Closes what the video opened once it is fully packed, once; a close failure is classified by its origin. */
  private void finish() {
    done = true;
    ClosableIterator<TranscriptChunk> toClose = chunks;
    chunks = null;
    try {
      toClose.close();
    } finally {
      utterances.close();
    }
  }

  private Chunk chunk(TranscriptChunk chunk) {
    if (chunk.ordinal() != nextOrdinal) {
      throw new IllegalStateException("chunker " + chunker.logicVersion() + " returned ordinal " + chunk.ordinal() + " where "
          + nextOrdinal + " was due (ordinals are dense from 0)");
    }
    nextOrdinal++;
    Map<String, String> aux = new HashMap<>();
    aux.put(AudioRows.AUX_START_MS, String.valueOf(chunk.startMs()));
    aux.put(AudioRows.AUX_END_MS, String.valueOf(chunk.endMs()));
    aux.put(AudioRows.AUX_SPEAKERS, String.join(",", chunk.speakers()));
    aux.put(AudioRows.AUX_SPEAKER_SCOPE, speakerScope);
    aux.put(AudioRows.AUX_FIRST_SEGMENT, String.valueOf(chunk.firstSegment()));
    aux.put(AudioRows.AUX_LAST_SEGMENT, String.valueOf(chunk.lastSegment()));
    aux.put(AudioRows.AUX_AUDIO_LOGIC, logicLine);
    return Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text(chunk.text()).tokenCount(chunk.tokenCount()).aux(aux).build();
  }

  /** The failure the framework sees for {@code e}: an interruption, a configuration error or the video's content failure. */
  private RuntimeException failed(RuntimeException e) {
    done = true;
    closeQuietly();
    ConfigurationFailure configuration = find(e, ConfigurationFailure.class);
    if (configuration != null) {
      return new PreprocessorConfigurationException("Configuration error preparing the speech of video " + input.sourcePathSha(),
          configuration.getCause());
    }
    RuntimeException round = PrepFailures.roundFailure(e);
    if (round != null) {
      return round;
    }
    if (isInterruption(e)) {
      return new SourceReadException("Interrupted preparing the speech of video " + input.sourcePathSha(), unwrap(e));
    }
    Throwable cause = unwrap(e);
    return new PreprocessingException(SPEECH_PREFIX + describe(cause), cause);
  }

  /**
   * Whether a failure interrupts the prep run rather than fail the video: a storage failure under the source's read that is not the
   * file's own fault, or a transcriber's retryable failure. A source failure anywhere in the chain decides first (a chunker may
   * wrap what its utterance iterator threw; one of them is thrown per failure).
   */
  static boolean isInterruption(Throwable failure) {
    SourceFailure source = find(failure, SourceFailure.class);
    if (source != null) {
      Throwable cause = source.getCause();
      return (BlobReadException.isStorageFailure(cause) || cause instanceof UncheckedIOException)
          && !SourceFailures.isContentFailure(cause);
    }
    TranscriberFailure transcriber = find(failure, TranscriberFailure.class);
    if (transcriber != null) {
      Throwable cause = transcriber.getCause();
      return cause instanceof IOException || cause instanceof UncheckedIOException;
    }
    return false;
  }

  /** What the implementation threw, without this class's origin wrappers. */
  private static Throwable unwrap(Throwable failure) {
    SourceFailure source = find(failure, SourceFailure.class);
    if (source != null) {
      return source.getCause();
    }
    TranscriberFailure transcriber = find(failure, TranscriberFailure.class);
    return transcriber != null ? transcriber.getCause() : failure;
  }

  private static <T extends Throwable> T find(Throwable failure, Class<T> type) {
    for (Throwable t : SourceCauses.getCauses(failure)) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
    }
    return null;
  }

  /** Class and message, plus the root cause's: the quarantine entry alone tells an operator what went wrong. */
  static String describe(Throwable failure) {
    Throwable root = SourceCauses.getRootCause(failure);
    String described = failure.getClass().getSimpleName() + ": " + failure.getMessage();
    return root == failure ? described
        : described + " (root cause " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
  }

  /** Closes the chunker's iterator (once: the field is cleared) and the utterances (idempotent), logging a failure. */
  private void closeQuietly() {
    ClosableIterator<TranscriptChunk> toClose = chunks;
    chunks = null;
    try {
      if (toClose != null) {
        toClose.close();
      }
    } catch (RuntimeException e) {
      LOG.warn("EMBED-SPEECH-CLOSE-FAILED chunker of video {}", input.sourcePathSha(), e);
    }
    if (utterances != null) {
      try {
        utterances.close();
      } catch (RuntimeException e) {
        LOG.warn("EMBED-SPEECH-CLOSE-FAILED segments of video {}", input.sourcePathSha(), e);
      }
    }
  }

  /**
   * The utterances of the video, pulled one segment at a time: a segment is transcribed only when the chunker asks for more text,
   * and only one segment's utterance iterator is open.
   */
  private final class Utterances implements Iterator<TranscriptChunker.SegmentUtterance>, AutoCloseable {
    private final ClosableIterator<AudioSegment> segments;
    private ClosableIterator<TranscriptSegment> current;
    private long currentIndex;
    private long currentStartMs;
    private long currentEndMs;
    private long taken;
    private long previousEndMs;
    private long previousUtteranceStartMs = -1;
    private boolean closed;

    private Utterances(ClosableIterator<AudioSegment> segments) {
      this.segments = segments;
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (current != null) {
          if (transcriberHasNext()) {
            return true;
          }
          closeCurrent();
        }
        if (closed || !sourceHasNext()) {
          return false;
        }
        AudioSegment segment = checked(sourceNext());
        taken++;
        previousEndMs = segment.endMs();
        currentIndex = segment.index();
        currentStartMs = segment.startMs();
        currentEndMs = segment.endMs();
        try {
          current = transcriber.transcribe(segment, props);
        } catch (IllegalArgumentException e) {
          throw new ConfigurationFailure(e);
        } catch (IOException | RuntimeException e) {
          throw new TranscriberFailure(e);
        }
      }
    }

    /** The segment guard: dense index, ordered and non-overlapping in time, a positive span from 0 on. */
    private AudioSegment checked(AudioSegment segment) {
      String what = null;
      if (segment.index() != taken) {
        what = SEGMENT + segment.index() + " where " + taken + " was due (indexes are dense from 0, in order)";
      } else if (segment.startMs() < 0 || segment.endMs() <= segment.startMs()) {
        what = SEGMENT + segment.index() + " spanning [" + segment.startMs() + ", " + segment.endMs() + ") ms (a span is "
            + "0 <= start < end)";
      } else if (taken > 0 && segment.startMs() < previousEndMs) {
        what = SEGMENT + segment.index() + STARTING_AT + segment.startMs() + " ms, before the end of segment "
            + (taken - 1) + " at " + previousEndMs + " ms (segments are in time order and do not overlap)";
      }
      if (what != null) {
        throw new SourceFailure(new IllegalStateException("segment source " + source.logicVersion() + " returned " + what));
      }
      return segment;
    }

    /** The utterance stays within its segment and starts no earlier than the previous utterance. */
    private void checked(TranscriptSegment utterance) {
      long start = utterance.getStartMs();
      String what = null;
      if (start < currentStartMs || start >= currentEndMs) {
        what = "an utterance of segment " + currentIndex + STARTING_AT + start + " ms, outside the segment's span ["
            + currentStartMs + ", " + currentEndMs + ") ms";
      } else if (utterance.getEndMs() > currentEndMs) {
        what = "an utterance of segment " + currentIndex + " ending at " + utterance.getEndMs()
            + " ms, after the segment's end at " + currentEndMs + " ms";
      } else if (start < previousUtteranceStartMs) {
        what = "an utterance of segment " + currentIndex + STARTING_AT + start + " ms, before the previous utterance's start"
            + " at " + previousUtteranceStartMs + " ms (starts are non-decreasing)";
      }
      if (what != null) {
        throw new TranscriberFailure(new IllegalStateException("transcriber " + transcriber.id() + " returned " + what));
      }
      previousUtteranceStartMs = start;
    }

    @Override
    public TranscriptChunker.SegmentUtterance next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      TranscriptSegment utterance;
      try {
        utterance = current.next();
      } catch (RuntimeException e) {
        throw new TranscriberFailure(e);
      }
      checked(utterance);
      long index = currentIndex;
      return new TranscriptChunker.SegmentUtterance() {
        @Override
        public long segmentIndex() {
          return index;
        }

        @Override
        public TranscriptSegment utterance() {
          return utterance;
        }
      };
    }

    private boolean transcriberHasNext() {
      try {
        return current.hasNext();
      } catch (RuntimeException e) {
        throw new TranscriberFailure(e);
      }
    }

    private boolean sourceHasNext() {
      try {
        return segments.hasNext();
      } catch (RuntimeException e) {
        throw new SourceFailure(e);
      }
    }

    private AudioSegment sourceNext() {
      try {
        return segments.next();
      } catch (RuntimeException e) {
        throw new SourceFailure(e);
      }
    }

    private void closeCurrent() {
      ClosableIterator<TranscriptSegment> toClose = current;
      current = null;
      try {
        toClose.close();
      } catch (RuntimeException e) {
        throw new TranscriberFailure(e);
      }
    }

    /**
     * Closes the open utterance iterator and the segments, once; a failure is classified by its origin. The first failure is
     * thrown, a later one suppressed on it.
     */
    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      RuntimeException failure = null;
      try {
        if (current != null) {
          closeCurrent();
        }
      } catch (RuntimeException e) {
        failure = e;
      } catch (Error e) {
        // the segments are still closed; their failure is kept on the Error, which propagates as it is
        RuntimeException segmentsFailure = closeSegments(null);
        if (segmentsFailure != null) {
          e.addSuppressed(segmentsFailure);
        }
        throw e;
      }
      failure = closeSegments(failure);
      if (failure != null) {
        throw failure;
      }
    }

    /** Closes the segments; returns {@code first}, with a segments failure suppressed on it, or that failure when there is none. */
    private RuntimeException closeSegments(RuntimeException first) {
      try {
        segments.close();
      } catch (RuntimeException e) {
        SourceFailure failure = new SourceFailure(e);
        if (first == null) {
          return failure;
        }
        first.addSuppressed(failure);
      }
      return first;
    }
  }

  /** A failure of the segment source, classified by {@link #isInterruption}. */
  private static final class SourceFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private SourceFailure(Throwable cause) {
      super(cause);
    }
  }

  /** A failure of the transcriber, classified by {@link #isInterruption}. */
  private static final class TranscriberFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private TranscriberFailure(Throwable cause) {
      super(cause);
    }
  }

  /** A configuration error from a {@code segments()}, {@code transcribe()} or {@code chunks()} call: fails the task. */
  private static final class ConfigurationFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private ConfigurationFailure(IllegalArgumentException cause) {
      super(cause);
    }
  }
}
