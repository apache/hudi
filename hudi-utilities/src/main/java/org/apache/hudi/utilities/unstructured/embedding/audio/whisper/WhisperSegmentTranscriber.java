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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.SpeakerLabels;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Speech-to-text for one bounded audio segment with whisper.cpp 1.7.1 through the whisper-jni 1.7.1 binding, running in the
 * executor. The default transcriber ({@code hoodie.unstructured.embedding.audio.transcriber.class} unset, or set to this class);
 * configured by the {@link WhisperConfig} keys (model, language, task, threads).
 *
 * <ul>
 *   <li>Input: one {@code pcm_s16le} 16 kHz mono segment of at most {@code ...prep.audio.segment.max.bytes} (default 1 MiB, 32 s);
 *       a shorter segment than whisper accepts (about 1.1 s) is padded with trailing silence to {@link #MIN_SAMPLES}.</li>
 *   <li>Output: whisper's utterances on the video's timeline (segment start added, clamped into the segment's span, starts
 *       non-decreasing); an utterance starting at or after the segment's end (in the padding) is dropped, as are empty
 *       text and whole-utterance annotations such as {@code [BLANK_AUDIO]} or {@code (music)}. Whisper has no speaker
 *       diarization, so every utterance carries the one file-scoped label {@link SpeakerLabels#FILE_SPEAKER}
 *       ({@link #fileScopedSpeakers()} is true); a diarizing transcriber would return its own labels through the same seam.
 *       Confidence is null (the binding exposes none).</li>
 *   <li>Memory: the model context lives in native memory, borrowed per call from the JVM's {@link WhisperContextPool}, so the
 *       contexts alive never exceed the transcriptions running at once (one per task slot); {@link #close()} lets the pool free
 *       them. Heap per call: the payload's samples as floats (4 bytes per sample, 2 MiB at the default bound).</li>
 *   <li>Failures: an {@link IOException} only when the payload cannot be read (an interruption of the prep run); a corrupt
 *       segment (it breaks what our own segment source promises) or a failed whisper run (whisper cannot tell its own allocation
 *       or compute failure from bad audio) is a {@link org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException}
 * (class P: the round fails, never the video); a missing or invalid
 *       model, an unavailable native
 *       library or a bad key is an {@link IllegalArgumentException} (a configuration error that fails the task).</li>
 * </ul>
 */
public class WhisperSegmentTranscriber implements SegmentTranscriber, AutoCloseable {

  /** Bump whenever the same whisper utterances map to different transcript records. */
  public static final String IMPLEMENTATION_VERSION = "whisper-transcriber-v1";
  /** The whisper-jni transcription logic of the engine under this transcriber; see {@code NativeWhisperEngine.LOGIC_VERSION}. */
  public static final String ENGINE_LOGIC_VERSION = NativeWhisperEngine.LOGIC_VERSION;

  /** Shortest input handed to whisper, 1.2 s at 16 kHz: whisper 1.7.1 returns no utterance for less than about 1.1 s. */
  static final int MIN_SAMPLES = 19_200;

  private static final long MS_PER_CENTI = 10L;
  /** A whole utterance that is only a non-speech annotation, e.g. {@code [BLANK_AUDIO]}, {@code [Music]}, {@code (upbeat music)}. */
  private static final Pattern TAG_ONLY = Pattern.compile("\\[[^\\]]*\\]|\\([^)]*\\)");

  private final WhisperContextPool pool;
  private WhisperConfig config;
  private boolean registered;

  public WhisperSegmentTranscriber() {
    this(WhisperContextPool.shared());
  }

  WhisperSegmentTranscriber(WhisperContextPool pool) {
    this.pool = pool;
  }

  /**
   * Engine, model, language and task ({@link WhisperConfig#id()}), from the properties this instance was given by
   * {@link #validate} or {@link #transcribe}. The model is chosen by non-style keys, so the id cannot be known before.
   *
   * @throws IllegalStateException when called before {@link #validate} or {@link #transcribe}
   */
  @Override
  public synchronized String id() {
    if (config == null) {
      throw new IllegalStateException(getClass().getSimpleName() + ".id() depends on the " + WhisperConfig.PREFIX
          + "* keys: call validate(props) first");
    }
    return config.id();
  }

  /**
   * The engine and model identity for a job's properties, for the audio logic line on the driver.
   *
   * @throws IllegalArgumentException naming an invalid key
   */
  public static String id(TypedProperties props) {
    return WhisperConfig.from(props).id();
  }

  @Override
  public boolean fileScopedSpeakers() {
    return true;
  }

  /** Checks the whisper keys and fixes this instance's configuration (and so its {@link #id()}). */
  @Override
  public void validate(TypedProperties props) {
    configure(props);
  }

  @Override
  public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) throws IOException {
    WhisperConfig resolved = resolve(props);
    PcmPayload.Samples samples = PcmPayload.read(segment, resolved.segmentMaxBytes(), MIN_SAMPLES);
    if (samples.count() == 0) {
      return iterator(new ArrayList<>());
    }
    WhisperEngine engine = pool.acquire(resolved);
    List<WhisperEngine.RawUtterance> raw;
    try {
      raw = engine.transcribe(resolved, samples.pcm(), samples.pcm().length);
    } finally {
      pool.release(resolved, engine);
    }
    return iterator(toUtterances(raw, segment.startMs(), segment.endMs()));
  }

  /** Lets the pool free this model's idle contexts once no other transcriber of it is open; idempotent. */
  @Override
  public synchronized void close() {
    if (registered) {
      registered = false;
      pool.unregister(config);
    }
  }

  private synchronized WhisperConfig configure(TypedProperties props) {
    WhisperConfig resolved = WhisperConfig.from(props);
    if (config != null && !config.equals(resolved)) {
      throw new IllegalArgumentException("one " + getClass().getSimpleName() + " serves one configuration: " + config
          + ", asked for " + resolved);
    }
    config = resolved;
    return resolved;
  }

  private synchronized WhisperConfig resolve(TypedProperties props) {
    WhisperConfig resolved = configure(props);
    if (!registered) {
      pool.register(resolved);
      registered = true;
    }
    return resolved;
  }

  static List<TranscriptSegment> toUtterances(List<WhisperEngine.RawUtterance> raw, long segmentStartMs, long segmentEndMs) {
    List<TranscriptSegment> utterances = new ArrayList<>(raw.size());
    long previousStart = segmentStartMs;
    for (WhisperEngine.RawUtterance utterance : raw) {
      String text = utterance.text() == null ? "" : utterance.text().trim();
      long rawStart = segmentStartMs + utterance.startCentis() * MS_PER_CENTI;
      // no text, a tag alone, or timed in the trailing silence padding (or past the span): not speech of this segment
      if (text.isEmpty() || TAG_ONLY.matcher(text).matches() || rawStart >= segmentEndMs) {
        continue;
      }
      long start = Math.max(Math.max(rawStart, segmentStartMs), previousStart);
      long end = clamp(segmentStartMs + utterance.endCentis() * MS_PER_CENTI, start, segmentEndMs);
      utterances.add(new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, text, start, end, null));
      previousStart = start;
    }
    return utterances;
  }

  private static long clamp(long value, long low, long high) {
    return Math.max(low, Math.min(high, value));
  }

  private static ClosableIterator<TranscriptSegment> iterator(List<TranscriptSegment> utterances) {
    Iterator<TranscriptSegment> it = utterances.iterator();
    return new ClosableIterator<TranscriptSegment>() {
      @Override
      public void close() {
        // the utterances are a list in memory: nothing to release
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public TranscriptSegment next() {
        return it.next();
      }
    };
  }
}
