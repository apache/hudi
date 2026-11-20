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

package org.apache.hudi.utilities.unstructured.embedding.audio;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.video.ClipDuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * TEST DOUBLE (no default names it; the defaults are the real implementations, {@link AudioConfig}).
 *
 * <p>DUMMY {@link AudioSegmentSource}, logic {@value #LOGIC_VERSION}: cuts a video into fixed segments of
 * {@link AudioConfigKeys#SEGMENT_MAX_MS} each, segment {@code k} = {@code [k·N, min((k+1)·N, D))}, where D is the duration
 * the video's container declares ({@link ClipDuration}, a header probe: no frame and no audio sample is decoded). It carries no
 * audio: every segment's payload is empty (encoding {@value #ENCODING}, 0 bytes). Whether the video has an audio stream at all
 * is not looked at; a real source answers that.
 *
 * <p>Deterministic in (duration, N), so {@code startSegment = k} resumes exactly; {@code startMs} is not needed. Failures: a
 * storage failure under the probe is a {@link org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException}
 * (an interruption of the prep run); a container that cannot be probed, or a duration above
 * {@link AudioConfigKeys#MAX_DURATION_MS}, is a content failure of the video (its quarantine entry).
 */
public class StubAudioSegmentSource implements AudioSegmentSource {

  /** The stub's segmentation logic; part of the audio logic line, so of every audio table's preprocessing style. */
  public static final String LOGIC_VERSION = "audio-stub-v0";
  /** Channels of the stub's (empty) payload: mono, as the real source resamples to. */
  public static final int CHANNELS = 1;
  /** Encoding of the stub's empty payload. */
  public static final String ENCODING = "none";

  @Override
  public String logicVersion() {
    return LOGIC_VERSION;
  }

  @Override
  public void validate(TypedProperties props) {
    AudioConfig config = AudioConfig.from(props);
    config.segmentMaxMs();
    config.maxDurationMs();
    config.sampleRate();
  }

  @Override
  public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props)
      throws IOException {
    if (startSegment < 0) {
      throw new IllegalArgumentException("startSegment must be >= 0, got " + startSegment);
    }
    AudioConfig config = AudioConfig.from(props);
    long segmentMs = config.segmentMaxMs();
    long maxDurationMs = config.maxDurationMs();
    int sampleRate = config.sampleRate();
    long durationMs = ClipDuration.millis(audio.bytes(), audio.sourcePathSha());
    if (durationMs > maxDurationMs) {
      throw new IllegalStateException("video " + audio.sourcePathSha() + " lasts " + durationMs + " ms, above "
          + AudioConfigKeys.MAX_DURATION_MS.key() + " = " + maxDurationMs + "; refused before any segment");
    }
    // ceil(duration / N) without overflow, whatever N is
    long count = durationMs / segmentMs + (durationMs % segmentMs == 0 ? 0 : 1);
    return new ClosableIterator<AudioSegment>() {
      private long next = startSegment;

      @Override
      public boolean hasNext() {
        return next < count;
      }

      @Override
      public AudioSegment next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        long index = next++;
        long start = index * segmentMs;
        return new EmptySegment(index, start, start + Math.min(segmentMs, durationMs - start), sampleRate);
      }

      @Override
      public void close() {
        // the stub holds nothing to release
      }
    };
  }

  /** A segment without audio: a time span only. */
  private static final class EmptySegment implements AudioSegment {
    private final long index;
    private final long startMs;
    private final long endMs;
    private final int sampleRate;

    private EmptySegment(long index, long startMs, long endMs, int sampleRate) {
      this.index = index;
      this.startMs = startMs;
      this.endMs = endMs;
      this.sampleRate = sampleRate;
    }

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
      return sampleRate;
    }

    @Override
    public int channels() {
      return CHANNELS;
    }

    @Override
    public String encoding() {
      return ENCODING;
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
      return "segment " + index + " [" + startMs + ", " + endMs + ") ms";
    }
  }
}
