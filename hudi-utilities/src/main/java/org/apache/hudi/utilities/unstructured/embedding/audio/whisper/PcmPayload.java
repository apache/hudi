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

import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads one segment's 16 kHz mono {@code pcm_s16le} payload into the float samples whisper takes, never holding more than the
 * declared payload: the declared size is checked against the configured bound before any byte is read, and a stream that runs
 * past or stops short of its declared size is refused after reading at most one byte past it. A refused segment breaks what our
 * own segment source promises under the same keys (16 kHz mono {@code pcm_s16le}, a span on the timeline, at most
 * {@code segment.max.bytes}): an invariant between our own components, so it is a {@link PrepPlatformException} (class P),
 * never the video's quarantine entry.
 */
final class PcmPayload {

  /** The payload encoding the transcriber accepts ({@link AudioSegment#encoding()}). */
  static final String PCM_S16LE = "pcm_s16le";

  private static final int BUFFER_BYTES = 8192;
  private static final String BYTES = " bytes";
  private static final String PAYLOAD_OF = "payload of ";

  /** 16 kHz mono 16-bit: 32 bytes per millisecond. */
  private static final long BYTES_PER_MS = 32L;

  /**
   * How far a payload may run past its declared span, ms: room for a source's millisecond rounding. The transcriber drops
   * utterances timed at or after the span's end (they are in the silence padding), so a payload must not hold audio past it.
   */
  private static final long SPAN_TOLERANCE_MS = 20L;

  private PcmPayload() {
  }

  /**
   * @param segment  the segment whose payload is read
   * @param maxBytes the configured payload bound ({@link WhisperConfig#segmentMaxBytes()})
   * @param minSamples the returned array is at least this long, zero-padded (trailing silence) past the payload's samples
   * @return the samples in {@code [-1, 1)}; {@link Samples#count()} of them come from the payload
   * @throws IOException           when the payload cannot be opened or read (an interruption of the prep run)
   * @throws PrepPlatformException when the segment is not 16 kHz mono pcm_s16le, its span is empty or negative or shorter than the
   *                               payload (beyond a 20 ms tolerance), its declared size is negative, odd or above
   *                               {@code maxBytes}, or its stream length differs from the declared size (a corrupt segment)
   */
  static Samples read(AudioSegment segment, long maxBytes, int minSamples) throws IOException {
    check(segment.sampleRate() == WhisperConfig.WHISPER_SAMPLE_RATE, segment, "sample rate " + segment.sampleRate()
        + " Hz, whisper takes " + WhisperConfig.WHISPER_SAMPLE_RATE);
    check(segment.channels() == 1, segment, segment.channels() + " channels, whisper takes mono");
    check(segment.startMs() >= 0 && segment.startMs() < segment.endMs(), segment, "span [" + segment.startMs() + ", "
        + segment.endMs() + ") ms is not a non-empty span on the timeline");
    check(PCM_S16LE.equals(segment.encoding()), segment, "encoding " + segment.encoding() + ", expected " + PCM_S16LE);
    long declared = segment.payloadBytes();
    check(declared >= 0 && declared % 2 == 0, segment, PAYLOAD_OF + declared + " bytes is not whole 16-bit samples");
    check(declared <= maxBytes, segment, PAYLOAD_OF + declared + " bytes is above the bound of " + maxBytes + BYTES);
    long payloadMs = declared / BYTES_PER_MS;
    long spanMs = segment.endMs() - segment.startMs();
    check(payloadMs <= spanMs + SPAN_TOLERANCE_MS, segment, PAYLOAD_OF + payloadMs + " ms is longer than its span of " + spanMs
        + " ms; utterances past the span would be dropped");
    int count = (int) (declared / 2);
    if (count == 0) {
      try (InputStream in = segment.openPayload()) {
        check(in.read() < 0, segment, "payload stream is longer than its declared " + declared + BYTES);
      }
      return new Samples(new float[0], 0);
    }
    float[] pcm = new float[Math.max(count, minSamples)];
    byte[] buffer = new byte[BUFFER_BYTES];
    long read = 0;
    int sample = 0;
    int pendingLow = -1;
    try (InputStream in = segment.openPayload()) {
      for (int want = (int) Math.min(buffer.length, declared + 1 - read); want > 0;
           want = (int) Math.min(buffer.length, declared + 1 - read)) {
        int n = in.read(buffer, 0, want);
        if (n < 0) {
          break;
        }
        read += n;
        check(read <= declared, segment, "payload stream is longer than its declared " + declared + BYTES);
        for (int i = 0; i < n; i++) {
          int b = buffer[i] & 0xFF;
          if (pendingLow < 0) {
            pendingLow = b;
          } else {
            pcm[sample++] = ((short) ((b << 8) | pendingLow)) / 32768f;
            pendingLow = -1;
          }
        }
      }
    }
    check(read == declared, segment, "payload stream ended after " + read + " of its declared " + declared + BYTES);
    return new Samples(pcm, count);
  }

  private static void check(boolean condition, AudioSegment segment, String message) {
    if (!condition) {
      throw new PrepPlatformException("corrupt audio segment " + segment.index() + ": " + message, null);
    }
  }

  /** Samples of one payload, possibly padded. */
  static final class Samples {
    private final float[] pcm;
    private final int count;

    Samples(float[] pcm, int count) {
      this.pcm = pcm;
      this.count = count;
    }

    /** The samples, padded with zeros up to the requested minimum. */
    float[] pcm() {
      return pcm;
    }

    /** How many samples came from the payload. */
    int count() {
      return count;
    }
  }
}
