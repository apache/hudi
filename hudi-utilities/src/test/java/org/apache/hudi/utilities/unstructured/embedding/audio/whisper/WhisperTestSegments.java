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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** In-memory {@link AudioSegment}s for the whisper tests. */
final class WhisperTestSegments {

  private WhisperTestSegments() {
  }

  /** A 16 kHz mono pcm_s16le segment over {@code samples}. */
  static TestSegment pcm(long index, long startMs, short[] samples) {
    byte[] bytes = new byte[samples.length * 2];
    for (int i = 0; i < samples.length; i++) {
      bytes[2 * i] = (byte) samples[i];
      bytes[2 * i + 1] = (byte) (samples[i] >> 8);
    }
    long endMs = startMs + Math.max(1, samples.length * 1000L / 16000);
    return new TestSegment(index, startMs, endMs, 16000, 1, PcmPayload.PCM_S16LE, bytes.length,
        () -> new ByteArrayInputStream(bytes));
  }

  /** {@code seconds} of a quiet 440 Hz tone. */
  static short[] tone(double seconds) {
    short[] samples = new short[(int) (seconds * 16000)];
    for (int i = 0; i < samples.length; i++) {
      samples[i] = (short) (3000 * Math.sin(2 * Math.PI * 440 * i / 16000.0));
    }
    return samples;
  }

  /** A segment whose every field is given; the stream is counted. */
  static final class TestSegment implements AudioSegment {
    private final long index;
    private final long startMs;
    private final long endMs;
    private final int sampleRate;
    private final int channels;
    private final String encoding;
    private final long payloadBytes;
    private final Supplier<InputStream> payload;
    final AtomicLong bytesRead = new AtomicLong();
    private IOException readFailure;

    TestSegment(long index, long startMs, long endMs, int sampleRate, int channels, String encoding, long payloadBytes,
                Supplier<InputStream> payload) {
      this.index = index;
      this.startMs = startMs;
      this.endMs = endMs;
      this.sampleRate = sampleRate;
      this.channels = channels;
      this.encoding = encoding;
      this.payloadBytes = payloadBytes;
      this.payload = payload;
    }

    TestSegment with(int sampleRate, int channels, String encoding, long payloadBytes) {
      return new TestSegment(index, startMs, endMs, sampleRate, channels, encoding, payloadBytes, payload);
    }

    TestSegment withSpan(long startMs, long endMs) {
      return new TestSegment(index, startMs, endMs, sampleRate, channels, encoding, payloadBytes, payload);
    }

    TestSegment failingReads(IOException failure) {
      TestSegment copy = new TestSegment(index, startMs, endMs, sampleRate, channels, encoding, payloadBytes, payload);
      copy.readFailure = failure;
      return copy;
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
      return channels;
    }

    @Override
    public String encoding() {
      return encoding;
    }

    @Override
    public long payloadBytes() {
      return payloadBytes;
    }

    @Override
    public InputStream openPayload() throws IOException {
      if (readFailure != null) {
        throw readFailure;
      }
      InputStream in = payload.get();
      return new InputStream() {
        @Override
        public int read() throws IOException {
          int b = in.read();
          if (b >= 0) {
            bytesRead.incrementAndGet();
          }
          return b;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          int n = in.read(buffer, offset, length);
          if (n > 0) {
            bytesRead.addAndGet(n);
          }
          return n;
        }
      };
    }
  }
}
