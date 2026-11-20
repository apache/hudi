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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The whisper transcriber over a fake engine: mapping, bounds, failure classes and the context lifecycle, with no model. */
class TestWhisperSegmentTranscriber {

  /** A fake engine factory: returns scripted utterances and records what it was given. */
  private static final class FakeEngines implements WhisperEngine.Factory {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final List<Integer> samplesSeen = Collections.synchronizedList(new ArrayList<>());
    volatile List<WhisperEngine.RawUtterance> script = Collections.emptyList();
    volatile RuntimeException failure;
    volatile CountDownLatch inside;
    volatile CountDownLatch proceed;

    @Override
    public WhisperEngine open(WhisperConfig config) {
      opened.incrementAndGet();
      return new WhisperEngine() {
        private boolean done;

        @Override
        public List<RawUtterance> transcribe(WhisperConfig c, float[] pcm, int samples) {
          samplesSeen.add(samples);
          if (inside != null) {
            inside.countDown();
            await(proceed);
          }
          if (failure != null) {
            throw failure;
          }
          return script;
        }

        @Override
        public void close() {
          if (!done) {
            done = true;
            closed.incrementAndGet();
          }
        }
      };
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static TypedProperties props(String... keyValues) {
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }

  private static WhisperEngine.RawUtterance raw(long t0, long t1, String text) {
    return new WhisperEngine.RawUtterance(t0, t1, text);
  }

  private static List<TranscriptSegment> drain(ClosableIterator<TranscriptSegment> it) {
    List<TranscriptSegment> out = new ArrayList<>();
    try {
      it.forEachRemaining(out::add);
    } finally {
      it.close();
    }
    return out;
  }

  private final FakeEngines engines = new FakeEngines();
  private final WhisperContextPool pool = new WhisperContextPool(engines);

  @Test
  void mapsCentisecondsOntoVideoTimeline() throws IOException {
    engines.script = Arrays.asList(raw(0, 250, " Hello there."), raw(250, 480, " General."));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      List<TranscriptSegment> out = drain(transcriber.transcribe(
          WhisperTestSegments.pcm(3, 10_000, WhisperTestSegments.tone(5)), props()));
      assertEquals(2, out.size());
      assertEquals(new TranscriptSegment("speaker-1", "Hello there.", 10_000, 12_500, null), out.get(0));
      assertEquals(new TranscriptSegment("speaker-1", "General.", 12_500, 14_800, null), out.get(1));
      assertNull(out.get(0).getConfidence());
    }
  }

  @Test
  void timestampsClampedIntoSegmentSpan() throws IOException {
    engines.script = Arrays.asList(raw(0, 90, "a"), raw(150, 400, "b"), raw(400, 900, "c"));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      List<TranscriptSegment> out = drain(transcriber.transcribe(
          WhisperTestSegments.pcm(0, 1_000, WhisperTestSegments.tone(2)), props()));
      long end = 3_000;
      assertEquals(2, out.size(), "the utterance timed at 5 s, past the 3 s segment end, is dropped: " + out);
      for (TranscriptSegment u : out) {
        assertTrue(u.getStartMs() >= 1_000 && u.getEndMs() <= end && u.getStartMs() < end, u.toString());
      }
      assertEquals(new TranscriptSegment("speaker-1", "b", 2_500, end, null), out.get(1));
    }
  }

  @Test
  void startsMadeNonDecreasing() throws IOException {
    engines.script = Arrays.asList(raw(100, 200, "a"), raw(50, 120, "b"), raw(300, 350, "c"));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      List<TranscriptSegment> out = drain(transcriber.transcribe(
          WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(5)), props()));
      assertEquals(Arrays.asList(1_000L, 1_000L, 3_000L), Arrays.asList(out.get(0).getStartMs(), out.get(1).getStartMs(),
          out.get(2).getStartMs()));
      assertEquals(1_200L, out.get(1).getEndMs());
    }
  }

  @Test
  void shortSegmentPaddedToMinSamples() throws IOException {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      drain(transcriber.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(0.5)), props()));
      drain(transcriber.transcribe(WhisperTestSegments.pcm(1, 500, WhisperTestSegments.tone(2)), props()));
    }
    assertEquals(Arrays.asList(WhisperSegmentTranscriber.MIN_SAMPLES, 32_000), engines.samplesSeen);
  }

  @Test
  void emptyPayloadYieldsNoUtterancesAndNoEngine() throws IOException {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      assertTrue(drain(transcriber.transcribe(WhisperTestSegments.pcm(0, 0, new short[0]), props())).isEmpty());
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void declaredEmptyPayloadStillChecksStreamLength() {
    WhisperTestSegments.TestSegment segment = new WhisperTestSegments.TestSegment(
        0, 0, 1, 16000, 1, PcmPayload.PCM_S16LE, 0, () -> new ByteArrayInputStream(new byte[] {1, 2}));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(segment, props()));
    }
    assertEquals(1, segment.bytesRead.get(), "read at most one byte beyond the declaration");
    assertEquals(0, engines.opened.get());
  }

  @Test
  void declaredEmptyPayloadPropagatesOpeningFailure() {
    IOException failure = new IOException("payload unavailable");
    WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, new short[0]).failingReads(failure);
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      assertSame(failure, assertThrows(IOException.class, () -> transcriber.transcribe(segment, props())));
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void blankTagsAndEmptyTextDropped() throws IOException {
    engines.script = Arrays.asList(raw(0, 100, " [BLANK_AUDIO]"), raw(100, 150, "  "), raw(150, 200, null),
        raw(200, 300, " [MUSIC PLAYING]"), raw(300, 320, " [Music]"), raw(320, 340, " (music)"), raw(340, 360, "(upbeat music) "),
        raw(360, 400, " A [pause] word."));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      List<TranscriptSegment> out = drain(transcriber.transcribe(
          WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(5)), props()));
      assertEquals(1, out.size());
      assertEquals("A [pause] word.", out.get(0).getText());
    }
  }

  @Test
  void oneEngineAcrossSegments() throws IOException {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      for (int i = 0; i < 3; i++) {
        drain(transcriber.transcribe(WhisperTestSegments.pcm(i, i * 2_000L, WhisperTestSegments.tone(2)), props()));
      }
      assertEquals(1, engines.opened.get());
      assertEquals(1, pool.open());
    }
  }

  @Test
  void manyUnclosedInstancesSequentiallyShareOneContext() throws IOException {
    for (int i = 0; i < 5; i++) {
      WhisperSegmentTranscriber leaked = new WhisperSegmentTranscriber(pool);
      drain(leaked.transcribe(WhisperTestSegments.pcm(i, 0, WhisperTestSegments.tone(2)), props()));
    }
    assertEquals(1, engines.opened.get());
    assertEquals(1, pool.open());
    assertEquals(0, engines.closed.get());
  }

  @Test
  void concurrentCallsOpenOneContextEach() throws Exception {
    engines.inside = new CountDownLatch(2);
    engines.proceed = new CountDownLatch(1);
    ExecutorService threads = Executors.newFixedThreadPool(2);
    try (WhisperSegmentTranscriber a = new WhisperSegmentTranscriber(pool);
         WhisperSegmentTranscriber b = new WhisperSegmentTranscriber(pool)) {
      Future<?> fa = threads.submit(() -> drain(a.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props())));
      Future<?> fb = threads.submit(() -> drain(b.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props())));
      await(engines.inside);
      assertEquals(2, pool.open());
      engines.proceed.countDown();
      fa.get(30, TimeUnit.SECONDS);
      fb.get(30, TimeUnit.SECONDS);
      assertEquals(2, pool.idle(WhisperConfig.from(props())));
      engines.inside = null;
      drain(a.transcribe(WhisperTestSegments.pcm(1, 0, WhisperTestSegments.tone(2)), props()));
      assertEquals(2, engines.opened.get());
    } finally {
      threads.shutdownNow();
    }
    assertEquals(0, pool.open());
    assertEquals(2, engines.closed.get());
  }

  @Test
  void closeOfLastInstanceFreesIdle() throws IOException {
    WhisperSegmentTranscriber a = new WhisperSegmentTranscriber(pool);
    WhisperSegmentTranscriber b = new WhisperSegmentTranscriber(pool);
    drain(a.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props()));
    drain(b.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props()));
    a.close();
    assertEquals(1, pool.open());
    assertEquals(0, engines.closed.get());
    b.close();
    assertEquals(0, pool.open());
    assertEquals(1, engines.closed.get());
  }

  @Test
  void closeIdempotent() throws IOException {
    WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool);
    transcriber.close();
    drain(transcriber.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props()));
    transcriber.close();
    transcriber.close();
    assertEquals(0, pool.open());
    assertEquals(1, engines.closed.get());
  }

  @Test
  void transcribeAfterCloseRecreatesOneContext() throws IOException {
    WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool);
    drain(transcriber.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props()));
    transcriber.close();
    drain(transcriber.transcribe(WhisperTestSegments.pcm(1, 0, WhisperTestSegments.tone(2)), props()));
    drain(transcriber.transcribe(WhisperTestSegments.pcm(2, 0, WhisperTestSegments.tone(2)), props()));
    assertEquals(2, engines.opened.get());
    assertEquals(1, pool.open());
    transcriber.close();
    assertEquals(0, pool.open());
  }

  @Test
  void engineReturnedToPoolAfterFailure() throws IOException {
    engines.failure = new IllegalStateException("whisper refused the audio segment: full() returned -7");
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
      TypedProperties props = props();
      IllegalStateException e = assertThrows(IllegalStateException.class, () -> transcriber.transcribe(segment, props));
      assertSame(engines.failure, e);
      assertEquals(1, pool.idle(WhisperConfig.from(props())));
      engines.failure = null;
      drain(transcriber.transcribe(WhisperTestSegments.pcm(1, 0, WhisperTestSegments.tone(2)), props()));
      assertEquals(1, engines.opened.get());
    }
  }

  /** Whisper cannot tell an allocation or compute failure inside ggml from bad audio: a failed run is the platform's (class P). */
  @Test
  void whisperRunThatFailsIsAPlatformFailure() {
    PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> NativeWhisperEngine.checkFullResult(-7));
    assertTrue(e.getMessage().contains("returned -7"), e.getMessage());
    NativeWhisperEngine.checkFullResult(0);
  }

  @Test
  void decodesSigned16BitLittleEndian() throws IOException {
    byte[] bytes = {0x00, (byte) 0x80, (byte) 0xFF, 0x7F, (byte) 0xFF, (byte) 0xFF, 0x01, 0x00, 0x00, 0x00};
    WhisperTestSegments.TestSegment segment = new WhisperTestSegments.TestSegment(0, 0, 1_000, 16000, 1, PcmPayload.PCM_S16LE,
        bytes.length, () -> new ByteArrayInputStream(bytes));
    PcmPayload.Samples samples = PcmPayload.read(segment, 1024, 8);
    assertEquals(5, samples.count());
    float[] expected = {-1f, 32767 / 32768f, -1 / 32768f, 1 / 32768f, 0f, 0f, 0f, 0f};
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, samples.pcm());
  }

  @Test
  void midStreamReadFailureIsIOException() {
    IOException failure = new IOException("connection reset after 100 bytes");
    WhisperTestSegments.TestSegment segment = new WhisperTestSegments.TestSegment(0, 0, 1_000, 16000, 1, PcmPayload.PCM_S16LE,
        4096, () -> new java.io.SequenceInputStream(new ByteArrayInputStream(new byte[100]), new java.io.InputStream() {
          @Override
          public int read() throws IOException {
            throw failure;
          }
        }));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      TypedProperties props = props();
      assertSame(failure, assertThrows(IOException.class, () -> transcriber.transcribe(segment, props)));
    }
    assertEquals(0, engines.opened.get());
  }

  /*
   * The segment source and the transcriber are both ours and bounded by the same keys: a payload that breaks what the source
   * promises breaks an invariant between our own components, the platform's failure (class P), never the video's.
   */

  @Test
  void payloadLongerThanItsSpanIsAPlatformFailure() throws IOException {
    WhisperTestSegments.TestSegment twoSeconds = WhisperTestSegments.pcm(0, 10_000, WhisperTestSegments.tone(2));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      WhisperTestSegments.TestSegment tooLong = twoSeconds.withSpan(10_000, 11_000);
      TypedProperties props = props();
      PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(tooLong, props));
      assertTrue(e.getMessage().contains("longer than its span"), e.getMessage());
      // within the 20 ms rounding tolerance: accepted
      drain(transcriber.transcribe(twoSeconds.withSpan(10_000, 11_985), props()));
    }
    assertEquals(1, engines.opened.get());
  }

  @Test
  void corruptSegmentTimesArePlatformFailures() {
    WhisperTestSegments.TestSegment good = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      for (WhisperTestSegments.TestSegment bad : Arrays.asList(good.withSpan(-5, 1_000), good.withSpan(2_000, 2_000),
          good.withSpan(3_000, 1_000))) {
        TypedProperties props = props();
        PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(bad, props));
        assertTrue(e.getMessage().startsWith("corrupt audio segment 0"), e.getMessage());
      }
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void payloadReadFailureIsIOException() {
    IOException failure = new IOException("spill file gone");
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)).failingReads(failure);
      TypedProperties props = props();
      IOException e = assertThrows(IOException.class, () -> transcriber.transcribe(segment, props));
      assertSame(failure, e);
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void oversizedStreamRefusedAtBoundAsAPlatformFailure() {
    byte[] large = new byte[2 * 1024 * 1024];
    WhisperTestSegments.TestSegment lying = new WhisperTestSegments.TestSegment(0, 0, 1_000, 16000, 1, PcmPayload.PCM_S16LE,
        1024, () -> new ByteArrayInputStream(large));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      TypedProperties props = props();
      PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(lying, props));
      assertTrue(e.getMessage().contains("longer than its declared"), e.getMessage());
    }
    assertTrue(lying.bytesRead.get() <= 1025, "read " + lying.bytesRead.get());
    assertEquals(0, engines.opened.get());
  }

  @Test
  void shortStreamRefusedAsAPlatformFailure() {
    WhisperTestSegments.TestSegment shortStream = new WhisperTestSegments.TestSegment(0, 0, 1_000, 16000, 1, PcmPayload.PCM_S16LE,
        4096, () -> new ByteArrayInputStream(new byte[100]));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      TypedProperties props = props();
      PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(shortStream, props));
      assertTrue(e.getMessage().contains("ended after 100"), e.getMessage());
    }
  }

  @Test
  void wrongRateChannelsEncodingOrOddBytesArePlatformFailures() {
    WhisperTestSegments.TestSegment good = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      for (WhisperTestSegments.TestSegment bad : Arrays.asList(
          good.with(8000, 1, PcmPayload.PCM_S16LE, good.payloadBytes()),
          good.with(16000, 2, PcmPayload.PCM_S16LE, good.payloadBytes()),
          good.with(16000, 1, "flac", good.payloadBytes()),
          good.with(16000, 1, PcmPayload.PCM_S16LE, good.payloadBytes() - 1),
          good.with(16000, 1, PcmPayload.PCM_S16LE, -2))) {
        TypedProperties props = props();
        PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(bad, props));
        assertTrue(e.getMessage().startsWith("corrupt audio segment 0"), e.getMessage());
      }
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void oversizedDeclaredPayloadIsAPlatformFailureBeforeAnyRead() {
    WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      TypedProperties props = props(AudioConfigKeys.SEGMENT_MAX_BYTES.key(), "4096");
      PrepPlatformException e = assertThrows(PrepPlatformException.class, () -> transcriber.transcribe(segment, props));
      assertTrue(e.getMessage().contains("above the bound of 4096"), e.getMessage());
    }
    assertEquals(0, segment.bytesRead.get());
  }

  @Test
  void configChangeMidInstanceRefused() throws IOException {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      drain(transcriber.transcribe(WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2)), props()));
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(1, 0, WhisperTestSegments.tone(2));
      TypedProperties changed = props(WhisperConfig.MODEL.key(), "small.en");
      assertThrows(IllegalArgumentException.class, () -> transcriber.transcribe(segment, changed));
    }
  }

  @Test
  void badKeyIsConfigurationError() {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      TypedProperties validated = props(WhisperConfig.THREADS.key(), "0");
      assertThrows(IllegalArgumentException.class, () -> transcriber.validate(validated));
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
      TypedProperties transcribed = props(WhisperConfig.THREADS.key(), "0");
      assertThrows(IllegalArgumentException.class, () -> transcriber.transcribe(segment, transcribed));
    }
  }

  @Test
  void idFollowsValidatedPropsAndSpeakersAreFileScoped() {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      IllegalStateException e = assertThrows(IllegalStateException.class, transcriber::id);
      assertTrue(e.getMessage().contains("validate"), e.getMessage());
      transcriber.validate(props(WhisperConfig.MODEL.key(), "small.en"));
      assertEquals("whisper-cpp-1.7.1-small.en/en/transcribe", transcriber.id());
      assertEquals(transcriber.id(), WhisperSegmentTranscriber.id(props(WhisperConfig.MODEL.key(), "small.en")));
      assertTrue(transcriber.fileScopedSpeakers());
    }
    assertEquals(0, engines.opened.get());
  }

  @Test
  void noArgConstructorForClassKeySelection() throws Exception {
    Object instance = Class.forName(WhisperSegmentTranscriber.class.getName()).getConstructor().newInstance();
    assertTrue(instance instanceof org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber);
    assertTrue(instance instanceof AutoCloseable);
    ((AutoCloseable) instance).close();
  }
}
