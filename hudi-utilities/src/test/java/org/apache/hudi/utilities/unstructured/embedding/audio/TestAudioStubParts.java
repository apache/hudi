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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.ClipDuration;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The audio stubs, the resolved audio configuration and the audio logic line (no Spark). */
class TestAudioStubParts {

  @TempDir
  Path tempDir;

  @Test
  void stubSourceCutsTheClipDurationIntoFixedSegmentsAndResumes() throws IOException {
    Path clip = VideoClips.generate(tempDir.resolve("five.mp4"), 5, 10, 2, 160, 120, -1, -1);
    long duration = ClipDuration.millis(VideoClips.supplier(clip), "five");
    assertTrue(duration >= 4900 && duration <= 5200, "declared duration " + duration);
    TypedProperties props = props();
    props.setProperty(AudioConfigKeys.SEGMENT_MAX_MS.key(), "2000");
    StubAudioSegmentSource source = new StubAudioSegmentSource();
    assertEquals("audio-stub-v0", source.logicVersion());
    List<AudioSegment> all = drain(source.segments(input(clip), 0, 0, props));
    assertEquals(Arrays.asList("segment 0 [0, 2000) ms", "segment 1 [2000, 4000) ms", "segment 2 [4000, " + duration + ") ms"),
        names(all));
    for (AudioSegment segment : all) {
      assertEquals(AudioConfigKeys.DEFAULT_SAMPLE_RATE, segment.sampleRate());
      assertEquals(1, segment.channels());
      assertEquals(StubAudioSegmentSource.ENCODING, segment.encoding());
      assertEquals(0L, segment.payloadBytes());
      assertEquals(-1, segment.openPayload().read(), "the stub carries no audio");
    }
    // resume at segment 2 (startMs = end of segment 1): the same segment, with its true index and span
    assertEquals(Collections.singletonList(names(all).get(2)), names(drain(source.segments(input(clip), 2, 4000, props))));
    assertTrue(drain(source.segments(input(clip), 3, duration, props)).isEmpty(), "every segment done");
    AudioInput input = input(clip);
    assertThrows(IllegalArgumentException.class, () -> source.segments(input, -1, 0, props));
  }

  @Test
  void segmentLongerThanTheClipIsTheWholeClip() throws IOException {
    Path clip = VideoClips.generate(tempDir.resolve("whole.mp4"), 3, 10, 2, 160, 120, -1, -1);
    long duration = ClipDuration.millis(VideoClips.supplier(clip), "whole");
    TypedProperties props = props();
    props.setProperty(AudioConfigKeys.SEGMENT_MAX_MS.key(), String.valueOf(Long.MAX_VALUE));
    assertEquals(Collections.singletonList("segment 0 [0, " + duration + ") ms"),
        names(drain(new StubAudioSegmentSource().segments(input(clip), 0, 0, props))));
  }

  @Test
  void stubSourceFailuresAreClassifiedByCause() throws IOException {
    Path clip = VideoClips.generate(tempDir.resolve("guard.mp4"), 3, 10, 2, 160, 120, -1, -1);
    TypedProperties props = props();
    props.setProperty(AudioConfigKeys.MAX_DURATION_MS.key(), "1000");
    StubAudioSegmentSource source = new StubAudioSegmentSource();
    AudioInput tooLongInput = input(clip);
    IllegalStateException tooLong = assertThrows(IllegalStateException.class, () -> source.segments(tooLongInput, 0, 0, props));
    assertTrue(tooLong.getMessage().contains(AudioConfigKeys.MAX_DURATION_MS.key()), tooLong.getMessage());

    // a storage failure under the probe: transient
    StubAudioSegmentSource storageSource = new StubAudioSegmentSource();
    AudioInput failingInput = input(VideoClips.failingSupplier(clip, 0));
    TypedProperties storageProps = props();
    IOException storage = assertThrows(IOException.class, () -> storageSource.segments(failingInput, 0, 0, storageProps));
    assertTrue(BlobReadException.isStorageFailure(storage), storage.toString());

    // bytes that are no container: the clip's data
    Path garbage = Files.write(tempDir.resolve("garbage.mp4"), "not a video at all".getBytes(StandardCharsets.UTF_8));
    StubAudioSegmentSource dataSource = new StubAudioSegmentSource();
    AudioInput garbageInput = input(garbage);
    TypedProperties dataProps = props();
    VideoDecodeException data = assertThrows(VideoDecodeException.class, () -> dataSource.segments(garbageInput, 0, 0, dataProps));
    assertFalse(BlobReadException.isStorageFailure(data), data.toString());
  }

  @Test
  void storageFailureUnderTheProbeIsNeverADuration() throws IOException {
    // storage that fails from offset k on, for k across the clip: whenever a read failed, the probe must not return a
    // duration (a decoder may swallow the failure and still report one), it must report the storage failure
    Path clip = VideoClips.generate(tempDir.resolve("sweep.mp4"), 4, 10, 2, 160, 120, -1, -1);
    long size = Files.size(clip);
    int swallowedOrThrown = 0;
    for (int step = 0; step <= 40; step++) {
      long failAt = size * step / 40;
      boolean[] failed = {false};
      SeekableBlobBytesSupplier failing = VideoClips.failingSupplier(clip, failAt);
      SeekableBlobBytesSupplier recording = new SeekableBlobBytesSupplier() {
        @Override
        public SeekableBlob openSeekable() throws IOException {
          SeekableBlob blob = failing.openSeekable();
          return new SeekableBlob() {
            @Override
            public long length() {
              return blob.length();
            }

            @Override
            public long getPos() throws IOException {
              return blob.getPos();
            }

            @Override
            public void seek(long pos) throws IOException {
              blob.seek(pos);
            }

            @Override
            public int read(byte[] buffer, int offset, int len) throws IOException {
              try {
                return blob.read(buffer, offset, len);
              } catch (IOException e) {
                failed[0] = true;
                throw e;
              }
            }

            @Override
            public void close() throws IOException {
              blob.close();
            }
          };
        }

        @Override
        public InputStream open() throws IOException {
          return failing.open();
        }
      };
      try {
        long duration = ClipDuration.millis(recording, "sweep");
        assertFalse(failed[0], "failAt " + failAt + " of " + size + ": a read failed, yet a duration " + duration + " came back");
      } catch (BlobReadException e) {
        assertTrue(failed[0], "failAt " + failAt + ": " + e);
        swallowedOrThrown++;
      } catch (VideoDecodeException e) {
        assertFalse(failed[0], "failAt " + failAt + ": a storage failure reported as the clip's data: " + e);
      }
    }
    assertTrue(swallowedOrThrown > 0, "the sweep met at least one storage failure");
  }

  @Test
  void stubTranscriberSaysOneUtterancePerSegmentByTheFileSpeaker() throws IOException {
    StubSegmentTranscriber transcriber = new StubSegmentTranscriber();
    assertEquals("stub/fixed-utterance-v0", transcriber.id());
    assertTrue(transcriber.fileScopedSpeakers());
    assertEquals(SpeakerLabels.SCOPE_FILE, SpeakerLabels.scopeOf(transcriber));
    Path clip = VideoClips.generate(tempDir.resolve("one.mp4"), 3, 10, 2, 160, 120, -1, -1);
    TypedProperties props = props();
    props.setProperty(AudioConfigKeys.SEGMENT_MAX_MS.key(), "1000");
    AudioSegment second = drain(new StubAudioSegmentSource().segments(input(clip), 1, 1000, props)).get(0);
    List<TranscriptSegment> said = drainUtterances(transcriber.transcribe(second, props));
    assertEquals(Collections.singletonList(new TranscriptSegment("speaker-1", "Utterance of audio segment 1.", 1000, 2000, null)),
        said);
  }

  @Test
  void stubChunkerPacksSpeakerLinesUnderTheTokenLimit() {
    WordCountTokenizer words = new WordCountTokenizer();
    List<TranscriptChunker.SegmentUtterance> three = Arrays.asList(utterance(4, "Hello   there.", 4000, 5000),
        utterance(5, "Second  thought.", 5000, 6000), utterance(6, "", 6000, 7000), utterance(6, "Third.", 6000, 7000));
    // everything fits one chunk; the empty utterance adds no line
    List<TranscriptChunk> one = chunks(three, 9, words, 256);
    assertEquals(Collections.singletonList(new TranscriptChunkValue(9, "[speaker-1] Hello there.\n[speaker-1] Second thought.\n" + "[speaker-1] Third.", 8,
        new TranscriptChunkValue.Range(4000, 7000), Collections.singletonList("speaker-1"), new TranscriptChunkValue.Range(4, 6))), one);
    // four words per chunk: one line per chunk
    List<TranscriptChunk> perLine = chunks(three, 0, words, 4);
    assertEquals(Arrays.asList("[speaker-1] Hello there.", "[speaker-1] Second thought.", "[speaker-1] Third."), texts(perLine));
    assertEquals(Arrays.asList(0L, 1L, 2L), Arrays.asList(perLine.get(0).ordinal(), perLine.get(1).ordinal(),
        perLine.get(2).ordinal()));
    // two words per chunk: a line is split at words, each piece keeps its speaker
    List<TranscriptChunk> split = chunks(Collections.singletonList(utterance(0, "one two three", 0, 900)), 0, words, 2);
    assertEquals(Arrays.asList("[speaker-1] one", "[speaker-1] two", "[speaker-1] three"), texts(split));
    for (TranscriptChunk chunk : split) {
      assertEquals(2, chunk.tokenCount());
      assertEquals(0L, chunk.firstSegment());
    }
    // a single word that never fits fails the video
    List<TranscriptChunker.SegmentUtterance> oneWord = Collections.singletonList(utterance(0, "word", 0, 900));
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> chunks(oneWord, 0, words, 1));
    assertTrue(e.getMessage().contains(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()), e.getMessage());
    assertEquals("transcript-stub-pack-v0", new StubTranscriptChunker().logicVersion());
  }

  @Test
  void logicLineIsPinnedAndNamesTheConfiguredImplementations() {
    TypedProperties off = AudioStubs.named(props());
    assertFalse(AudioConfig.from(off).enabled());
    assertEquals("audio=audio-stub-v0;asr=stub/fixed-utterance-v0;pack=transcript-stub-pack-v0;tokenizer=openai-bpe:cl100k_base;speech=prep-v1",
        AudioConfig.from(off).logicLine(), "the default tokenizer");

    TypedProperties on = AudioStubs.named(props());
    on.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    on.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    String line = "audio=audio-stub-v0;asr=stub/fixed-utterance-v0;pack=transcript-stub-pack-v0;tokenizer=word-count;speech=prep-v1";
    assertEquals(line, AudioConfig.from(on).logicLine());
    // the same classes named explicitly give the same line
    TypedProperties named = TypedProperties.copy(on);
    named.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    named.setProperty(AudioConfig.TRANSCRIBER_PREFIX + "model", "unused-by-the-stub");
    assertEquals(line, AudioConfig.from(named).logicLine());
    // another transcriber is named by its own id
    named.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), OtherTranscriber.class.getName());
    assertTrue(AudioConfig.from(named).logicLine().contains(";asr=other/model-x;"), AudioConfig.from(named).logicLine());
  }

  /** The source can be named independently of the default transcriber and chunker. */
  @Test
  void classKeysDefaultToTheRealImplementationsAndAreCheckedOnTheDriver() {
    TypedProperties available = props();
    available.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), StubAudioSegmentSource.class.getName());
    AudioConfig defaults = AudioConfig.from(available);
    assertTrue(defaults.newSegmentSource() instanceof StubAudioSegmentSource);
    assertTrue(defaults.newTranscriber() instanceof WhisperSegmentTranscriber);
    assertTrue(defaults.newChunker() instanceof TranscriptPacker);
    // the default whisper model needs its directory: a job without it is refused at start, naming the key
    IllegalArgumentException noModel = assertThrows(IllegalArgumentException.class, defaults::validate);
    assertTrue(noModel.getMessage().startsWith(WhisperConfig.MODEL_DIR.key() + " "), noModel.getMessage());
    TypedProperties withModel = TypedProperties.copy(available);
    withModel.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    AudioConfig.from(withModel).validate();
    assertEquals("audio=audio-stub-v0;asr=whisper-cpp-1.7.1-base.en/en/transcribe;pack="
        + "transcript-pack-v2;tokenizer=word-count;speech=prep-v1", AudioConfig.from(withModel).logicLine(new WordCountTokenizer()));
    AudioConfig stubs = AudioConfig.from(AudioStubs.named(props()));
    assertTrue(stubs.newSegmentSource() instanceof StubAudioSegmentSource);
    assertTrue(stubs.newTranscriber() instanceof StubSegmentTranscriber);
    assertTrue(stubs.newChunker() instanceof StubTranscriptChunker);
    stubs.validate();

    TypedProperties plugged = props();
    plugged.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), OtherTranscriber.class.getName());
    assertTrue(AudioConfig.from(plugged).newTranscriber() instanceof OtherTranscriber);

    assertRefused(AudioConfigKeys.TRANSCRIBER_CLASS.key(), String.class.getName());
    assertRefused(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), "org.example.NoSuchSource");
    assertRefused(AudioConfigKeys.ENABLED.key(), "yes");
    assertRefused(AudioConfigKeys.SEGMENT_MAX_MS.key(), "-5");
  }

  @Test
  void everyInstanceOfAnAutoCloseableImplementationIsClosedOnce() {
    TypedProperties props = AudioStubs.named(props());
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), ClosingTranscriber.class.getName());
    ClosingTranscriber.reset();
    AudioConfig config = AudioConfig.from(props);
    config.validate();
    config.logicLine();
    config.logicLine();
    assertTrue(ClosingTranscriber.CREATED.get() >= 3, "driver instances: " + ClosingTranscriber.CREATED);
    assertEquals(ClosingTranscriber.CREATED.get(), ClosingTranscriber.CLOSED.get(), "every driver instance closed");

    // a task's instance is closed by the video preprocessor that built it (TestVideoFramePreprocessor)
  }

  @Test
  void anIdBuiltFromTheImplementationsOwnKeysIsReadAfterValidate() {
    TypedProperties props = AudioStubs.named(props());
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), ConfiguredTranscriber.class.getName());
    props.setProperty(ConfiguredTranscriber.MODEL, "base.en");
    String line = "audio=audio-stub-v0;asr=configured/base.en;pack=transcript-stub-pack-v0;tokenizer=openai-bpe:cl100k_base;speech=prep-v1";
    assertEquals(line, AudioConfig.from(props).logicLine());
    // the model is the transcriber's own key: another model is another logic line, through id()
    props.setProperty(ConfiguredTranscriber.MODEL, "small.en");
    assertTrue(AudioConfig.from(props).logicLine().contains(";asr=configured/small.en;"));
  }

  /** A transcriber whose id() names its configured model, which it learns in validate() (as an engine-backed one does). */
  public static class ConfiguredTranscriber extends StubSegmentTranscriber {
    static final String MODEL = AudioConfig.TRANSCRIBER_PREFIX + "configured.model";
    private String model;

    @Override
    public void validate(TypedProperties props) {
      model = props.getString(MODEL);
    }

    @Override
    public String id() {
      if (model == null) {
        throw new IllegalStateException("id() before validate()");
      }
      return "configured/" + model;
    }
  }

  /** A transcriber holding a resource (as an engine context would): counts its instances and closes. */
  public static class ClosingTranscriber extends StubSegmentTranscriber implements AutoCloseable {
    static final AtomicInteger CREATED = new AtomicInteger();
    static final AtomicInteger CLOSED = new AtomicInteger();

    public ClosingTranscriber() {
      CREATED.incrementAndGet();
    }

    static void reset() {
      CREATED.set(0);
      CLOSED.set(0);
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  private static void assertRefused(String key, String value) {
    TypedProperties props = AudioStubs.named(props());
    props.setProperty(key, value);
    AudioConfig config = AudioConfig.from(props);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> enabledThenValidate(config), key + "=" + value);
    assertTrue(e.getMessage().contains(key), e.getMessage());
  }

  /** Reads {@link AudioConfig#enabled()} then validates: a refused value fails one or the other. */
  private static void enabledThenValidate(AudioConfig config) {
    config.enabled();
    config.validate();
  }

  /** A transcriber registered by its class key alone, as a real implementation registers. */
  public static class OtherTranscriber extends StubSegmentTranscriber {
    @Override
    public String id() {
      return "other/model-x";
    }
  }

  private static TypedProperties props() {
    return new TypedProperties();
  }

  private static AudioInput input(Path file) {
    return input(VideoClips.supplier(file));
  }

  private static AudioInput input(BlobBytesSupplier bytes) {
    return new AudioInput() {
      @Override
      public String sourcePathSha() {
        return "fid-1";
      }

      @Override
      public String sourcePath() {
        return null;
      }

      @Override
      public String sourcePartitionPath() {
        return null;
      }

      @Override
      public long sizeBytes() {
        return -1;
      }

      @Override
      public String extension() {
        return "mp4";
      }

      @Override
      public BlobBytesSupplier bytes() {
        return bytes;
      }
    };
  }

  private static TranscriptChunker.SegmentUtterance utterance(long segment, String text, long startMs, long endMs) {
    TranscriptSegment utterance = new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, text, startMs, endMs, null);
    return new TranscriptChunker.SegmentUtterance() {
      @Override
      public long segmentIndex() {
        return segment;
      }

      @Override
      public TranscriptSegment utterance() {
        return utterance;
      }
    };
  }

  private static List<TranscriptChunk> chunks(List<TranscriptChunker.SegmentUtterance> utterances, long firstOrdinal,
                                              WordCountTokenizer tokenizer, int maxTokens) {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), String.valueOf(maxTokens));
    Iterator<TranscriptChunker.SegmentUtterance> input = utterances.iterator();
    List<TranscriptChunk> chunks = new ArrayList<>();
    try (ClosableIterator<TranscriptChunk> packed = new StubTranscriptChunker().chunks(input, firstOrdinal, tokenizer, props)) {
      packed.forEachRemaining(chunks::add);
    }
    assertFalse(input.hasNext(), "the chunker packs the whole group");
    return chunks;
  }

  private static List<String> texts(List<TranscriptChunk> chunks) {
    List<String> texts = new ArrayList<>();
    chunks.forEach(chunk -> texts.add(chunk.text()));
    return texts;
  }

  private static List<AudioSegment> drain(ClosableIterator<AudioSegment> segments) {
    List<AudioSegment> all = new ArrayList<>();
    try (ClosableIterator<AudioSegment> closing = segments) {
      closing.forEachRemaining(all::add);
    }
    return all;
  }

  private static List<TranscriptSegment> drainUtterances(ClosableIterator<TranscriptSegment> utterances) {
    List<TranscriptSegment> all = new ArrayList<>();
    try (ClosableIterator<TranscriptSegment> closing = utterances) {
      closing.forEachRemaining(all::add);
    }
    return all;
  }

  private static List<String> names(List<AudioSegment> segments) {
    List<String> names = new ArrayList<>();
    segments.forEach(segment -> names.add(segment.toString()));
    return names;
  }
}
