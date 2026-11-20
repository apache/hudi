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

package org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.LogCapture;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptPacker;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.bytedeco.ffmpeg.avformat.AVFormatContext;
import org.bytedeco.ffmpeg.avformat.AVStream;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avformat;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The FFmpeg audio track source over generated clips and one public-domain speech clip. */
class TestFfmpegAudioSegmentSource {

  /** Tone 0-2 s, silence 1 s, tone 1.5 s, silence 0.3 s (shorter than vad.min.silence.ms: merged), tone 1.2 s, silence 2 s, tone 3 s. */
  private static final long[] LAYOUT = {2000, 1000, 1500, 300, 1200, 2000, 3000};
  private static final long[][] LAYOUT_SEGMENTS = {{0, 2000}, {3000, 6000}, {8000, 11000}};
  /** A boundary may move by the codec's priming and smearing and by the 30 ms window grid. */
  private static final long TOLERANCE_MS = 90;
  /** The read budgets of the probe-failure sweep: one every 512 bytes up to 64 KiB, past the probe of the small clips. */
  private static final long PROBE_BUDGET_STEP = 512;
  private static final long PROBE_BUDGET_LIMIT = 64 * 1024;
  /** The speech clip: 12 s of LibriVox speech (public domain) over H.264 video. */
  private static final String SPEECH_CLIP = "/video/h264-aac-speech-12s.mp4";

  @TempDir
  static Path dir;
  private static Path aac;
  private static Path mp3;
  private static Path speech;

  @BeforeAll
  static void generate() throws IOException, URISyntaxException {
    aac = AudioClips.generate(dir.resolve("mpeg4-aac.mp4"), AudioClips.Codec.AAC, 44100, 2, LAYOUT);
    mp3 = AudioClips.generate(dir.resolve("mpeg4-mp3.mp4"), AudioClips.Codec.MP3, 44100, 2, LAYOUT);
    speech = Paths.get(Objects.requireNonNull(TestFfmpegAudioSegmentSource.class.getResource(SPEECH_CLIP)).toURI());
  }

  /** Concrete defaults first become available with the FFmpeg implementation at this position. */
  @Test
  void concreteDefaultsAreValidatedOnTheDriver() {
    TypedProperties props = props(AudioConfigKeys.ENABLED.key(), true);
    AudioConfig defaults = AudioConfig.from(props);
    assertTrue(defaults.newSegmentSource() instanceof FfmpegAudioSegmentSource);
    assertTrue(defaults.newTranscriber() instanceof WhisperSegmentTranscriber);
    assertTrue(defaults.newChunker() instanceof TranscriptPacker);
    IllegalArgumentException noModel = assertThrows(IllegalArgumentException.class, defaults::validate);
    assertTrue(noModel.getMessage().startsWith(WhisperConfig.MODEL_DIR.key() + " "));
    props.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    AudioConfig configured = AudioConfig.from(props);
    configured.validate();
    assertEquals("audio=" + FfmpegAudioSegmentSource.LOGIC_VERSION + ";asr=whisper-cpp-1.7.1-base.en/en/transcribe;pack="
        + "transcript-pack-v2;tokenizer=word-count;speech=prep-v1", configured.logicLine(new WordCountTokenizer()));
  }

  @Test
  void firstAudioTrackSuppliesTheSegments() throws Exception {
    Path tone = AudioClips.generate(dir.resolve("first-tone.mp4"), AudioClips.Codec.AAC, 16000, 1, 1000);
    Path silence = AudioClips.generate(dir.resolve("second-silence.mp4"), AudioClips.Codec.AAC, 16000, 1, 0, 1000);
    Path twoTracks = MuxedFixtures.remux(dir.resolve("two-audio.mp4"), MuxedFixtures.Pick.video(tone, 0),
        MuxedFixtures.Pick.audio(tone, 0).disposition(avformat.AV_DISPOSITION_DEFAULT),
        MuxedFixtures.Pick.audio(silence, 0).disposition(0));
    try (FFmpegFrameGrabber probe = new FFmpegFrameGrabber(twoTracks.toFile())) {
      probe.start();
      assertEquals(3, probe.getFormatContext().nb_streams());
      assertEquals(1, probe.getFormatContext().streams(1).disposition());
      assertEquals(0, probe.getFormatContext().streams(2).disposition());
    }
    assertTrue(segments(silence).isEmpty());
    assertFalse(segments(tone).isEmpty());
    assertFalse(segments(twoTracks).isEmpty(), "the first audio track contains the tone");
  }

  @Test
  void shortAudioInLongVideoUsesTheAudioDuration() throws Exception {
    Path tone = AudioClips.generate(dir.resolve("short-tone.mp4"), AudioClips.Codec.AAC, 16000, 1, 1000);
    Path mixed = MuxedFixtures.remux(dir.resolve("long-video-short-audio.mp4"), MuxedFixtures.Pick.video(aac, 0),
        MuxedFixtures.Pick.audio(tone, 0));
    try (FFmpegFrameGrabber probe = new FFmpegFrameGrabber(mixed.toFile())) {
      probe.start();
      org.bytedeco.ffmpeg.avformat.AVStream audio = probe.getFormatContext().streams(probe.getAudioStream());
      double seconds = audio.duration() * (double) audio.time_base().num() / audio.time_base().den();
      assertTrue(seconds > 0.8 && seconds < 1.2);
      assertTrue(probe.getLengthInTime() > 2_000_000L);
    }
    List<Seen> expected = segments(mixed);
    assertFalse(expected.isEmpty());
    TypedProperties limit = props(AudioConfigKeys.MAX_DURATION_MS.key(), 2000);
    assertEquals(expected, assertDoesNotThrow(() -> segments(mixed, 0, 0, limit)));
    assertThrows(VideoDecodeException.class, () -> segments(aac, 0, 0, limit));
  }

  private static AudioInput failingOpen(IOException cause) {
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        throw cause;
      }

      @Override
      public InputStream open() {
        throw new AssertionError("expected the seekable open path");
      }
    };
    return AudioClips.input("open-failure", aac, bytes);
  }

  @Test
  void openingTimeoutCarriesTheStorageFailureMarker() {
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    Exception timeout = assertThrows(Exception.class, () -> drain(source,
        failingOpen(new java.net.SocketTimeoutException("opening the object")), false));
    assertTrue(BlobReadException.isStorageFailure(timeout), timeout::toString);
    IOException shortObject = new org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException(
        "stored object ends inside its recorded range");
    Exception truncated = assertThrows(Exception.class, () -> drain(source, failingOpen(shortObject), false));
    assertFalse(BlobReadException.isStorageFailure(truncated), truncated::toString);
  }

  @Test
  void missingObjectAtOpenRemainsAContentFailure() {
    IOException missing = new java.io.FileNotFoundException("stored object is missing");
    Exception failure = assertThrows(Exception.class,
        () -> drain(new FfmpegAudioSegmentSource(), failingOpen(missing), false));
    assertFalse(BlobReadException.isStorageFailure(failure));
    assertSame(missing, failure);
  }

  @Test
  void incompleteProbeIsNotAnEmptyAudioResult() throws Exception {
    AVFormatContext format = avformat.avformat_alloc_context();
    try {
      avformat.avformat_new_stream(format, null).codecpar().codec_type(org.bytedeco.ffmpeg.global.avutil.AVMEDIA_TYPE_VIDEO);
      FFmpegFrameGrabber grabber = mock(FFmpegFrameGrabber.class);
      when(grabber.getFormatContext()).thenReturn(format);
      when(grabber.getAudioStream()).thenReturn(-1);
      FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource() {
        @Override
        FFmpegFrameGrabber newGrabber(InputStream in) {
          return grabber;
        }
      };
      try (FfmpegAudioSegmentSource.Segments empty = source.segments(input(aac), 0, 0, new TypedProperties())) {
        assertFalse(empty.hasNext());
        assertEquals(FfmpegAudioSegmentSource.REASON_NO_AUDIO_STREAM, empty.endReason());
      }
      org.mockito.Mockito.doThrow(new FFmpegFrameGrabber.Exception("fixture probe ended early")).when(grabber).start();
      assertThrows(VideoDecodeException.class, () -> source.segments(input(aac), 0, 0, new TypedProperties()));
    } finally {
      avformat.avformat_free_context(format);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, 9_223_350_000L})
  void segmentTimesFollowRawAudioPts(long originMicros) throws IOException {
    AVFormatContext format = avformat.avformat_alloc_context();
    try (AVFrame first = new AVFrame(); AVFrame second = new AVFrame()) {
      format.start_time(originMicros).duration(60_000L);
      format.iformat(avformat.av_find_input_format("matroska"));
      AVStream stream = avformat.avformat_new_stream(format, null);
      stream.time_base().num(1).den(1_000_000_000);
      stream.start_time(originMicros * 1000).duration(60_000_000L);
      stream.codecpar().codec_type(org.bytedeco.ffmpeg.global.avutil.AVMEDIA_TYPE_AUDIO)
          .codec_id(org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_PCM_S16LE).sample_rate(16000);
      Frame a = decodedWindow(first, originMicros);
      Frame b = decodedWindow(second, originMicros + 30_000L);
      if (originMicros > 0) {
        assertTrue(a.timestamp > 0 && b.timestamp < 0, "fixture crosses the JavaCV timestamp overflow");
      }
      FFmpegFrameGrabber grabber = mock(FFmpegFrameGrabber.class);
      when(grabber.getFormatContext()).thenReturn(format);
      when(grabber.getAudioStream()).thenReturn(0);
      when(grabber.getAudioChannels()).thenReturn(1);
      when(grabber.getSampleRate()).thenReturn(16000);
      when(grabber.getLengthInTime()).thenReturn(60_000L);
      when(grabber.grabSamples()).thenReturn(a, b, (Frame) null);
      FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource() {
        @Override
        FFmpegFrameGrabber newGrabber(InputStream in) {
          return grabber;
        }
      };
      try (FfmpegAudioSegmentSource.Segments it = source.segments(input(aac), 0, 0, new TypedProperties())) {
        AudioSegment segment = it.next();
        assertEquals(0, segment.startMs());
        assertEquals(60, segment.endMs());
        assertEquals(1920, segment.payloadBytes());
        assertFalse(it.hasNext());
      }
    } finally {
      avformat.avformat_free_context(format);
    }
  }

  private static Frame decodedWindow(AVFrame raw, long micros) {
    long pts = micros * 1000L;
    raw.best_effort_timestamp(pts).nb_samples(480);
    Frame frame = new Frame();
    frame.opaque = raw;
    frame.timestamp = 1_000_000L * pts / 1_000_000_000L;
    frame.sampleRate = 16000;
    frame.audioChannels = 1;
    float[] samples = new float[480];
    Arrays.fill(samples, 0.25f);
    frame.samples = new FloatBuffer[] {FloatBuffer.wrap(samples)};
    return frame;
  }

  @Test
  void largestSilenceSettingRetainsAPositiveWindowCount() {
    assertEquals(Integer.MAX_VALUE, FfmpegAudioConfig.from(
        props(AudioConfigKeys.VAD_MIN_SILENCE_MS.key(), Long.MAX_VALUE)).getMinSilenceWindows());
    assertEquals(2, FfmpegAudioConfig.from(props(AudioConfigKeys.VAD_MIN_SILENCE_MS.key(), 31)).getMinSilenceWindows());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10})
  void mp3ResumeFollowsTheContainerTimeline(int audioOffsetSeconds) throws Exception {
    Path video = MuxedFixtures.encode(dir.resolve("resume-video-" + audioOffsetSeconds + ".mp4"), 32, 0, 0, 0, 0);
    Path audio = MuxedFixtures.encode(dir.resolve("resume-audio-" + audioOffsetSeconds + ".mp4"), 0, 10,
        avcodec.AV_CODEC_ID_MP3, 44100, 64000);
    Path clip = MuxedFixtures.remux(dir.resolve("resume-offset-" + audioOffsetSeconds + ".mp4"),
        MuxedFixtures.Pick.video(video, 0), MuxedFixtures.Pick.audio(audio, 0).offsetSeconds(audioOffsetSeconds));
    TypedProperties p = props(AudioConfigKeys.SEGMENT_MAX_MS.key(), 1500);
    List<Seen> full = segments(clip, 0, 0, p);
    int checkpoint = 3;
    assertTrue(full.size() > checkpoint);
    long end = full.get(checkpoint - 1).endMs;
    assertTrue(end > audioOffsetSeconds * 1000L + 1000L);
    assertEquals(full.subList(checkpoint, full.size()), segments(clip, checkpoint, end, p));
  }

  @Test
  void readableAacIsIndependentOfVideoDecoderAvailability() throws Exception {
    Path ordinary = MuxedFixtures.encode(dir.resolve("aac-with-ordinary-video.mp4"), 2, 2, avcodec.AV_CODEC_ID_AAC, 16000, 32000);
    Path videoOnly = MuxedFixtures.remux(dir.resolve("ordinary-video-only.mp4"), MuxedFixtures.Pick.video(ordinary, 0));
    try (FfmpegAudioSegmentSource.Segments it = new FfmpegAudioSegmentSource()
        .segments(input(videoOnly), 0, 0, new TypedProperties())) {
      assertFalse(it.hasNext());
      assertEquals(FfmpegAudioSegmentSource.REASON_NO_AUDIO_STREAM, it.endReason());
    }
    // Only the synthetic video's sample description changes. This source reads AAC packets.
    byte[] bytes = Files.readAllBytes(ordinary);
    ByteBuffer boxes = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    String tags = new String(bytes, StandardCharsets.ISO_8859_1);
    int stsd = tags.indexOf("stsd");
    assertTrue(stsd >= 0);
    assertEquals("mp4v", tags.substring(stsd + 16, stsd + 20));
    int entryEnd = stsd + 12 + boxes.getInt(stsd + 12);
    int esds = tags.indexOf("esds", stsd + 20);
    assertTrue(esds >= stsd + 20 && esds + 4 <= entryEnd);
    System.arraycopy("evc1".getBytes(StandardCharsets.US_ASCII), 0, bytes, stsd + 16, 4);
    System.arraycopy("free".getBytes(StandardCharsets.US_ASCII), 0, bytes, esds, 4);
    Path withEvc = Files.write(dir.resolve("aac-with-evc-description.mp4"), bytes);

    List<Seen> expected = segments(ordinary);
    assertFalse(expected.isEmpty());
    assertEquals(expected, segments(withEvc));
  }

  /** What a test keeps of a segment: its span, payload size and payload digest. */
  private static final class Seen {
    private final long index;
    private final long startMs;
    private final long endMs;
    private final long bytes;
    private final String sha;

    private Seen(AudioSegment segment) throws IOException {
      this.index = segment.index();
      this.startMs = segment.startMs();
      this.endMs = segment.endMs();
      this.bytes = segment.payloadBytes();
      try (InputStream in = segment.openPayload()) {
        byte[] payload = readAll(in);
        assertEquals(bytes, payload.length, "payload length of " + segment);
        this.sha = sha256(payload);
      }
      assertEquals(16000, segment.sampleRate());
      assertEquals(1, segment.channels());
      assertEquals("pcm_s16le", segment.encoding());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Seen)) {
        return false;
      }
      Seen other = (Seen) o;
      return index == other.index && startMs == other.startMs && endMs == other.endMs && bytes == other.bytes && sha.equals(other.sha);
    }

    @Override
    public int hashCode() {
      return Objects.hash(index, startMs, endMs, bytes, sha);
    }

    @Override
    public String toString() {
      return index + ":[" + startMs + "," + endMs + ")" + bytes + "B:" + sha.substring(0, 8);
    }
  }

  private static List<Seen> segments(Path clip, long startSegment, long startMs, TypedProperties props) throws IOException {
    List<Seen> seen = new ArrayList<>();
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(clip), startSegment, startMs,
        props)) {
      while (iterator.hasNext()) {
        seen.add(new Seen(iterator.next()));
      }
    }
    return seen;
  }

  private static void drain(FfmpegAudioSegmentSource source, AudioInput input, boolean failIfDrained) throws IOException {
    try (FfmpegAudioSegmentSource.Segments iterator = source.segments(input, 0, 0, new TypedProperties())) {
      while (iterator.hasNext()) {
        iterator.next();
      }
      if (failIfDrained) {
        fail("ended as: " + iterator.endReason());
      }
    }
  }

  private static List<Seen> segments(Path clip) throws IOException {
    return segments(clip, 0, 0, new TypedProperties());
  }

  private static AudioInput input(Path clip) {
    return AudioClips.input("fid-" + clip.getFileName(), clip, VideoClips.supplier(clip));
  }

  private static TypedProperties props(String key, Object value) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, String.valueOf(value));
    return props;
  }

  @ParameterizedTest
  @ValueSource(strings = {"aac", "mp3"})
  void segmentsFollowTheToneLayout(String codec) throws IOException {
    List<Seen> seen = segments(codec.equals("aac") ? aac : mp3);
    assertEquals(LAYOUT_SEGMENTS.length, seen.size(), "segments " + seen);
    for (int k = 0; k < seen.size(); k++) {
      Seen segment = seen.get(k);
      assertEquals(k, segment.index);
      assertTrue(Math.abs(segment.startMs - LAYOUT_SEGMENTS[k][0]) <= TOLERANCE_MS, "start of " + segment);
      assertTrue(Math.abs(segment.endMs - LAYOUT_SEGMENTS[k][1]) <= TOLERANCE_MS, "end of " + segment);
      assertEquals(0, segment.startMs % FfmpegAudioConfig.WINDOW_MS, "on the window grid");
      assertEquals((segment.endMs - segment.startMs) * 16 * 2, segment.bytes, "16 kHz mono 16-bit");
    }
  }

  @Test
  void speechClipIsCutAtItsPauses() throws IOException {
    // the clip's pauses of at least 0.5 s by FFmpeg's own detector (silencedetect, -30 dB peak, 0.5 s, on this file): 0.08-0.59,
    // 3.08-3.70, 5.53-6.37, 9.46-10.04; the speech ends at 11.58 s (-35 dB on the source recording)
    List<Seen> seen = segments(speech);
    assertEquals(4, seen.size(), "segments " + seen);
    long[][] expected = {{590, 3084}, {3696, 5530}, {6372, 9459}, {10037, 11580}};
    for (int k = 0; k < seen.size(); k++) {
      assertTrue(Math.abs(seen.get(k).startMs - expected[k][0]) <= 150, "start of " + seen.get(k));
      assertTrue(Math.abs(seen.get(k).endMs - expected[k][1]) <= 150, "end of " + seen.get(k));
    }
  }

  /**
   * A resume from any checkpoint yields the same segments, payloads included, for every codec: MP3 resumes by seeking, whose
   * decoder gives the same samples after a seek; AAC resumes by decoding from the start and skipping, because FFmpeg's AAC
   * decoder does not give the same samples after a seek as from the start (a difference of up to 1e-4 on the generated clip and
   * 0.04 on the speech clip, which can also move a segment boundary near the threshold).
   */
  @ParameterizedTest
  @ValueSource(strings = {"aac", "mp3", "speech"})
  void resumeFromEveryCheckpointYieldsTheSameSegments(String clipName) throws IOException {
    Path clip = clipName.equals("aac") ? aac : clipName.equals("mp3") ? mp3 : speech;
    TypedProperties props = props(AudioConfigKeys.SEGMENT_MAX_MS.key(), 1500);
    List<Seen> full = segments(clip, 0, 0, props);
    assertTrue(full.size() >= 6, "enough checkpoints: " + full);
    int fromTheStart = 0;
    int pastThePreRoll = 0;
    for (int k = 1; k <= full.size(); k++) {
      List<Seen> rest = full.subList(k, full.size());
      long checkpointMs = full.get(k - 1).endMs;
      try (LogCapture logs = LogCapture.of(FfmpegAudioSegmentSource.class)) {
        assertEquals(rest, segments(clip, k, checkpointMs, props), "resume at the checkpoint after segment " + (k - 1));
        if (checkpointMs > FfmpegAudioSegmentSource.PRE_ROLL_MS) {
          pastThePreRoll++;
          fromTheStart += logs.lines().stream().anyMatch(line -> line.contains("by decoding from the start")) ? 1 : 0;
        }
      }
      assertEquals(rest, segments(clip, k, 0, props), "skip resume after segment " + (k - 1));
    }
    // the path taken: MP3 seeks past the pre-roll, AAC always decodes from the start
    assertTrue(pastThePreRoll > 0, "some checkpoints are past the pre-roll: " + full);
    assertEquals(clipName.equals("mp3") ? 0 : pastThePreRoll, fromTheStart, "resumes that decoded from the start");
  }

  /**
   * A storage failure FFmpeg swallows while it probes leaves the grabber started with incomplete parameters; here a grabber that
   * reads (a read that fails in storage, which the stream remembers and JavaCV reports to FFmpeg as an end of file) and then
   * reports no audio stream, as FFmpeg would. The result is a retryable storage failure, not an empty iterator with "no audio stream".
   */
  @Test
  void storageFailureSwallowedByTheProbeIsRetryableNotNoAudio() {
    FfmpegAudioSegmentSource probeSwallowsAFailure = new FfmpegAudioSegmentSource() {
      @Override
      FFmpegFrameGrabber newGrabber(InputStream stream) {
        return new FFmpegFrameGrabber(stream) {
          @Override
          public void start() {
            // JavaCV's read callback reports it to FFmpeg as an end of file; the stream remembers it
            assertThrows(IOException.class, () -> stream.read(new byte[64]), "the read budget is 0: the first read fails in storage");
          }

          @Override
          public int getAudioStream() {
            return -1;
          }
        };
      }
    };
    AudioInput failing = AudioClips.input("f", aac, VideoClips.failingAfterBytesRead(aac, 0));
    UncheckedIOException failure = assertThrows(UncheckedIOException.class, () -> drain(probeSwallowsAFailure, failing, true));
    assertTrue(BlobReadException.isStorageFailure(failure), "RETRYABLE: " + failure);
  }

  /**
   * A storage failure FFmpeg swallows while it probes the file (the stream reports it as an end of file, and FFmpeg can still
   * open the file with incomplete audio parameters) is a retryable storage failure, never an empty result: at read budgets every
   * {@value #PROBE_BUDGET_STEP} bytes up to the first {@value #PROBE_BUDGET_LIMIT} bytes of an MPEG-TS clip (whose audio
   * parameters come from its packets) and of an MP4 clip.
   */
  @ParameterizedTest
  @ValueSource(strings = {"mpegts", "mp4"})
  void storageFailureSwallowedWhileProbingIsRetryable(String format) throws IOException {
    Path clip = format.equals("mp4") ? aac
        : AudioClips.generate(dir.resolve("probe-aac.ts"), "mpegts", AudioClips.Codec.AAC, 44100, 2, LAYOUT);
    long size = Files.size(clip);
    for (long budget = 0; budget < Math.min(size, PROBE_BUDGET_LIMIT); budget += PROBE_BUDGET_STEP) {
      AudioInput failing = AudioClips.input("f", clip, VideoClips.failingAfterBytesRead(clip, budget));
      String endReason = "segments";
      RuntimeException failure = null;
      try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(failing, 0, 0,
          new TypedProperties())) {
        while (iterator.hasNext()) {
          iterator.next();
        }
        endReason = iterator.endReason();
      } catch (UncheckedIOException | VideoDecodeException e) {
        failure = e;
      }
      assertNotNull(failure, "budget " + budget + " of " + size + " ended without a failure, as: " + endReason);
      assertTrue(BlobReadException.isStorageFailure(failure), "budget " + budget + " of " + size + ": RETRYABLE: " + failure);
    }
  }

  /**
   * Matroska stamps audio in whole milliseconds, so a frame's timestamp is not its exact sample position: such a video is resumed
   * by decoding from the start and skipping, and the resumed segments are then identical, payloads included.
   */
  @Test
  void resumeInAContainerWithRoundedTimestampsYieldsTheSameSegments() throws IOException {
    Path mkv = AudioClips.generate(dir.resolve("aac.mkv"), "matroska", AudioClips.Codec.AAC, 44100, 2, LAYOUT);
    TypedProperties props = props(AudioConfigKeys.SEGMENT_MAX_MS.key(), 1500);
    List<Seen> full = segments(mkv, 0, 0, props);
    assertTrue(full.size() >= 6, "enough checkpoints: " + full);
    for (int k = 1; k <= full.size(); k++) {
      assertEquals(full.subList(k, full.size()), segments(mkv, k, full.get(k - 1).endMs, props), "resume after segment " + (k - 1));
    }
  }

  /** Two recordings joined byte for byte: the second one's timestamps start again, which is a data error, never a short result. */
  @Test
  void timestampsThatJumpBackArePermanent() throws IOException {
    Path first = AudioClips.generate(dir.resolve("part1.ts"), "mpegts", AudioClips.Codec.AAC, 44100, 1, 4000);
    Path second = AudioClips.generate(dir.resolve("part2.ts"), "mpegts", AudioClips.Codec.AAC, 44100, 1, 4000);
    byte[] a = Files.readAllBytes(first);
    byte[] b = Files.readAllBytes(second);
    byte[] joined = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, joined, a.length, b.length);
    Path concatenated = Files.write(dir.resolve("joined.ts"), joined);
    VideoDecodeException failure = assertThrows(VideoDecodeException.class, () -> segments(concatenated));
    assertTrue(failure.getMessage().contains("jump back"), failure.getMessage());
  }

  @Test
  void resumeAfterTheLastSegmentEndsWithoutAReason() throws IOException {
    List<Seen> full = segments(aac);
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(aac), full.size(),
        full.get(full.size() - 1).endMs, new TypedProperties())) {
      assertFalse(iterator.hasNext());
      assertNull(iterator.endReason(), "a resumed video that is done is DONE, not NO_SPEECH");
    }
  }

  @Test
  void longSpeechIsCutAtTheTimeBound() throws IOException {
    Path clip = AudioClips.generate(dir.resolve("long-tone.mp4"), AudioClips.Codec.AAC, 44100, 1, 70_000);
    List<Seen> seen = segments(clip);
    assertEquals(3, seen.size(), "70 s at 30 s each: " + seen);
    for (int k = 0; k < seen.size(); k++) {
      assertTrue(seen.get(k).endMs - seen.get(k).startMs <= 30_000, "time bound: " + seen.get(k));
      assertTrue(seen.get(k).bytes <= AudioConfigKeys.DEFAULT_SEGMENT_MAX_BYTES, "byte bound: " + seen.get(k));
      if (k > 0) {
        assertEquals(seen.get(k - 1).endMs, seen.get(k).startMs, "a cut loses nothing");
      }
    }
  }

  @Test
  void byteBoundBindsBeforeTheTimeBound() throws IOException {
    // 96,000 bytes = 3 s of 16 kHz mono 16-bit
    List<Seen> seen = segments(aac, 0, 0, props(AudioConfigKeys.SEGMENT_MAX_BYTES.key(), 96_000));
    assertTrue(seen.size() >= 5, "segments " + seen);
    for (Seen segment : seen) {
      assertTrue(segment.bytes <= 96_000 && segment.endMs - segment.startMs <= 3000, "bound: " + segment);
    }
  }

  @Test
  void otherSampleRateIsHonoured() throws IOException {
    TypedProperties props = props(AudioConfigKeys.SAMPLE_RATE.key(), 8000);
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(aac), 0, 0, props)) {
      AudioSegment first = iterator.next();
      assertEquals(8000, first.sampleRate());
      assertEquals((first.endMs() - first.startMs()) * 8 * 2, first.payloadBytes());
    }
  }

  @Test
  void videoWithoutAudioStreamHasNoSpeech() throws IOException {
    Path clip = VideoClips.generate(dir.resolve("video-only.mp4"), 4, 5, 1, 64, 48, -1, -1);
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(clip), 0, 0,
        new TypedProperties())) {
      assertFalse(iterator.hasNext());
      assertEquals(FfmpegAudioSegmentSource.REASON_NO_AUDIO_STREAM, iterator.endReason());
    }
  }

  @Test
  void silentAudioTrackHasNoSpeech() throws IOException {
    Path clip = AudioClips.generate(dir.resolve("silent.mp4"), AudioClips.Codec.AAC, 44100, 2, 0, 5000);
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(clip), 0, 0,
        new TypedProperties())) {
      assertFalse(iterator.hasNext());
      assertEquals(FfmpegAudioSegmentSource.REASON_NO_SPEECH, iterator.endReason());
    }
  }

  @Test
  void truncatedContainerIsPermanent() throws IOException {
    byte[] bytes = Files.readAllBytes(aac);
    Path truncated = Files.write(dir.resolve("truncated.mp4"), Arrays.copyOf(bytes, bytes.length * 6 / 10));
    RuntimeException failure = assertThrows(RuntimeException.class, () -> segments(truncated));
    assertTrue(failure instanceof VideoDecodeException, "PERMANENT: " + failure);
    assertFalse(BlobReadException.isStorageFailure(failure));
  }

  @Test
  void unreadableBytesArePermanent() throws IOException {
    byte[] garbage = new byte[64 * 1024];
    new Random(7).nextBytes(garbage);
    Path file = Files.write(dir.resolve("garbage.mp4"), garbage);
    assertThrows(VideoDecodeException.class, () -> segments(file));
  }

  /**
   * The default source never throws an {@link IllegalArgumentException} on corrupt content (prep reads
   * it as a configuration error and fails the task on every run): garbage, a container cut anywhere, a header followed by zeros,
   * and a stream with corrupted bytes in its middle either decode or fail as the video's {@link VideoDecodeException}.
   */
  @ParameterizedTest
  @ValueSource(strings = {"garbage", "cut-10", "cut-35", "cut-80", "cut-99", "header-then-zeros", "flipped-middle", "flipped-header"})
  void corruptContentIsNeverAnIllegalArgumentException(String kind) throws IOException {
    byte[] bytes = Files.readAllBytes(aac);
    byte[] corrupt;
    Random random = new Random(kind.hashCode());
    if (kind.equals("garbage")) {
      corrupt = new byte[bytes.length];
      random.nextBytes(corrupt);
    } else if (kind.startsWith("cut-")) {
      corrupt = Arrays.copyOf(bytes, (int) ((long) bytes.length * Integer.parseInt(kind.substring(4)) / 100));
    } else if (kind.equals("header-then-zeros")) {
      corrupt = new byte[bytes.length];
      System.arraycopy(bytes, 0, corrupt, 0, 64);
    } else {
      corrupt = bytes.clone();
      int from = kind.equals("flipped-header") ? 8 : bytes.length / 2;
      for (int i = from; i < Math.min(bytes.length, from + 4096); i++) {
        corrupt[i] = (byte) random.nextInt();
      }
    }
    Path file = Files.write(dir.resolve("corrupt-" + kind + ".mp4"), corrupt);
    try {
      segments(file);
    } catch (RuntimeException failure) {
      assertFalse(failure instanceof IllegalArgumentException, kind + ": " + failure);
      assertTrue(failure instanceof VideoDecodeException, kind + ": the video's content failure, not " + failure);
    }
  }

  @Test
  void bytesThatAreNotSeekableOrAbsentArePermanent() {
    BlobBytesSupplier plain = () -> new ByteArrayInputStream(new byte[0]);
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    AudioInput notSeekable = AudioClips.input("f", aac, plain);
    AudioInput absent = AudioClips.input("f", aac, null);
    TypedProperties props = new TypedProperties();
    assertThrows(VideoDecodeException.class, () -> source.segments(notSeekable, 0, 0, props));
    assertThrows(VideoDecodeException.class, () -> source.segments(absent, 0, 0, props));
  }

  @ParameterizedTest
  @ValueSource(longs = {2_000, 30_000})
  void storageFailureIsRetryableWhereverItHappens(long budget) {
    AudioInput failing = AudioClips.input("f", aac, VideoClips.failingAfterBytesRead(aac, budget));
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    UncheckedIOException failure = assertThrows(UncheckedIOException.class, () -> drain(source, failing, false));
    assertTrue(BlobReadException.isStorageFailure(failure), "RETRYABLE: " + failure);
  }

  /**
   * An {@link Error} while the source opens (here thrown from the blob under the grabber's start, as a failing native link would
   * be) is rethrown as it is, and the blob stream the grabber was given is closed: opening leaves nothing open on any way out.
   */
  @Test
  void errorWhileOpeningClosesTheBlobStreamAndIsRethrown() {
    LinkageError linkFailure = new LinkageError("test: a native library failed to link while the grabber started");
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        opened.incrementAndGet();
        RandomAccessFile file = new RandomAccessFile(aac.toFile(), "r");
        return new SeekableBlob() {
          @Override
          public long length() {
            try {
              return file.length();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          @Override
          public long getPos() {
            // the grabber's start marks the stream (InputStream.mark, outside its read callbacks): the Error escapes start()
            throw linkFailure;
          }

          @Override
          public void seek(long pos) throws IOException {
            file.seek(pos);
          }

          @Override
          public int read(byte[] buffer, int offset, int len) throws IOException {
            return file.read(buffer, offset, len);
          }

          @Override
          public void close() throws IOException {
            closed.incrementAndGet();
            file.close();
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(aac);
      }
    };
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    AudioInput input = AudioClips.input("f", aac, bytes);
    TypedProperties props = new TypedProperties();
    Error thrown = assertThrows(Error.class, () -> source.segments(input, 0, 0, props));
    assertSame(linkFailure, thrown);
    assertEquals(1, opened.get(), "one blob stream opened");
    assertEquals(1, closed.get(), "the blob stream is closed once");
  }

  /**
   * A track whose header declares a sample rate above the ceiling (384 kHz) is the video's bad input before any sample is
   * resampled: the resampler's table and its work per output sample grow with the declared input rate.
   */
  @Test
  void declaredSampleRateAboveTheCeilingIsPermanentBeforeDecoding() throws IOException {
    Path oversized = wav(dir.resolve("rate-1000000.wav"), 1_000_000, 300);
    VideoDecodeException failure = assertThrows(VideoDecodeException.class, () -> segments(oversized));
    assertTrue(failure.getMessage().contains("1000000 Hz"), failure.getMessage());
    assertFalse(BlobReadException.isStorageFailure(failure));
  }

  /** Control: a track declaring the ceiling itself decodes and is segmented. */
  @Test
  void declaredSampleRateAtTheCeilingStillDecodes() throws IOException {
    List<Seen> seen = segments(wav(dir.resolve("rate-384000.wav"), 384_000, 1500));
    assertFalse(seen.isEmpty(), "the tone is speech");
    assertTrue(seen.get(seen.size() - 1).endMs >= 1000, "the tone: " + seen);
  }

  /** A blob stream that fails to close changes no outcome but leaves a WARN naming the video. */
  @Test
  void closeFailureIsLoggedWithTheVideoName() throws IOException {
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new VideoClips.FileBlob(aac, Long.MAX_VALUE) {
          @Override
          public void close() throws IOException {
            super.close();
            throw new IOException("test: the blob stream failed to close");
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(aac);
      }
    };
    List<String> warnings;
    try (LogCapture logs = LogCapture.of(FfmpegAudioSegmentSource.class)) {
      try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(
          AudioClips.input("fid-close", aac, bytes), 0, 0, new TypedProperties())) {
        assertTrue(iterator.hasNext(), "the segments are out");
        while (iterator.hasNext()) {
          iterator.next();
        }
      }
      warnings = logs.warnings();
    }
    assertEquals(1, warnings.size(), "the one blob stream: " + warnings);
    assertTrue(warnings.get(0).contains("fid-close"), warnings.get(0));
    assertTrue(warnings.get(0).contains("failed to close"), warnings.get(0));
  }

  /**
   * The grabber's close fails on the blob stream's first close, which leaves the blob open (as a pooled connection whose release
   * throws would): the blob stream is still closed afterwards, and the failure is logged.
   */
  @Test
  void blobStreamIsClosedWhenTheGrabbersCloseFails() throws IOException {
    AtomicInteger closeCalls = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new VideoClips.FileBlob(aac, Long.MAX_VALUE) {
          @Override
          public void close() throws IOException {
            if (closeCalls.incrementAndGet() == 1) {
              throw new IOException("test: the blob stream failed to close");
            }
            super.close();
            closed.incrementAndGet();
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(aac);
      }
    };
    List<String> warnings;
    try (LogCapture logs = LogCapture.of(FfmpegAudioSegmentSource.class)) {
      try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(
          AudioClips.input("fid-close-retry", aac, bytes), 0, 0, new TypedProperties())) {
        while (iterator.hasNext()) {
          iterator.next();
        }
      }
      warnings = logs.warnings();
    }
    assertEquals(1, closed.get(), "the blob stream is closed after the grabber's close failed");
    assertEquals(1, warnings.size(), warnings.toString());
    assertTrue(warnings.get(0).contains("fid-close-retry") && warnings.get(0).contains("failed to close"), warnings.get(0));
  }

  @Test
  void trackLongerThanTheMaximumIsPermanentBeforeDecoding() {
    TypedProperties props = props(AudioConfigKeys.MAX_DURATION_MS.key(), 5000);
    VideoDecodeException failure = assertThrows(VideoDecodeException.class, () -> segments(aac, 0, 0, props));
    assertTrue(failure.getMessage().contains(AudioConfigKeys.MAX_DURATION_MS.key()), failure.getMessage());
  }

  @Test
  void payloadIsReleasedWhenTheNextSegmentIsRequested() throws IOException {
    try (FfmpegAudioSegmentSource.Segments iterator = new FfmpegAudioSegmentSource().segments(input(aac), 0, 0,
        new TypedProperties())) {
      AudioSegment first = iterator.next();
      byte[] once = readAll(first.openPayload());
      assertEquals(sha256(once), sha256(readAll(first.openPayload())), "re-readable while held");
      InputStream open = first.openPayload();
      assertTrue(iterator.hasNext());
      assertThrows(IllegalStateException.class, first::openPayload);
      assertThrows(IOException.class, open::read, "a stream opened before the next segment was requested fails too");
    }
  }

  @Test
  void invalidSettingsNameTheirKey() {
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    for (String[] bad : new String[][] {
        {AudioConfigKeys.SAMPLE_RATE.key(), "22050"}, {AudioConfigKeys.SAMPLE_RATE.key(), "4000"}, {AudioConfigKeys.VAD_THRESHOLD_DB.key(), "0"},
        {AudioConfigKeys.VAD_MIN_SILENCE_MS.key(), "10"}, {AudioConfigKeys.SEGMENT_MAX_MS.key(), "29"}, {AudioConfigKeys.SEGMENT_MAX_BYTES.key(), "100"},
        {AudioConfigKeys.MAX_DURATION_MS.key(), "0"}, {AudioConfigKeys.MAX_DURATION_MS.key(), String.valueOf(Long.MAX_VALUE)},
        {AudioConfigKeys.SEGMENT_MAX_MS.key(), "abc"}}) {
      TypedProperties props = props(bad[0], bad[1]);
      IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> source.validate(props), bad[0] + "=" + bad[1]);
      assertTrue(failure.getMessage().contains(bad[0]), failure.getMessage());
    }
    source.validate(new TypedProperties());
    assertEquals("ffmpeg-vad-energy-v1", source.logicVersion());
  }

  /**
   * A mono 16-bit PCM WAV built here: a canonical 44-byte header declaring {@code declaredRate}, then {@code ms} of a tone at that
   * rate.
   */
  private static Path wav(Path out, int declaredRate, int ms) throws IOException {
    int samples = (int) ((long) declaredRate * ms / 1000);
    ByteBuffer wav = ByteBuffer.allocate(44 + 2 * samples).order(ByteOrder.LITTLE_ENDIAN);
    wav.put("RIFF".getBytes(StandardCharsets.US_ASCII)).putInt(36 + 2 * samples).put("WAVE".getBytes(StandardCharsets.US_ASCII));
    // fmt: PCM (1), 1 channel, the declared rate, its byte rate, 2-byte blocks, 16 bits per sample
    wav.put("fmt ".getBytes(StandardCharsets.US_ASCII)).putInt(16).putShort((short) 1).putShort((short) 1).putInt(declaredRate)
        .putInt(2 * declaredRate).putShort((short) 2).putShort((short) 16);
    wav.put("data".getBytes(StandardCharsets.US_ASCII)).putInt(2 * samples);
    for (int i = 0; i < samples; i++) {
      wav.putShort((short) Math.round(0.25 * 32767 * Math.sin(2 * Math.PI * 440 * i / declaredRate)));
    }
    return Files.write(out, wav.array());
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int n;
    while ((n = in.read(buffer)) > 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static String sha256(byte[] bytes) {
    try {
      StringBuilder hex = new StringBuilder();
      for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
