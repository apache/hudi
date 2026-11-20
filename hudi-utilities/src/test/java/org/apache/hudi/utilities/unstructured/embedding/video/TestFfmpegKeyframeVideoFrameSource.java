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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.LogCapture;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobInputStream;

import org.bytedeco.ffmpeg.avformat.AVStream;
import org.bytedeco.ffmpeg.avutil.AVRational;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FFmpeg closest-keyframe-every-X frame extraction strategy over generated clips: which frames it keeps (the dedup golden),
 * their dense indexes and timestamps, and that each kept frame is a JPEG at quality 90 at the frame's NATIVE size.
 */
class TestFfmpegKeyframeVideoFrameSource {

  /** The 32 s clip: keyframes at 0, 2, ..., 30 s; seconds 10-19 are one still picture (keyframes 10..18 s repeat). */
  private static final List<Long> KEPT_TIMESTAMPS = Arrays.asList(0L, 2000L, 4000L, 6000L, 8000L, 10000L, 20000L, 22000L,
      24000L, 26000L, 28000L, 30000L);

  @TempDir
  static Path dir;
  private static Path clip;
  private static Path wide;

  @BeforeAll
  static void generate() throws IOException {
    clip = VideoClips.generate(dir.resolve("clip.mp4"), 32, 10, 2, 320, 240, 10, 20);
    wide = VideoClips.generate(dir.resolve("wide.mp4"), 4, 10, 2, 1280, 320, -1, -1);
  }

  @Test
  void closestKeyframeEverySecondWithTheStillSegmentDeduplicated() throws IOException {
    List<ExtractedFrame> frames = frames(clip, new TypedProperties());
    List<Long> timestamps = new ArrayList<>();
    List<String> keys = new ArrayList<>();
    for (int k = 0; k < frames.size(); k++) {
      ExtractedFrame frame = frames.get(k);
      assertEquals(k, frame.getFrameIndex(), "dense indexes from 0");
      assertTrue(frame.isKeyframe());
      timestamps.add(frame.getTimeMs());
      keys.add(frame.getDedupKey());
      assertEquals(16, frame.getDedupKey().length(), "a 64-bit dhash in hex: " + frame.getDedupKey());
      assertEquals("image/jpeg", frame.getMimeType());
      byte[] jpeg = bytes(frame);
      assertEquals(frame.getSizeBytes(), jpeg.length);
      BufferedImage stored = ImageFixtures.decode(jpeg);
      assertEquals("320x240", stored.getWidth() + "x" + stored.getHeight(), "native size, nothing resized");
      assertEquals(320, frame.getWidth());
      assertEquals(240, frame.getHeight());
    }
    // 16 keyframes, the 4 repeats of the still picture (12, 14, 16, 18 s) dropped
    assertEquals(KEPT_TIMESTAMPS, timestamps);
    assertEquals(timestamps.size(), new HashSet<>(keys).size(), "distinct dedup keys: " + keys);
  }

  /** the stored frame is a baseline JPEG at quality 90, its quantization tables those of quality 90. */
  @Test
  void framesAreJpegAtQuality90() throws IOException {
    ExtractedFrame first = frames(clip, new TypedProperties()).get(0);
    byte[] jpeg = bytes(first);
    assertEquals(0xFF, jpeg[0] & 0xFF);
    assertEquals(0xD8, jpeg[1] & 0xFF);
    assertEquals(90, FfmpegKeyframeVideoFrameSource.JPEG_QUALITY);
    // the encoder's luma table at quality 90 starts with 3 (the standard table's 16 scaled by (200 - 2 * 90) / 100, rounded)
    assertEquals(3, firstLumaQuantizer(jpeg));
  }

  /**
   * a frame is encoded in memory, never through ImageIO's file cache. With the cache on and its directory gone (a full or
   * unwritable temp directory), the frames still come out: an encode failure there would have made a readable video a FAILED row.
   */
  @Test
  void framesAreEncodedWithoutImageIoFileCache() throws IOException {
    boolean useCache = ImageIO.getUseCache();
    java.io.File cacheDirectory = ImageIO.getCacheDirectory();
    Path gone = Files.createDirectory(dir.resolve("imageio-cache-gone"));
    try {
      ImageIO.setUseCache(true);
      ImageIO.setCacheDirectory(gone.toFile());
      Files.delete(gone);
      assertEquals(KEPT_TIMESTAMPS.size(), frames(clip, new TypedProperties()).size());
    } finally {
      ImageIO.setCacheDirectory(cacheDirectory);
      ImageIO.setUseCache(useCache);
    }
  }

  @Test
  void theSameVideoAlwaysYieldsTheSameFrames() throws IOException {
    List<ExtractedFrame> first = frames(clip, new TypedProperties());
    List<ExtractedFrame> second = frames(clip, new TypedProperties());
    assertEquals(first.size(), second.size());
    for (int k = 0; k < first.size(); k++) {
      assertEquals(first.get(k).getTimeMs(), second.get(k).getTimeMs());
      assertEquals(first.get(k).getDedupKey(), second.get(k).getDedupKey());
      assertArrayEquals(bytes(first.get(k)), bytes(second.get(k)), "frame " + k);
    }
  }

  @Test
  void dedupNoneKeepsEveryChosenKeyframeAndSha256DropsExactRepeats() throws IOException {
    List<ExtractedFrame> none = frames(clip, props(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none"));
    assertEquals(16, none.size());
    for (ExtractedFrame frame : none) {
      assertEquals(64, frame.getDedupKey().length(), "sha-256 hex of the encoded frame");
    }
    List<ExtractedFrame> exact = frames(clip, props(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "sha256"));
    assertEquals(KEPT_TIMESTAMPS.size(), exact.size(), "the still picture's keyframes encode to identical bytes");
    // a threshold of 0 still drops the identical still frames, the moving ones differ by more
    assertEquals(KEPT_TIMESTAMPS.size(), frames(clip, props(EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "0")).size());
  }

  @Test
  void theIntervalChoosesTheClosestKeyframes() throws IOException {
    // every 5 s over keyframes every 2 s: targets 0, 5 (tie 4/6 → 4), 10, 15 (tie 14/16 → 14, a repeat of 10), 20, 25 (→ 24),
    // 30
    TypedProperties props = props(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "5000");
    List<Long> timestamps = new ArrayList<>();
    for (ExtractedFrame frame : frames(clip, props)) {
      timestamps.add(frame.getTimeMs());
    }
    assertEquals(Arrays.asList(0L, 4000L, 10000L, 20000L, 24000L, 30000L), timestamps);
  }

  @Test
  void wideFrameIsStoredAtItsNativeSize() throws IOException {
    List<ExtractedFrame> frames = frames(wide, new TypedProperties());
    assertEquals(2, frames.size());
    BufferedImage stored = ImageFixtures.decode(bytes(frames.get(0)));
    assertEquals("1280x320", stored.getWidth() + "x" + stored.getHeight(), "no resize in prep: compute resizes in memory");
  }

  @Test
  void videoAboveThePixelGuardIsRefusedBeforeAnyFrame() {
    TypedProperties props = props(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), "76799");
    UncheckedIOException refused = assertThrows(UncheckedIOException.class, () -> frames(clip, props));
    assertTrue(refused.getCause() instanceof ImageRefusedException, String.valueOf(refused.getCause()));
    assertTrue(refused.getCause().getMessage().contains("320x240 = 76800 pixels"), refused.getCause().getMessage());
  }

  @Test
  void corruptOrTruncatedBytesAreDataErrorsNotStorageFailures() throws IOException {
    byte[] noise = new byte[64 * 1024];
    new Random(7).nextBytes(noise);
    Path garbage = Files.write(dir.resolve("noise.mp4"), noise);
    TypedProperties props = new TypedProperties();
    RuntimeException corrupt = assertThrows(RuntimeException.class, () -> frames(garbage, props));
    assertTrue(corrupt instanceof VideoDecodeException, corrupt.toString());
    byte[] whole = Files.readAllBytes(clip);
    // the mp4 index (moov) is at the end: the first half alone has no index
    Path half = Files.write(dir.resolve("half.mp4"), Arrays.copyOf(whole, whole.length / 2));
    RuntimeException truncated = assertThrows(RuntimeException.class, () -> frames(half, props));
    assertTrue(truncated instanceof VideoDecodeException, truncated.toString());
    assertFalse(BlobReadException.isStorageFailure(truncated));
  }

  /** a failure reason names the video by its fid, never an identity hash. */
  @Test
  void failureReasonsNameTheVideoNotItsIdentityHash() throws IOException {
    byte[] noise = new byte[64 * 1024];
    new Random(7).nextBytes(noise);
    Path garbage = Files.write(dir.resolve("noise.mp4"), noise);
    FileMeta file = new FileMeta("fid-noise", "mp4", noise.length);
    SeekableBlobBytesSupplier seekable = VideoClips.supplier(garbage);
    TypedProperties props = new TypedProperties();
    VideoDecodeException opened = assertThrows(VideoDecodeException.class, () -> drain(seekable, file, props));
    assertEquals("video source_path_sha fid-noise (mp4, 65536 bytes) cannot be opened", opened.getMessage());
    BlobBytesSupplier plain = () -> new ByteArrayInputStream(noise);
    FileMeta plainFile = new FileMeta("fid-plain", "mkv", 3L);
    VideoDecodeException notSeekable = assertThrows(VideoDecodeException.class, () -> drain(plain, plainFile, props));
    assertEquals("video source_path_sha fid-plain (mkv, 3 bytes) bytes are not seekable", notSeekable.getMessage());
  }

  @Test
  void storageFailureUnderAReadIsReportedAsOne() throws IOException {
    SeekableBlobBytesSupplier failing = VideoClips.failingSupplier(clip, Files.size(clip) / 2);
    FileMeta file = meta(clip);
    TypedProperties props = new TypedProperties();
    UncheckedIOException failed = assertThrows(UncheckedIOException.class, () -> drain(failing, file, props));
    assertTrue(BlobReadException.isStorageFailure(failed), failed.toString());
  }

  /**
   * The framework's blob view throws an unchecked SourceReadException; JavaCV's read callback swallows it and FFmpeg sees an I/O
   * error. The failure that leaves the source must still fail the round, never read as undecodable video data.
   */
  @Test
  void uncheckedStorageFailureUnderAReadFailsTheRound() throws IOException {
    long failAt = Files.size(clip) / 2;
    SeekableBlobBytesSupplier throttled = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new VideoClips.FileBlob(clip, Long.MAX_VALUE) {
          @Override
          public int read(byte[] buffer, int offset, int len) throws IOException {
            if (getPos() + len > failAt) {
              throw new SourceReadException("Failed to read the blob source", new IOException("503 slow down"));
            }
            return super.read(buffer, offset, len);
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(clip);
      }
    };
    FileMeta file = meta(clip);
    TypedProperties props = new TypedProperties();
    RuntimeException failed = assertThrows(RuntimeException.class, () -> drain(throttled, file, props));
    assertTrue(PrepFailures.roundFailure(failed) instanceof SourceReadException, failed.toString());
    assertFalse(failed instanceof VideoDecodeException, failed.toString());
  }

  @Test
  void storageFailurePartWayThroughTheScanYieldsNoWrongFrame() throws IOException {
    // X = 2.5 s over keyframes every 2 s: target 7.5 s takes 8 s, but a scan cut short after 6 s would take 6 s. The scanner's view
    // drops after a growing share of the file: every frame that comes out before the storage failure is the true frame k
    TypedProperties props = props(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "2500");
    List<ExtractedFrame> truth = frames(clip, props);
    long size = Files.size(clip);
    int failures = 0;
    for (int percent = 5; percent <= 95; percent += 3) {
      List<ExtractedFrame> yielded = new ArrayList<>();
      try (ClosableIterator<ExtractedFrame> iterator = new FfmpegKeyframeVideoFrameSource().frames(
          VideoClips.failingAfterBytesRead(clip, size * percent / 100), meta(clip), props)) {
        while (iterator.hasNext()) {
          yielded.add(iterator.next());
        }
        assertEquals(truth.size(), yielded.size(), percent + "%: a run that ends normally yields every frame");
      } catch (UncheckedIOException e) {
        assertTrue(BlobReadException.isStorageFailure(e), percent + "%: " + e);
        failures++;
      }
      for (int k = 0; k < yielded.size(); k++) {
        assertEquals(truth.get(k).getTimeMs(), yielded.get(k).getTimeMs(), percent + "%: frame " + k);
        assertEquals(truth.get(k).getDedupKey(), yielded.get(k).getDedupKey(), percent + "%: frame " + k);
      }
    }
    assertTrue(failures > 0, "some cut points fail the video");
  }

  @Test
  void videoBytesMustBeSeekable() {
    BlobBytesSupplier plain = () -> new ByteArrayInputStream(new byte[0]);
    FileMeta file = new FileMeta("fid-1", "mp4", 0L);
    TypedProperties props = new TypedProperties();
    assertThrows(VideoDecodeException.class, () -> drain(plain, file, props));
  }

  @Test
  void configurationErrorsAreIllegalArgumentsNamingTheKey() {
    FfmpegKeyframeVideoFrameSource source = new FfmpegKeyframeVideoFrameSource();
    assertEquals("video-keyframe-v3", source.name());
    source.validateConfig(new TypedProperties());
    String[][] bad = {
        {EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "0"},
        {EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "1s"},
        {EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), String.valueOf(VideoFrameConfig.MAX_INTERVAL_MS + 1)},
        {EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "phash"},
        {EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "65"},
        {EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), "0"},
        {EmbeddingTableConfig.VIDEO_FFMPEG_THREADS.key(), "0"}};
    for (String[] entry : bad) {
      TypedProperties validated = props(entry[0], entry[1]);
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> source.validateConfig(validated));
      assertTrue(e.getMessage().contains(entry[0]), e.getMessage());
      TypedProperties extracted = props(entry[0], entry[1]);
      assertThrows(IllegalArgumentException.class, () -> frames(clip, extracted));
    }
  }

  @Test
  void moreDecoderThreadsGiveTheSameFrames() throws IOException {
    List<ExtractedFrame> one = frames(clip, new TypedProperties());
    List<ExtractedFrame> four = frames(clip, props(EmbeddingTableConfig.VIDEO_FFMPEG_THREADS.key(), "4"));
    assertEquals(one.size(), four.size());
    for (int k = 0; k < one.size(); k++) {
      assertEquals(one.get(k).getTimeMs(), four.get(k).getTimeMs());
      assertEquals(one.get(k).getDedupKey(), four.get(k).getDedupKey());
      assertArrayEquals(bytes(one.get(k)), bytes(four.get(k)), "frame " + k);
    }
  }

  /**
   * A frame's time is its keyframe's PTS (the demuxer applying the edit list)
   * minus the CONTAINER start time, clamped at 0 — the origin speech is timed from. The checked-in H.264 + AAC mp4 has B-frames
   * (decode order differs from presentation order) and a NON-ZERO edit list skipping the encoder delay (media time 2048 ticks of
   * 1/10240 s). An oracle reading the file's own boxes ({@code stts}, {@code ctts}, {@code stss}, {@code elst}) gives every
   * keyframe's presentation time on the edited timeline; the kept frames are among them and are the dedup golden (6 s repeats the
   * still picture of 4 s).
   */
  @Test
  void h264WithBFramesAndAnEncoderDelayEditListIsTimedFromTheContainerStart() throws IOException {
    Path h264 = resource("h264-aac-bframes-12s.mp4");
    Mp4VideoTrack track = Mp4VideoTrack.read(Files.readAllBytes(h264));
    assertEquals(10240, track.timescale);
    assertEquals(2048, track.editMediaTime, "a non-zero edit list");
    List<Long> presented = track.presentedKeyframeMillis();
    assertEquals(Arrays.asList(0L, 2000L, 4000L, 6000L, 8000L, 10000L), presented);
    List<Long> timestamps = timestamps(h264);
    assertTrue(presented.containsAll(timestamps), timestamps + " within " + presented);
    assertEquals(Arrays.asList(0L, 2000L, 4000L, 8000L, 10000L), timestamps, "6 s repeats the still picture of 4 s");
  }

  /**
   * A copy trimmed with {@code ffmpeg -ss 3 -i h264-aac-bframes-12s.mp4 -map 0 -c copy} (JavaCV 1.5.11's ffmpeg
   * 7.1 binary) starts its edit list inside a GOP (media time 12288 ticks of
   * 1/10240 s): the demuxer reports the earlier keyframe at -1 s, flagged DISCARD. That
   * keyframe is never chosen and never becomes the origin; the first kept frame is at its true offset, 1 s, and the video is not
   * failed by the seek check. The oracle (one edit) agrees: the keyframes presented at or after the edit start are at 1, 3, 5, 7 s.
   */
  @Test
  void trimmedEditListNeverChoosesTheDiscardedKeyframe() throws IOException {
    Path trimmed = resource("h264-aac-bframes-trimmed-ss3.mp4");
    Mp4VideoTrack track = Mp4VideoTrack.read(Files.readAllBytes(trimmed));
    assertEquals(12288, track.editMediaTime);
    assertEquals(Arrays.asList(1000L, 3000L, 5000L, 7000L), track.presentedKeyframeMillis());
    // 3 s (the original 6 s) repeats the still picture of 1 s (the original 4 s)
    assertEquals(Arrays.asList(1000L, 5000L, 7000L), timestamps(trimmed));
  }

  /**
   * A video track delayed 1.5 s by an empty edit ({@code ffmpeg -i h264-aac-bframes-12s.mp4 -itsoffset 1.5 -i
   * h264-aac-bframes-12s.mp4 -map 1:v -map 0:a -c copy}, the same binary; the container starts at 0) is timed at its true offset.
   */
  @Test
  void delayedVideoTrackIsTimedAtItsTrueOffset() throws IOException {
    assertEquals(Arrays.asList(1500L, 3500L, 5500L, 9500L, 11500L), timestamps(resource("h264-aac-bframes-video-delayed-1500ms.mp4")));
  }

  /**
   * A presentation time at a 1 ns time base (a Matroska TimestampScale of 1, an MP4 timescale of 1e9) converts without overflow
   * past 2.56 h: 10,000 s is 1e10 microseconds, as at a coarse time base.
   */
  @Test
  void fineTimeBaseConvertsLongPresentationTimesWithoutOverflow() {
    AVRational nanos = new AVRational().num(1).den(1_000_000_000);
    AVRational millis = new AVRational().num(1).den(1000);
    AVRational mpegTs = new AVRational().num(1).den(90_000);
    try {
      assertEquals(10_000_000_000L, FfmpegKeyframeVideoFrameSource.toMicros(10_000L * 1_000_000_000L, nanos));
      assertEquals(10_000_000_000L, FfmpegKeyframeVideoFrameSource.toMicros(10_000_000L, millis));
      assertEquals(2_000L, FfmpegKeyframeVideoFrameSource.toMicros(2_000_000L, nanos));
      // truncated, not rounded: 5 ticks of 1/90000 s are 55.6 microseconds, converted to 55
      assertEquals(55L, FfmpegKeyframeVideoFrameSource.toMicros(5L, mpegTs));
    } finally {
      nanos.close();
      millis.close();
      mpegTs.close();
    }
  }

  /** At a 1 ns time base JavaCV's {@code 1000000L * pts} overflows from pts {@code Long.MAX_VALUE / 1000000 + 1} on. */
  @Test
  void javacvTimestampOverflowsJustPastItsRange() {
    AVRational nanos = new AVRational().num(1).den(1_000_000_000);
    try {
      long last = Long.MAX_VALUE / 1_000_000L;
      assertEquals(9_223_372_036_854L, last);
      assertFalse(FfmpegKeyframeVideoFrameSource.javacvTimestampOverflows(last, nanos));
      assertTrue(FfmpegKeyframeVideoFrameSource.javacvTimestampOverflows(last + 1, nanos));
      assertFalse(FfmpegKeyframeVideoFrameSource.javacvTimestampOverflows(0, nanos));
    } finally {
      nanos.close();
    }
  }

  /**
   * JavaCV times a frame as {@code 1000000L * pts * num / den} in plain long arithmetic, which overflows at a 1 ns time base past
   * 9223.37 s. A video on such a time base starting 4 s before that point, with a keyframe every 2 s: the keyframes at 4 and 6 s
   * (past the point) cannot be extracted, so the video fails as incomplete, a content failure naming the video and the first
   * such keyframe, before any frame (the ones at 0 and 2 s included) is handed out.
   */
  @Test
  void keyframesPastJavaCvsTimestampRangeFailTheVideoBeforeAnyFrame() throws IOException {
    Path late = VideoClips.generate(dir.resolve("late-1ns.mp4"), 8, 10, 2, 160, 120, -1, -1, 9220, 1_000_000_000L);
    try (FFmpegFrameGrabber probe = new FFmpegFrameGrabber(late.toFile())) {
      probe.start();
      AVStream stream = probe.getFormatContext().streams(probe.getVideoStream());
      assertEquals(1_000_000_000, stream.time_base().den(), "precondition: a 1 ns time base");
      assertEquals(1, stream.time_base().num());
      assertTrue(stream.start_time() > 9_220_000_000_000L - 1_000_000_000L, "precondition: starts at 9220 s: " + stream.start_time());
      probe.stop();
    }
    FileMeta file = new FileMeta("fid-late", "mp4", Files.size(late));
    List<ExtractedFrame> frames = new ArrayList<>();
    SeekableBlobBytesSupplier bytes = VideoClips.supplier(late);
    VideoDecodeException incomplete = assertThrows(VideoDecodeException.class, () -> drainInto(bytes, file, frames));
    assertTrue(frames.isEmpty(), "no frame before the failure: " + frames.size());
    assertTrue(incomplete.getMessage().contains(file.toString()) && incomplete.getMessage().contains("9224000000000")
        && incomplete.getMessage().contains("refused as incomplete"), incomplete.getMessage());
  }

  /**
   * An {@link Error} while the video opens, after its blob is open (thrown from the blob, as a failing native link would be): it
   * is rethrown as it is and the blob is closed once.
   */
  @Test
  void errorWhileOpeningClosesTheBlobAndIsRethrown() throws IOException {
    LinkageError linkFailure = new LinkageError("test: a native library failed to link while the video opened");
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        opened.incrementAndGet();
        return new VideoClips.FileBlob(clip, Long.MAX_VALUE) {
          @Override
          public long length() {
            // asked once, as the video opens, outside FFmpeg's read callbacks: the Error escapes the open
            throw linkFailure;
          }

          @Override
          public void close() throws IOException {
            closed.incrementAndGet();
            super.close();
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(clip);
      }
    };
    FileMeta file = meta(clip);
    TypedProperties props = new TypedProperties();
    Error thrown = assertThrows(Error.class, () -> drain(bytes, file, props));
    assertSame(linkFailure, thrown);
    assertEquals(1, opened.get(), "one blob per video");
    assertEquals(1, closed.get(), "the blob is closed once");
  }

  /** A blob stream that fails to close changes no outcome but leaves a WARN naming the video. */
  @Test
  void closeFailureIsLoggedWithTheVideoName() throws IOException {
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new VideoClips.FileBlob(clip, Long.MAX_VALUE) {
          @Override
          public void close() throws IOException {
            super.close();
            throw new IOException("test: the blob stream failed to close");
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(clip);
      }
    };
    FileMeta file = new FileMeta("fid-close", "mp4", Files.size(clip));
    List<String> warnings;
    try (LogCapture logs = LogCapture.of(FfmpegKeyframeVideoFrameSource.class)) {
      assertEquals(KEPT_TIMESTAMPS.size(), drain(bytes, file, new TypedProperties()).size(), "the frames are out");
      warnings = logs.warnings();
    }
    assertFalse(warnings.isEmpty(), "a WARN for the stream that failed to close");
    for (String warning : warnings) {
      assertTrue(warning.contains(file.toString()), warning);
      assertTrue(warning.contains("failed to close"), warning);
    }
  }

  /**
   * The grabber's close fails on its blob stream's first close, which leaves the blob open (as a pooled connection whose release
   * throws would): the blob stream is still closed afterwards, and the failure is logged.
   */
  @Test
  void blobStreamsAreClosedWhenTheGrabbersCloseFails() throws IOException {
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    SeekableBlobBytesSupplier bytes = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        opened.incrementAndGet();
        AtomicInteger closeCalls = new AtomicInteger();
        return new VideoClips.FileBlob(clip, Long.MAX_VALUE) {
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
        return Files.newInputStream(clip);
      }
    };
    FileMeta file = new FileMeta("fid-close-retry", "mp4", Files.size(clip));
    List<String> warnings;
    try (LogCapture logs = LogCapture.of(FfmpegKeyframeVideoFrameSource.class)) {
      assertEquals(KEPT_TIMESTAMPS.size(), drain(bytes, file, new TypedProperties()).size(), "the frames are out");
      warnings = logs.warnings();
    }
    assertEquals(1, opened.get(), "one blob stream per video");
    assertEquals(1, closed.get(), "the blob stream is closed after the grabber's close failed");
    assertEquals(1, warnings.size(), warnings.toString());
    for (String warning : warnings) {
      assertTrue(warning.contains(file.toString()) && warning.contains("failed to close"), warning);
    }
  }

  private static Path resource(String name) throws IOException {
    Path path = dir.resolve(name);
    if (!Files.exists(path)) {
      try (InputStream in = TestFfmpegKeyframeVideoFrameSource.class.getResourceAsStream("/video/" + name)) {
        assertNotNull(in, "test resource /video/" + name);
        Files.copy(in, path);
      }
    }
    return path;
  }

  private static List<Long> timestamps(Path video) throws IOException {
    List<Long> timestamps = new ArrayList<>();
    for (ExtractedFrame frame : frames(video, new TypedProperties())) {
      timestamps.add(frame.getTimeMs());
      assertEquals("160x120", frame.getWidth() + "x" + frame.getHeight());
    }
    return timestamps;
  }

  private static int firstLumaQuantizer(byte[] jpeg) {
    for (int i = 2; i + 5 < jpeg.length; i++) {
      // DQT marker, 8-bit table 0: the first coefficient follows the length and the table id
      if ((jpeg[i] & 0xFF) == 0xFF && (jpeg[i + 1] & 0xFF) == 0xDB && (jpeg[i + 4] & 0xFF) == 0) {
        return jpeg[i + 5] & 0xFF;
      }
    }
    throw new AssertionError("no luma quantization table");
  }

  private static TypedProperties props(String key, String value) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, value);
    return props;
  }

  private static FileMeta meta(Path file) throws IOException {
    return new FileMeta("fid-1", "mp4", Files.size(file));
  }

  private static byte[] bytes(ExtractedFrame frame) throws IOException {
    try (InputStream in = frame.getImage().open()) {
      return readAll(in);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static List<ExtractedFrame> frames(Path file, TypedProperties props) throws IOException {
    return drain(VideoClips.supplier(file), meta(file), props);
  }

  /** Drains the strategy, copying each frame's bytes (an encoded frame is valid until the next is asked for). */
  private static List<ExtractedFrame> drain(BlobBytesSupplier video, FileMeta file, TypedProperties props) throws IOException {
    List<ExtractedFrame> frames = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> iterator = new FfmpegKeyframeVideoFrameSource().frames(video, file, props)) {
      assertNotNull(iterator);
      while (iterator.hasNext()) {
        ExtractedFrame frame = iterator.next();
        byte[] copy = bytes(frame);
        ExtractedFrame.EncodedImage encoded = new ExtractedFrame.EncodedImage(frame.getWidth(), frame.getHeight(), frame.getMimeType(),
            frame.getSizeBytes(), () -> new ByteArrayInputStream(copy));
        frames.add(new ExtractedFrame(frame.getTimeMs(), frame.getFrameIndex(), frame.isKeyframe(), frame.getDedupKey(), encoded));
      }
    }
    return frames;
  }

  /**
   * Whatever way a grabber fails to start, it is released (its close, which also closes the stream) before the failure
   * propagates; an exception is classified as the source classifies it, an Error (from the natives, or memory) propagates as
   * itself.
   */
  @Test
  void grabberThatFailsToStartIsReleasedHoweverItFails() throws IOException {
    FileMeta file = new FileMeta("fid-start", "mp4", Files.size(clip));
    Throwable[] failures = {new OutOfMemoryError("native allocation"), new IllegalStateException("start"),
        new FFmpegFrameGrabber.Exception("open")};
    for (Throwable failure : failures) {
      SeekableBlobInputStream stream = new SeekableBlobInputStream(new VideoClips.FileBlob(clip, Long.MAX_VALUE));
      FailingStartGrabber grabber = new FailingStartGrabber(stream, failure);
      RuntimeException classified = new VideoDecodeException("classified", null);
      Throwable thrown = assertThrows(Throwable.class,
          () -> FfmpegKeyframeVideoFrameSource.startOrRelease(grabber, stream, file, e -> classified));
      assertSame(failure instanceof Error ? failure : classified, thrown, failure.toString());
      assertTrue(grabber.closed, "released after " + failure);
    }
  }

  /** A grabber whose start fails as given; records its close. */
  private static final class FailingStartGrabber extends FFmpegFrameGrabber {
    private final Throwable failure;
    private boolean closed;

    FailingStartGrabber(InputStream in, Throwable failure) {
      super(in);
      this.failure = failure;
    }

    @Override
    public void start() throws FFmpegFrameGrabber.Exception {
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      throw (FFmpegFrameGrabber.Exception) failure;
    }

    @Override
    public void close() throws FrameGrabber.Exception {
      closed = true;
      super.close();
    }
  }

  /** Reads every frame of {@code file} into {@code frames}, closing the iterator. */
  private static void drainInto(SeekableBlobBytesSupplier bytes, FileMeta file, List<ExtractedFrame> frames) throws IOException {
    try (ClosableIterator<ExtractedFrame> iterator = new FfmpegKeyframeVideoFrameSource().frames(bytes, file, new TypedProperties())) {
      while (iterator.hasNext()) {
        frames.add(iterator.next());
      }
    }
  }
}
