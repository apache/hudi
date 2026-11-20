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
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.jcodec.codecs.h264.H264Utils;
import org.jcodec.codecs.h264.io.model.SeqParameterSet;
import org.jcodec.codecs.h264.mp4.AvcCBox;
import org.jcodec.containers.mp4.demuxer.MP4Demuxer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The JCodec frame strategy ({@link JcodecKeyframeVideoFrameSource}) on the checked-in codec matrix: H.264 in MP4 yields its
 * keyframes at the times ffprobe measured (codec-matrix-expected.txt), every other type is refused naming the reason and the
 * FFmpeg preprocessor, and a storage failure stays a storage failure.
 */
class TestJcodecKeyframeVideoFrameSource {

  private static final String MATRIX = "/video/codec-matrix/";
  /** c01's presented keyframes (codec-matrix-expected.txt, measured with ffprobe). */
  private static final List<Long> C01_KEYFRAMES = Arrays.asList(0L, 1000L, 2000L, 3000L, 4000L, 5000L);

  @TempDir
  Path dir;

  @Test
  void testH264Mp4FramesAreTheKeyframesFfprobeMeasured() throws Exception {
    Path c01 = resource(MATRIX + "c01-h264-aac.mp4");
    List<ExtractedFrame> jcodec = new ArrayList<>();
    List<byte[]> images = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), c01, "mp4", new TypedProperties(), jcodec, images);

    assertFalse(jcodec.isEmpty(), "c01 yields frames");
    assertTrue(C01_KEYFRAMES.containsAll(times(jcodec)), "kept frames are c01's keyframes: " + times(jcodec));
    for (int i = 0; i < jcodec.size(); i++) {
      ExtractedFrame frame = jcodec.get(i);
      assertEquals(i, frame.getFrameIndex(), "dense frame index");
      assertTrue(frame.isKeyframe());
      assertEquals(ImageContentTypes.JPEG, frame.getMimeType());
      assertEquals(images.get(i).length, frame.getSizeBytes());
      assertFalse(frame.getDedupKey().isEmpty());
      BufferedImage decoded = ImageIO.read(new ByteArrayInputStream(images.get(i)));
      assertNotNull(decoded, "frame " + i + " is a readable JPEG");
      assertEquals(160, decoded.getWidth(), "native width");
      assertEquals(120, decoded.getHeight(), "native height");
      assertEquals(160, frame.getWidth());
      assertEquals(120, frame.getHeight());
    }
  }

  /** Without dedup, one keyframe per second of c01 at the presentation times ffprobe measured, from the container's start. */
  @Test
  void testEveryScheduledKeyframeIsKeptAtItsPresentationTime() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    List<ExtractedFrame> frames = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), resource(MATRIX + "c01-h264-aac.mp4"), "mp4", props, frames, new ArrayList<>());
    assertEquals(C01_KEYFRAMES, times(frames));
  }

  /**
   * A multi-slice H.264 stream (hardware and sliced-thread encoders write them) decodes on the task's thread: no decoder thread
   * outlives the video, however many keyframes it has (JCodec's decoder would otherwise start a pool per keyframe).
   */
  @Test
  void testAMultiSliceVideoDecodesWithoutLeavingThreads() throws Exception {
    // JCodec starts its decoder pool only on a machine with more than one CPU
    assumeTrue(Runtime.getRuntime().availableProcessors() > 1, "a single CPU: JCodec starts no decoder thread");
    Path clip = resource("/video/h264-4slices.mp4");
    Set<Thread> before = new HashSet<>(Thread.getAllStackTraces().keySet());
    List<ExtractedFrame> frames = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), clip, "mp4", new TypedProperties(), frames, new ArrayList<>());
    Set<Thread> started = new HashSet<>(Thread.getAllStackTraces().keySet());
    started.removeAll(before);
    // only executor pool threads ("pool-N-thread-M", JCodec's); the JDK starts its own daemons on a first JPEG encode
    started.removeIf(thread -> !thread.isAlive() || !thread.getName().startsWith("pool-"));
    assertTrue(started.isEmpty(), "threads left by decoding: " + started);
    assertFalse(frames.isEmpty(), "the multi-slice clip yields frames");
  }

  @Test
  void testTheSameVideoYieldsTheSameBytes() throws Exception {
    Path c01 = resource(MATRIX + "c01-h264-aac.mp4");
    List<byte[]> first = new ArrayList<>();
    List<byte[]> second = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), c01, "mp4", new TypedProperties(), new ArrayList<>(), first);
    drain(new JcodecKeyframeVideoFrameSource(), c01, "mp4", new TypedProperties(), new ArrayList<>(), second);
    assertEquals(first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      assertArrayEquals(first.get(i), second.get(i), "frame " + i + " is deterministic");
    }
  }

  @Test
  void testTheIntervalKeyIsHonoured() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "2000");
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    List<ExtractedFrame> frames = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), resource(MATRIX + "c01-h264-aac.mp4"), "mp4", props, frames, new ArrayList<>());
    assertEquals(Arrays.asList(0L, 2000L, 4000L), times(frames));
  }

  /** Every type but H.264 in MP4 is refused before a frame is decoded, naming the reason and the FFmpeg way out. */
  @ParameterizedTest
  @CsvSource({
      "c03-hevc-aac.mp4, HEVC (hvc1)",
      "c05-av1-aac.mp4, AV1 (av01)",
      "c04-vp9-opus.mp4, VP9 (vp09)",
      "c02-mpeg4-mp3.mp4, MPEG-4 Part 2 (mp4v)",
      "t02-h264-aac-video-delayed.mp4, edit list of 2 entries",
      "t01-h264-aac-trimmed-in-gop.mp4, before the start of its edit list"})
  void testOtherTypesAreRefusedNamingFfmpeg(String file, String reason) throws Exception {
    Path clip = resource(MATRIX + file);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertRefusal(refused, reason, "mp4");
  }

  @Test
  void testAJcodecThatCannotDecodeOnOneThreadIsAConfigurationErrorAtRunTime() {
    // an executor whose class path holds another JCodec than the driver checked: the platform's, never the video's (class P)
    IllegalStateException unavailable = new IllegalStateException("JCodec's H264Decoder has no usable 'tp'/'threaded' fields");
    PreprocessorConfigurationException e = assertThrows(PreprocessorConfigurationException.class,
        () -> JcodecKeyframeVideoFrameSource.SingleThreadedDecoders.of(null, unavailable));
    assertEquals(unavailable, e.getCause());
  }

  @Test
  void testAnotherExtensionIsRefusedBeforeAnyRead() throws Exception {
    Path clip = resource(MATRIX + "c13-h264-aac.mov");
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mov"));
    assertRefusal(refused, "extension 'mov'", "mov");
  }

  /** An MP4 without a video track (c01 with its video track's handler relabelled as a text track) has no video stream. */
  @Test
  void testAnMp4WithoutAVideoTrackHasNoVideoStream() throws Exception {
    byte[] bytes = c01Bytes();
    // hdlr: size, type, version and flags, pre-defined, then the handler type
    int hdlr = indexOf(bytes, "vide".getBytes(StandardCharsets.US_ASCII), box(bytes, "hdlr"));
    assertEquals(box(bytes, "hdlr") + 16, hdlr, "c01's first handler is its video track's");
    System.arraycopy("text".getBytes(StandardCharsets.US_ASCII), 0, bytes, hdlr, 4);
    Path clip = write("no-video-track.mp4", bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertTrue(refused.getMessage().contains("has no video stream"), refused.getMessage());
  }

  /**
   * An H.264 stream JCodec cannot decode is the file's failure, naming JCodec and FFmpeg: c01 with the payload of every NAL unit
   * of its first keyframe replaced by noise (its NAL lengths and headers kept).
   */
  @Test
  void testAStreamJcodecCannotDecodeIsAVideoDecodeException() throws Exception {
    byte[] bytes = c01Bytes();
    // stco: size, type, version and flags, count, then the first chunk's offset; stsz: the first sample's size
    int sample = (int) getInt(bytes, box(bytes, "stco") + 16);
    int end = sample + (int) getInt(bytes, box(bytes, "stsz") + 20);
    Random noise = new Random(7);
    for (int nal = sample; nal + 4 < end; nal += 4 + (int) getInt(bytes, nal)) {
      for (int i = nal + 5; i < Math.min(end, nal + 4 + getInt(bytes, nal)); i++) {
        bytes[i] = (byte) noise.nextInt(256);
      }
    }
    Path clip = write("noise-keyframe.mp4", bytes);
    VideoDecodeException failed = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertRefusal(failed, "by JCodec", "mp4");
  }

  @Test
  void testThePixelGuardRefusesBeforeDecode() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), String.valueOf(160 * 120 - 1));
    Path clip = resource(MATRIX + "c01-h264-aac.mp4");
    List<ExtractedFrame> kept = new ArrayList<>();
    UncheckedIOException refused = assertThrows(UncheckedIOException.class, () -> drainJcodec(clip, "mp4", props, kept));
    assertTrue(refused.getCause() instanceof ImageRefusedException, String.valueOf(refused.getCause()));
    assertTrue(refused.getCause().getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key()), refused.getCause().getMessage());
  }

  /** A video declaring more than 1920x1080 is refused before decode, naming FFmpeg (c01 with its declared size raised). */
  @Test
  void testAFrameAboveTheJcodecCeilingIsRefused() throws Exception {
    byte[] bytes = Files.readAllBytes(resource(MATRIX + "c01-h264-aac.mp4"));
    // the sample entry, inside stsd (the ftyp box names avc1 as a compatible brand first)
    int stsd = indexOf(bytes, "stsd".getBytes(StandardCharsets.US_ASCII), 0);
    int entry = indexOf(bytes, "avc1".getBytes(StandardCharsets.US_ASCII), stsd);
    assertTrue(stsd > 0 && entry > stsd, "c01 has an avc1 sample entry");
    // VideoSampleEntry: after its fourcc, 24 bytes (reserved, data reference, version, revision, vendor, qualities), then
    // the 16-bit width and height
    putShort(bytes, entry + 28, 2000);
    putShort(bytes, entry + 30, 1080);
    Path big = dir.resolve("declared-2000x1080.mp4");
    Files.write(big, bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(big, "mp4"));
    assertRefusal(refused, "2000x1080", "mp4");
    assertTrue(refused.getMessage().contains(String.valueOf(JcodecKeyframeVideoFrameSource.MAX_PIXELS)), refused.getMessage());
  }

  @Test
  void testAFileCutShortIsTruncated() throws Exception {
    byte[] bytes = Files.readAllBytes(resource(MATRIX + "c01-h264-aac.mp4"));
    Path half = dir.resolve("half.mp4");
    Files.write(half, Arrays.copyOf(bytes, bytes.length / 2));
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(half, "mp4"));
    assertTrue(refused.getMessage().contains("truncated"), refused.getMessage());
  }

  @Test
  void testGarbageIsAVideoDecodeException() throws Exception {
    Path garbage = dir.resolve("garbage.mp4");
    byte[] bytes = new byte[4096];
    new Random(7).nextBytes(bytes);
    Files.write(garbage, bytes);
    assertThrows(VideoDecodeException.class, () -> drainJcodec(garbage, "mp4"));
  }

  /** A storage failure under a read reaches the caller as one (a BlobReadException in the chain), never as corrupt data. */
  @Test
  void testAStorageFailureIsReportedAsSuch() throws Exception {
    Path c01 = resource(MATRIX + "c01-h264-aac.mp4");
    for (long failAfter : new long[] {0L, 100L, Files.size(c01) / 2}) {
      SeekableBlobBytesSupplier failing = VideoClips.failingSupplier(c01, failAfter);
      FileMeta meta = new FileMeta("fid-1", "mp4", Files.size(c01));
      Exception failed = assertThrows(Exception.class, () -> drainSupplier(failing, meta));
      assertTrue(hasCause(failed, BlobReadException.class), "fail after " + failAfter + ": " + failed);
      assertFalse(failed instanceof VideoDecodeException, "not a content failure: " + failed);
    }
  }

  /**
   * A storage failure anywhere — the box headers, the index, a keyframe's sample — reaches the caller as a storage failure: the
   * connection drops after every budget from 0 to the file's size, in steps that land in each of those reads.
   */
  @Test
  void testAStorageFailureAtAnyPointOfTheReadIsReportedAsSuch() throws Exception {
    Path c01 = resource(MATRIX + "c01-h264-aac.mp4");
    int failures = 0;
    for (long budget = 0; budget <= Files.size(c01); budget += 499) {
      try {
        drainSupplier(VideoClips.failingAfterBytesRead(c01, budget), new FileMeta("fid-1", "mp4", Files.size(c01)));
      } catch (Exception failed) {
        failures++;
        assertTrue(hasCause(failed, BlobReadException.class), "budget " + budget + ": " + failed);
        assertFalse(failed instanceof VideoDecodeException, "budget " + budget + " is not a content failure: " + failed);
      }
    }
    assertTrue(failures > 0, "some budgets end inside the reads");
  }

  /** A sample table declaring more entries than its box holds is refused before JCodec allocates for them (an oversized count). */
  @Test
  void testATableDeclaringMoreEntriesThanItHoldsIsRefusedBeforeParsing() throws Exception {
    for (long count : new long[] {0x7FFFFFF0L, 1000L}) {
      byte[] bytes = c01Bytes();
      int stss = box(bytes, "stss");
      // stss: size, type, version and flags, then the entry count
      putInt(bytes, stss + 12, count);
      Path clip = write("stss-" + count + ".mp4", bytes);
      VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
      assertTrue(refused.getMessage().contains("'stss' table") && refused.getMessage().contains(String.valueOf(count)),
          refused.getMessage());
    }
  }

  /**
   * A sync-sample table whose size and count are valid but whose entries are not strictly increasing within [1, sampleCount] is
   * refused when the track opens, before any frame: c01 lists [1, 11, 21, 31, 41, 51] of its 60 samples.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({
      "unordered, '1, 31, 21, 41, 51, 60', entry 3 (21) is not above the one before it (31)",
      "duplicate, '1, 11, 11, 31, 41, 51', entry 3 (11) is not above the one before it (11)",
      "zero, '0, 11, 21, 31, 41, 51', entry 1 (0) is outside [1, 60]",
      "pastTheEnd, '1, 11, 21, 31, 41, 61', entry 6 (61) is outside [1, 60]"})
  void testASyncSampleTableOutOfOrderOrRangeIsRefusedBeforeAnyFrame(String name, String entries, String reason)
      throws Exception {
    byte[] bytes = c01Bytes();
    int stss = box(bytes, "stss");
    assertEquals(6, getInt(bytes, stss + 12), "precondition: c01 lists 6 sync samples");
    String[] values = entries.split(",\\s*");
    for (int i = 0; i < values.length; i++) {
      // stss: size, type, version and flags, the entry count, then the entries
      putInt(bytes, stss + 16 + 4 * i, Long.parseLong(values[i]));
    }
    List<ExtractedFrame> frames = new ArrayList<>();
    Path clip = write("stss-" + name + ".mp4", bytes);
    TypedProperties none = new TypedProperties();
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4", none, frames));
    assertTrue(refused.getMessage().contains("sync-sample table (stss)") && refused.getMessage().contains(reason),
        refused.getMessage());
    assertTrue(frames.isEmpty(), "refused before any frame: " + frames.size());
  }

  /**
   * A composition-offset table present but covering fewer samples than the track (its entries fit in the box) is refused when
   * the track opens, before any frame, rather than giving the uncovered samples an offset of 0: c01's first entry covers 3
   * samples of its 60; made to cover 1, the table covers 58.
   */
  @Test
  void testACompositionOffsetTableCoveringFewerSamplesThanTheTrackIsRefusedBeforeAnyFrame() throws Exception {
    byte[] bytes = c01Bytes();
    int ctts = box(bytes, "ctts");
    // ctts: size, type, version and flags, the entry count, then {sample count, offset} entries
    assertEquals(3, getInt(bytes, ctts + 16), "precondition: c01's first composition entry covers 3 samples");
    putInt(bytes, ctts + 16, 1);
    List<ExtractedFrame> frames = new ArrayList<>();
    Path clip = write("ctts-short.mp4", bytes);
    TypedProperties none = new TypedProperties();
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4", none, frames));
    assertTrue(refused.getMessage().contains("corrupt composition-offset table (ctts): it covers 58 of the track's 60 samples"),
        refused.getMessage());
    assertTrue(frames.isEmpty(), "refused before any frame: " + frames.size());
  }

  /**
   * A table JCodec parses outside the sample table box (it parses a box of a table type under any parent) or of a type the
   * demuxer reads beside the sync samples ({@code stps}) is checked too: c01's edit box relabelled as user data holding a sync
   * table, and its sync table relabelled as partial sync, each with an oversized count.
   */
  @Test
  void testATableAnywhereInTheIndexIsCheckedBeforeParsing() throws Exception {
    byte[] underUserData = c01Bytes();
    int edts = box(underUserData, "edts");
    int elst = box(underUserData, "elst");
    System.arraycopy("udta".getBytes(StandardCharsets.US_ASCII), 0, underUserData, edts + 4, 4);
    System.arraycopy("stss".getBytes(StandardCharsets.US_ASCII), 0, underUserData, elst + 4, 4);
    putInt(underUserData, elst + 12, 0x7FFFFFF0L);
    byte[] partialSync = c01Bytes();
    int stss = box(partialSync, "stss");
    System.arraycopy("stps".getBytes(StandardCharsets.US_ASCII), 0, partialSync, stss + 4, 4);
    putInt(partialSync, stss + 12, 0x7FFFFFF0L);
    for (Object[] patched : new Object[][] {{"under-udta.mp4", underUserData, "'stss' table"}, {"stps.mp4", partialSync, "'stps' table"}}) {
      Path clip = write((String) patched[0], (byte[]) patched[1]);
      VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
      assertTrue(refused.getMessage().contains((String) patched[2]), refused.getMessage());
    }
  }

  /**
   * Text in the index that happens to spell a table's name (metadata such as "Subscribe to my channel", "trunk") is not a table:
   * the file is prepared as without it (c01 with a free box of such text appended to its index, which ends the file).
   */
  @Test
  void testTextInTheIndexThatSpellsATableNameIsNotATable() throws Exception {
    byte[] original = c01Bytes();
    int moov = box(original, "moov");
    assertEquals(original.length, moov + getInt(original, moov), "c01's index ends the file");
    byte[] text = ("Subscribe to my channel for more videos; the trunk road; stts stsz ctts stco stsc stss stps elst co64")
        .getBytes(StandardCharsets.US_ASCII);
    byte[] bytes = Arrays.copyOf(original, original.length + 8 + text.length);
    putInt(bytes, original.length, 8 + text.length);
    System.arraycopy("free".getBytes(StandardCharsets.US_ASCII), 0, bytes, original.length + 4, 4);
    System.arraycopy(text, 0, bytes, original.length + 8, text.length);
    putInt(bytes, moov, getInt(original, moov) + 8 + text.length);
    List<ExtractedFrame> frames = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), write("text-in-index.mp4", bytes), "mp4", new TypedProperties(), frames,
        new ArrayList<>());
    List<ExtractedFrame> expected = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), resource(MATRIX + "c01-h264-aac.mp4"), "mp4", new TypedProperties(), expected,
        new ArrayList<>());
    assertEquals(times(expected), times(frames));
  }

  /**
   * A table whose 64-bit size is negative is still parsed by JCodec (its fit test passes, it parses the low 32 bits' worth of
   * body), so its oversized count is refused like any other (c01 with such a time-to-sample table appended to its index).
   */
  @Test
  void testATableWithANegativeLargeSizeIsChecked() throws Exception {
    byte[] original = c01Bytes();
    int moov = box(original, "moov");
    assertEquals(original.length, moov + getInt(original, moov), "c01's index ends the file");
    // size 1 (a 64-bit size follows), 'stts', size 0x80000000_00000018 (body: its low 32 bits less the 16-byte header, 8
    // bytes), version and flags, the count
    byte[] table = new byte[24];
    putInt(table, 0, 1);
    System.arraycopy("stts".getBytes(StandardCharsets.US_ASCII), 0, table, 4, 4);
    putInt(table, 8, 0x80000000L);
    putInt(table, 12, 0x18);
    putInt(table, 20, 0x7FFFFFF0L);
    byte[] bytes = Arrays.copyOf(original, original.length + table.length);
    System.arraycopy(table, 0, bytes, original.length, table.length);
    putInt(bytes, moov, getInt(original, moov) + table.length);
    Path clip = write("negative-size-table.mp4", bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertTrue(refused.getMessage().contains("'stts' table") && refused.getMessage().contains(String.valueOf(0x7FFFFFF0L)),
        refused.getMessage());
  }

  /**
   * A constant-size sample table (PCM audio declares one entry per audio sample) keeps no per-sample array in JCodec: its count
   * is not capped, and the video beside it is prepared (c01's audio sizes made constant, with two million samples).
   */
  @Test
  void testAConstantSizeSampleTableIsNotCapped() throws Exception {
    byte[] bytes = c01Bytes();
    int audioSizes = indexOf(bytes, "stsz".getBytes(StandardCharsets.US_ASCII), box(bytes, "stsz") + 8) - 4;
    assertTrue(audioSizes > box(bytes, "stsz"), "c01 has a second (audio) stsz");
    putInt(bytes, audioSizes + 12, 1024);
    putInt(bytes, audioSizes + 16, 2_000_000L);
    List<ExtractedFrame> frames = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), write("constant-audio-sizes.mp4", bytes), "mp4", new TypedProperties(), frames,
        new ArrayList<>());
    List<ExtractedFrame> original = new ArrayList<>();
    drain(new JcodecKeyframeVideoFrameSource(), resource(MATRIX + "c01-h264-aac.mp4"), "mp4", new TypedProperties(), original,
        new ArrayList<>());
    assertEquals(times(original), times(frames));
  }

  /** The parameter-set bounds JCodec sizes its decoder from: c01's own passes, each bound refuses. */
  @Test
  void testParameterSetBounds() throws Exception {
    SeqParameterSet c01 = c01Sps();
    assertNull(JcodecKeyframeVideoFrameSource.spsRefusal(c01), "c01's parameter set is decodable");
    SeqParameterSet wide = c01Sps();
    wide.picWidthInMbsMinus1 = 2000 / 16;
    wide.picHeightInMapUnitsMinus1 = 1200 / 16;
    assertTrue(JcodecKeyframeVideoFrameSource.spsRefusal(wide).contains("pixels per frame"));
    SeqParameterSet frameNumbers = c01Sps();
    frameNumbers.log2MaxFrameNumMinus4 = 13;
    assertTrue(JcodecKeyframeVideoFrameSource.spsRefusal(frameNumbers).contains("log2_max_frame_num_minus4 13"));
    SeqParameterSet references = c01Sps();
    references.numRefFrames = 17;
    assertTrue(JcodecKeyframeVideoFrameSource.spsRefusal(references).contains("max_num_ref_frames 17"));
    SeqParameterSet interlaced = c01Sps();
    interlaced.frameMbsOnlyFlag = false;
    assertTrue(JcodecKeyframeVideoFrameSource.spsRefusal(interlaced).contains("interlaced"));
    SeqParameterSet tenBit = c01Sps();
    tenBit.bitDepthLumaMinus8 = 2;
    assertTrue(JcodecKeyframeVideoFrameSource.spsRefusal(tenBit).contains("10-bit"));
  }

  /** c01's parameter set, read from its avcC as the strategy reads it. */
  private static SeqParameterSet c01Sps() throws Exception {
    byte[] bytes = c01Bytes();
    int avcC = box(bytes, "avcC");
    ByteBuffer body = ByteBuffer.wrap(bytes, avcC + 8, (int) getInt(bytes, avcC) - 8);
    return H264Utils.readSPS(AvcCBox.parseAvcCBox(body).getSpsList().get(0));
  }

  /** A keyframe declaring more bytes than the file holds is refused before its buffer is allocated (an oversized sample). */
  @Test
  void testAKeyframeDeclaringMoreBytesThanTheFileIsRefused() throws Exception {
    byte[] bytes = c01Bytes();
    // stsz: size, type, version and flags, constant size, count, then the first sample's size
    putInt(bytes, box(bytes, "stsz") + 20, 0x7FFFFFF0L);
    Path clip = write("oversized-sample.mp4", bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertTrue(refused.getMessage().contains("keyframe sample 0"), refused.getMessage());
  }

  /** A keyframe whose bytes run past the file's end (its index points beyond the data) is a truncated file. */
  @Test
  void testAKeyframeRunningPastTheFileEndIsTruncated() throws Exception {
    byte[] bytes = c01Bytes();
    // stco: size, type, version and flags, count, then the first chunk's offset
    long firstChunk = getInt(bytes, box(bytes, "stco") + 16);
    putInt(bytes, box(bytes, "stsz") + 20, bytes.length - firstChunk + 10);
    Path clip = write("sample-past-end.mp4", bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertTrue(refused.getMessage().contains("ends inside sample 0") && refused.getMessage().contains("truncated"),
        refused.getMessage());
  }

  /** A movie fragment ({@code moof}) is refused naming FFmpeg (c01 with its media box relabelled as a fragment). */
  @Test
  void testAFragmentedMp4IsRefused() throws Exception {
    byte[] bytes = c01Bytes();
    int mdat = box(bytes, "mdat");
    System.arraycopy("moof".getBytes(StandardCharsets.US_ASCII), 0, bytes, mdat + 4, 4);
    Path clip = write("fragmented.mp4", bytes);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainJcodec(clip, "mp4"));
    assertRefusal(refused, "is a fragmented MP4", "mp4");
  }

  /**
   * A video refused after its demuxer is built (an index carrying a movie-extends box, c01 with its udta relabelled) closes that
   * demuxer: the blob is closed once, through {@code MP4Demuxer.close()}.
   */
  @Test
  void testTheDemuxerIsClosedWhenAVideoIsRefusedAfterItOpens() throws Exception {
    byte[] bytes = c01Bytes();
    System.arraycopy("mvex".getBytes(StandardCharsets.US_ASCII), 0, bytes, box(bytes, "udta") + 4, 4);
    Path file = write("movie-extends.mp4", bytes);
    List<StackTraceElement[]> closes = new ArrayList<>();
    SeekableBlobBytesSupplier supplier = new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new VideoClips.FileBlob(file, Long.MAX_VALUE) {
          @Override
          public void close() throws IOException {
            closes.add(new Throwable().getStackTrace());
            super.close();
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(file);
      }
    };
    FileMeta meta = new FileMeta("fid-1", "mp4", bytes.length);
    VideoDecodeException refused = assertThrows(VideoDecodeException.class, () -> drainSupplier(supplier, meta));
    assertRefusal(refused, "is a fragmented MP4", "mp4");
    assertEquals(1, closes.size(), "the blob is closed once");
    assertTrue(Arrays.stream(closes.get(0)).anyMatch(frame -> frame.getClassName().equals(MP4Demuxer.class.getName())
        && frame.getMethodName().equals("close")), "the blob is closed by closing its demuxer");
  }

  @Test
  void testBadKeysAreRefusedOnTheDriver() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "0");
    JcodecKeyframeVideoFrameSource strategy = new JcodecKeyframeVideoFrameSource();
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> strategy.validateConfig(props));
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key()), refused.getMessage());
  }

  @Test
  void testTheLogicVersionNamesTheLogicNotTheLibrary() {
    assertEquals("video-keyframe-h264mp4-v2", new JcodecKeyframeVideoFrameSource().name());
  }

  private static void assertRefusal(Exception refused, String reason, String extension) {
    String message = refused.getMessage();
    assertTrue(message.contains(reason), "names the reason '" + reason + "': " + message);
    assertTrue(message.contains(VideoFramePreprocessor.class.getName()), "names FFmpeg: " + message);
    assertTrue(message.contains(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX), "names the extension key: " + message);
  }

  private static void drain(FrameExtractionStrategy strategy, Path file, String extension, TypedProperties props,
                            List<ExtractedFrame> frames, List<byte[]> images) throws IOException {
    try (ClosableIterator<ExtractedFrame> iterator = strategy.frames(VideoClips.supplier(file),
        new FileMeta("fid-1", extension, Files.size(file)), props)) {
      while (iterator.hasNext()) {
        ExtractedFrame frame = iterator.next();
        frames.add(frame);
        try (InputStream in = frame.getImage().open()) {
          images.add(readAll(in));
        }
      }
    }
  }

  /** Drains the JCodec strategy over {@code file} with the default keys, keeping nothing: one call for an assertThrows. */
  private static void drainJcodec(Path file, String extension) throws IOException {
    drainJcodec(file, extension, new TypedProperties(), new ArrayList<>());
  }

  /** Drains the JCodec strategy over {@code file} into {@code frames}: one call for an assertThrows. */
  private static void drainJcodec(Path file, String extension, TypedProperties props, List<ExtractedFrame> frames)
      throws IOException {
    drain(new JcodecKeyframeVideoFrameSource(), file, extension, props, frames, new ArrayList<>());
  }

  private static void drainSupplier(BlobBytesSupplier supplier, FileMeta file) throws IOException {
    try (ClosableIterator<ExtractedFrame> iterator = new JcodecKeyframeVideoFrameSource().frames(supplier, file, new TypedProperties())) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
  }

  private static List<Long> times(List<ExtractedFrame> frames) {
    List<Long> times = new ArrayList<>();
    frames.forEach(frame -> times.add(frame.getTimeMs()));
    return times;
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  private static int indexOf(byte[] haystack, byte[] needle, int from) {
    for (int i = from; i + needle.length <= haystack.length; i++) {
      if (Arrays.equals(Arrays.copyOfRange(haystack, i, i + needle.length), needle)) {
        return i;
      }
    }
    return -1;
  }

  private static byte[] c01Bytes() throws Exception {
    return Files.readAllBytes(resource(MATRIX + "c01-h264-aac.mp4"));
  }

  /** The offset of the first box of type {@code fourcc} (its size field), found by its fourcc. */
  private static int box(byte[] bytes, String fourcc) {
    int at = indexOf(bytes, fourcc.getBytes(StandardCharsets.US_ASCII), 0);
    assertTrue(at >= 4, "c01 has a '" + fourcc + "' box");
    return at - 4;
  }

  private Path write(String name, byte[] bytes) throws IOException {
    Path file = dir.resolve(name);
    Files.write(file, bytes);
    return file;
  }

  private static long getInt(byte[] bytes, int at) {
    return ((bytes[at] & 0xffL) << 24) | ((bytes[at + 1] & 0xffL) << 16) | ((bytes[at + 2] & 0xffL) << 8) | (bytes[at + 3] & 0xffL);
  }

  private static void putInt(byte[] bytes, int at, long value) {
    bytes[at] = (byte) (value >>> 24);
    bytes[at + 1] = (byte) (value >>> 16);
    bytes[at + 2] = (byte) (value >>> 8);
    bytes[at + 3] = (byte) value;
  }

  private static void putShort(byte[] bytes, int at, int value) {
    bytes[at] = (byte) (value >>> 8);
    bytes[at + 1] = (byte) value;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static Path resource(String name) throws URISyntaxException {
    return Paths.get(TestJcodecKeyframeVideoFrameSource.class.getResource(name).toURI());
  }
}
