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
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.CloseOnFailure;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.Release;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.jcodec.codecs.h264.H264Decoder;
import org.jcodec.codecs.h264.H264Utils;
import org.jcodec.codecs.h264.io.model.SeqParameterSet;
import org.jcodec.codecs.h264.mp4.AvcCBox;
import org.jcodec.common.DemuxerTrack;
import org.jcodec.common.model.ColorSpace;
import org.jcodec.common.model.Picture;
import org.jcodec.containers.mp4.MP4Packet;
import org.jcodec.containers.mp4.boxes.Box;
import org.jcodec.containers.mp4.boxes.CompositionOffsetsBox;
import org.jcodec.containers.mp4.boxes.Edit;
import org.jcodec.containers.mp4.boxes.NodeBox;
import org.jcodec.containers.mp4.boxes.SampleSizesBox;
import org.jcodec.containers.mp4.boxes.SyncSamplesBox;
import org.jcodec.containers.mp4.boxes.TimeToSampleBox;
import org.jcodec.containers.mp4.boxes.TrakBox;
import org.jcodec.containers.mp4.boxes.VideoSampleEntry;
import org.jcodec.containers.mp4.demuxer.AbstractMP4DemuxerTrack;
import org.jcodec.containers.mp4.demuxer.MP4Demuxer;
import org.jcodec.scale.AWTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * The pure-Java {@link FrameExtractionStrategy} through JCodec 0.2.5 (BSD), logic {@value #LOGIC_VERSION}: the
 * same rule as the FFmpeg strategy — the closest keyframe every X milliseconds ({@link KeyframeSchedule}), consecutive repeats
 * dropped ({@link FrameDedup}), each kept frame a JPEG at quality {@value FrameJpegEncoder#JPEG_QUALITY} at its native size
 * ({@link FrameJpegEncoder}) — for ONE type of video: H.264 (8-bit 4:2:0, progressive) in a non-fragmented MP4. It needs
 * no native library, so a job that prepares video with it runs without FFmpeg. A frame's bytes
 * differ from FFmpeg's (another YUV to RGB conversion), hence its own logic version.
 *
 * <p>Per video it reads the MP4's box headers and its {@code moov} index ({@link BlobByteChannel}: range reads, nothing
 * buffers the file), then walks the video track's sample tables: each sync sample's presentation time is its decode time
 * ({@code stts}) plus its composition offset ({@code ctts}) minus the edit list's media time, from the container's start (0 for
 * an MP4, as FFmpeg reports it), clamped at 0; the times feed the schedule without reading any sample. Each chosen
 * keyframe is then read alone and decoded alone by a fresh decoder (an IDR needs no other frame) into a BGR raster: at most
 * one decoded frame, its raster and its JPEG are alive at a time, whatever the video's length. JCodec keeps the parsed index
 * (a few bytes per sample) while the video is open; an index above {@value #MAX_INDEX_BYTES} bytes is refused before it is
 * read.
 *
 * <p>Refused before any frame is decoded, each a {@link VideoDecodeException} that names the reason and the FFmpeg preprocessor
 * as the way out: an extension other than {@code mp4}; a file whose boxes do not lie within it (cut short) or that has no
 * index; a fragmented MP4; no video track; a codec other than H.264 ({@code hvc1}, {@code av01}, ...); an H.264 stream that
 * is not 8-bit 4:2:0 progressive; an H.264 parameter set whose counts or ids pass H.264's bounds
 * ({@link H264ParameterSetAdmission}, checked before JCodec parses it); a track without a sync-sample table; an edit list other than one entry at rate 1
 * (the one entry every encoder writes for its delay is applied); a keyframe before that
 * entry's start; a frame above {@value #MAX_PIXELS} pixels (JCodec decodes on the heap). A frame above
 * {@link VideoFrameConfig#getMaxPixels()} is refused as by the FFmpeg strategy (an {@link UncheckedIOException} over an
 * {@link ImageRefusedException}). A sample past the file's end is a truncated file; anything JCodec throws while parsing or
 * decoding is a {@link VideoDecodeException}; a storage failure under a read is an {@link UncheckedIOException} over a
 * {@link BlobReadException} (the caller decides whether the source is gone or the read was interrupted).
 */
public class JcodecKeyframeVideoFrameSource implements FrameExtractionStrategy {

  public static final String LOGIC_VERSION = "video-keyframe-h264mp4-v2";
  /**
   * The logic of decoding with JCodec (jcodec's {@link H264Decoder} and jcodec-javase's {@link AWTUtil} conversion to RGB),
   * pinned in the video prep logic version instead of the two jars' versions. Bump only when a dependency upgrade or code change
   * alters prep output; the golden test below proves it: {@code TestJcodecKeyframeVideoFrameSource} checks which frames are
   * kept and their timestamps. It pins no pixel digest, so a JCodec upgrade also compares the frame JPEG bytes before and after.
   */
  public static final String DECODE_LOGIC_VERSION = "jcodec-decode-v1";
  /** The one extension this strategy prepares. */
  public static final String EXTENSION = "mp4";
  /**
   * The largest frame JCodec decodes, 1920x1080: about 7.5 bytes per pixel on the heap (YUV picture, BGR conversion, raster),
   * about 15 MB per decoded frame. A larger video belongs to FFmpeg.
   */
  public static final long MAX_PIXELS = 1920L * 1080;
  /** The largest {@code moov} index read: JCodec parses it into memory whole (a long video's is a few MB). */
  static final long MAX_INDEX_BYTES = 4L * 1024 * 1024;
  /**
   * The most entries a table may declare (2^20): a bound on what JCodec allocates per table; in practice the 4 MiB index
   * cap binds first, at roughly two hours of 30 fps video with AAC audio.
   */
  static final long MAX_SAMPLES = 1L << 20;
  /** The largest coded picture: MAX_PIXELS rounded up to whole macroblocks (1920x1088), with a little headroom. */
  static final long MAX_CODED_PIXELS = MAX_PIXELS + MAX_PIXELS / 8;
  /** The largest keyframe sample read: an intra frame at the ceiling compresses below its 1.5-byte-per-pixel YUV picture. */
  static final long MAX_SAMPLE_BYTES = 4 * MAX_PIXELS;
  private static final Logger LOG = LoggerFactory.getLogger(JcodecKeyframeVideoFrameSource.class);
  /** Bytes per entry of the tables JCodec reads into arrays of their declared count, after version, flags and the count. */
  private static final Map<String, Long> TABLE_ENTRY_BYTES;
  /** Tables JCodec also sizes from a declared count, whose layout the pre-check reads apiece. */
  private static final Set<String> VARIABLE_TABLES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("stsz", "chan", "trun")));
  /** Every table type the pre-check reads, by its four bytes as a big-endian int. */
  private static final Map<Integer, String> TABLE_TYPES;
  private static final int NAL_SPS = 7;
  private static final int NAL_PPS = 8;
  /** How every message about a video starts. */
  private static final String VIDEO_PREFIX = "video ";
  /** The frame-size ceiling, as a refusal names it after "above the ". */
  private static final String CEILING = MAX_PIXELS + " (1920x1080) " + LOGIC_VERSION + " decodes";
  /** How a message about a file cut short ends. */
  private static final String TRUNCATED = ": the file is truncated";

  static {
    Map<String, Long> entryBytes = new HashMap<>();
    entryBytes.put("stts", 8L);
    entryBytes.put("ctts", 8L);
    entryBytes.put("stss", 4L);
    entryBytes.put("stps", 4L);
    entryBytes.put("stsc", 12L);
    entryBytes.put("stco", 4L);
    entryBytes.put("co64", 8L);
    entryBytes.put("elst", 12L);
    TABLE_ENTRY_BYTES = Collections.unmodifiableMap(entryBytes);
    Map<Integer, String> types = new HashMap<>();
    for (String fourcc : entryBytes.keySet()) {
      types.put(ByteBuffer.wrap(fourcc.getBytes(StandardCharsets.ISO_8859_1)).getInt(), fourcc);
    }
    for (String fourcc : VARIABLE_TABLES) {
      types.put(ByteBuffer.wrap(fourcc.getBytes(StandardCharsets.ISO_8859_1)).getInt(), fourcc);
    }
    TABLE_TYPES = Collections.unmodifiableMap(types);
  }

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long MICROS_PER_MILLI = 1000L;
  private static final int MACROBLOCK = 16;

  @Override
  public String name() {
    return LOGIC_VERSION;
  }

  /** Checks the frame keys (interval, dedup, pixel guard) and that this JCodec can decode on one thread, on the driver. */
  @Override
  public void validateConfig(TypedProperties props) {
    VideoFrameConfig.from(props);
    try {
      SingleThreadedDecoders.check();
    } catch (IllegalStateException e) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + " selects " + LOGIC_VERSION
          + ", which cannot run on this class path: " + e.getMessage(), e);
    }
  }

  @Override
  public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) throws IOException {
    VideoFrameConfig config = VideoFrameConfig.from(props);
    String extension = file.getFileExtension() == null ? "" : file.getFileExtension().toLowerCase(Locale.ROOT);
    if (!EXTENSION.equals(extension)) {
      throw new VideoDecodeException(VIDEO_PREFIX + file + " has extension '" + extension + "'; " + LOGIC_VERSION
          + " prepares H.264 in MP4 (.mp4) only" + wayOut(extension));
    }
    if (!(video instanceof SeekableBlobBytesSupplier)) {
      throw new VideoDecodeException(VIDEO_PREFIX + file + " bytes are not seekable");
    }
    return new KeyframeFrames(file, (SeekableBlobBytesSupplier) video, config);
  }

  /** How a refused video is prepared instead: the FFmpeg preprocessor, for its extension or for every video extension. */
  static String wayOut(String extension) {
    return "; prepare it with FFmpeg: set " + EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX + " (or " + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + ") to "
        + VideoFramePreprocessor.class.getName();
  }

  /** The common name of an MP4 video sample entry's codec, for a refusal. */
  static String codecName(String fourcc) {
    switch (fourcc) {
      case "hvc1":
      case "hev1":
        return "HEVC (" + fourcc + ")";
      case "av01":
        return "AV1 (" + fourcc + ")";
      case "vp08":
        return "VP8 (" + fourcc + ")";
      case "vp09":
        return "VP9 (" + fourcc + ")";
      case "mp4v":
        return "MPEG-4 Part 2 (" + fourcc + ")";
      default:
        return "'" + fourcc + "'";
    }
  }

  /**
   * JCodec 0.2.5's {@link H264Decoder} makes a thread pool of one thread per core in its constructor, decodes a multi-slice
   * frame's slices on it and never shuts it down: a decoder per keyframe would leave threads behind for every keyframe, and one
   * decode would use every core of the executor from one task slot. This turns the pool off before any slice is
   * submitted: the pool is shut down (it has started no thread yet) and the decoder decodes slices one after another, on the
   * task's thread, as it does on a single-core machine. The two private fields are JCodec 0.2.5's ({@code tp},
   * {@code threaded}); the version is pinned in the root pom. The driver checks them ({@link #check()}); an executor whose class
   * path holds another JCodec fails its decode with a {@link PreprocessorConfigurationException} (class P), never the video.
   */
  static final class SingleThreadedDecoders {
    private static final VarHandle POOL;
    private static final VarHandle THREADED;
    /**
     * Why the fields cannot be used (another JCodec on the class path), or null: kept, never thrown from class initialisation,
     * where it would be an Error that ends a Spark executor.
     */
    private static final IllegalStateException UNAVAILABLE;

    static {
      VarHandle pool = null;
      VarHandle threaded = null;
      IllegalStateException unavailable = null;
      try {
        MethodHandles.Lookup decoderFields = MethodHandles.privateLookupIn(H264Decoder.class, MethodHandles.lookup());
        pool = decoderFields.findVarHandle(H264Decoder.class, "tp", ExecutorService.class);
        threaded = decoderFields.findVarHandle(H264Decoder.class, "threaded", boolean.class);
      } catch (ReflectiveOperationException | RuntimeException e) {
        unavailable = new IllegalStateException("JCodec's H264Decoder has no usable 'tp'/'threaded' fields: "
            + LOGIC_VERSION + " needs org.jcodec:jcodec:0.2.5 on the class path", e);
      }
      POOL = pool;
      THREADED = threaded;
      UNAVAILABLE = unavailable;
    }

    private SingleThreadedDecoders() {
    }

    /** @throws IllegalStateException when this JCodec's decoder threading cannot be turned off */
    static void check() {
      if (UNAVAILABLE != null) {
        throw UNAVAILABLE;
      }
    }

    /**
     * {@code decoder}, its slice thread pool shut down and its slices decoded on the calling thread.
     *
     * @throws PreprocessorConfigurationException when this JCodec's decoder threading cannot be turned off
     */
    static H264Decoder of(H264Decoder decoder) {
      return of(decoder, UNAVAILABLE);
    }

    /** {@link #of(H264Decoder)} with {@code unavailable} as why the fields cannot be used, or null; a test seam. */
    static H264Decoder of(H264Decoder decoder, IllegalStateException unavailable) {
      if (unavailable != null) {
        throw new PreprocessorConfigurationException(LOGIC_VERSION + " cannot decode on one thread with this executor's JCodec: "
            + unavailable.getMessage(), unavailable);
      }
      try {
        ExecutorService pool = (ExecutorService) POOL.get(decoder);
        if (pool != null) {
          pool.shutdownNow();
        }
        THREADED.set(decoder, false);
      } catch (RuntimeException e) {
        throw new PreprocessorConfigurationException(LOGIC_VERSION + ": JCodec's H264Decoder threading cannot be turned off", e);
      }
      return decoder;
    }
  }

  /**
   * Why JCodec must not decode with {@code sps}, or null: not 8-bit 4:2:0 progressive, a coded size above the ceiling, or a
   * frame-number range or reference count outside H.264's bounds (7.4.2.1.1: log2_max_frame_num_minus4 in [0, 12],
   * max_num_ref_frames at most 16), which JCodec sizes its reference list from.
   */
  static String spsRefusal(SeqParameterSet sps) {
    if (sps.chromaFormatIdc != ColorSpace.YUV420J || sps.bitDepthLumaMinus8 != 0 || sps.bitDepthChromaMinus8 != 0
        || !sps.frameMbsOnlyFlag) {
      return "is H.264 profile " + sps.profileIdc + " with " + sps.chromaFormatIdc + " chroma, " + (8 + sps.bitDepthLumaMinus8)
          + "-bit, " + (sps.frameMbsOnlyFlag ? "progressive" : "interlaced") + "; " + LOGIC_VERSION
          + " decodes 8-bit 4:2:0 progressive H.264 only";
    }
    long width = (sps.picWidthInMbsMinus1 + 1L) * MACROBLOCK;
    long height = (sps.picHeightInMapUnitsMinus1 + 1L) * MACROBLOCK;
    if (sps.picWidthInMbsMinus1 < 0 || sps.picHeightInMapUnitsMinus1 < 0 || width * height > MAX_CODED_PIXELS) {
      return "codes " + width + "x" + height + " pixels per frame, above the " + CEILING;
    }
    if (sps.log2MaxFrameNumMinus4 < 0 || sps.log2MaxFrameNumMinus4 > 12 || sps.numRefFrames < 0 || sps.numRefFrames > 16) {
      return "has an H.264 parameter set out of range (log2_max_frame_num_minus4 " + sps.log2MaxFrameNumMinus4
          + ", max_num_ref_frames " + sps.numRefFrames + ")";
    }
    return null;
  }

  /** The kept frames of one video, decoded and encoded on demand. */
  private static final class KeyframeFrames implements ClosableIterator<ExtractedFrame> {
    /** {@link #tableLayout} of a table too short to hold its fixed part: nothing is parsed from it. */
    private static final long[] NO_TABLE = new long[0];
    private final FileMeta video;
    private final String extension;
    private final KeyframeSchedule schedule;
    private final FrameDedup dedup;
    private final long maxPixels;
    /** Chosen keyframes not yet decoded, as {time from the origin in microseconds, sample number}: at most two. */
    private final Deque<long[]> chosen = new ArrayDeque<>(2);
    private BlobByteChannel channel;
    /** Reads {@link #channel}; closing it closes the channel. */
    private MP4Demuxer demuxer;
    private AbstractMP4DemuxerTrack track;
    private ByteBuffer codecPrivate;
    private SampleSizesBox sampleSizes;
    private int codedWidth;
    private int codedHeight;
    private FrameJpegEncoder encoder;
    private long timescale;
    /** The edit list's media time (track timescale): the presentation origin; 0 without an edit list. */
    private long editMediaTime;
    // the sample-table walk: one cursor over stts, ctts and stss, advanced one sample at a time
    private TimeToSampleBox.TimeToSampleEntry[] decodeDeltas;
    private CompositionOffsetsBox.Entry[] compositionOffsets;
    private int[] syncSamples;
    private long sampleCount;
    private long nextSample;
    private long decodeTime;
    private int deltaEntry;
    private long deltaUsed;
    private int offsetEntry;
    private long offsetUsed;
    private int syncIndex;
    private long previousKeyframeMicros = -1;
    private long previousKeyframeSample;
    private long lastFrameMicros = -1;
    private boolean scannedToEnd;
    private long nextIndex;
    private ExtractedFrame next;
    private boolean closed;

    private KeyframeFrames(FileMeta video, SeekableBlobBytesSupplier bytes, VideoFrameConfig config) throws IOException {
      this.video = video;
      this.extension = video.getFileExtension().toLowerCase(Locale.ROOT);
      this.schedule = new KeyframeSchedule(config.getIntervalMs() * MICROS_PER_MILLI);
      this.dedup = new FrameDedup(config.getDedupMethod(), config.getMaxDistance());
      this.maxPixels = config.getMaxPixels();
      // whatever way opening fails, an Error included, what it opened is closed; a close failure is suppressed into the
      // opening's failure, which stays the one thrown
      try (CloseOnFailure<RuntimeException> guard = new CloseOnFailure<>(this::close)) {
        openOrRefuse(bytes);
        guard.succeeded();
      }
    }

    /** {@link #open}, anything JCodec throws while opening refused as the video's data. */
    private void openOrRefuse(SeekableBlobBytesSupplier bytes) throws IOException {
      try {
        open(bytes);
      } catch (VideoDecodeException | UncheckedIOException e) {
        throw e;
      } catch (RuntimeException e) {
        throw failure("cannot be opened", e);
      }
    }

    private void open(SeekableBlobBytesSupplier bytes) throws IOException {
      channel = new BlobByteChannel(bytes.openSeekable());
      checkBoxes();
      demuxer = MP4Demuxer.createMP4Demuxer(channel);
      if (NodeBox.findFirst(demuxer.getMovie(), Box.class, "mvex") != null) {
        throw refused("is a fragmented MP4");
      }
      List<DemuxerTrack> videoTracks = demuxer.getVideoTracks();
      if (videoTracks.isEmpty()) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has no video stream");
      }
      track = (AbstractMP4DemuxerTrack) videoTracks.get(0);
      String fourcc = track.getFourcc();
      if (!"avc1".equals(fourcc) && !"avc3".equals(fourcc)) {
        throw refused("has video codec " + codecName(fourcc) + "; " + LOGIC_VERSION + " decodes H.264 only");
      }
      TrakBox trak = track.getBox();
      sampleCount = track.getFrameCount();
      SyncSamplesBox stss = NodeBox.findFirstPath(trak, SyncSamplesBox.class, Box.path("mdia.minf.stbl.stss"));
      TimeToSampleBox stts = NodeBox.findFirstPath(trak, TimeToSampleBox.class, Box.path("mdia.minf.stbl.stts"));
      sampleSizes = NodeBox.findFirstPath(trak, SampleSizesBox.class, Box.path("mdia.minf.stbl.stsz"));
      if (sampleCount <= 0 || stts == null || sampleSizes == null) {
        throw refused("has no video samples in its index (a fragmented MP4?)");
      }
      if (stss == null) {
        throw refused("has no sync-sample table (stss)");
      }
      checkStream((VideoSampleEntry) track.getSampleEntries()[0]);
      editMediaTime = editMediaTime(track.getEdits());
      syncSamples = stss.getSyncSamples();
      checkSyncSamples();
      decodeDeltas = stts.getEntries();
      CompositionOffsetsBox ctts = NodeBox.findFirstPath(trak, CompositionOffsetsBox.class, Box.path("mdia.minf.stbl.ctts"));
      compositionOffsets = ctts == null ? new CompositionOffsetsBox.Entry[0] : ctts.getEntries();
      if (ctts != null) {
        checkCompositionOffsetsCoverTheTrack();
      }
      timescale = track.getTimescale();
      if (timescale <= 0) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " declares a video timescale of " + timescale);
      }
      codecPrivate = track.getMeta().getCodecPrivate();
      encoder = new FrameJpegEncoder();
    }

    /**
     * The sync-sample walk ({@link #isSync}) assumes stss lists sample numbers strictly increasing within [1, sampleCount]; an
     * entry out of order would be passed over and one out of range never met, so listed keyframes would be omitted without an
     * error. Such a table is a corrupt index, refused before any frame.
     */
    private void checkSyncSamples() {
      long previous = 0;
      for (int i = 0; i < syncSamples.length; i++) {
        // an entry is an unsigned 32-bit sample number, read by JCodec into an int
        long entry = syncSamples[i] & 0xffffffffL;
        if (entry < 1 || entry > sampleCount) {
          throw new VideoDecodeException(VIDEO_PREFIX + video + " has a corrupt sync-sample table (stss): entry " + (i + 1) + " ("
              + entry + ") is outside [1, " + sampleCount + "]");
        }
        if (entry <= previous) {
          throw new VideoDecodeException(VIDEO_PREFIX + video + " has a corrupt sync-sample table (stss): it is not strictly "
              + "increasing, entry " + (i + 1) + " (" + entry + ") is not above the one before it (" + previous + ")");
        }
        previous = entry;
      }
    }

    /**
     * An absent ctts means every composition offset is 0; a present one must cover every sample of the track, or the samples past
     * its end would take an offset of 0 they do not have, moving their presentation times (and so the keyframe schedule) without an
     * error. Such a table is a corrupt index, refused before any frame.
     */
    private void checkCompositionOffsetsCoverTheTrack() {
      long covered = 0;
      for (CompositionOffsetsBox.Entry entry : compositionOffsets) {
        // a sample count is an unsigned 32-bit value, read by JCodec into an int
        covered += entry.getCount() & 0xffffffffL;
      }
      if (covered < sampleCount) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has a corrupt composition-offset table (ctts): it covers " + covered
            + " of the track's " + sampleCount + " samples");
      }
    }

    /**
     * Walks the boxes by their headers before JCodec parses anything (JCodec trusts every declared size): each top-level box must
     * lie within the file (a box past the end is a file cut short), the index ({@code moov}) must be there and not above
     * MAX_INDEX_BYTES, and a movie fragment ({@code moof}) is refused. Every table in the index is checked
     * ({@link #checkIndex}). JCodec then walks the same headers, so it never meets a size it would loop or over-allocate on.
     */
    private void checkBoxes() throws IOException {
      long size = channel.size();
      long offset = 0;
      boolean index = false;
      while (size - offset >= 8) {
        long[] box = boxAt(offset, size, "the file");
        String fourcc = fourcc(box);
        if ("moof".equals(fourcc)) {
          throw refused("is a fragmented MP4");
        }
        if ("moov".equals(fourcc)) {
          if (box[0] - box[1] > MAX_INDEX_BYTES) {
            throw refused("has an index (moov) of " + (box[0] - box[1]) + " bytes, above the " + MAX_INDEX_BYTES
                + " bytes " + LOGIC_VERSION + " reads");
          }
          checkIndex(offset + box[1], box[0] - box[1]);
          index = true;
        }
        offset += box[0];
      }
      if (!index) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has no index (moov box): truncated or not an MP4");
      }
    }

    /**
     * The sample tables anywhere in the index ({@code moov} body at {@code start}, {@code length} bytes). JCodec builds a box of
     * a table type under many parents ({@code udta}, {@code skip}, {@code meta}, a sample entry, ...) and allocates an array of
     * its declared count before reading an entry; it parses a box when its declared body fits (compared as a long, so a negative
     * 64-bit body passes, and then {@code (int) body} bytes are parsed); a box that does not fit is skipped, its bytes read as
     * siblings. So every position whose next four bytes name such a table is checked as if a box started there, with the body
     * JCodec would parse when that body fits in the index: its declared count must fit that body and stay within MAX_SAMPLES.
     * A table's name inside other data (metadata text) reads as a box that does not fit, which JCodec never parses, and is
     * passed over; the scan does not depend on knowing JCodec's containers.
     */
    private void checkIndex(long start, long length) throws IOException {
      ByteBuffer index = readAt(start, (int) length);
      if (index.remaining() < length) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " ends inside its index: the file is truncated");
      }
      for (int type = 4; type + 4 <= index.limit(); type++) {
        byte first = index.get(type);
        // every table type starts with 's', 'c', 'e' or 't': most positions are passed over without a lookup
        if (first != 's' && first != 'c' && first != 'e' && first != 't') {
          continue;
        }
        String fourcc = TABLE_TYPES.get(index.getInt(type));
        if (fourcc != null) {
          checkTable(fourcc, index, type - 4);
        }
      }
    }

    /**
     * A table at {@code boxStart} of {@code index}, if a box JCodec would parse starts there (its declared body fits in the
     * index): its declared entry count against that body and against MAX_SAMPLES.
     */
    private void checkTable(String fourcc, ByteBuffer index, int boxStart) {
      long size = index.getInt(boxStart) & 0xFFFFFFFFL;
      int headerSize = 8;
      if (size == 1) {
        if (boxStart + 16 > index.limit()) {
          return;
        }
        size = index.getLong(boxStart + 8);
        headerSize = 16;
      } else if (size < 8) {
        // JCodec stops at a header it cannot read: nothing is parsed from here
        return;
      }
      int bodyStart = boxStart + headerSize;
      long body = size - headerSize;
      if (body < 0) {
        // JCodec's fit test compares the body as a long, which a negative one passes; it then parses (int) body bytes
        body = (int) body;
      }
      if (body < 0 || body > index.limit() - (long) bodyStart) {
        // JCodec parses no box whose body does not fit where it sits (text that spells a table's name reads as one)
        return;
      }
      long[] layout = tableLayout(fourcc, index, bodyStart, body);
      if (layout.length == 0) {
        return;
      }
      long fixedBytes = layout[0];
      long count = layout[1];
      long entryBytes = layout[2];
      if (entryBytes == 0) {
        return;
      }
      if (count > MAX_SAMPLES) {
        throw refused("declares " + count + " entries in its '" + fourcc + "' table, above the " + MAX_SAMPLES + " samples "
            + LOGIC_VERSION + " reads");
      }
      if (fixedBytes + count * entryBytes > body) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + hasA(fourcc) + "table declaring " + count + " entries in "
            + body + " bytes: corrupt index");
      }
    }

    /**
     * The declared layout of a {@code fourcc} table whose body ({@code body} bytes) starts at {@code bodyStart} of {@code index}:
     * {fixed bytes, entry count, bytes per entry}, or {@link #NO_TABLE} when the body is too short to hold the fixed part. Every
     * such table is a full box, whose body starts with its version and flags (4 bytes).
     */
    private static long[] tableLayout(String fourcc, ByteBuffer index, int bodyStart, long body) {
      switch (fourcc) {
        case "stsz":
          if (body < 12) {
            return NO_TABLE;
          }
          // a constant sample size: JCodec keeps no per-sample array
          return new long[] {12, index.getInt(bodyStart + 8) & 0xFFFFFFFFL, index.getInt(bodyStart + 4) == 0 ? 4 : 0};
        case "chan":
          if (body < 16) {
            return NO_TABLE;
          }
          return new long[] {16, index.getInt(bodyStart + 12) & 0xFFFFFFFFL, 20};
        case "trun":
          if (body < 8) {
            return NO_TABLE;
          }
          int flags = index.getInt(bodyStart) & 0xFFFFFF;
          long fixedBytes = 8L + ((flags & 0x1) == 0 ? 0 : 4) + ((flags & 0x4) == 0 ? 0 : 4);
          // one int per sample for each of duration, size, flags and composition offset present
          return new long[] {fixedBytes, index.getInt(bodyStart + 4) & 0xFFFFFFFFL, 4L * Integer.bitCount(flags & 0xF00)};
        default:
          if (body < 8) {
            return NO_TABLE;
          }
          return new long[] {8, index.getInt(bodyStart + 4) & 0xFFFFFFFFL, TABLE_ENTRY_BYTES.get(fourcc)};
      }
    }

    /**
     * The box header at {@code offset}, which must lie before {@code end}: {size, header size, fourcc as an int}.
     * @param within what {@code end} is the end of, for the message
     */
    private long[] boxAt(long offset, long end, String within) throws IOException {
      ByteBuffer header = readAt(offset, (int) Math.min(16, end - offset));
      if (header.remaining() < 8) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " ends inside a box header at byte " + offset + TRUNCATED);
      }
      long boxSize = header.getInt() & 0xFFFFFFFFL;
      int type = header.getInt();
      String fourcc = fourcc(new long[] {0, 0, type});
      long headerSize = 8;
      if (boxSize == 1) {
        if (header.remaining() < 8) {
          throw new VideoDecodeException(VIDEO_PREFIX + video + " ends inside the header of box '" + fourcc + "': the file is truncated");
        }
        boxSize = header.getLong();
        headerSize = 16;
      } else if (boxSize == 0) {
        throw refused("has a '" + fourcc + "' box that runs to the end of " + within + ", which " + LOGIC_VERSION
            + " does not read");
      }
      if (boxSize < headerSize) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + hasA(fourcc) + "box of " + boxSize + " bytes at byte " + offset);
      }
      if (boxSize > end - offset) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + hasA(fourcc) + "box of " + boxSize + " bytes at byte " + offset
            + ", past the end of " + within + " at byte " + end + TRUNCATED);
      }
      return new long[] {boxSize, headerSize, type};
    }

    /** " has a '{@code fourcc}' ", the middle of a message about one of the video's boxes. */
    private static String hasA(String fourcc) {
      return " has a '" + fourcc + "' ";
    }

    private static String fourcc(long[] box) {
      int type = (int) box[2];
      // a box type is four Latin-1 characters
      return String.valueOf(new char[] {(char) (type >>> 24 & 0xff), (char) (type >>> 16 & 0xff), (char) (type >>> 8 & 0xff),
          (char) (type & 0xff)});
    }

    /** Up to {@code length} bytes at {@code position} (fewer at the file's end), ready to read. */
    private ByteBuffer readAt(long position, int length) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      channel.setPosition(position);
      while (buffer.hasRemaining() && channel.read(buffer) > 0) {
        // read all of it
      }
      buffer.flip();
      return buffer;
    }

    /**
     * The stream's declared size against both guards, and every parameter set of its {@code avcC}: JCodec decodes 8-bit 4:2:0
     * progressive H.264 only, and sizes its buffers from them.
     */
    private void checkStream(VideoSampleEntry entry) {
      long width = entry.getWidth();
      long height = entry.getHeight();
      if (width <= 0 || height <= 0) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " declares no frame size");
      }
      checkPixels(width, height, "declares");
      AvcCBox avcC = H264Utils.parseAVCC(entry);
      if (avcC == null || avcC.getSpsList().isEmpty()) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has no H.264 parameter sets (avcC)");
      }
      // every decoder is created from these parameter sets (the track's codec private data), so all are admitted here
      for (ByteBuffer each : avcC.getPpsList()) {
        admit(H264ParameterSetAdmission.ppsRefusal(each));
      }
      for (ByteBuffer each : avcC.getSpsList()) {
        admit(H264ParameterSetAdmission.spsRefusal(each));
        SeqParameterSet sps = H264Utils.readSPS(each);
        growPicture(sps);
      }
    }

    /** Refuses a parameter set {@link H264ParameterSetAdmission} turned away, before JCodec parses it. */
    private void admit(String refusal) {
      if (refusal != null) {
        throw refused(refusal);
      }
    }

    /**
     * Checks {@code sps} and grows the decoder's picture to cover it: the decoder writes whole macroblocks, so its picture is the
     * largest coded size, cropped to the display size afterwards. Width and height may come from different parameter sets, so
     * the grown picture is checked again.
     */
    private void growPicture(SeqParameterSet sps) {
      checkSps(sps);
      codedWidth = Math.max(codedWidth, (sps.picWidthInMbsMinus1 + 1) * MACROBLOCK);
      codedHeight = Math.max(codedHeight, (sps.picHeightInMapUnitsMinus1 + 1) * MACROBLOCK);
      if ((long) codedWidth * codedHeight > MAX_CODED_PIXELS) {
        throw refused("codes pictures up to " + codedWidth + "x" + codedHeight + " across its parameter sets, above the "
            + CEILING);
      }
    }

    /**
     * One sequence parameter set, from the {@code avcC} or carried in a sample: JCodec sizes its per-macroblock state and its
     * reference list from it before any guard sees a decoded frame, so its coded size, frame-number range and reference count
     * are checked first.
     */
    private void checkSps(SeqParameterSet sps) {
      String refusal = spsRefusal(sps);
      if (refusal != null) {
        throw refused(refusal);
      }
    }

    /**
     * The parameter sets a keyframe sample carries in band (JCodec puts the avcC's in front of an IDR, an encoder may add its
     * own) are checked like the avcC's before the sample is decoded, since the decoder parses them as it meets them; the
     * decoder's picture grows to the largest coded size.
     */
    private void checkInBandParameterSets(ByteBuffer data, long sample) {
      List<SeqParameterSet> parameterSets = new ArrayList<>(2);
      try {
        for (ByteBuffer nal : H264Utils.splitFrame(data.duplicate())) {
          int type = nal.remaining() > 1 ? nal.get(nal.position()) & 0x1f : -1;
          if (type == NAL_SPS || type == NAL_PPS) {
            ByteBuffer body = nal.duplicate();
            body.get();
            if (type == NAL_PPS) {
              admit(H264ParameterSetAdmission.ppsRefusal(body));
            } else {
              admit(H264ParameterSetAdmission.spsRefusal(body));
              parameterSets.add(H264Utils.readSPS(body));
            }
          }
        }
      } catch (VideoDecodeException e) {
        throw e;
      } catch (RuntimeException e) {
        throw failure("has an unreadable parameter set at sample " + sample, e);
      }
      for (SeqParameterSet sps : parameterSets) {
        growPicture(sps);
      }
    }

    /** Refuses a frame size above the configured guard (as FFmpeg's strategy) or above JCodec's own ceiling. */
    private void checkPixels(long width, long height, String what) {
      long pixels = width * height;
      if (pixels > maxPixels) {
        throw new UncheckedIOException(new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + VIDEO_PREFIX + video
            + " " + what + " " + width + "x" + height + " = " + pixels + " pixels per frame, above the configured maximum ("
            + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key() + ") of " + maxPixels));
      }
      if (pixels > MAX_PIXELS) {
        throw refused(what + " " + width + "x" + height + " = " + pixels + " pixels per frame, above the " + CEILING);
      }
    }

    /** The media time of the one edit the track may carry, or 0 without one; any other edit list is refused. */
    private long editMediaTime(List<Edit> edits) {
      if (edits == null || edits.isEmpty()) {
        return 0;
      }
      Edit edit = edits.get(0);
      if (edits.size() != 1 || edit.getMediaTime() < 0 || edit.getRate() != 1.0f) {
        StringBuilder list = new StringBuilder();
        for (Edit each : edits) {
          list.append(list.length() == 0 ? "" : ", ").append("[media time ").append(each.getMediaTime()).append(", duration ")
              .append(each.getDuration()).append(", rate ").append(each.getRate()).append(']');
        }
        throw refused("has an edit list of " + edits.size() + " entr" + (edits.size() == 1 ? "y" : "ies") + " (" + list
            + "); " + LOGIC_VERSION + " applies a single entry at rate 1 only");
      }
      return edit.getMediaTime();
    }

    @Override
    public boolean hasNext() {
      while (next == null && !closed) {
        long[] keyframe = nextChosen();
        if (keyframe == null) {
          return false;
        }
        next = frameAt(keyframe[0], keyframe[1]);
      }
      return next != null;
    }

    @Override
    public ExtractedFrame next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ExtractedFrame frame = next;
      next = null;
      return frame;
    }

    /** The next chosen keyframe ({microseconds from the origin, sample number}), walking the sample tables as far as needed. */
    private long[] nextChosen() {
      while (chosen.isEmpty() && !scannedToEnd) {
        if (nextSample >= sampleCount) {
          scannedToEnd = true;
          if (previousKeyframeMicros >= 0) {
            choose(schedule.onEnd(lastFrameMicros), -1, -1);
          }
          break;
        }
        long sample = nextSample++;
        long presentation = decodeTime + compositionOffset() - editMediaTime;
        decodeTime += decodeDelta();
        long micros = toMicros(Math.max(0, presentation));
        lastFrameMicros = Math.max(lastFrameMicros, micros);
        if (isSync(sample)) {
          acceptKeyframe(sample, presentation, micros);
        }
      }
      return chosen.isEmpty() ? null : chosen.removeFirst();
    }

    /**
     * A sync sample at {@code presentation} (track timescale, from the origin), {@code micros} as the schedule sees it: refused
     * before the edit list's start, else offered to the schedule when later than the previous keyframe.
     */
    private void acceptKeyframe(long sample, long presentation, long micros) {
      if (presentation < 0) {
        throw refused("has a keyframe (sample " + sample + ") before the start of its edit list; " + LOGIC_VERSION
            + " does not trim");
      }
      if (micros > previousKeyframeMicros) {
        choose(schedule.onKeyframe(micros), micros, sample);
        previousKeyframeMicros = micros;
        previousKeyframeSample = sample;
      }
    }

    /** The decode duration of the current sample (stts), moving the cursor to the next sample. */
    private long decodeDelta() {
      while (deltaEntry < decodeDeltas.length && deltaUsed >= decodeDeltas[deltaEntry].getSampleCount()) {
        deltaEntry++;
        deltaUsed = 0;
      }
      if (deltaEntry >= decodeDeltas.length) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has more samples than its time-to-sample table (stts) covers");
      }
      deltaUsed++;
      return decodeDeltas[deltaEntry].getSampleDuration();
    }

    /** The composition offset of the current sample (ctts; 0 without one), moving that cursor too. */
    private long compositionOffset() {
      while (offsetEntry < compositionOffsets.length && offsetUsed >= (compositionOffsets[offsetEntry].getCount() & 0xffffffffL)) {
        offsetEntry++;
        offsetUsed = 0;
      }
      if (offsetEntry >= compositionOffsets.length) {
        // no ctts: a present one covers every sample (checkCompositionOffsetsCoverTheTrack)
        return 0;
      }
      offsetUsed++;
      return compositionOffsets[offsetEntry].getOffset();
    }

    /** Whether {@code sample} (from 0) is a sync sample; stss lists them from 1, strictly ascending ({@link #checkSyncSamples}). */
    private boolean isSync(long sample) {
      while (syncIndex < syncSamples.length && syncSamples[syncIndex] - 1L < sample) {
        syncIndex++;
      }
      return syncIndex < syncSamples.length && syncSamples[syncIndex] - 1L == sample;
    }

    /** Queues the keyframes the schedule chose: each is the previous keyframe or the current one ({@code micros}, {@code sample}). */
    private void choose(List<Long> chosenMicros, long micros, long sample) {
      for (long each : chosenMicros) {
        chosen.addLast(new long[] {each, each == micros ? sample : previousKeyframeSample});
      }
    }

    /** Decodes the chosen keyframe; the frame when it is kept, else null. */
    private ExtractedFrame frameAt(long micros, long sample) {
      BufferedImage image = decodeKeyframe(sample);
      long timestampMs = Math.round(micros / (double) MICROS_PER_MILLI);
      String key;
      byte[] jpeg;
      if (dedup.decidesBeforePrepare()) {
        key = dedup.keepDecoded(image);
        if (key == null) {
          return null;
        }
        jpeg = encoder.encode(image, video);
      } else {
        jpeg = encoder.encode(image, video);
        key = dedup.keepPrepared(jpeg);
        if (key == null) {
          return null;
        }
      }
      ExtractedFrame.EncodedImage encoded = new ExtractedFrame.EncodedImage(image.getWidth(), image.getHeight(), ImageContentTypes.JPEG,
          jpeg.length, () -> new ByteArrayInputStream(jpeg));
      return new ExtractedFrame(timestampMs, nextIndex++, true, key, encoded);
    }

    private BufferedImage decodeKeyframe(long sample) {
      long declaredBytes = sampleSizes.getDefaultSize() == 0 ? sampleSizes.getSizes()[(int) sample] : sampleSizes.getDefaultSize();
      if (declaredBytes < 0 || declaredBytes > Math.min(channel.size(), MAX_SAMPLE_BYTES)) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " declares " + (declaredBytes & 0xFFFFFFFFL) + " bytes for keyframe sample "
            + sample + ", more than the file holds or a " + MAX_PIXELS + "-pixel keyframe needs");
      }
      MP4Packet packet = readSample(sample);
      if (packet == null || packet.getData() == null) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " ends inside sample " + sample + TRUNCATED);
      }
      if (!packet.isKeyFrame()) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " sample " + sample + " is listed as a keyframe but is not one");
      }
      checkInBandParameterSets(packet.getData(), sample);
      Picture picture;
      try {
        // a fresh decoder per keyframe: an IDR needs no other frame, and no decoder state outlives the frame
        H264Decoder decoder = SingleThreadedDecoders.of(H264Decoder.createH264DecoderFromCodecPrivate(codecPrivate.duplicate()));
        byte[][] planes = Picture.create(codedWidth, codedHeight, ColorSpace.YUV420J).getData();
        picture = decoder.decodeFrame(packet.getData(), planes);
      } catch (PreprocessorConfigurationException e) {
        throw e;
      } catch (RuntimeException e) {
        throw failure("cannot be decoded at sample " + sample, e);
      }
      if (picture == null) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " keyframe at sample " + sample + " decodes to no picture" + wayOut(extension));
      }
      long width = picture.getCroppedWidth();
      long height = picture.getCroppedHeight();
      if (width * height > maxPixels || width * height > MAX_PIXELS) {
        // the declared size passed the guards, this frame does not: refused before any more work
        checkPixels(width, height, "has a frame of");
      }
      try {
        return AWTUtil.toBufferedImage(picture);
      } catch (RuntimeException e) {
        throw failure("cannot be converted at sample " + sample, e);
      }
    }

    /** Sample {@code sample} (from 0), read through the demuxer's track. */
    private MP4Packet readSample(long sample) {
      try {
        if (!track.gotoFrame(sample)) {
          throw new VideoDecodeException(VIDEO_PREFIX + video + " has no sample " + sample);
        }
        return (MP4Packet) track.nextFrame();
      } catch (VideoDecodeException e) {
        throw e;
      } catch (IOException | RuntimeException e) {
        throw failure("cannot be read at sample " + sample, e);
      }
    }

    private long toMicros(long ticks) {
      return ticks / timescale * MICROS_PER_SECOND + ticks % timescale * MICROS_PER_SECOND / timescale;
    }

    /** A refusal of this video's type: the reason and the way out. */
    private VideoDecodeException refused(String reason) {
      return new VideoDecodeException(VIDEO_PREFIX + video + " " + reason + wayOut(extension));
    }

    /**
     * The failure of a JCodec call: a storage failure under it is reported as such (the caller decides whether it interrupts
     * the round), anything else is the video's data, refused with the way out.
     */
    private RuntimeException failure(String what, Exception cause) {
      for (Throwable each : ExceptionUtil.getCauses(cause)) {
        if (each instanceof BlobReadException) {
          return new UncheckedIOException(VIDEO_PREFIX + video + " " + what + ": storage failure", (BlobReadException) each);
        }
      }
      return new VideoDecodeException(VIDEO_PREFIX + video + " " + what + " by JCodec (" + cause + ")" + wayOut(extension), cause);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      next = null;
      // the demuxer and blob channel are closed whatever the encoder's close threw; the encoder's failure stays the one thrown,
      // an input failure suppressed into it
      try (Release<RuntimeException> inputClose = this::closeInput) {
        if (encoder != null) {
          encoder.close();
        }
      }
    }

    /**
     * Closes the demuxer, which closes the blob channel it reads, then the channel itself (a no-op once the demuxer closed it; the
     * close when opening failed before the demuxer, or when the demuxer's close failed); an unchecked close failure propagates.
     */
    private void closeInput() {
      try {
        if (demuxer != null) {
          demuxer.close();
        }
      } catch (IOException e) {
        // a close failure changes no outcome, but a stream left open (a pooled connection) must leave a trace
        LOG.warn("The demuxer of video {} failed to close", video, e);
      } finally {
        closeChannel();
      }
    }

    private void closeChannel() {
      try {
        if (channel != null) {
          channel.close();
        }
      } catch (IOException e) {
        LOG.warn("The blob channel of video {} failed to close", video, e);
      }
    }
  }
}
