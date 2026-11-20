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
import org.apache.hudi.utilities.unstructured.embedding.prep.FfmpegLibraryVersions;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.CloseOnFailure;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.Release;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobInputStream;

import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avformat.AVFormatContext;
import org.bytedeco.ffmpeg.avformat.AVStream;
import org.bytedeco.ffmpeg.avutil.AVRational;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avformat;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.ffmpeg.global.swscale;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * The FFmpeg {@link FrameExtractionStrategy} through JavaCV (JavaCV 1.5.11 / FFmpeg 7.1, LGPL natives),
 * logic {@value #LOGIC_VERSION}: the closest keyframe every X milliseconds ({@link KeyframeSchedule}), consecutive repeats
 * dropped ({@link FrameDedup}), each kept frame encoded JPEG at quality {@value #JPEG_QUALITY} at its NATIVE size (no
 * resize; JPEG q90 is the shared frame representation). Data prep stores the encoded frame inline as the frame row's
 * payload; compute resizes it in memory.
 *
 * <p>Per video it opens the blob once and starts one grabber over it ({@link SeekableBlobInputStream} over a
 * {@link BlockBufferedBlob}: reads go to storage in 1 MiB blocks, at most two of them kept, never the whole file). The grabber
 * first SCANS: it reads every packet, never asked for a frame (starting it probes the stream, which FFmpeg may do by decoding a
 * few frames), and feeds the keyframe timestamps to the schedule, which chooses the keyframes to keep. Then it DECODES: it seeks
 * to each chosen keyframe and decodes that one frame ({@code thread_count} = {@link VideoFrameConfig#getThreads()}) straight
 * into a BGR {@link BufferedImage}. Frames come out one per {@code next()}: at most one decoded frame, its raster and its
 * encoded JPEG are alive at a time, whatever the video's length; the chosen keyframes are two timestamps each, staged in a
 * sequential local file and deleted on exhaustion or close. A scan that
 * ends in a failure (a cut file, unreadable packets, a storage failure) yields the frames chosen before it, then throws it,
 * where reading and decoding side by side did; a keyframe past the timestamps JavaCV can represent fails the video as
 * incomplete before any frame. (A storage failure part way through a file, a failure of the round, may be
 * reported a few frames later than then.)
 *
 * <p>Time: a frame's timestamp is its keyframe's PTS minus the CONTAINER's
 * start time, clamped at 0, in milliseconds, with the PTS as the FFmpeg demuxer reports them (it applies an MP4/MOV edit list itself, its
 * default). A keyframe the demuxer flags {@code AV_PKT_FLAG_DISCARD} (before the start of a trimmed edit list) is never chosen.
 * The "every X" targets count from that origin. Frame indexes are the dense kept-frame ordinal.
 *
 * <p>A video whose stream declares more than {@link VideoFrameConfig#getMaxPixels()} pixels is refused before any frame is
 * decoded, and a decoded frame above it (a mid-stream size change) before it is hashed or encoded (an
 * {@link UncheckedIOException} over an {@link ImageRefusedException}); corrupt or unreadable data is a
 * {@link VideoDecodeException}; a storage failure under a read is an {@link UncheckedIOException} over a {@link BlobReadException}
 * whose cause is the storage failure (the caller decides whether the source is gone or the read was interrupted).
 *
 * <p>A file cut short is a {@link VideoDecodeException}, never a shorter run of frames presented as the whole video.
 * An MP4 or MOV cut before its index (written last by default) cannot be opened.
 * Whenever FFmpeg takes the duration from the container's own declaration ({@code AVFMT_DURATION_FROM_STREAM}: Matroska, WebM,
 * an MP4 or MOV whose index comes first), the file is refused when the scan ends and the last audio or video packet ends more
 * than {@link #TRUNCATION_TOLERANCE_MICROS} (or two audio or video packet intervals, if more) before the declared end; a data
 * track's single long packet (a MOV timecode) would hide a cut, so it does not count. That end is the declared duration, or the
 * start time plus it when that is earlier: FFmpeg reports some containers' duration as an end time (a Matroska file whose
 * streams all start late) and others' as a length; the cost is that a cut shorter than a positive start offset is missed. An
 * AVI is judged by its extent instead, at open: refused when a RIFF chunk declares more bytes than the file holds
 * ({@link RiffExtent}); its duration is not used, because FFmpeg re-derives a cut AVI's from the packets present and reads a
 * placeholder from one whose header was never patched. Not checked: an AVI whose RIFF size was never patched (written to a
 * pipe), a duration FFmpeg computed from the timestamps (MPEG-TS) or the bitrate, and a container that declares none (a Matroska
 * or WebM written live, a fragmented MP4 without one): such a file cut short yields the frames before the cut.
 */
public class FfmpegKeyframeVideoFrameSource implements FrameExtractionStrategy {

  /**
   * Bytes a clip's buffered block reads hold at most (two 1 MiB blocks). A compile-time constant, so a reader of it (the driver's
   * heap check) does not load this class or FFmpeg.
   */
  public static final long BUFFERED_READ_BYTES = (long) BlockBufferedBlob.BLOCK_BYTES * BlockBufferedBlob.BLOCKS;

  /**
   * The keyframe logic, with FFmpeg/JavaCV decoding, pinned in the video prep logic version instead of the FFmpeg and JavaCV
   * versions. Bump only when a dependency upgrade or code change alters prep output; the golden test below proves it:
   * {@code TestFfmpegKeyframeVideoFrameSource} pins the frames kept (the dedup golden).
   */
  public static final String LOGIC_VERSION = "video-keyframe-v3";
  /** JPEG quality of an encoded frame, the {@link FrameJpegEncoder}'s. */
  public static final int JPEG_QUALITY = FrameJpegEncoder.JPEG_QUALITY;

  private static final Logger LOG = LoggerFactory.getLogger(FfmpegKeyframeVideoFrameSource.class);
  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long MICROS_PER_MILLI = 1000L;
  /** JavaCV and FFmpeg round timestamps independently; one microsecond is the tolerated difference. */
  private static final long TIMESTAMP_TOLERANCE_MICROS = 1;
  /** How far the packets may end before a declared duration (at least; two packet intervals when larger) before a cut is assumed. */
  static final long TRUNCATION_TOLERANCE_MICROS = 250_000L;
  /** Channels of a BGR24 frame, the pixel format the grabber converts every decoded frame to. */
  private static final int BGR_CHANNELS = 3;
  /** How every message about a video starts. */
  private static final String VIDEO_PREFIX = "video ";

  @Override
  public String name() {
    return LOGIC_VERSION;
  }

  /**
   * Checks the frame keys only. FFmpeg itself (org.bytedeco, not shipped by Hudi: the job adds its jars) is loaded by the first
   * video's frames, so the driver of a job that prepares no video never needs it.
   */
  @Override
  public void validateConfig(TypedProperties props) {
    VideoFrameConfig.from(props);
  }

  @Override
  public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) throws IOException {
    VideoFrameConfig config = VideoFrameConfig.from(props);
    if (!(video instanceof SeekableBlobBytesSupplier)) {
      throw new VideoDecodeException(VIDEO_PREFIX + file + " bytes are not seekable");
    }
    return new KeyframeFrames(file, (SeekableBlobBytesSupplier) video, config);
  }

  /** {@code pts} in {@code timeBase} units, in microseconds. */
  static long toMicros(long pts, AVRational timeBase) {
    // a * b / c without overflowing on a fine time base (a 1 ns one overflows a plain product after 2.56 h), truncated toward
    // zero as the plain product was, so a time base finer than a microsecond keeps the timestamps it gave before
    return avutil.av_rescale_rnd(pts, timeBase.num() * MICROS_PER_SECOND, timeBase.den(), avutil.AV_ROUND_ZERO);
  }

  /**
   * Whether JavaCV's timestamp of {@code pts} overflows: FFmpegFrameGrabber times a frame as {@code 1000000L * pts * num / den}
   * in plain long arithmetic (a 1 ns time base overflows past 2.56 h) and seeks by that value, so such a keyframe cannot be
   * decoded and matched through it.
   */
  static boolean javacvTimestampOverflows(long pts, AVRational timeBase) {
    return productOverflows(MICROS_PER_SECOND, pts) || productOverflows(MICROS_PER_SECOND * pts, timeBase.num());
  }

  private static boolean productOverflows(long a, long b) {
    return Math.multiplyHigh(a, b) != ((a * b) >> 63);
  }

  /**
   * Starts {@code grabber} over {@code stream}. A grabber that failed to start still holds native contexts, so whatever way it
   * failed it is released (its close closes the stream too) before the failure propagates: an exception as {@code classify}
   * makes it, an Error (from the natives, or memory) as itself, never as the video's failure.
   */
  static FFmpegFrameGrabber startOrRelease(FFmpegFrameGrabber grabber, SeekableBlobInputStream stream, FileMeta video,
                                           Function<Exception, RuntimeException> classify) {
    boolean started = false;
    RuntimeException failure = null;
    try {
      grabber.start();
      started = true;
    } catch (Exception e) {
      failure = classify.apply(e);
    } finally {
      if (!started) {
        // an Error from the release propagates in place of the start's failure
        KeyframeFrames.closeQuietly(grabber, stream, video);
      }
    }
    if (failure != null) {
      throw failure;
    }
    return grabber;
  }

  /** The kept frames of one video, decoded and encoded on demand. */
  private static final class KeyframeFrames implements ClosableIterator<ExtractedFrame> {
    /** {@link #nextChosen()} at the end: no keyframe left. */
    private static final long[] NO_KEYFRAME = new long[0];
    private final FileMeta video;
    private final KeyframeSchedule schedule;
    private final FrameDedup dedup;
    /** The chosen keyframes, in order, as {relative to the origin, as the demuxer reported it} in microseconds. */
    private final TimestampSpool chosen = new TimestampSpool();
    private final Java2DFrameConverter converter = new Java2DFrameConverter();
    private final long maxPixels;
    private BlockBufferedBlob blob;
    private SeekableBlobInputStream stream;
    private FFmpegFrameGrabber grabber;
    /** The blob's failure count when the scan ended: a decode that sees more met a storage failure of its own. */
    private long failuresAfterScan;
    private FrameJpegEncoder encoder;
    private int videoStream;
    private AVRational timeBase;
    private long streamStartMicros;
    /** The container's start time, the origin of every frame time (0 when the container declares none). */
    private long originMicros;
    /** The last chosen-eligible keyframe seen, as the schedule saw it (from the origin) and as the demuxer reported it. */
    private long previousRelativeMicros = -1;
    private long previousKeyframeMicros;
    private long lastFrameMicros = -1;
    /** The latest audio or video packet end, in microseconds on the demuxer's timeline (-1 before the first timed one). */
    private long packetsEndMicros = -1;
    /** Per stream, the PTS (microseconds) of its previous timed packet, or Long.MIN_VALUE. */
    private long[] previousPtsMicros;
    /** Per stream, its largest packet duration and its smallest positive PTS step (Long.MAX_VALUE until seen), microseconds. */
    private long[] largestDurationMicros;
    /** Per stream, its time base and whether it is audio or video. */
    private AVRational[] streamTimeBases;
    private boolean[] mediaStreams;
    /** An AVI: its extent is checked at open, never its duration (re-derived when cut, a placeholder when never patched). */
    private boolean riff;
    private long[] smallestStepMicros;
    private boolean scanned;
    /** How the scan ended when it did not reach the end of a whole file: thrown once the frames chosen before it are out. */
    private RuntimeException scanFailure;
    private long nextIndex;
    private ExtractedFrame next;
    private boolean closed;

    private KeyframeFrames(FileMeta video, SeekableBlobBytesSupplier bytes, VideoFrameConfig config) throws IOException {
      this.video = video;
      this.schedule = new KeyframeSchedule(config.getIntervalMs() * MICROS_PER_MILLI);
      this.dedup = new FrameDedup(config.getDedupMethod(), config.getMaxDistance());
      this.maxPixels = config.getMaxPixels();
      // any way out of opening, an Error from the natives included, leaves no grabber and no blob stream open; a close failure
      // is suppressed into the opening's failure
      try (CloseOnFailure<RuntimeException> guard = new CloseOnFailure<>(this::close)) {
        open(bytes, config);
        guard.succeeded();
      }
    }

    private void open(SeekableBlobBytesSupplier bytes, VideoFrameConfig config) throws IOException {
      Natives.init();
      SeekableBlob source = bytes.openSeekable();
      // not wrapped yet, so close() would not reach it
      try (CloseOnFailure<IOException> guard = new CloseOnFailure<>(source::close)) {
        blob = new BlockBufferedBlob(source);
        guard.succeeded();
      }
      stream = new SeekableBlobInputStream(blob);
      grabber = start(new FFmpegFrameGrabber(stream), stream, config);
      if (stream.storageFailure() != null) {
        // FFmpeg probed the stream over a read that failed in storage: what it found (or did not) says nothing about the video
        throw failure("could not be probed", null, true);
      }
      riff = "avi".equals(grabber.getFormatContext().iformat().name().getString());
      if (riff) {
        String cut = RiffExtent.truncation(bytes, video);
        if (cut != null) {
          throw new VideoDecodeException(VIDEO_PREFIX + video + " " + cut + ": the file is truncated");
        }
      }
      int streams = grabber.getFormatContext().nb_streams();
      previousPtsMicros = new long[streams];
      Arrays.fill(previousPtsMicros, Long.MIN_VALUE);
      largestDurationMicros = new long[streams];
      streamTimeBases = new AVRational[streams];
      mediaStreams = new boolean[streams];
      for (int i = 0; i < streams; i++) {
        AVStream each = grabber.getFormatContext().streams(i);
        streamTimeBases[i] = each.time_base();
        int type = each.codecpar().codec_type();
        mediaStreams[i] = type == avutil.AVMEDIA_TYPE_VIDEO || type == avutil.AVMEDIA_TYPE_AUDIO;
      }
      smallestStepMicros = new long[streams];
      Arrays.fill(smallestStepMicros, Long.MAX_VALUE);
      videoStream = grabber.getVideoStream();
      if (videoStream < 0) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " has no video stream");
      }
      long pixels = (long) grabber.getImageWidth() * grabber.getImageHeight();
      if (grabber.getImageWidth() <= 0 || grabber.getImageHeight() <= 0) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " declares no frame size");
      }
      if (pixels > config.getMaxPixels()) {
        throw new UncheckedIOException(new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + VIDEO_PREFIX + video
            + " declares " + grabber.getImageWidth() + "x" + grabber.getImageHeight() + " = " + pixels
            + " pixels per frame, above the configured maximum (" + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key() + ") of "
            + config.getMaxPixels()));
      }
      AVStream videoAvStream = grabber.getFormatContext().streams(videoStream);
      timeBase = videoAvStream.time_base();
      streamStartMicros = videoAvStream.start_time() == avutil.AV_NOPTS_VALUE ? 0 : toMicros(videoAvStream.start_time(), timeBase);
      long containerStart = grabber.getFormatContext().start_time();
      originMicros = containerStart == avutil.AV_NOPTS_VALUE ? 0 : containerStart;
      encoder = new FrameJpegEncoder();
    }

    private FFmpegFrameGrabber start(FFmpegFrameGrabber grabber, SeekableBlobInputStream stream, VideoFrameConfig config) {
      // the decoder's thread count is explicit (FFmpeg's 0 means "one per core this process may use")
      grabber.setVideoOption("threads", String.valueOf(config.getThreads()));
      grabber.setImageMode(FrameGrabber.ImageMode.COLOR);
      grabber.setPixelFormat(avutil.AV_PIX_FMT_BGR24);
      // the YUV to BGR conversion is bit-exact, so a frame's pixels (its dedup key and its JPEG) do not depend on the CPU
      grabber.setImageScalingFlags(swscale.SWS_BILINEAR | swscale.SWS_ACCURATE_RND | swscale.SWS_BITEXACT);
      return startOrRelease(grabber, stream, video, e -> failure("cannot be opened", e, stream.storageFailure() != null));
    }

    @Override
    public boolean hasNext() {
      while (next == null && !closed) {
        long[] keyframe = nextChosen();
        if (keyframe.length == 0) {
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

    /**
     * The next chosen keyframe ({relative, as reported}), scanning the whole file first; {@link #NO_KEYFRAME} at the end. A scan
     * that failed throws its failure once every keyframe chosen before it was handed out.
     */
    private long[] nextChosen() {
      if (!scanned) {
        scanned = true;
        scan();
        failuresAfterScan = blob.failures();
      }
      long[] nextTimestamp = chosen.next();
      if (nextTimestamp != null) {
        return nextTimestamp;
      }
      if (scanFailure != null) {
        throw scanFailure;
      }
      return NO_KEYFRAME;
    }

    /** Reads every packet, choosing keyframes as they come; how it ended, when not at the end of a whole file, is {@link #scanFailure}. */
    private void scan() {
      while (true) {
        AVPacket packet;
        try {
          packet = grabber.grabPacket();
        } catch (FrameGrabber.Exception e) {
          scanFailure = failure("cannot be read", e, stream.storageFailure() != null);
          return;
        }
        if (packet == null) {
          endScan();
          return;
        }
        try {
          trackPacketEnd(packet);
          if (!chooseFrom(packet)) {
            return;
          }
        } finally {
          avcodec.av_packet_unref(packet);
        }
      }
    }

    /** At the end of the packets: how the scan ended ({@link #scanFailure}), else the schedule's choice at the end. */
    private void endScan() {
      if (stream.storageFailure() != null) {
        // the scan stopped on a storage failure, not at the video's end: the remaining targets are not decidable
        scanFailure = failure("could not be read to its end", null, true);
        return;
      }
      try {
        checkNotCutShort();
      } catch (VideoDecodeException cut) {
        scanFailure = cut;
        return;
      }
      if (previousRelativeMicros >= 0) {
        choose(schedule.onEnd(lastFrameMicros), -1, -1);
      }
    }

    /**
     * Chooses from {@code packet} when it is a timed, presented video keyframe later than the previous one; false when the scan
     * must stop ({@link #scanFailure} set).
     */
    private boolean chooseFrom(AVPacket packet) {
      // a packet the demuxer flags DISCARD lies before the edit list's start: it is never presented, so never chosen
      if (packet.stream_index() != videoStream || packet.pts() == avutil.AV_NOPTS_VALUE
          || (packet.flags() & avcodec.AV_PKT_FLAG_DISCARD) != 0) {
        return true;
      }
      long ptsMicros = toMicros(packet.pts(), timeBase);
      long relativeMicros = Math.max(0, ptsMicros - originMicros);
      lastFrameMicros = Math.max(lastFrameMicros, relativeMicros);
      if ((packet.flags() & avcodec.AV_PKT_FLAG_KEY) == 0 || relativeMicros <= previousRelativeMicros) {
        return true;
      }
      if (javacvTimestampOverflows(packet.pts(), timeBase)) {
        // JavaCV cannot seek to it or time it, so it and every later keyframe cannot be extracted: the video would be
        // incomplete, which is a failure of the whole video, before any of its frames
        chosen.close();
        scanFailure = new VideoDecodeException(VIDEO_PREFIX + video + " has a keyframe at pts " + packet.pts()
            + " (time base " + timeBase.num() + "/" + timeBase.den() + ") past the timestamps FFmpegFrameGrabber can "
            + "represent (1000000 x pts x time base numerator overflows a long): its keyframes from there on cannot be "
            + "extracted, so the video is refused as incomplete");
        return false;
      }
      choose(schedule.onKeyframe(relativeMicros), relativeMicros, ptsMicros);
      previousRelativeMicros = relativeMicros;
      previousKeyframeMicros = ptsMicros;
      return true;
    }

    /**
     * Records a timed packet's stream interval and, for audio and video, where it ends; the interval is the largest declared
     * packet duration, or the smallest PTS step when packets declare none (the smallest, because reordered B-frames make some
     * steps several frames).
     */
    private void trackPacketEnd(AVPacket packet) {
      int index = packet.stream_index();
      if (packet.pts() == avutil.AV_NOPTS_VALUE || index < 0 || index >= previousPtsMicros.length) {
        return;
      }
      AVRational base = streamTimeBases[index];
      long scale = base.num() * MICROS_PER_SECOND;
      // av_rescale: a * b / c without overflowing on a fine time base
      long ptsMicros = avutil.av_rescale(packet.pts(), scale, base.den());
      long durationMicros = packet.duration() > 0 ? avutil.av_rescale(packet.duration(), scale, base.den()) : 0;
      if (previousPtsMicros[index] != Long.MIN_VALUE && ptsMicros > previousPtsMicros[index]) {
        smallestStepMicros[index] = Math.min(smallestStepMicros[index], ptsMicros - previousPtsMicros[index]);
      }
      largestDurationMicros[index] = Math.max(largestDurationMicros[index], durationMicros);
      previousPtsMicros[index] = ptsMicros;
      if (mediaStreams[index]) {
        // a data track (a MOV timecode) may be one packet as long as the movie: only audio and video show where the file ends
        packetsEndMicros = Math.max(packetsEndMicros, ptsMicros + durationMicros);
      }
    }

    /**
     * At the end of the scan: when the container declares its duration, the packets must reach its end (within the tolerance);
     * a file whose packets stop well before it was cut.
     */
    private void checkNotCutShort() {
      AVFormatContext format = grabber.getFormatContext();
      long declared = format.duration();
      if (riff || format.duration_estimation_method() != avformat.AVFMT_DURATION_FROM_STREAM || declared == avutil.AV_NOPTS_VALUE
          || declared <= 0 || packetsEndMicros < 0) {
        return;
      }
      // a length from the start, or an end time: the earlier reading, so a file whose streams all start late is never refused
      long declaredEndMicros = Math.min(declared, originMicros + declared);
      long interval = 0;
      for (int i = 0; i < largestDurationMicros.length; i++) {
        if (!mediaStreams[i]) {
          // a subtitle cue may last a minute: only audio and video packets set how far a cut may hide
          continue;
        }
        long step = smallestStepMicros[i] == Long.MAX_VALUE ? 0 : smallestStepMicros[i];
        interval = Math.max(interval, largestDurationMicros[i] > 0 ? largestDurationMicros[i] : step);
      }
      long tolerance = Math.max(TRUNCATION_TOLERANCE_MICROS, 2 * interval);
      if (declaredEndMicros - packetsEndMicros > tolerance) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " ends at " + Math.max(0, packetsEndMicros - originMicros) / MICROS_PER_MILLI
            + " ms, before its declared end at " + (declaredEndMicros - originMicros) / MICROS_PER_MILLI + " ms: the file is truncated");
      }
    }

    /**
     * Queues the keyframes the schedule chose: each is the previous keyframe or the current one ({@code relativeMicros} as the
     * schedule saw it, {@code ptsMicros} as the demuxer reported it; -1 at the end of the stream).
     */
    private void choose(List<Long> chosenRelative, long relativeMicros, long ptsMicros) {
      for (long relative : chosenRelative) {
        chosen.add(relative, relative == relativeMicros ? ptsMicros : previousKeyframeMicros);
      }
    }

    /** Decodes the chosen keyframe (at {@code keyframeMicros} as the demuxer reported it); the frame when it is kept, else null. */
    private ExtractedFrame frameAt(long relativeMicros, long keyframeMicros) {
      BufferedImage image = decodeKeyframe(keyframeMicros);
      long timestampMs = Math.round(relativeMicros / (double) MICROS_PER_MILLI);
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

    private BufferedImage decodeKeyframe(long keyframeMicros) {
      Frame frame;
      try {
        grabber.setVideoTimestamp(keyframeMicros - streamStartMicros);
        frame = grabber.grabImage();
      } catch (FrameGrabber.Exception e) {
        throw failure("cannot be decoded at " + keyframeMicros + "us", e, decodeMetStorageFailure());
      }
      if (decodeMetStorageFailure()) {
        throw failure("could not be read at " + keyframeMicros + "us", null, true);
      }
      if (frame == null || !frame.keyFrame || Math.abs(frame.timestamp - keyframeMicros) > TIMESTAMP_TOLERANCE_MICROS) {
        throw failure("seek to the keyframe at " + keyframeMicros + "us landed on "
            + (frame == null ? "the end of the stream" : frame.timestamp + "us, keyframe=" + frame.keyFrame), null, false);
      }
      if ((long) frame.imageWidth * frame.imageHeight > maxPixels) {
        // the declared size passed the guard, this frame does not (a mid-stream resolution change): refused before any more work
        throw new UncheckedIOException(new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + VIDEO_PREFIX + video
            + " has a " + frame.imageWidth + "x" + frame.imageHeight + " frame at " + keyframeMicros + "us, above the configured "
            + "maximum (" + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key() + ") of " + maxPixels + " pixels"));
      }
      // the grabber converts every frame to BGR24, 3 bytes a pixel. JavaCV derives the channel count as the row stride over the
      // width, and pads a BGR24 row to a multiple of 64 pixels, so a frame 129 to 144 pixels wide (65 to 96, or at most 48) would
      // read as 4 or more channels: a 4-byte ABGR image of the wrong pixels. The stride stays as JavaCV set it.
      frame.imageChannels = BGR_CHANNELS;
      // the converter reuses one TYPE_3BYTE_BGR raster per video: it is consumed (hashed, encoded) before the next decode
      return converter.getBufferedImage(frame);
    }

    /** Whether the decodes since the scan met a storage failure of their own. */
    private boolean decodeMetStorageFailure() {
      return blob.failures() > failuresAfterScan;
    }

    /**
     * The failure of an FFmpeg call: a storage failure under it ({@code storageFailed}) is reported as such (the caller decides
     * whether it interrupts the round), anything else is the video's data.
     */
    private RuntimeException failure(String what, Exception cause, boolean storageFailed) {
      IOException storage = storageFailed && stream != null ? stream.storageFailure() : null;
      if (storage != null) {
        BlobReadException read = storage instanceof BlobReadException ? (BlobReadException) storage : new BlobReadException(storage);
        if (cause != null) {
          read.addSuppressed(cause);
        }
        return new UncheckedIOException(VIDEO_PREFIX + video + " " + what + ": storage failure", read);
      }
      return new VideoDecodeException(VIDEO_PREFIX + video + " " + what, cause);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      next = null;
      // Resources close in reverse declaration order, preserving every close even when a prior close fails.
      try (Release<RuntimeException> encoderClose = this::closeEncoder;
           Release<RuntimeException> converterClose = converter::close;
           Release<RuntimeException> grabberClose = () -> closeQuietly(grabber, stream, video)) {
        chosen.close();
      }
    }

    /** Closes the JPEG encoder, when opening got that far. */
    private void closeEncoder() {
      if (encoder != null) {
        encoder.close();
      }
    }

    private static void closeQuietly(FFmpegFrameGrabber grabber, SeekableBlobInputStream stream, FileMeta video) {
      Exception failure = null;
      try {
        if (grabber != null) {
          // stops and releases the native contexts; closes the stream it was given
          grabber.close();
        }
      } catch (Exception e) {
        failure = e;
      } finally {
        // the stream's close is idempotent: a no-op after the grabber closed it, a close when the grabber's close failed first
        if (stream != null) {
          try {
            stream.close();
          } catch (Exception e) {
            if (failure == null) {
              failure = e;
            } else {
              failure.addSuppressed(e);
            }
          }
        }
      }
      if (failure != null) {
        // a close failure changes no outcome, but a stream left open (a pooled connection) must leave a trace
        LOG.warn("The FFmpeg grabber or blob stream of video {} failed to close", video, failure);
      }
    }
  }

  /**
   * Once per JVM, before the first grabber: the FFmpeg jars must be the versions this build declares (else the task
   * fails, {@link FfmpegLibraryVersions#requireDeclared}), and FFmpeg's own logging is quiet (errors only).
   */
  private static final class Natives {
    private static volatile boolean initialized;

    private static void init() {
      if (!initialized) {
        synchronized (Natives.class) {
          if (!initialized) {
            FfmpegLibraryVersions.requireDeclared(Natives.class.getClassLoader());
            avutil.av_log_set_level(avutil.AV_LOG_ERROR);
            initialized = true;
          }
        }
      }
    }
  }
}
