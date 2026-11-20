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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.prep.FfmpegLibraryVersions;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.CloseOnFailure;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobInputStream;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.bytedeco.ffmpeg.avformat.AVFormatContext;
import org.bytedeco.ffmpeg.avformat.AVStream;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.ffmpeg.avutil.AVRational;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * The real {@link AudioSegmentSource} on FFmpeg through JavaCV (JavaCV 1.5.11 / FFmpeg 7.1, LGPL natives),
 * logic {@value #LOGIC_VERSION}: the video's first audio stream, decoded at its own rate, mixed to mono, resampled to
 * {@link AudioConfigKeys#SAMPLE_RATE} on the absolute timeline ({@link AnchoredResampler}) and cut into speech segments by an energy
 * gate over 30 ms windows ({@link EnergyVadSegmenter}), each at most {@link AudioConfigKeys#SEGMENT_MAX_MS} long and
 * {@link AudioConfigKeys#SEGMENT_MAX_BYTES} of pcm_s16le.
 *
 * <p>The video is opened the way the frame source opens it ({@link SeekableBlobInputStream}: seeks, never a buffered copy of the
 * file) by one grabber that decodes audio only (video packets are read and dropped, never decoded). Segments come out one per
 * {@code next()}; one segment's payload is held at a time, in a buffer reused for the next one, so the heap needed is one segment
 * plus fixed terms whatever the video's length. A video with several audio streams is read from the first.
 *
 * <p>Resume: with {@code startMs > 0} the grabber seeks to {@value #PRE_ROLL_MS} ms before it (decoder and filter history) and
 * segmentation restarts at the first window at or after {@code startMs}; this is exact because the segmenter's state after a segment
 * depends only on the audio after its end and the resampler's output only on the audio around each sample. With
 * {@code startMs = 0} and {@code startSegment > 0} the video is decoded from the start and the first segments are skipped; so
 * is a track of any codec but MP3 at any {@code startMs}: a resumed segment must be byte for byte the one a run from the start
 * gives, and only MP3's decoder is known to give the same samples after a seek (FFmpeg's AAC decoder does not).
 *
 * <p>Failures: a storage failure under any read (the stream remembers it; JavaCV reports it as an end of file) is an
 * {@link UncheckedIOException} over a {@link BlobReadException} (an interruption of the prep run); unreadable data (no seekable
 * bytes, a container or
 * codec FFmpeg cannot read, a truncated file, a track longer than {@link AudioConfigKeys#MAX_DURATION_MS}) is a
 * {@link VideoDecodeException} (a content failure of the video); an invalid key is an {@link IllegalArgumentException}. No audio
 * stream, or no window at
 * or above the threshold, is an empty iterator ({@link Segments#endReason()} says which).
 */
public class FfmpegAudioSegmentSource implements AudioSegmentSource {

  /**
   * The segmenting logic, with FFmpeg/JavaCV decoding and resampling, pinned in the audio prep logic version instead of the FFmpeg
   * and JavaCV versions. Bump only when a dependency upgrade or code change alters prep output; the golden test below proves it:
   * {@code TestFfmpegAudioSegmentSource} checks the segments of generated clips.
   */
  public static final String LOGIC_VERSION = "ffmpeg-vad-energy-v1";
  public static final String ENCODING = "pcm_s16le";
  /** Audio decoded before a resume point and dropped, so the decoder (MP3 bit reservoir) and filter have history. */
  public static final long PRE_ROLL_MS = 1000;
  /** Why a video produced no segment. */
  public static final String REASON_NO_AUDIO_STREAM = "no audio stream";
  public static final String REASON_NO_SPEECH = "no window at or above the speech threshold";

  private static final Logger LOG = LoggerFactory.getLogger(FfmpegAudioSegmentSource.class);
  private static final long MICROS_PER_SECOND = 1_000_000L;
  /** How every message about a video starts. */
  private static final String VIDEO_PREFIX = "video ";
  private static final long MICROS_PER_MILLI = 1000L;
  /**
   * After a seek, the first decoded frame must start at least this long before the first input sample the output needs: the frames
   * right after a seek are decoder warm-up (an MP3 frame lacks its bit reservoir) and must not reach a kept sample.
   */
  private static final long SEEK_MARGIN_MS = 100;
  /** A frame whose timestamp is further than this from the running sample count re-anchors the timeline (a gap or a jump). */
  private static final long ANCHOR_TOLERANCE_MS = 10;
  /**
   * The highest input sample rate a track may declare: the resampler's kernel table and its work per output sample grow with the
   * declared rate, so a header declaring more is the video's bad input, refused before any sample is resampled.
   */
  private static final int MAX_INPUT_SAMPLE_RATE = 384_000;

  @Override
  public String logicVersion() {
    return LOGIC_VERSION;
  }

  /**
   * Checks the keys only. FFmpeg itself (org.bytedeco, not shipped by Hudi: the job adds its jars) is loaded by the first video's
   * segments, so the driver of a job that prepares no video never needs it, even with audio on.
   */
  @Override
  public void validate(TypedProperties props) {
    FfmpegAudioConfig.from(props);
  }

  @Override
  public Segments segments(AudioInput audio, long startSegment, long startMs, TypedProperties props) throws IOException {
    FfmpegAudioConfig config = FfmpegAudioConfig.from(props);
    if (startSegment < 0 || startMs < 0) {
      throw new IllegalArgumentException("startSegment and startMs must not be negative: " + startSegment + ", " + startMs);
    }
    BlobBytesSupplier bytes = audio.bytes();
    if (bytes == null) {
      throw new VideoDecodeException(VIDEO_PREFIX + describe(audio) + " has no bytes (its blob row is gone)");
    }
    if (!(bytes instanceof SeekableBlobBytesSupplier)) {
      throw new VideoDecodeException(VIDEO_PREFIX + describe(audio) + " bytes are not seekable");
    }
    return new Segments(describe(audio), (SeekableBlobBytesSupplier) bytes, startSegment, startMs, config,
        maxSegmentWindows(config), this::newGrabber);
  }

  /** The grabber over a video's stream; a test overrides it to stand in for FFmpeg's probe. */
  FFmpegFrameGrabber newGrabber(InputStream stream) {
    return new FFmpegFrameGrabber(stream);
  }

  /** The most windows one segment holds; a test overrides it to show what an unbounded segment costs. */
  int maxSegmentWindows(FfmpegAudioConfig config) {
    return config.getMaxSegmentWindows();
  }

  private static String describe(AudioInput audio) {
    return audio.sourcePath() == null ? audio.sourcePathSha() : audio.sourcePathSha() + " (" + audio.sourcePath() + ")";
  }

  /** The speech segments of one video, decoded on demand. */
  public static final class Segments implements ClosableIterator<AudioSegment> {
    private final String video;
    private final FfmpegAudioConfig config;
    private final Function<InputStream, FFmpegFrameGrabber> grabbers;
    private final int windowSamples;
    private long maxDurationSamples;
    private SeekableBlobInputStream stream;
    private FFmpegFrameGrabber grabber;
    private AnchoredResampler resampler;
    private EnergyVadSegmenter segmenter;
    private long originMicros;
    private int inRate;
    private int channels;
    /** Input samples a frame's timestamp may be off the running count before it counts as a gap or an overlap. */
    private long anchorTolerance;
    /** The first input sample index past {@link AudioConfigKeys#MAX_DURATION_MS}. */
    private long maxDurationInputs;
    /** True while the first frame after a resume seek is awaited; a seek that lands too late sets {@link #seekOvershot}. */
    private boolean seeking;
    private boolean seekOvershot;

    /** The frame being consumed: its interleaved samples and read position. */
    private FloatBuffer frameSamples;
    /** Silent input samples still to push before the frame's own (a gap in the stream's timestamps). */
    private long gapInputs;
    private boolean anchored;
    private boolean decodedToEnd;
    private boolean finished;
    /** Output samples not yet handed to a window, in order; the first is the sample at {@link #nextSample}. */
    private short[] pending;
    private int pendingCount;
    /** Absolute index of the next output sample a window takes. */
    private long nextSample;

    private long nextIndex;
    private long toSkip;
    private SegmentView next;
    private SegmentView current;
    private long generation;
    private String endReason;
    private boolean closed;

    private Segments(String video, SeekableBlobBytesSupplier bytes, long startSegment, long startMs, FfmpegAudioConfig config,
                     int maxSegmentWindows, Function<InputStream, FFmpegFrameGrabber> grabbers) throws IOException {
      this.video = video;
      this.config = config;
      this.grabbers = grabbers;
      this.windowSamples = config.getWindowSamples();
      this.pending = new short[2 * windowSamples];
      // any way out of opening, an Error from the natives included, leaves no grabber and no blob stream open; a close failure
      // is suppressed into the opening's failure
      try (CloseOnFailure<RuntimeException> guard = new CloseOnFailure<>(this::close)) {
        open(bytes, startSegment, startMs, maxSegmentWindows);
        guard.succeeded();
      }
    }

    private void open(SeekableBlobBytesSupplier bytes, long startSegment, long startMs, int maxSegmentWindows) throws IOException {
      Natives.init();
      startGrabber(bytes);
      if (grabber.getAudioStream() < 0 || grabber.getAudioChannels() <= 0 || grabber.getSampleRate() <= 0) {
        endReason = REASON_NO_AUDIO_STREAM;
        finished = true;
        LOG.info("Video {} has no audio stream: no speech segments", video);
        return;
      }
      if (grabber.getSampleRate() > MAX_INPUT_SAMPLE_RATE) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " declares an audio sample rate of " + grabber.getSampleRate()
            + " Hz, above the " + MAX_INPUT_SAMPLE_RATE + " Hz an audio track may declare");
      }
      AVStream audio = grabber.getFormatContext().streams(grabber.getAudioStream());
      AVRational timeBase = audio.time_base();
      if (timeBase.num() <= 0 || timeBase.den() <= 0) {
        throw failure("declares an invalid audio time base", null);
      }
      long durationMicros = audio.duration() == avutil.AV_NOPTS_VALUE ? 0
          : avutil.av_rescale_rnd(audio.duration(), (long) timeBase.num() * MICROS_PER_SECOND, timeBase.den(), avutil.AV_ROUND_ZERO);
      if (durationMicros > config.getMaxDurationMs() * MICROS_PER_MILLI) {
        throw new VideoDecodeException(VIDEO_PREFIX + video + " lasts " + durationMicros / MICROS_PER_MILLI + " ms, above "
            + configuredMaximum());
      }
      long start = grabber.getFormatContext().start_time();
      originMicros = start == avutil.AV_NOPTS_VALUE ? 0 : start;
      inRate = grabber.getSampleRate();
      channels = grabber.getAudioChannels();
      long audioStart = audio.start_time() == avutil.AV_NOPTS_VALUE ? originMicros
          : avutil.av_rescale_rnd(audio.start_time(), (long) timeBase.num() * MICROS_PER_SECOND, timeBase.den(), avutil.AV_ROUND_ZERO);
      long offsetMicros = Math.max(0, audioStart - originMicros);
      // a time base in which every sample has a whole timestamp (mp4 and mov use 1 / sample rate): each frame's place is exactly
      // its timestamp, in a run from the start and after a seek alike; otherwise (Matroska's 1 ms, MPEG-TS's 90 kHz) timestamps
      // are rounded, so frames follow the running count and a resume decodes from the start instead of seeking
      boolean sampleExact = timeBase.num() > 0 && timeBase.den() % ((long) timeBase.num() * inRate) == 0;
      anchorTolerance = sampleExact ? 0 : inRate * ANCHOR_TOLERANCE_MS / 1000;
      long endMicros = Math.addExact(offsetMicros, config.getMaxDurationMs() * MICROS_PER_MILLI);
      maxDurationInputs = avutil.av_rescale_rnd(endMicros, inRate, MICROS_PER_SECOND, avutil.AV_ROUND_UP);
      maxDurationSamples = avutil.av_rescale_rnd(endMicros, config.getSampleRate(), MICROS_PER_SECOND, avutil.AV_ROUND_UP);
      resampler = new AnchoredResampler(inRate, config.getSampleRate(), this::accept);
      positionAt(bytes, startSegment, startMs, sampleExact);
      segmenter = new EnergyVadSegmenter(this::readWindow, windowSamples, config.getVadThresholdDb(), config.getMinSilenceWindows(),
          maxSegmentWindows, nextSample / windowSamples);
    }

    /**
     * Positions the decode at the start of segment {@code startSegment} (at {@code startMs}): by seeking where that decodes the
     * samples a run from the start does, else by decoding from the start and skipping the segments before it.
     */
    private void positionAt(SeekableBlobBytesSupplier bytes, long startSegment, long startMs, boolean sampleExact)
        throws IOException {
      long seekMs = Math.max(0, startMs - PRE_ROLL_MS);
      // only MP3 is known to decode the same samples after a seek as from the start (AAC does not): any other codec resumes by
      // decoding from the start, so a resumed payload is the one a run from the start gives
      AVStream audio = grabber.getFormatContext().streams(grabber.getAudioStream());
      AVRational timeBase = audio.time_base();
      // JavaCV adds the audio stream's start to a seek and multiplies before dividing. Replay offset streams or an unsafe
      // timestamp scale; replay also keeps the payload identical when a seek cannot establish the required pre-roll.
      boolean safeSeekScale = seekMs <= Long.MAX_VALUE / MICROS_PER_MILLI
          && seekMs * MICROS_PER_MILLI <= Long.MAX_VALUE / timeBase.den();
      boolean seekDeterministic = sampleExact && grabber.getAudioCodec() == avcodec.AV_CODEC_ID_MP3
          && originMicros == 0 && audio.start_time() == 0 && safeSeekScale;
      if (startMs > 0 && seekMs > 0 && !seekDeterministic) {
        LOG.info("Video {}: resuming at {} ms by decoding from the start ({} audio is not resumed by seeking)", video, startMs,
            sampleExact ? "codec " + grabber.getAudioCodecName() : "this container's");
      }
      if (startMs > 0 && (seekMs == 0 || seekDeterministic)) {
        long startSample = startMs * config.getSampleRate() / 1000;
        long firstWindow = (startSample + windowSamples - 1) / windowSamples;
        nextSample = firstWindow * windowSamples;
        nextIndex = startSegment;
        if (seekMs > 0 && !seekTo(seekMs)) {
          // the seek landed past the pre-roll: decode from the start and skip instead
          LOG.info("Video {}: the audio seek to {} ms landed late; resuming by decoding from the start", video, seekMs);
          closeGrabber();
          startGrabber(bytes);
          resampler.restartAt(0);
          resumeBySkipping(startSegment);
        }
      } else {
        resumeBySkipping(startSegment);
      }
    }

    private void startGrabber(SeekableBlobBytesSupplier bytes) throws IOException {
      try {
        stream = new SeekableBlobInputStream(bytes.openSeekable());
      } catch (IOException e) {
        throw e instanceof BlobReadException || SourceFailures.isContentFailure(e) ? e : new BlobReadException(e);
      } catch (RuntimeException e) {
        if (!SourceFailures.isContentFailure(e)) {
          for (Throwable cause : ExceptionUtil.getCauses(e)) {
            if (cause instanceof IOException) {
              throw new BlobReadException(e);
            }
          }
        }
        throw e;
      }
      grabber = grabbers.apply(stream);
      // video packets are read and dropped: no colour conversion buffers; audio decoded with one thread, at its own rate and
      // channel count (JavaCV converts only the sample format, to interleaved float)
      grabber.setImageMode(FrameGrabber.ImageMode.RAW);
      // In pinned JavaCV, a positive nonmatching index skips video decoder opening; negative indexes select video.
      grabber.setVideoStream(Integer.MAX_VALUE);
      grabber.setSampleMode(FrameGrabber.SampleMode.FLOAT);
      // Select the first audio stream regardless of its disposition flags.
      grabber.setAudioDisposition(-1);
      grabber.setAudioOption("threads", "1");
      try {
        grabber.start();
      } catch (FrameGrabber.Exception | RuntimeException e) {
        checkStorage("could not be read while it was opened");
        // JavaCV reports no selected stream for a video-only container. Its probed metadata still identifies that case.
        AVFormatContext format = grabber.getFormatContext();
        if (!(e instanceof FFmpegFrameGrabber.Exception) || e.getMessage() == null
            || !e.getMessage().startsWith("Did not find a video or audio stream inside \"")
            || format == null || format.isNull()) {
          throw failure("cannot be opened", e);
        }
        boolean videoStream = false;
        for (int i = 0; i < format.nb_streams(); i++) {
          int type = format.streams(i).codecpar().codec_type();
          if (type == avutil.AVMEDIA_TYPE_AUDIO) {
            throw failure("cannot be opened", e);
          }
          videoStream |= type == avutil.AVMEDIA_TYPE_VIDEO;
        }
        if (!videoStream) {
          throw failure("cannot be opened", e);
        }
      }
      // FFmpeg may swallow a read failure while it probes (the stream reports it as an end of file) and open the file with
      // incomplete stream parameters: before any of them is read, a remembered storage failure is retryable, not the content
      checkStorage("could not be read while it was opened");
    }

    /** Seeks and takes the first frame after it; false when that frame starts later than the pre-roll needs. */
    private boolean seekTo(long seekMs) {
      try {
        grabber.setAudioTimestamp(seekMs * MICROS_PER_MILLI);
      } catch (FrameGrabber.Exception | RuntimeException e) {
        throw failure("cannot be read at " + seekMs + " ms", e);
      }
      checkStorage("could not be read at " + seekMs + " ms");
      seeking = true;
      boolean landed = grabFrame();
      seeking = false;
      return landed && !seekOvershot;
    }

    private void resumeBySkipping(long startSegment) {
      anchored = false;
      seekOvershot = false;
      frameSamples = null;
      gapInputs = 0;
      decodedToEnd = false;
      nextSample = 0;
      nextIndex = 0;
      toSkip = startSegment;
    }

    /**
     * Why the iterator ended without (more) segments: {@link #REASON_NO_AUDIO_STREAM}, {@link #REASON_NO_SPEECH} (nothing found
     * from the start position) or null while segments remain or after at least one segment was produced.
     */
    public String endReason() {
      return endReason;
    }

    /** Heap bytes this iterator's own buffers hold now (segment buffer, levels, resampler table and ring, sample queue). */
    public long allocatedBytes() {
      return (segmenter == null ? 0 : segmenter.allocatedBytes()) + (resampler == null ? 0 : resampler.allocatedBytes())
          + (long) pending.length * Short.BYTES;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }
      if (closed || segmenter == null) {
        return false;
      }
      while (next == null) {
        // the segmenter reuses its buffer from here on: the segment returned last is no longer held
        generation++;
        EnergyVadSegmenter.Span span = segmenter.next();
        if (span == null) {
          checkStorage("could not be read to its end");
          if (nextIndex == 0 && endReason == null && toSkip == 0) {
            endReason = REASON_NO_SPEECH;
          }
          segmenter = null;
          return false;
        }
        if (toSkip > 0) {
          toSkip--;
          nextIndex++;
          continue;
        }
        next = new SegmentView(nextIndex++, span, segmenter, generation);
      }
      return true;
    }

    @Override
    public AudioSegment next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      current = next;
      next = null;
      return current;
    }

    /** Fills one window from the decoded stream; false when the stream has no more samples. */
    private boolean readWindow(short[] window) {
      while (pendingCount < windowSamples && !finished) {
        if (!decodeMore()) {
          resampler.finish();
          finished = true;
        }
      }
      if (pendingCount == 0) {
        return false;
      }
      int taken = Math.min(pendingCount, windowSamples);
      System.arraycopy(pending, 0, window, 0, taken);
      for (int i = taken; i < windowSamples; i++) {
        window[i] = 0;
      }
      System.arraycopy(pending, taken, pending, 0, pendingCount - taken);
      pendingCount -= taken;
      nextSample += windowSamples;
      return true;
    }

    /** One resampled output sample, in index order; samples before the start position are dropped. */
    private void accept(long index, short sample) {
      long queuedIndex = nextSample + pendingCount;
      if (index < queuedIndex) {
        return;
      }
      if (index != queuedIndex) {
        // the input is continuous from the first frame on (gaps are pushed as silence), so the output is too
        throw failure("produced output sample " + index + " after " + (queuedIndex - 1), null);
      }
      if (index >= maxDurationSamples) {
        throw failure("has audio past " + configuredMaximum(), null);
      }
      if (pendingCount == pending.length) {
        // one input can complete several outputs (up-sampling) and the end flushes the filter's tail: a few samples past a window
        pending = Arrays.copyOf(pending, pending.length * 2);
      }
      pending[pendingCount++] = sample;
    }

    /** Pushes input samples until a window's worth of output is queued or the frame is used up; false at the end. */
    private boolean decodeMore() {
      if (gapInputs == 0 && (frameSamples == null || !frameSamples.hasRemaining()) && !grabFrame()) {
        return false;
      }
      while (gapInputs > 0 && pendingCount < windowSamples) {
        resampler.push(0f);
        gapInputs--;
      }
      while (gapInputs == 0 && frameSamples.hasRemaining() && pendingCount < windowSamples) {
        float sum = 0;
        for (int c = 0; c < channels; c++) {
          sum += frameSamples.get();
        }
        resampler.push(sum / channels);
      }
      return true;
    }

    private boolean grabFrame() {
      while (true) {
        Frame frame = decodeFrame();
        if (frame == null) {
          decodedToEnd = true;
          checkStorage("could not be read to its end");
          return false;
        }
        FloatBuffer samples = samplesOf(frame);
        if (samples != null) {
          anchor(timestampMicros(frame), samples);
          if (seekOvershot) {
            return false;
          }
          frameSamples = samples;
          if (gapInputs > 0 || samples.remaining() >= channels) {
            return true;
          }
        }
      }
    }

    /** Rescale the decoder timestamp without JavaCV's intermediate signed multiplication. */
    private long timestampMicros(Frame frame) {
      if (!(frame.opaque instanceof AVFrame)) {
        return frame.timestamp;
      }
      long pts = ((AVFrame) frame.opaque).best_effort_timestamp();
      if (pts == avutil.AV_NOPTS_VALUE) {
        throw failure("has audio without a timestamp", null);
      }
      AVRational base = grabber.getFormatContext().streams(grabber.getAudioStream()).time_base();
      return avutil.av_rescale_rnd(pts, (long) base.num() * MICROS_PER_SECOND, base.den(), avutil.AV_ROUND_ZERO);
    }

    /** The next decoded frame, or null at the end of the stream. */
    private Frame decodeFrame() {
      try {
        return decodedToEnd ? null : grabber.grabSamples();
      } catch (FrameGrabber.Exception | RuntimeException e) {
        throw failure("cannot be decoded", e);
      }
    }

    /**
     * The frame's interleaved samples, or null when it holds no whole sample (it is passed over).
     *
     * @throws RuntimeException the video's failure, when the frame changes the audio format mid-stream
     */
    private FloatBuffer samplesOf(Frame frame) {
      if (frame.samples == null || frame.samples.length == 0 || !(frame.samples[0] instanceof FloatBuffer)) {
        return null;
      }
      if (frame.audioChannels != channels || frame.sampleRate != inRate) {
        // a mid-stream format change: the timeline and filter assume one rate and channel count per video
        throw failure("changes its audio format mid-stream (" + inRate + " Hz x" + channels + " -> " + frame.sampleRate + " Hz x"
            + frame.audioChannels + ")", null);
      }
      FloatBuffer samples = (FloatBuffer) frame.samples[0];
      return samples.remaining() < channels ? null : samples;
    }

    /**
     * Places the frame on the timeline. The first frame starts the input at its timestamp, or earlier with silence so that the
     * filter has input from before the first sample the windows need (after a seek it must start at least {@value #SEEK_MARGIN_MS} ms
     * earlier than that, else {@link #seekOvershot} is set). A later frame continues the running count unless its timestamp is
     * more than the tolerance off it
     * (none for a sample-exact time base, {@value #ANCHOR_TOLERANCE_MS} ms otherwise): ahead is a gap, pushed as silence; behind
     * is an overlap, dropped, unless the whole frame lies behind (a timestamp discontinuity), which is a data error.
     */
    private void anchor(long timestampMicros, FloatBuffer samples) {
      long expected = Math.round((timestampMicros - originMicros) * (double) inRate / MICROS_PER_SECOND);
      if (expected >= maxDurationInputs) {
        throw failure("has audio at " + (timestampMicros - originMicros) / MICROS_PER_MILLI + " ms, past " + configuredMaximum(),
            null);
      }
      if (!anchored) {
        long needed = Math.floorDiv(nextSample * inRate, config.getSampleRate()) - resampler.halfWidth() - 1;
        if (seeking && expected > needed - inRate * SEEK_MARGIN_MS / 1000) {
          seekOvershot = true;
          return;
        }
        anchored = true;
        long start = Math.min(expected, needed);
        resampler.restartAt(start);
        gapInputs = expected - start;
        return;
      }
      long running = resampler.nextInput();
      if (expected > running + anchorTolerance) {
        gapInputs = expected - running;
      } else if (expected < running - anchorTolerance) {
        long overlap = running - expected;
        long frameSamplesLeft = samples.remaining() / channels;
        if (overlap >= frameSamplesLeft) {
          throw failure("has timestamps that jump back by " + overlap * MICROS_PER_SECOND / inRate / MICROS_PER_MILLI
              + " ms (a discontinuity): its audio cannot be placed on one timeline", null);
        }
        samples.position((int) (samples.position() + overlap * channels));
      }
    }

    /** "the configured maximum (key) of N ms", the duration cap as a refusal names it. */
    private String configuredMaximum() {
      return "the configured maximum (" + AudioConfigKeys.MAX_DURATION_MS.key() + ") of " + config.getMaxDurationMs() + " ms";
    }

    private void checkStorage(String what) {
      IOException storage = stream == null ? null : stream.storageFailure();
      if (storage != null) {
        throw storageFailure(what, storage, null);
      }
    }

    /** A storage failure seen under the stream interrupts the prep run; anything else is a content failure of the video. */
    private RuntimeException failure(String what, Exception cause) {
      IOException storage = stream == null ? null : stream.storageFailure();
      if (storage != null) {
        return storageFailure(what, storage, cause);
      }
      return new VideoDecodeException(VIDEO_PREFIX + video + " " + what, cause);
    }

    private UncheckedIOException storageFailure(String what, IOException storage, Exception cause) {
      BlobReadException retryable = storage instanceof BlobReadException ? (BlobReadException) storage : new BlobReadException(storage);
      if (cause != null) {
        retryable.addSuppressed(cause);
      }
      return new UncheckedIOException(VIDEO_PREFIX + video + " " + what + ": storage failure", retryable);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      next = null;
      current = null;
      segmenter = null;
      frameSamples = null;
      closeGrabber();
    }

    private void closeGrabber() {
      frameSamples = null;
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
      grabber = null;
      stream = null;
    }

    /** A segment whose payload lives in the segmenter's buffer until the next segment is requested. */
    private final class SegmentView implements AudioSegment {
      private final long index;
      private final long startMs;
      private final long endMs;
      private final int bytes;
      private final EnergyVadSegmenter owner;
      private final long viewGeneration;

      private SegmentView(long index, EnergyVadSegmenter.Span span, EnergyVadSegmenter owner, long viewGeneration) {
        this.index = index;
        this.startMs = span.firstWindow() * FfmpegAudioConfig.WINDOW_MS;
        this.endMs = (span.firstWindow() + span.windows()) * FfmpegAudioConfig.WINDOW_MS;
        this.bytes = span.windows() * owner.windowBytes();
        this.owner = owner;
        this.viewGeneration = viewGeneration;
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
        return config.getSampleRate();
      }

      @Override
      public int channels() {
        return 1;
      }

      @Override
      public String encoding() {
        return ENCODING;
      }

      @Override
      public long payloadBytes() {
        return bytes;
      }

      /** Valid until the next segment is requested from the iterator or the iterator is closed. */
      @Override
      public InputStream openPayload() {
        if (closed || generation != viewGeneration) {
          throw new IllegalStateException("segment " + index + " of video " + video + " is no longer held: its payload is valid only "
              + "until the next segment is requested");
        }
        return new PayloadStream(new ByteArrayInputStream(owner.buffer(), 0, bytes));
      }

      /** Reads the payload while this segment is held; a read after the next segment was requested fails instead of returning it. */
      private final class PayloadStream extends InputStream {
        private final InputStream in;

        private PayloadStream(InputStream in) {
          this.in = in;
        }

        private void checkHeld() throws IOException {
          if (closed || generation != viewGeneration) {
            throw new IOException("segment " + index + " of video " + video + " is no longer held: the next segment was requested");
          }
        }

        @Override
        public int read() throws IOException {
          checkHeld();
          return in.read();
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
          checkHeld();
          return in.read(buffer, offset, length);
        }

        @Override
        public long skip(long n) throws IOException {
          checkHeld();
          return in.skip(n);
        }

        @Override
        public int available() throws IOException {
          checkHeld();
          return in.available();
        }
      }

      @Override
      public String toString() {
        return "AudioSegment{index=" + index + ", ms=[" + startMs + ", " + endMs + "), bytes=" + bytes + "}";
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
