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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.SpeakerLabels;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.CloseOnFailure;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoFrameConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Data prep of a video file, {@value #NAME} ("video -> images (no resize)"): it runs a {@link FrameExtractionStrategy} (by default
 * {@value #FFMPEG_STRATEGY_CLASS}, the closest keyframe every X with consecutive repeats dropped) and emits ONE chunk per kept
 * frame, {@code modality=image} (a frame is an image to the model), no text, carrying its {@link Chunk#getFrameIndex() frame index},
 * whose payload is the frame encoded as the strategy encodes it (JPEG quality 90 at the frame's NATIVE size for the default): the
 * framework stores it inline in the row's {@code prepared_image} and pins the row at {@code chunk_id = frame_index}; compute resizes
 * it in memory before the model call. Aux: {@link EmbeddingTableSchema#AUX_FRAME_INDEX}, {@value #AUX_TIMESTAMP_MS} (from the
 * container start), {@value #AUX_DEDUP_KEY}, {@value #AUX_VIDEO_LOGIC} (the strategy's logic version), {@value #AUX_WIDTH} /
 * {@value #AUX_HEIGHT} and the stored input's {@link EmbeddingTableSchema#AUX_INPUT_WIDTH} /
 * {@link EmbeddingTableSchema#AUX_INPUT_HEIGHT} (the frame's: nothing is resized), {@link EmbeddingTableSchema#AUX_INPUT_LOGIC} =
 * {@link #inputLogic()}, and the declared {@value #AUX_SIZE_BYTES} and {@value #AUX_EXTENSION}.
 *
 * <p>Frames stream: the strategy decodes and encodes the next frame when the framework asks for it, and the framework stores it
 * before asking for the one after, so one decoded frame and one encoded frame are alive per task whatever the video's length.
 *
 * <p>With audio enabled ({@link AudioConfigKeys#ENABLED}) the video's SPEECH follows its frames, from the same iterator: the
 * configured audio segment source, transcriber and chunker turn the audio track into {@code text} rows ({@link VideoSpeechChunks}),
 * numbered by the framework like any text chunk, dense from 0 (their key's modality {@code text} keeps them apart from the
 * frames). A video's frames and its speech rows are written by one task in the round's one commit: there is no separate
 * transcript pass, commit or checkpoint. The audio implementations are built once per task, on its first video with audio on
 * (the source and the transcriber validated), and closed with the task ({@link #close()}).
 *
 * <p>FFmpeg is optional: Hudi does not ship it (org.bytedeco), and the default strategy is named by its class name only, loaded
 * when the first video's frames are opened. {@link #validateConfig} never loads it, so a job without video runs without the
 * FFmpeg jars; a video on a JVM without them is quarantined with a {@link PreprocessingException} naming
 * the FFmpeg jars ({@value #FFMPEG_JARS_SETTING}) and {@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}. Broken native linkage still fails the round. The
 * default FFmpeg audio segment source ({@link AudioConfig#DEFAULT_SEGMENT_SOURCE_CLASS}) is also loaded lazily. With a custom
 * frame strategy, unavailable audio decoding still fails the first video's speech with a configuration error naming
 * the FFmpeg jars ({@value #FFMPEG_JARS_SETTING}) and {@link AudioConfigKeys#SEGMENT_SOURCE_CLASS}.
 *
 * <p>Failures: a {@code file_size} above {@link EmbeddingTableConfig#PREP_VIDEO_MAX_BYTES} (20 GiB unless the job sets it) fails the file before any read
 * (frames and speech). A content failure of the frames (a video refused by the pixel guard, a corrupt or undecodable stream
 * ({@link VideoDecodeException}), no video stream, a source that is gone, short or whose blob row names no readable range
 * ({@link SourceFailures})) keeps the frames already emitted; with audio off it fails the file at once, with audio on the speech
 * is still prepared and the failure is thrown after the speech rows (the audio track is transcribed whenever it decodes). A
 * content failure of the speech fails the file after the rows already emitted; when both fail, the one quarantine entry names
 * both ({@value #FRAMES_PREFIX}...; {@value VideoSpeechChunks#SPEECH_PREFIX}...). Any other storage failure reading the source,
 * in either phase, and a transcriber's retryable failure are a {@link SourceReadException}: an interruption that fails the prep
 * round, so a video a later run can read is never marked FAILED. A failure whose chain holds a round failure
 * ({@link PrepFailures#roundFailure}) propagates as one and is never held behind the speech. A video whose strategy keeps no
 * frame and that has no speech yields no chunk (the framework's empty file).
 */
public class VideoFramePreprocessor implements FilePreprocessor, AutoCloseable {

  public static final String NAME = "video-frames-v1";
  /** {@link EmbeddingTableSchema#AUX_INPUT_LOGIC} of a stored FFmpeg frame: a kept frame, encoded at its native size. */
  public static final String INPUT_LOGIC = NAME;
  public static final String AUX_SIZE_BYTES = "size_bytes";
  public static final String AUX_EXTENSION = "extension";
  /** Aux key: the frame's time in milliseconds from the container start, clamped at 0. */
  public static final String AUX_TIMESTAMP_MS = "frame_timestamp_ms";
  /** Aux key: the key the strategy's dedup compared the frame by. */
  public static final String AUX_DEDUP_KEY = "frame_dedup_key";
  /** Aux key: the {@link FrameExtractionStrategy#name() logic version} that made the frame. */
  public static final String AUX_VIDEO_LOGIC = "video_logic";
  public static final String AUX_WIDTH = "width";
  public static final String AUX_HEIGHT = "height";
  /** Prefix of the frames' part of a quarantine entry that names a frame and a speech failure. */
  public static final String FRAMES_PREFIX = "frames: ";

  /**
   * The default strategy, FFmpeg's keyframe source, by name: Hudi does not ship FFmpeg (org.bytedeco), so the class is loaded only
   * when a video is prepared, and a job that prepares none runs without the FFmpeg jars.
   */
  public static final String FFMPEG_STRATEGY_CLASS =
      "org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource";
  /** The Spark setting that adds the FFmpeg jars to a job, named in each FFmpeg configuration error. */
  public static final String FFMPEG_JARS_SETTING = "spark.jars";
  private static final String FFMPEG_JARS = "add to the Spark setting " + FFMPEG_JARS_SETTING + " the locations of the "
      + "FFmpeg jars (org.bytedeco:javacv, org.bytedeco:javacpp and org.bytedeco:ffmpeg, plus the javacpp and ffmpeg jars of this "
      + "platform's classifier, " + System.getProperty("os.name") + "/" + System.getProperty("os.arch") + ")";

  /** The strategy, or null until the first use of the default FFmpeg one ({@link #strategy()}). */
  private FrameExtractionStrategy strategy;
  /** The task's audio implementations, built on its first video with audio on; closed by {@link #close()}. */
  private AudioParts audioParts;

  public VideoFramePreprocessor() {
    this.strategy = null;
  }

  /** A preprocessor over another strategy: a subclass with a public no-arg constructor and its own {@link #name()}. */
  protected VideoFramePreprocessor(FrameExtractionStrategy strategy) {
    this.strategy = strategy;
  }

  /**
   * The frame extraction strategy, the default FFmpeg one loaded after {@link #open} checks optional decoder availability.
   * Missing strategy classes or broken native linkage remain configuration/platform failures of the round.
   */
  private FrameExtractionStrategy strategy() {
    if (strategy == null) {
      try {
        strategy = ReflectionUtils.loadClass(FFMPEG_STRATEGY_CLASS);
      } catch (LinkageError e) {
        throw ffmpegMissing(e);
      } catch (HoodieException e) {
        if (e.getCause() instanceof ClassNotFoundException) {
          // the strategy class itself is not on the class path (a build without the FFmpeg module)
          throw ffmpegMissing(e.getCause());
        }
        throw e;
      }
    }
    return strategy;
  }

  /** The configuration error of a default (FFmpeg) strategy whose classes or natives cannot be linked. */
  private static PreprocessorConfigurationException ffmpegMissing(Throwable cause) {
    return new PreprocessorConfigurationException("Video preprocessor " + NAME + " (the default of "
        + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + ") prepares video with FFmpeg, which Hudi does not ship and "
        + "this JVM cannot load: " + FFMPEG_JARS + "; or set " + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key()
        + " to the non-video extensions the job prepares (e.g. pdf,png,jpg) so it prepares no video; or set "
        + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + " to your own video preprocessor", cause);
  }

  @Override
  public String name() {
    return NAME;
  }

  /**
   * The {@link EmbeddingTableSchema#AUX_INPUT_LOGIC} of this preprocessor's stored frames: {@value #INPUT_LOGIC} for FFmpeg's; a
   * subclass whose strategy encodes other bytes names its own.
   */
  protected String inputLogic() {
    return INPUT_LOGIC;
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.VIDEO;
  }

  /** A frame row stores its encoded frame as the model input. */
  @Override
  public boolean storesInputs() {
    return true;
  }

  /** A video's frames are {@code image} rows; with audio on ({@link AudioConfig#enabled()}) its speech adds {@code text} rows. */
  @Override
  public Set<String> modalities(TypedProperties props) {
    Set<String> modalities = new LinkedHashSet<>();
    modalities.add(EmbeddingTableSchema.MODALITY_IMAGE);
    if (AudioConfig.from(props).enabled()) {
      modalities.add(EmbeddingTableSchema.MODALITY_TEXT);
    }
    return modalities;
  }

  /**
   * Checks {@link EmbeddingTableConfig#PREP_VIDEO_MAX_BYTES} and the frame keys on the driver, so a bad value fails the job once,
   * naming the key. For the default FFmpeg strategy the frame keys (interval, dedup, pixel guard, decoder threads) are checked
   * through {@link VideoFrameConfig#from}: FFmpeg is neither loaded nor instantiated, so a job whose batches hold no video runs
   * without it. A custom strategy (the protected constructor) validates itself. With audio on, the audio keys are checked
   * ({@link AudioConfig#validate()}: the default FFmpeg segment source reads its keys only, FFmpeg is not loaded).
   */
  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    maxBytes(props);
    if (strategy == null) {
      VideoFrameConfig.from(props);
    } else {
      strategy.validateConfig(props);
    }
    AudioConfig audio = AudioConfig.from(props);
    if (audio.enabled()) {
      audio.validate();
    }
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
      throws IOException {
    long maxBytes = maxBytes(props);
    if (file.getFileSize() > maxBytes) {
      throw new PreprocessingException("video declares " + file.getFileSize() + " bytes, above "
          + EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key() + " = " + maxBytes + "; refused before any read");
    }
    if (strategy == null && (getClass().getClassLoader().getResource("org/bytedeco/javacv/FFmpegFrameGrabber.class") == null
        || getClass().getClassLoader().getResource("org/bytedeco/ffmpeg/global/avutil.class") == null)) {
      // FFmpeg is optional: an unavailable video goes to the error table without blocking unrelated files every round.
      throw new PreprocessingException("FFMPEG_UNAVAILABLE: " + ffmpegMissing(null).getMessage());
    }
    Map<String, String> fileAux = new HashMap<>();
    fileAux.put(AUX_SIZE_BYTES, String.valueOf(file.getFileSize()));
    fileAux.put(AUX_EXTENSION, file.getFileExtension());
    AudioConfig audio = AudioConfig.from(props);
    if (!audio.enabled()) {
      return new VideoChunks(openFrames(bytes, file, props), null, fileAux, () -> null);                 // no speech
    }
    AudioParts parts = audioParts(audio, tokenizer, props);
    ClosableIterator<ExtractedFrame> frames = null;
    Exception frameFailure = null;
    try {
      frames = openFrames(bytes, file, props);
    } catch (SourceReadException | PreprocessorConfigurationException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      PrepFailures.rethrowRoundFailure(e);
      // the frames' content failure is the file's, but its speech is still prepared
      frameFailure = e;
    }
    VideoSpeechChunks.Engines engines = new VideoSpeechChunks.Engines(parts.source, parts.transcriber, parts.chunker);
    // the returned VideoChunks creates, owns and closes the speech; VideoSpeechChunks opens nothing before its first hasNext
    return new VideoChunks(frames, frameFailure, fileAux,
        () -> new VideoSpeechChunks(engines, audioInput(bytes, file), tokenizer, props, parts.logicLine, parts.speakerScope));
  }

  /** The strategy's frames of the video; a storage failure that is not the file's own fault is a {@link SourceReadException}. */
  private ClosableIterator<ExtractedFrame> openFrames(BlobBytesSupplier bytes, FileMeta file, TypedProperties props)
      throws IOException {
    FrameExtractionStrategy frames = strategy();
    try {
      return frames.frames(bytes, file, props);
    } catch (LinkageError e) {
      rethrowFfmpegLinkage(frames, e);
      throw e;
    } catch (IOException | RuntimeException e) {
      if (isInterruption(e)) {
        throw new SourceReadException("Storage failure opening the video source", e);
      }
      throw e;
    }
  }

  /**
   * The task's audio segment source, transcriber and chunker, built and validated on its first video (one instance of each per
   * task, used for its videos one after another); a failure to build or validate them is a configuration error of the task.
   */
  private AudioParts audioParts(AudioConfig audio, Tokenizer tokenizer, TypedProperties props) {
    if (audioParts == null) {
      try {
        AudioSegmentSource source = audio.newSegmentSource();
        try (CloseOnFailure<RuntimeException> sourceGuard = new CloseOnFailure<>(() -> AudioConfig.closeQuietly(source))) {
          SegmentTranscriber transcriber = audio.newTranscriber();
          try (CloseOnFailure<RuntimeException> transcriberGuard = new CloseOnFailure<>(() -> AudioConfig.closeQuietly(transcriber))) {
            TranscriptChunker chunker = audio.newChunker();
            try (CloseOnFailure<RuntimeException> chunkerGuard = new CloseOnFailure<>(() -> AudioConfig.closeQuietly(chunker))) {
              source.validate(props);
              transcriber.validate(props);
              audioParts = new AudioParts(ffmpegLinkageGuarded(source), transcriber, chunker,
                  AudioConfig.logicLine(source, transcriber, chunker, tokenizer), SpeakerLabels.scopeOf(transcriber));
              sourceGuard.succeeded();
              transcriberGuard.succeeded();
              chunkerGuard.succeeded();
            }
          }
        }
      } catch (RuntimeException e) {
        throw new PreprocessorConfigurationException("The audio implementations of " + NAME + " cannot be built", e);
      } catch (LinkageError e) {
        throw audioLinkage(e);
      }
    }
    return audioParts;
  }

  /** The configuration error of audio implementations whose classes or natives cannot be linked (the default needs FFmpeg). */
  private static PreprocessorConfigurationException audioLinkage(Throwable cause) {
    return new PreprocessorConfigurationException("The audio implementations of " + NAME + " cannot be linked ("
        + AudioConfigKeys.SEGMENT_SOURCE_CLASS.key() + ", " + AudioConfigKeys.TRANSCRIBER_CLASS.key() + "); the default "
        + "segment source prepares speech with FFmpeg, which Hudi does not ship and this JVM cannot load: " + FFMPEG_JARS
        + "; or set " + AudioConfigKeys.ENABLED.key() + " to false; or set " + AudioConfigKeys.SEGMENT_SOURCE_CLASS.key()
        + " to your own segment source", cause);
  }

  /**
   * The default FFmpeg segment source links FFmpeg on a video's first segments, not when it is built or validated: a
   * {@link LinkageError} it throws then is the task's configuration error ({@link #audioLinkage}), never an {@link Error} that
   * escapes the round. Any other source is used as it is.
   */
  private static AudioSegmentSource ffmpegLinkageGuarded(AudioSegmentSource source) {
    if (!source.getClass().getName().equals(AudioConfig.DEFAULT_SEGMENT_SOURCE_CLASS)) {
      return source;
    }
    return new FfmpegLinkageGuard(source);
  }

  /** What the audio segment source is given about the video: the same bytes the frames are read from. */
  private static AudioInput audioInput(BlobBytesSupplier bytes, FileMeta file) {
    return new AudioInput() {
      @Override
      public String sourcePathSha() {
        return file.getSourcePathSha();
      }

      /** Not passed to a preprocessor (FileMeta has no path): null, as the interface allows. */
      @Override
      public String sourcePath() {
        return null;
      }

      /** Not passed to a preprocessor (FileMeta has no partition path): null, as the interface allows. */
      @Override
      public String sourcePartitionPath() {
        return null;
      }

      @Override
      public long sizeBytes() {
        return file.getFileSize();
      }

      @Override
      public String extension() {
        return file.getFileExtension();
      }

      @Override
      public BlobBytesSupplier bytes() {
        return bytes;
      }
    };
  }

  /**
   * Closes the task's audio segment source, transcriber and chunker (a native engine context among them), once; the framework
   * calls it when its task ends ({@link PreprocessorRegistry#close()}).
   */
  @Override
  public void close() {
    AudioParts parts = audioParts;
    audioParts = null;
    if (parts != null) {
      AudioConfig.closeQuietly(parts.source);
      AudioConfig.closeQuietly(parts.transcriber);
      AudioConfig.closeQuietly(parts.chunker);
    }
  }

  /**
   * Rethrows a {@link LinkageError} of the default FFmpeg strategy (its natives load lazily, on the first video) as its
   * configuration error; returns for any other strategy's, which the caller rethrows as it is.
   */
  private static void rethrowFfmpegLinkage(FrameExtractionStrategy frames, LinkageError e) {
    if (frames.getClass().getName().equals(FFMPEG_STRATEGY_CLASS)) {
      throw ffmpegMissing(e);
    }
  }

  /**
   * Whether {@code failure} of the strategy interrupts the prep run: a storage failure of the source that is not the file's own fault
   * ({@link SourceFailures}). Anything else is a content failure of the file.
   */
  private static boolean isInterruption(Exception failure) {
    return isStorageFailure(failure) && !SourceFailures.isContentFailure(failure);
  }

  /**
   * Whether {@code failure} comes from the storage under the source: a {@link BlobReadException} in its causes (a read the
   * strategy saw fail), or an I/O failure opening it. A refused or undecodable video is not one.
   */
  private static boolean isStorageFailure(Exception failure) {
    if (failure instanceof VideoDecodeException || SourceCauses.isCausedBy(failure, ImageRefusedException.class)) {
      return false;
    }
    return SourceCauses.isCausedBy(failure, BlobReadException.class) || failure instanceof IOException || failure instanceof HoodieIOException;
  }

  /** {@link EmbeddingTableConfig#PREP_VIDEO_MAX_BYTES}; its default when unset. */
  private static long maxBytes(TypedProperties props) {
    String raw = ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES, null);
    if (raw == null || raw.trim().isEmpty()) {
      return EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_BYTES;
    }
    long maxBytes;
    try {
      maxBytes = Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key() + " must be an integer, got "
          + raw, e);
    }
    if (maxBytes <= 0) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key() + " must be a positive byte "
          + "count, got " + maxBytes);
    }
    return maxBytes;
  }

  /** The default FFmpeg segment source with each {@link LinkageError} it throws turned into {@link #audioLinkage}. */
  static final class FfmpegLinkageGuard implements AudioSegmentSource, AutoCloseable {
    private final AudioSegmentSource source;

    FfmpegLinkageGuard(AudioSegmentSource source) {
      this.source = source;
    }

    @Override
    public String logicVersion() {
      return source.logicVersion();
    }

    @Override
    public void validate(TypedProperties props) {
      source.validate(props);
    }

    @Override
    public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props)
        throws IOException {
      ClosableIterator<AudioSegment> segments;
      try {
        segments = source.segments(audio, startSegment, startMs, props);
      } catch (LinkageError e) {
        throw audioLinkage(e);
      }
      return new ClosableIterator<AudioSegment>() {
        @Override
        public boolean hasNext() {
          try {
            return segments.hasNext();
          } catch (LinkageError e) {
            throw audioLinkage(e);
          }
        }

        @Override
        public AudioSegment next() {
          try {
            return segments.next();
          } catch (LinkageError e) {
            throw audioLinkage(e);
          }
        }

        @Override
        public void close() {
          segments.close();
        }
      };
    }

    @Override
    public void close() {
      AudioConfig.closeQuietly(source);
    }
  }

  /** The task's audio implementations and what is derived from them once. */
  private static final class AudioParts {
    private final AudioSegmentSource source;
    private final SegmentTranscriber transcriber;
    private final TranscriptChunker chunker;
    private final String logicLine;
    private final String speakerScope;

    private AudioParts(AudioSegmentSource source, SegmentTranscriber transcriber, TranscriptChunker chunker, String logicLine,
                       String speakerScope) {
      this.source = source;
      this.transcriber = transcriber;
      this.chunker = chunker;
      this.logicLine = logicLine;
      this.speakerScope = speakerScope;
    }
  }

  /**
   * The chunks of one video: one per kept frame, then, with audio on ({@code speech} not null), its speech rows. With audio off a
   * failure of the strategy fails the file when it is met, keeping the frames already emitted; with audio on a content failure of
   * the frames (at the open or while they are read) is held until the speech is done, then thrown. A storage interruption is
   * rethrown as a {@link SourceReadException} at once.
   */
  private final class VideoChunks implements ClosableIterator<Chunk> {
    private ClosableIterator<ExtractedFrame> frames;
    private Exception frameFailure;
    private final Map<String, String> fileAux;
    private final VideoSpeechChunks speech;
    private PreprocessingException speechFailure;
    /** The frame {@link #hasNext()} read, which {@link #next()} returns: a failure reading it is met in hasNext. */
    private ExtractedFrame nextFrame;
    private boolean framesDone;

    private VideoChunks(ClosableIterator<ExtractedFrame> frames, Exception frameFailure, Map<String, String> fileAux,
                        Supplier<VideoSpeechChunks> speech) {
      this.frames = frames;
      this.frameFailure = frameFailure;
      this.fileAux = fileAux;
      this.speech = speech.get();
      this.framesDone = frames == null;
    }

    @Override
    public boolean hasNext() {
      if (!framesDone) {
        if (nextFrame != null || readFrame()) {
          return true;
        }
        framesDone = true;
        // a video whose frame logic keeps no frame (e.g. all deduplicated) yields no frame row: not a failure
        closeFrames();
        if (speech == null && frameFailure != null) {
          throw asRuntime(frameFailure);
        }
      }
      if (speech != null && speechFailure == null) {
        try {
          if (speech.hasNext()) {
            return true;
          }
        } catch (PreprocessingException e) {
          speechFailure = e;
        }
      }
      if (frameFailure != null || speechFailure != null) {
        throw fileFailure();
      }
      return false;
    }

    @Override
    public Chunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (!framesDone) {
        ExtractedFrame frame = nextFrame;
        nextFrame = null;
        return frameChunk(frame);
      }
      try {
        return speech.next();
      } catch (PreprocessingException e) {
        speechFailure = e;
        throw fileFailure();
      }
    }

    /**
     * Reads the next frame into {@link #nextFrame}, so a failure of the strategy's {@code hasNext()} or {@code next()} is met here;
     * with audio on, a content failure ends the frames and is held. Returns whether a frame was read.
     */
    private boolean readFrame() {
      try {
        if (frames.hasNext()) {
          nextFrame = frames.next();
          return true;
        }
        return false;
      } catch (LinkageError e) {
        rethrowFfmpegLinkage(strategy(), e);
        throw e;
      } catch (RuntimeException e) {
        RuntimeException failure = interruptionOr(e);
        if (speech == null || failure instanceof SourceReadException) {
          throw failure;
        }
        PrepFailures.rethrowRoundFailure(failure);
        frameFailure = failure;
        return false;
      }
    }

    private RuntimeException interruptionOr(RuntimeException failure) {
      return isInterruption(failure) ? new SourceReadException("Storage failure reading the video source", failure) : failure;
    }

    /** The file's one failure: the frames' alone as thrown, the speech's alone, or one naming both. */
    private RuntimeException fileFailure() {
      if (speechFailure == null) {
        return asRuntime(frameFailure);
      }
      if (frameFailure == null) {
        return speechFailure;
      }
      String framesText = frameFailure instanceof PreprocessingException ? frameFailure.getMessage()
          : VideoSpeechChunks.describe(frameFailure);
      PreprocessingException both = new PreprocessingException(FRAMES_PREFIX + framesText + "; " + speechFailure.getMessage(),
          speechFailure);
      both.addSuppressed(frameFailure);
      return both;
    }

    private void closeFrames() {
      ClosableIterator<ExtractedFrame> toClose = frames;
      frames = null;
      if (toClose != null) {
        try {
          toClose.close();
        } catch (RuntimeException e) {
          if (frameFailure != null) {
            PrepFailures.rethrowRoundFailure(e);
            frameFailure.addSuppressed(e);
          } else if (speech == null || isInterruption(e)) {
            throw interruptionOr(e);
          } else {
            PrepFailures.rethrowRoundFailure(e);
            // with audio on, a content failure closing the frames is held like any other frame failure
            frameFailure = e;
          }
        }
      }
    }

    private Chunk frameChunk(ExtractedFrame frame) {
      Map<String, String> aux = new HashMap<>(fileAux);
      aux.put(EmbeddingTableSchema.AUX_FRAME_INDEX, String.valueOf(frame.getFrameIndex()));
      aux.put(AUX_TIMESTAMP_MS, String.valueOf(frame.getTimeMs()));
      aux.put(AUX_DEDUP_KEY, frame.getDedupKey());
      aux.put(AUX_VIDEO_LOGIC, strategy().name());
      aux.put(AUX_WIDTH, String.valueOf(frame.getWidth()));
      aux.put(AUX_HEIGHT, String.valueOf(frame.getHeight()));
      aux.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, String.valueOf(frame.getWidth()));
      aux.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, String.valueOf(frame.getHeight()));
      aux.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, inputLogic());
      return Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).frameIndex(frame.getFrameIndex()).aux(aux)
          .payload(frame.getImage(), frame.getSizeBytes(), frame.getMimeType()).build();
    }

    @Override
    public void close() {
      ClosableIterator<ExtractedFrame> toClose = frames;
      frames = null;
      try {
        if (toClose != null) {
          toClose.close();
        }
      } finally {
        if (speech != null) {
          speech.close();
        }
      }
    }
  }

  private static RuntimeException asRuntime(Exception failure) {
    return failure instanceof RuntimeException ? (RuntimeException) failure
        : new UncheckedIOException("video frames cannot be read", (IOException) failure);
  }
}
