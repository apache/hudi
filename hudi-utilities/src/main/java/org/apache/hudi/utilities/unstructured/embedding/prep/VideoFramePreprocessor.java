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
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoFrameConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
 * <p>FFmpeg is optional: Hudi does not ship it (org.bytedeco), and the default strategy is named by its class name only, loaded
 * when the first video's frames are opened. {@link #validateConfig} never loads it, so a job without video runs without the
 * FFmpeg jars. Missing optional decoder jars produce a per-file quarantine entry naming {@value #FFMPEG_JARS_SETTING}
 * and {@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}. Broken native linkage still fails the round with a
 * {@link PreprocessorConfigurationException}.
 *
 * <p>Failures: a {@code file_size} above {@link EmbeddingTableConfig#PREP_VIDEO_MAX_BYTES} fails the file before any read. A
 * content failure (a video refused by the pixel guard, a corrupt or undecodable stream ({@link VideoDecodeException}), no video
 * stream, a source that is gone, short or whose blob row names no readable range ({@link SourceFailures})) fails the file at once,
 * keeping the frames already emitted. Any other storage failure reading the source is a {@link SourceReadException}: an
 * interruption that fails the prep round, so a video a later run can read is never marked FAILED. A failure whose chain holds a
 * round failure ({@link PrepFailures#roundFailure}) propagates as one. A video whose strategy keeps no frame yields no chunk (the
 * framework's empty file).
 */
public class VideoFramePreprocessor implements FilePreprocessor {

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

  public VideoFramePreprocessor() {
    this.strategy = null;
  }

  /** A preprocessor over another strategy: a subclass with a public no-arg constructor and its own {@link #name()}. */
  protected VideoFramePreprocessor(FrameExtractionStrategy strategy) {
    this.strategy = strategy;
  }

  /**
   * The frame extraction strategy, loaded after open checks optional decoder availability. A missing strategy class or broken
   * native linkage fails the round with a {@link PreprocessorConfigurationException}.
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

  /** A video's frames are {@code image} rows. */
  @Override
  public Set<String> modalities(TypedProperties props) {
    return Collections.singleton(EmbeddingTableSchema.MODALITY_IMAGE);
  }

  /**
   * Checks {@link EmbeddingTableConfig#PREP_VIDEO_MAX_BYTES} and the frame keys on the driver, so a bad value fails the job once,
   * naming the key. For the default FFmpeg strategy the frame keys (interval, dedup, pixel guard, decoder threads) are checked
   * through {@link VideoFrameConfig#from}: FFmpeg is neither loaded nor instantiated, so a job whose batches hold no video runs
   * without it. A custom strategy (the protected constructor) validates itself.
   */
  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    maxBytes(props);
    if (strategy == null) {
      VideoFrameConfig.from(props);
    } else {
      strategy.validateConfig(props);
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
    return new VideoChunks(openFrames(bytes, file, props), fileAux);
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

  /**
   * The chunks of one video: one per kept frame. A failure of the strategy fails the file when it is met, keeping the frames
   * already emitted; a storage interruption is rethrown as a {@link SourceReadException}.
   */
  private final class VideoChunks implements ClosableIterator<Chunk> {
    private ClosableIterator<ExtractedFrame> frames;
    private final Map<String, String> fileAux;
    /** The frame {@link #hasNext()} read, which {@link #next()} returns: a failure reading it is met in hasNext. */
    private ExtractedFrame nextFrame;

    private VideoChunks(ClosableIterator<ExtractedFrame> frames, Map<String, String> fileAux) {
      this.frames = frames;
      this.fileAux = fileAux;
    }

    @Override
    public boolean hasNext() {
      if (nextFrame != null) {
        return true;
      }
      if (frames == null) {
        return false;
      }
      if (readFrame()) {
        return true;
      }
      // a video whose frame logic keeps no frame (e.g. all deduplicated) yields no frame row: not a failure
      closeFrames();
      return false;
    }

    @Override
    public Chunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ExtractedFrame frame = nextFrame;
      nextFrame = null;
      return frameChunk(frame);
    }

    /** Reads the next frame into {@link #nextFrame}, so a failure of the strategy's {@code hasNext()} or {@code next()} is met here. */
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
        throw interruptionOr(e);
      }
    }

    private RuntimeException interruptionOr(RuntimeException failure) {
      return isInterruption(failure) ? new SourceReadException("Storage failure reading the video source", failure) : failure;
    }

    private void closeFrames() {
      ClosableIterator<ExtractedFrame> toClose = frames;
      frames = null;
      if (toClose != null) {
        try {
          toClose.close();
        } catch (RuntimeException e) {
          throw interruptionOr(e);
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
      if (toClose != null) {
        toClose.close();
      }
    }
  }
}
