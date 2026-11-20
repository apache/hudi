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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data prep of a video file, {@code video-frames-v1}, over a scripted {@link FrameExtractionStrategy} (no FFmpeg): one image chunk
 * per kept frame carrying the frame as its payload and its index; it declares that it stores inputs; a size guard before any
 * read; its failures split into content failures of the file (keeping the frames already emitted) and interruptions of the round.
 */
class TestVideoFramePreprocessor {

  private static final String LOGIC = "scripted-frames-v0";
  private static final long VIDEO_SIZE = 12345L;

  @ParameterizedTest
  @ValueSource(strings = {"mp4", "mov", "mkv", "webm", "avi"})
  void everyVideoExtensionMapsToTheFramePreprocessorWhichStoresInputs(String extension) {
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    FilePreprocessor preprocessor = registry.forExtension(extension).get();
    assertEquals(VideoFramePreprocessor.class, preprocessor.getClass());
    assertTrue(preprocessor.storesInputs(), "a frame row stores its input");
    assertEquals("video-frames-v1", preprocessor.name());
    assertEquals(SourceKind.VIDEO, preprocessor.sourceKind());
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, preprocessor.sourceKind().markerModality(), "a video's marker is image");
    assertEquals(Collections.singleton(EmbeddingTableSchema.MODALITY_IMAGE), preprocessor.modalities(new TypedProperties()));
  }

  @Test
  void oneChunkPerKeptFrameCarryingItsImageIndexAndAux() throws IOException {
    ScriptedStrategy strategy = new ScriptedStrategy(3, -1, null);
    List<Chunk> chunks = drain(new ScriptedPreprocessor(strategy).open(video(), file(VIDEO_SIZE), null, new TypedProperties()));

    assertEquals(3, chunks.size(), chunks.toString());
    for (int k = 0; k < 3; k++) {
      Chunk frame = chunks.get(k);
      assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, frame.getModality(), "a frame is an image row");
      assertEquals(Long.valueOf(k), frame.getFrameIndex(), "the frame index rides on the chunk");
      assertNull(frame.getText());
      assertNull(frame.getTokenCount());
      assertEquals(ImageContentTypes.JPEG, frame.getPayloadContentType());
      assertFalse(frame.isPayloadSource(), "a frame is derived, never the source");
      assertArrayEquals(ScriptedStrategy.jpeg(k), bytes(frame));
      assertEquals(ScriptedStrategy.jpeg(k).length, frame.getPayloadSizeBytes());
      Map<String, String> expected = new HashMap<>();
      expected.put(EmbeddingTableSchema.AUX_FRAME_INDEX, String.valueOf(k));
      expected.put(VideoFramePreprocessor.AUX_TIMESTAMP_MS, String.valueOf(2000L * k));
      expected.put(VideoFramePreprocessor.AUX_DEDUP_KEY, "dedup-" + k);
      expected.put(VideoFramePreprocessor.AUX_VIDEO_LOGIC, LOGIC);
      expected.put(VideoFramePreprocessor.AUX_WIDTH, "160");
      expected.put(VideoFramePreprocessor.AUX_HEIGHT, "120");
      expected.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, "160");
      expected.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, "120");
      expected.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, VideoFramePreprocessor.INPUT_LOGIC);
      expected.put(VideoFramePreprocessor.AUX_SIZE_BYTES, String.valueOf(VIDEO_SIZE));
      expected.put(VideoFramePreprocessor.AUX_EXTENSION, "mp4");
      assertEquals(expected, frame.getAux());
    }
    assertEquals(1, strategy.closed.get(), "the frames are closed once at their end");
  }

  @Test
  void videoAboveTheSizeGuardIsRefusedBeforeAnyRead() {
    AtomicInteger opens = new AtomicInteger();
    BlobBytesSupplier counting = () -> {
      opens.incrementAndGet();
      throw new AssertionError("an oversized video is never read");
    };
    ScriptedStrategy strategy = new ScriptedStrategy(3, -1, null);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "4999");
    ScriptedPreprocessor preprocessor = new ScriptedPreprocessor(strategy);
    FileMeta oversized = file(5000L);
    PreprocessingException refused = assertThrows(PreprocessingException.class, () -> preprocessor.open(counting, oversized, null, props));
    assertEquals("video declares 5000 bytes, above " + EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key()
        + " = 4999; refused before any read", refused.getMessage());
    assertEquals(0, opens.get());
    assertEquals(0, strategy.opened.get(), "the strategy is never asked for frames");
    assertEquals(20L * 1024 * 1024 * 1024, EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_BYTES);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void videoAtTwentyGiBIsAdmittedByTheDefaultOrAnExplicitLimit(boolean explicitCap) throws Exception {
    TypedProperties props = new TypedProperties();
    if (explicitCap) {
      props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "21474836480");
    }
    ScriptedStrategy strategy = new ScriptedStrategy(1, -1, null);
    List<Chunk> chunks = assertDoesNotThrow(() -> drain(new ScriptedPreprocessor(strategy).open(
        video(), file(20L * 1024 * 1024 * 1024), null, props)));
    assertEquals(1, chunks.size());
    assertArrayEquals(ScriptedStrategy.jpeg(0), bytes(chunks.get(0)));
    assertEquals("21474836480", chunks.get(0).getAux().get(VideoFramePreprocessor.AUX_SIZE_BYTES));
    assertEquals(1, strategy.opened.get());
    assertEquals(1, strategy.closed.get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void videoAboveTwentyGiBIsRefusedBeforeReading(boolean explicitCap) {
    TypedProperties props = new TypedProperties();
    if (explicitCap) {
      props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "21474836480");
    }
    ScriptedStrategy strategy = new ScriptedStrategy(1, -1, null);
    PreprocessingException refused = assertThrows(PreprocessingException.class, () -> drain(new ScriptedPreprocessor(strategy).open(
        video(), file(20L * 1024 * 1024 * 1024 + 1), null, props)));
    assertTrue(refused.getMessage().contains(" = 21474836480;"), refused.getMessage());
    assertEquals(0, strategy.opened.get());
  }

  @Test
  void videoAboveTwoGiBIsPreparedByDefault() throws IOException {
    ScriptedStrategy strategy = new ScriptedStrategy(3, -1, null);
    try (ClosableIterator<Chunk> chunks = new ScriptedPreprocessor(strategy).open(video(), file(3L << 30), null, new TypedProperties())) {
      assertTrue(chunks.hasNext());
    }
    assertEquals(1, strategy.opened.get());
  }

  /** The default (FFmpeg) preprocessor checks the frame keys itself, without loading FFmpeg, naming the offending key. */
  @Test
  void configurationIsValidatedOnTheDriverNamingTheKey() {
    VideoFramePreprocessor preprocessor = new VideoFramePreprocessor();
    preprocessor.validateConfig(new TypedProperties(), null);
    String[][] bad = {
        {EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "0"},
        {EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "-1"},
        {EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "2GiB"},
        {EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "-1"},
        {EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "phash"},
        {EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "65"},
        {EmbeddingTableConfig.VIDEO_FFMPEG_THREADS.key(), "0"},
        {EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), "0"}};
    for (String[] entry : bad) {
      TypedProperties props = new TypedProperties();
      props.setProperty(entry[0], entry[1]);
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> preprocessor.validateConfig(props, null));
      assertTrue(e.getMessage().contains(entry[0]), e.getMessage());
    }
  }

  /** A custom strategy validates itself; the size guard is still the preprocessor's. */
  @Test
  void customStrategyValidatesItsOwnConfiguration() {
    ScriptedStrategy strategy = new ScriptedStrategy(1, -1, null);
    VideoFramePreprocessor preprocessor = new ScriptedPreprocessor(strategy);
    preprocessor.validateConfig(new TypedProperties(), null);
    assertEquals(1, strategy.validated.get());

    TypedProperties refused = new TypedProperties();
    refused.setProperty(ScriptedStrategy.REFUSED_KEY, "true");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> preprocessor.validateConfig(refused, null));
    assertEquals("Config " + ScriptedStrategy.REFUSED_KEY + " is refused", e.getMessage());

    TypedProperties badSize = new TypedProperties();
    badSize.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "-5");
    assertTrue(assertThrows(IllegalArgumentException.class, () -> preprocessor.validateConfig(badSize, null)).getMessage()
        .contains(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key()));
  }

  /** Content failures of the file at the open (its quarantine entry, the round advances): refused, undecodable, gone, short, malformed. */
  @Test
  void refusedCorruptGoneShortOrMalformedSourcesAtTheOpenAreContentFailures() {
    UncheckedIOException pixelGuard = new UncheckedIOException("refused",
        new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + "3840x2161 is above the pixel guard"));
    VideoDecodeException noVideo = new VideoDecodeException("video v has no video stream");
    for (RuntimeException fault : Arrays.asList(pixelGuard, noVideo)) {
      ScriptedPreprocessor preprocessor = new ScriptedPreprocessor(new ScriptedStrategy(0, 0, fault));
      RuntimeException thrown = assertThrows(RuntimeException.class, () -> openVideo(preprocessor));
      assertSame(fault, thrown);
    }
    for (IOException fault : Arrays.asList(new FileNotFoundException("gone.mp4"), new BlobTruncatedException("ended early"),
        new BlobReferenceException("an escaping path"))) {
      ScriptedPreprocessor preprocessor = new ScriptedPreprocessor(new ScriptedStrategy(fault));
      IOException thrown = assertThrows(IOException.class, () -> openVideo(preprocessor));
      assertSame(fault, thrown, "a content failure is the file's own: " + thrown);
    }
  }

  /** A frame logic that keeps no frame is not a failure: the file yields no chunk (the framework's empty file). */
  @Test
  void strategyThatKeepsNoFrameYieldsNoChunk() throws IOException {
    ScriptedStrategy strategy = new ScriptedStrategy(0, -1, null);
    assertEquals(0, drain(new ScriptedPreprocessor(strategy).open(video(), file(VIDEO_SIZE), null, new TypedProperties())).size());
    assertEquals(1, strategy.closed.get());
  }

  /**
   * A content failure while frames are read (in the strategy's hasNext, its next, or closing it after a clean end) fails the file
   * at once, keeping the frames emitted before it.
   */
  @ParameterizedTest
  @ValueSource(ints = {ScriptedStrategy.IN_HAS_NEXT, ScriptedStrategy.IN_NEXT, ScriptedStrategy.AT_CLOSE})
  void contentFailureWhileReadingKeepsTheFramesBeforeIt(int where) throws IOException {
    VideoDecodeException corrupt = new VideoDecodeException("video v cannot be decoded at 4000000us");
    int framesBefore = where == ScriptedStrategy.AT_CLOSE ? 3 : 2;
    List<Chunk> before = new ArrayList<>();
    ClosableIterator<Chunk> chunks = new ScriptedPreprocessor(new ScriptedStrategy(where == ScriptedStrategy.AT_CLOSE ? 3 : 5,
        where == ScriptedStrategy.AT_CLOSE ? -1 : 2, corrupt, where)).open(video(), file(VIDEO_SIZE), null, new TypedProperties());
    assertSame(corrupt, assertThrows(VideoDecodeException.class, () -> drainInto(chunks, before)));
    assertEquals(framesBefore, before.size());
    for (int k = 0; k < framesBefore; k++) {
      assertEquals(Long.valueOf(k), before.get(k).getFrameIndex());
    }
  }

  /** A failure that fails the round (here a platform failure: a codec missing from the executor) propagates as it is. */
  @ParameterizedTest
  @ValueSource(ints = {ScriptedStrategy.AT_OPEN, ScriptedStrategy.IN_HAS_NEXT, ScriptedStrategy.IN_NEXT, ScriptedStrategy.AT_CLOSE})
  void roundFailuresPropagate(int where) {
    PrepPlatformException platform = new PrepPlatformException("no JPEG writer", null);
    ScriptedPreprocessor preprocessor = new ScriptedPreprocessor(
        new ScriptedStrategy(where == ScriptedStrategy.AT_CLOSE ? 2 : 4, where == ScriptedStrategy.AT_CLOSE ? -1 : 1, platform, where));
    RuntimeException failure = assertThrows(RuntimeException.class, () -> openAndDrain(preprocessor));
    assertSame(platform, PrepFailures.roundFailure(failure), where + ": " + failure);
  }

  /** Interruptions of the round (never a FAILED file): any other storage failure, at the open or while frames are read. */
  @Test
  void otherStorageFailuresOpeningOrReadingTheSourceAreInterruptions() throws IOException {
    IOException slowDown = new IOException("503 Slow Down");
    ScriptedPreprocessor slowDownPreprocessor = new ScriptedPreprocessor(new ScriptedStrategy(slowDown));
    SourceReadException atOpen = assertThrows(SourceReadException.class, () -> openVideo(slowDownPreprocessor));
    assertSame(slowDown, atOpen.getCause());

    // a denied read is never the file's fault, even when the storage calls it not found
    IOException denied = new AccessDeniedException("video.mp4");
    denied.initCause(new FileNotFoundException("video.mp4"));
    ScriptedPreprocessor deniedPreprocessor = new ScriptedPreprocessor(new ScriptedStrategy(denied));
    assertThrows(SourceReadException.class, () -> openVideo(deniedPreprocessor));

    UncheckedIOException reset = new UncheckedIOException("read", new BlobReadException(new IOException("connection reset")));
    List<Chunk> before = new ArrayList<>();
    ClosableIterator<Chunk> chunks = new ScriptedPreprocessor(new ScriptedStrategy(4, 2, reset, ScriptedStrategy.IN_NEXT))
        .open(video(), file(VIDEO_SIZE), null, new TypedProperties());
    SourceReadException partWay = assertThrows(SourceReadException.class, () -> drainInto(chunks, before));
    // SourceReadException wraps an unchecked cause in an IOException
    assertSame(reset, partWay.getCause().getCause());
    assertEquals(2, before.size());

    // a source found gone part way is the file's own failure, not an interruption
    UncheckedIOException gone = new UncheckedIOException("read", new BlobReadException(new FileNotFoundException("gone.mp4")));
    ScriptedPreprocessor gonePreprocessor = new ScriptedPreprocessor(new ScriptedStrategy(4, 1, gone, ScriptedStrategy.IN_HAS_NEXT));
    assertSame(gone, assertThrows(UncheckedIOException.class, () -> openAndDrain(gonePreprocessor)));
  }

  /** A linkage error of a custom strategy is not FFmpeg's: it propagates as it is (only the default maps to a configuration error). */
  @Test
  void linkageErrorOfACustomStrategyPropagatesAsItIs() {
    NoClassDefFoundError missing = new NoClassDefFoundError("com/example/Codec");
    ScriptedStrategy strategy = new ScriptedStrategy(0, 0, null) {
      @Override
      public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) {
        throw missing;
      }
    };
    ScriptedPreprocessor preprocessor = new ScriptedPreprocessor(strategy);
    assertSame(missing, assertThrows(NoClassDefFoundError.class, () -> openVideo(preprocessor)));
  }

  /** Closing a video part way closes its frames once. */
  @Test
  void closingPartWayClosesTheFramesOnce() throws IOException {
    ScriptedStrategy strategy = new ScriptedStrategy(5, -1, null);
    ClosableIterator<Chunk> chunks = new ScriptedPreprocessor(strategy).open(video(), file(VIDEO_SIZE), null, new TypedProperties());
    assertEquals(Long.valueOf(0L), chunks.next().getFrameIndex());
    chunks.close();
    chunks.close();
    assertEquals(1, strategy.closed.get());
  }

  /** A preprocessor over the scripted strategy, as a custom strategy plugs in. */
  private static final class ScriptedPreprocessor extends VideoFramePreprocessor {
    private ScriptedPreprocessor(FrameExtractionStrategy strategy) {
      super(strategy);
    }
  }

  /**
   * {@code frames} frames of 160x120, one every 2 s, failing with {@code failure} at the open, in {@code hasNext()} or
   * {@code next()} after {@code failAfter} frames, or when the frames are closed.
   */
  private static class ScriptedStrategy implements FrameExtractionStrategy {
    static final int AT_OPEN = 0;
    static final int IN_HAS_NEXT = 1;
    static final int IN_NEXT = 2;
    static final int AT_CLOSE = 3;
    static final String REFUSED_KEY = "test.scripted.refused";

    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final AtomicInteger validated = new AtomicInteger();
    private final int frames;
    private final int failAfter;
    private final RuntimeException failure;
    private final IOException openFailure;
    private final int where;

    ScriptedStrategy(int frames, int failAfter, RuntimeException failure) {
      this(frames, failAfter, failure, AT_OPEN);
    }

    ScriptedStrategy(int frames, int failAfter, RuntimeException failure, int where) {
      this.frames = frames;
      this.failAfter = failAfter;
      this.failure = failure;
      this.openFailure = null;
      this.where = where;
    }

    ScriptedStrategy(IOException openFailure) {
      this.frames = 0;
      this.failAfter = -1;
      this.failure = null;
      this.openFailure = openFailure;
      this.where = AT_OPEN;
    }

    static byte[] jpeg(int k) {
      return ("jpeg-frame-" + k).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String name() {
      return LOGIC;
    }

    @Override
    public void validateConfig(TypedProperties props) {
      validated.incrementAndGet();
      if (props.containsKey(REFUSED_KEY)) {
        throw new IllegalArgumentException("Config " + REFUSED_KEY + " is refused");
      }
    }

    @Override
    public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) throws IOException {
      opened.incrementAndGet();
      if (openFailure != null) {
        throw openFailure;
      }
      if (failure != null && where == AT_OPEN) {
        throw failure;
      }
      return new ClosableIterator<ExtractedFrame>() {
        private int taken;

        @Override
        public boolean hasNext() {
          if (where == IN_HAS_NEXT && taken == failAfter) {
            throw failure;
          }
          return taken < frames;
        }

        @Override
        public ExtractedFrame next() {
          if (where == IN_NEXT && taken == failAfter) {
            throw failure;
          }
          int k = taken++;
          byte[] image = jpeg(k);
          ExtractedFrame.EncodedImage encoded = new ExtractedFrame.EncodedImage(160, 120, ImageContentTypes.JPEG, image.length,
              () -> new ByteArrayInputStream(image));
          return new ExtractedFrame(2000L * k, k, true, "dedup-" + k, encoded);
        }

        @Override
        public void close() {
          closed.incrementAndGet();
          if (where == AT_CLOSE && failure != null) {
            throw failure;
          }
        }
      };
    }
  }

  private static ClosableIterator<Chunk> openVideo(ScriptedPreprocessor preprocessor) throws IOException {
    return preprocessor.open(video(), file(VIDEO_SIZE), null, new TypedProperties());
  }

  private static List<Chunk> openAndDrain(ScriptedPreprocessor preprocessor) throws IOException {
    return drain(openVideo(preprocessor));
  }

  private static BlobBytesSupplier video() {
    return () -> {
      throw new AssertionError("the scripted strategy never reads the video");
    };
  }

  private static FileMeta file(long size) {
    return new FileMeta("fid-1", "mp4", size);
  }

  private static byte[] bytes(Chunk chunk) throws IOException {
    try (InputStream in = chunk.getPayload().open()) {
      return in.readAllBytes();
    }
  }

  private static void drainInto(ClosableIterator<Chunk> iterator, List<Chunk> chunks) {
    try {
      iterator.forEachRemaining(chunks::add);
    } finally {
      iterator.close();
    }
  }

  private static List<Chunk> drain(ClosableIterator<Chunk> iterator) {
    List<Chunk> chunks = new ArrayList<>();
    drainInto(iterator, chunks);
    return chunks;
  }
}
