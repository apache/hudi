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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.sources.blob.BlobInlineCeiling;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoFrameConfig;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The frame-heap start check of an embedding job that preps video ({@link #prepsVideo}): each round is refused on the driver,
 * before any Spark work, unless
 * {@code slots × (6 · maxPixels + 16 MiB + 10 MiB) + reserve ≤ heap − reserved − fixed}, where
 * <ul>
 *   <li>{@code maxPixels} is {@link EmbeddingTableConfig#PREP_VIDEO_MAX_PIXELS}: the largest frame a video may declare;</li>
 *   <li>{@code 6 · maxPixels} is the per-task decode estimate (measured on the compute
 *       path);</li>
 *   <li>16 MiB per slot is decode overhead, one writer row group and the JPEG encoder's output;</li>
 *   <li>10 MiB per slot is {@link #STREAMING_BUFFER_BYTES}: the FFmpeg frame source's block reads (2 MiB) and the task's
 *       background out-of-line frame writes (4 MiB pending plus one payload of at most 4 MiB waiting for room);</li>
 *   <li>{@code reserve = 4 × ⌈3 · maxPixels / R⌉ × R}, 0 when {@code 3 · maxPixels < R / 2}, is the per-executor contiguity
 *       reserve described below, R being the G1 region size;</li>
 *   <li>heap, reserved and slots are what {@link BlobInlineCeiling#onDriver} reads on the driver: the executor heap
 *       ({@code spark.executor.memory}, or this JVM's max heap in local mode), Spark's reserved memory, and the task slots per
 *       executor;</li>
 *   <li>fixed is {@link EmbeddingTableConfig#PREP_VIDEO_HEAP_FIXED_BYTES}, the per-executor rest.</li>
 * </ul>
 *
 * <p>A frame raster of {@code 3 · maxPixels} bytes is one large frame buffer: once it is at least half a G1 region, G1 places it
 * in whole contiguous regions, and measured on the prep path (2160p prep legs at 1, 2 and 4 task slots) the heap ran out of
 * contiguous regions beside Spark's own long-lived large buffers before it ran out of bytes, by a shortfall that is once per
 * executor rather than per slot. The reserve therefore counts {@link #RESERVE_RASTERS} rasters per executor in whole regions,
 * whatever the collector. R is an explicit {@code -XX:G1HeapRegionSize} (the last occurrence wins, as in the JVM: in local mode
 * from this JVM's launch arguments, on a cluster from spark.executor.defaultJavaOptions then spark.executor.extraJavaOptions),
 * else G1's ergonomic size on JDK 17: heap / 2048 rounded up to a power of two, clamped to [1 MiB, 32 MiB]. An explicit size is
 * taken as the JVM takes it: rounded up to a power of two and clamped to [1 MiB, 512 MiB] ({@code =0} lets G1 choose). An
 * explicit {@code -XX:G1HeapRegionSize} is therefore the exact override where a JDK chooses its regions differently.
 *
 * <p>Data prep's read ahead ({@link EmbeddingTableConfig#PREP_PREFETCH_MAX_BYTES}) never changes this admission: a job reads
 * ahead only when the slot term plus {@code 2 · prefetch} still fits ({@link #admittedPrefetchBytes}), else it reads nothing ahead.
 *
 * <p>Counted per slot besides the measured 16 MiB, which was measured before they existed: the FFmpeg frame source's block reads
 * (at most 2 MiB per clip) and a task's background out-of-line frame writes (at most 4 MiB pending plus the payload of at most
 * 4 MiB waiting for room), {@link #STREAMING_BUFFER_BYTES} in all.
 *
 * <p>It is static: it never waits and never defers. The operator lowers the pixel limit or the slots, or gives the executor more
 * heap. It reads only the job's properties and the Spark conf: no decoder class is loaded on the driver. A job whose prepped
 * extensions map to no video preprocessor is never checked: a job of images and PDFs that lists its extensions in
 * {@link EmbeddingTableConfig#PREP_FILE_EXTENSIONS} runs on any heap.
 */
public final class VideoFrameHeapCheck {

  /** Per-slot bytes beside the decoded frame: decode overhead, one writer row group, the encoder's output. */
  static final long SLOT_OVERHEAD_BYTES = 16L * 1024 * 1024;
  /**
   * Per-slot bytes of the buffers that came after the {@link #SLOT_OVERHEAD_BYTES} measurement: the FFmpeg frame source's block
   * reads and a task's background out-of-line frame writes (pending, plus one payload waiting for room): 10 MiB.
   */
  static final long STREAMING_BUFFER_BYTES = FfmpegKeyframeVideoFrameSource.BUFFERED_READ_BYTES + 2L * OutOfLineWritePool.MAX_PENDING_BYTES;
  private static final Logger LOG = LoggerFactory.getLogger(VideoFrameHeapCheck.class);
  /** The read-ahead buffers a prep task holds at once: the file it prepares and the next one. */
  static final long PREFETCH_BUFFERS_PER_TASK = 2L;
  /** Bytes per declared pixel a decoding task slot holds. */
  static final long BYTES_PER_PIXEL = 6L;
  /** Bytes per pixel of one frame raster (three 8-bit channels): the large frame buffer that needs contiguous regions. */
  static final long RASTER_BYTES_PER_PIXEL = 3L;
  /** Frame rasters per executor the contiguity reserve counts in whole G1 regions (measured). */
  static final int RESERVE_RASTERS = 4;
  /** G1's smallest ergonomic region size. */
  static final long MIN_REGION_BYTES = 1024L * 1024;
  /** G1's largest ergonomic region size. */
  static final long MAX_REGION_BYTES = 32L * 1024 * 1024;
  /** G1's largest explicit region size on JDK 17: a larger {@code -XX:G1HeapRegionSize} is clamped to it. */
  static final long MAX_EXPLICIT_REGION_BYTES = 512L * 1024 * 1024;
  /** G1's ergonomic target number of regions in the heap. */
  static final long TARGET_REGION_COUNT = 2048L;
  /** Spark's executor JVM options set by the administrator; read before {@link #EXECUTOR_EXTRA_JAVA_OPTIONS}. */
  static final String EXECUTOR_DEFAULT_JAVA_OPTIONS = "spark.executor.defaultJavaOptions";
  /** Spark's executor JVM options set by the user; read last, so its region size wins. */
  static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
  /** The JVM option that sets G1's region size. */
  static final String REGION_SIZE_OPTION = "-XX:G1HeapRegionSize=";
  /** A size as the JVM reads it: bytes, or a number with a k, m or g suffix (either case). */
  private static final Pattern SIZE = Pattern.compile("(\\d+)([kKmMgG]?)");

  private VideoFrameHeapCheck() {
  }

  /**
   * Whether the job preps video: an extension it preps ({@link EmbeddingTableConfig#PREP_FILE_EXTENSIONS}, or without that list
   * every extension {@code registry} maps) resolves to a preprocessor of {@link SourceKind#VIDEO}.
   */
  public static boolean prepsVideo(EmbeddingTableConfig config, PreprocessorRegistry registry) {
    Option<List<String>> allowed = config.prepFileExtensions();
    Collection<String> extensions = allowed.isPresent() ? allowed.get() : registry.extensions();
    return extensions.stream().map(registry::forExtension)
        .anyMatch(preprocessor -> preprocessor.isPresent() && preprocessor.get().sourceKind() == SourceKind.VIDEO);
  }

  /**
   * @throws HoodieValidationException naming the keys and every term when the job's video decodes do not fit the executor heap
   * @throws IllegalArgumentException   naming the key of a malformed frame, heap or region size setting
   */
  public static void check(EmbeddingTableConfig config, SparkConf conf) {
    check(config, conf, ManagementFactory.getRuntimeMXBean().getInputArguments());
  }

  /** {@link #check(EmbeddingTableConfig, SparkConf)} with this JVM's launch arguments given, for local mode. */
  static void check(EmbeddingTableConfig config, SparkConf conf, List<String> thisJvmArguments) {
    HoodieValidationException refused = refusal(config, conf, thisJvmArguments, 0L);
    if (refused != null) {
      throw refused;
    }
  }

  /**
   * The read-ahead bound a job that preps video runs with: {@link EmbeddingTableConfig#PREP_PREFETCH_MAX_BYTES} when the frame
   * decodes still fit the executor heap with a task's two read-ahead buffers ({@code 2 · prefetch} more per task slot), else 0 (no
   * read ahead, with a WARN), so the read ahead never refuses a job the frame decodes alone admit. A job {@link #check} refuses
   * is refused there first.
   */
  public static long admittedPrefetchBytes(EmbeddingTableConfig config, SparkConf conf) {
    return admittedPrefetchBytes(config, conf, ManagementFactory.getRuntimeMXBean().getInputArguments());
  }

  static long admittedPrefetchBytes(EmbeddingTableConfig config, SparkConf conf, List<String> thisJvmArguments) {
    long prefetch = config.prepPrefetchMaxBytes();
    if (prefetch == 0L) {
      return 0L;
    }
    HoodieValidationException refused = refusal(config, conf, thisJvmArguments, prefetch);
    if (refused == null) {
      return prefetch;
    }
    LOG.warn("This round preps video without reading files ahead: {} = {} needs {} x {} more bytes per task slot, which does not fit "
        + "the executor heap beside the frame decodes. The round runs as without a read ahead; give the executor more heap or lower "
        + "{} to read ahead.", EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), prefetch, PREFETCH_BUFFERS_PER_TASK, prefetch,
        EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key());
    return 0L;
  }

  /** The refusal of a job whose frame decodes, with {@code prefetch}-byte read-ahead buffers, do not fit, or null when they fit. */
  private static HoodieValidationException refusal(EmbeddingTableConfig config, SparkConf conf, List<String> thisJvmArguments,
                                                   long prefetch) {
    long maxPixels = VideoFrameConfig.from(config.getProps()).getMaxPixels();
    long fixed = config.videoHeapFixedBytes();
    BlobInlineCeiling executor = BlobInlineCeiling.onDriver(conf, 1.0);
    G1Region g1Region;
    if (executor.readsThisJvm()) {
      g1Region = g1Region(explicitRegionBytes(thisJvmArguments, "this JVM's launch arguments"), "this JVM",
          executor.getExecutorHeapBytes());
    } else {
      long extra = explicitRegionBytes(options(conf, EXECUTOR_EXTRA_JAVA_OPTIONS), EXECUTOR_EXTRA_JAVA_OPTIONS);
      g1Region = extra >= 0
          ? g1Region(extra, EXECUTOR_EXTRA_JAVA_OPTIONS, executor.getExecutorHeapBytes())
          : g1Region(explicitRegionBytes(options(conf, EXECUTOR_DEFAULT_JAVA_OPTIONS), EXECUTOR_DEFAULT_JAVA_OPTIONS),
              EXECUTOR_DEFAULT_JAVA_OPTIONS, executor.getExecutorHeapBytes());
    }
    long region = g1Region.bytes;
    // saturating: a pixel limit large enough to overflow needs more heap than any executor has
    long slotTerm = saturatedAdd(saturatedAdd(saturatedMultiply(BYTES_PER_PIXEL, maxPixels), SLOT_OVERHEAD_BYTES + STREAMING_BUFFER_BYTES),
        saturatedMultiply(PREFETCH_BUFFERS_PER_TASK, prefetch));
    long reserve = humongousReserveBytes(maxPixels, region);
    long needed = saturatedAdd(saturatedMultiply(executor.slots(), slotTerm), reserve);
    long available = executor.getExecutorHeapBytes() - executor.getReservedBytes() - fixed;
    if (needed > available) {
      return new HoodieValidationException("This job preps video, but its frame decodes do not fit the executor heap: "
          + executor.slots() + " task slots x (" + BYTES_PER_PIXEL + " x " + maxPixels + " pixels ("
          + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key() + ") + " + SLOT_OVERHEAD_BYTES + " + " + STREAMING_BUFFER_BYTES
          + " (block reads and out-of-line writes)"
          + (prefetch == 0 ? "" : " + " + PREFETCH_BUFFERS_PER_TASK + " x " + prefetch + " (" + EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key() + ")")
          + ") + reserve " + RESERVE_RASTERS
          + " frame rasters (" + RASTER_BYTES_PER_PIXEL + " x " + maxPixels + " bytes each) in " + reserveRegions(maxPixels, region)
          + " regions each x " + region + " (G1 region, " + g1Region.source + ") = " + reserve + "; needed " + needed
          + " bytes > executor heap " + executor.getExecutorHeapBytes() + " [" + executor.getExecutorHeapSource() + "] - reserved "
          + executor.getReservedBytes() + " - fixed " + fixed + " (" + EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key() + ") = "
          + available + " bytes. Lower " + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key()
          + (prefetch == 0 ? "" : " or " + EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key())
          + " or the task slots per executor, or give the executor more heap"
          // without an allow-list every mapped extension is prepped, the video ones included
          + (config.prepFileExtensions().isPresent() ? "" : ", or, for a job without videos, list its extensions in "
              + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key())
          + "; nothing was written.");
    }
    return null;
  }

  /**
   * G1's region size for a heap started with {@code jvmArguments}: the last explicit {@code -XX:G1HeapRegionSize}, else the
   * ergonomic size for {@code heapBytes}.
   *
   * @throws IllegalArgumentException when the explicit region size is not a size
   */
  static long g1RegionBytes(long heapBytes, List<String> jvmArguments) {
    return g1Region(explicitRegionBytes(jvmArguments, "the JVM arguments"), "the JVM arguments", heapBytes).bytes;
  }

  /**
   * The one choice of G1's region: an explicit size as the JVM takes it when {@code explicitBytes} is positive, else (none, or
   * {@code =0}) the ergonomic size for {@code heapBytes}; the source names {@code explicitOrigin} or G1's default.
   */
  private static G1Region g1Region(long explicitBytes, String explicitOrigin, long heapBytes) {
    if (explicitBytes > 0) {
      return new G1Region(powerOfTwoRegion(explicitBytes, MAX_EXPLICIT_REGION_BYTES), "-XX:G1HeapRegionSize in " + explicitOrigin);
    }
    return new G1Region(ergonomicRegionBytes(heapBytes), "G1 default for the heap");
  }

  /**
   * The contiguity reserve: {@link #RESERVE_RASTERS} frame rasters of {@code 3 · maxPixels} bytes, each in whole regions of
   * {@code regionBytes}; 0 when a raster is below half a region (G1 then never needs contiguous regions for it). Saturating.
   */
  static long humongousReserveBytes(long maxPixels, long regionBytes) {
    return saturatedMultiply(RESERVE_RASTERS, saturatedMultiply(reserveRegions(maxPixels, regionBytes), regionBytes));
  }

  /** Whole regions one frame raster occupies, 0 below half a region. */
  private static long reserveRegions(long maxPixels, long regionBytes) {
    long rasterBytes = saturatedMultiply(RASTER_BYTES_PER_PIXEL, maxPixels);
    if (rasterBytes < regionBytes / 2) {
      return 0L;
    }
    return rasterBytes / regionBytes + (rasterBytes % regionBytes == 0 ? 0 : 1);
  }

  /** G1's ergonomic region size on JDK 17: heap / 2048 rounded up to a power of two, clamped to [1 MiB, 32 MiB]. */
  private static long ergonomicRegionBytes(long heapBytes) {
    long target = heapBytes / TARGET_REGION_COUNT + (heapBytes % TARGET_REGION_COUNT == 0 ? 0 : 1);
    return powerOfTwoRegion(target, MAX_REGION_BYTES);
  }

  /** {@code bytes} rounded up to a power of two and clamped to [{@link #MIN_REGION_BYTES}, {@code maxBytes}], as G1 sizes regions. */
  private static long powerOfTwoRegion(long bytes, long maxBytes) {
    if (bytes <= MIN_REGION_BYTES) {
      return MIN_REGION_BYTES;
    }
    if (bytes >= maxBytes) {
      return maxBytes;
    }
    long power = Long.highestOneBit(bytes);
    return power == bytes ? power : power << 1;
  }

  /**
   * The last {@code -XX:G1HeapRegionSize} in {@code arguments} in bytes (0 lets G1 choose, as in the JVM), or -1 when there is
   * none.
   *
   * @throws IllegalArgumentException naming {@code origin} when the value is not a size
   */
  private static long explicitRegionBytes(List<String> arguments, String origin) {
    String value = null;
    for (String argument : arguments) {
      if (argument.startsWith(REGION_SIZE_OPTION)) {
        value = argument.substring(REGION_SIZE_OPTION.length());
      }
    }
    if (value == null) {
      return -1L;
    }
    Matcher size = SIZE.matcher(value);
    if (!size.matches()) {
      throw new IllegalArgumentException(REGION_SIZE_OPTION + value + " in " + origin
          + " is not a size: expected bytes or a number with a k, m or g suffix");
    }
    long multiplier;
    switch (size.group(2).toLowerCase(Locale.ROOT)) {
      case "k":
        multiplier = 1024L;
        break;
      case "m":
        multiplier = 1024L * 1024;
        break;
      case "g":
        multiplier = 1024L * 1024 * 1024;
        break;
      default:
        multiplier = 1L;
    }
    long number;
    try {
      number = Long.parseLong(size.group(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(REGION_SIZE_OPTION + value + " in " + origin + " is not a size: " + e.getMessage(), e);
    }
    return saturatedMultiply(number, multiplier);
  }

  /** The whitespace-separated JVM options under {@code key}, empty when unset. */
  private static List<String> options(SparkConf conf, String key) {
    return Arrays.asList(conf.get(key, "").trim().split("\\s+"));
  }

  /** A G1 region size and where it came from, for the refusal message. */
  private static final class G1Region {
    private final long bytes;
    private final String source;

    private G1Region(long bytes, String source) {
      this.bytes = bytes;
      this.source = source;
    }
  }

  private static long saturatedMultiply(long a, long b) {
    try {
      return Math.multiplyExact(a, b);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  private static long saturatedAdd(long a, long b) {
    try {
      return Math.addExact(a, b);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}
