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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFrameHeapCheck;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.streamer.NoFfmpegClassPath;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.memory.VideoMemoryCellFixtures.Scene;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Memory cells of video frames in the single-writer embedding round. Each cell generates its video at test time
 * ({@link VideoMemoryCellFixtures}: one seeded still picture per second, a keyframe at the start of every second, no
 * {@code ffmpeg} program needed), announces {@value #COPIES} copies of it (or {@value #STORE_COPIES} for {@code video-store})
 * under their own keys in ONE events commit, then runs the embedding job over the unstructured data source on S3 ({@code prep_and_compute},
 * the dummy multimodal model, every persist DISK_ONLY, prep allow-list {@code mp4}) in a child JVM ({@link VideoRoundMemoryChild})
 * at the cell's heap, local[4] under G1. The default size and type aware balance gives the videos, far under the video budget,
 * the group's floor of 4 prep tasks (P = 4), so 4 videos are decoded at once, the case the video frame heap start check sizes.
 * The job declares
 * {@code prep.video.max.pixels} = 1920x1080 (the largest video of the cells), the source objects are above the blob inline
 * threshold (prep streams each video from its object) and the input store's inline threshold is the 80 KiB cap of Spark 3.5's
 * Parquet ({@link ParquetInlineCap#MAX_BYTES}), as the video round tests set them.
 *
 * <p>A cell passes when the child ends its one round without an exhausted heap, the start check admitted the job, and the table
 * holds exactly the frames the same frame source keeps when the parent reads the video straight from its file: every frame row
 * of every copy, each an image row with its embedding. The child runs with {@code -XX:+HeapDumpOnOutOfMemoryError} (dump
 * {@code target/memory-cells/<cell>-first-oom.hprof}) and with every folder holding an {@code ffmpeg} program removed from its
 * {@code PATH}. Each cell writes its report ({@code MEMORY_CELL} lines) and its child's log under {@code target/memory-cells/}.
 *
 * <p>Gated: a cell runs only when named in {@code -Dvideo.memory.cells} (a comma list), so the class costs nothing in the
 * ordinary test runs. The reference heap is {@value #REFERENCE_HEAP} (the embedding cells' one) unless {@code -Dvideo.memory.heap}
 * says otherwise. Cells:
 * <ul>
 *   <li>{@code video-light}: 4 copies of a 20 s 320x240 video (FFmpeg MPEG-4, frames small enough to be stored inline), the
 *   reference heap: the cell shape in seconds;</li>
 *   <li>{@code video-ffmpeg}: 4 copies of a 10-minute 1920x1080 video (FFmpeg MPEG-4, 2 fps, about 600 frames each, every frame
 *   JPEG about 100 KB, so stored out of line) through the default FFmpeg preprocessor, the reference heap: 4 concurrent 1080p
 *   decodes over a long video; the heap a task holds must not grow with the video's length;</li>
 *   <li>{@code video-jcodec}: 4 copies of a 5-minute 1920x1080 H.264 video (JCodec's encoder, 2 fps) through the JCodec
 *   preprocessor, in a child JVM WITHOUT the FFmpeg jars on its class path (as a job without FFmpeg runs), the reference heap:
 *   JCodec decodes on the heap, 4 at once;</li>
 *   <li>{@code video-store}: {@value #STORE_COPIES} copies of a 5-minute 1280x720 zone-plate video (FFmpeg MPEG-4), every kept
 *   frame a JPEG above the 80 KiB cap (about 150 to 215 KB, about 2400 frames, over 300 MB of payload in one round, about half
 *   the heap and more), the reference heap: every frame written out of line by the embedding input store under the table's
 *   {@code .hoodie/.blob} folder, then read back by compute; the payload bytes must stream, never pile up on the heap;</li>
 *   <li>{@code video-fixed-term}: the {@code video-ffmpeg} job at the SMALLEST executor heap the start check accepts with the
 *   default fixed term ({@link EmbeddingTableConfig#DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES}, 64 MiB): the heap is searched in whole
 *   MiB with the check itself ({@link #smallestAdmittedHeapMib}). For the cells' settings (4 slots, 1920x1080, Spark's 300 MiB
 *   reserved memory, 1 MiB G1 regions) that is 4 x (6 x 2073600 + 16 MiB) + 4 x 6 x 1 MiB + 300 MiB + 64 MiB = 523722752
 *   bytes, so 500 MiB. Passing says the default fixed term covers the rest of the executor heap of this round; an exhausted
 *   heap says it does not.</li>
 *   <li>{@code video-length-light} and {@code video-length}: the {@code video-ffmpeg} round over a short video and then over a
 *   long one, in two children; the live heap must not grow with the video's length ({@link #heapDoesNotGrowWithVideoLength}).</li>
 * </ul>
 */
public class TestVideoRoundMemoryCells extends EmbeddingRoundTestBase {

  static final String REFERENCE_HEAP = TestEmbeddingRoundMemoryCells.REFERENCE_HEAP;
  private static final long CELL_TIMEOUT_MINUTES = 15;
  /** Prep tasks and compute tasks: local[4]. */
  private static final int TASKS = 4;
  /** Videos of a cell, one per prep task. */
  private static final int COPIES = 4;
  /** Videos of {@code video-store}: two per prep task, twice the payload. */
  private static final int STORE_COPIES = 8;
  /** The largest frame the job declares, the frame of every heavy cell. */
  private static final long MAX_PIXELS = 1920L * 1080;
  private static final int FPS = 2;
  private static final long MIB = 1024L * 1024;
  private static final String FIXED_TERM_CELL = "video-fixed-term";
  /** The length cells' bound on the growth of the live heap from the short video to the long one, see the length cells. */
  private static final long LENGTH_DELTA_MIB = 32;
  /** The length cells' period of sampling full collections: a light run of under a minute still sees dozens. */
  private static final long LENGTH_SAMPLE_PERIOD_MS = 1000;

  private static List<String> selectedCells() {
    String cells = System.getProperty("video.memory.cells", "");
    return Arrays.stream(cells.split(",")).map(String::trim).filter(cell -> !cell.isEmpty()).collect(Collectors.toList());
  }

  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"video-light", "video-ffmpeg", "video-jcodec", "video-store", FIXED_TERM_CELL})
  public void cellPassesAtItsHeap(String cell) throws Exception {
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dvideo.memory.cells)");
    Path work = localPath(basePath()).resolve("cell-" + cell);
    Files.createDirectories(work);
    boolean jcodec = "video-jcodec".equals(cell);
    boolean store = "video-store".equals(cell);
    Path video;
    switch (cell) {
      case "video-light":
        video = VideoMemoryCellFixtures.ffmpegMp4(work.resolve("light.mp4"), 20, FPS, 320, 240, Scene.MOSAIC);
        break;
      case "video-ffmpeg":
      case FIXED_TERM_CELL:
        video = VideoMemoryCellFixtures.ffmpegMp4(work.resolve("hd.mp4"), 600, FPS, 1920, 1080, Scene.MOSAIC);
        break;
      case "video-jcodec":
        video = VideoMemoryCellFixtures.jcodecMp4(work.resolve("hd-h264.mp4"), 300, FPS, 1920, 1080, Scene.MOSAIC);
        break;
      case "video-store":
        video = VideoMemoryCellFixtures.ffmpegMp4(work.resolve("zone.mp4"), 300, FPS, 1280, 720, Scene.ZONE_PLATE);
        break;
      default:
        throw new IllegalArgumentException(cell);
    }
    byte[] bytes = Files.readAllBytes(video);
    List<FixtureFile> files = new ArrayList<>();
    for (int i = 0; i < (store ? STORE_COPIES : COPIES); i++) {
      files.add(object("cells/" + cell + "/video-" + i + ".mp4", bytes));
    }
    announce(files.toArray(new FixtureFile[0]));

    TypedProperties props = cellProps(jcodec);
    // the expected rows: the frames the same source keeps, read straight from the file
    FrameStats expected = keptFrames(jcodec ? new JcodecKeyframeVideoFrameSource() : new FfmpegKeyframeVideoFrameSource(), video,
        files.get(0), props);
    assertTrue(expected.frames > 1, "the video keeps several frames: " + expected.frames);
    if (store) {
      assertTrue(expected.minBytes > ParquetInlineCap.MAX_BYTES, "every frame is above the 80 KiB cap: " + expected.minBytes);
    }
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + cell);
    }

    String heap;
    long admittedHeapMib = -1;
    if (FIXED_TERM_CELL.equals(cell)) {
      admittedHeapMib = smallestAdmittedHeapMib(props);
      heap = admittedHeapMib + "m";
    } else {
      heap = System.getProperty("video.memory.heap", REFERENCE_HEAP);
    }
    long start = System.nanoTime();
    Path reports = Paths.get("target", "memory-cells").toAbsolutePath();
    Files.createDirectories(reports);
    Path childLog = reports.resolve(cell + "-child.log");
    List<String> output = runChild(cell, heap, Collections.emptyList(), propsFile, jcodec, reports, childLog);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    // graded on the child's own report and the committed table
    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog);
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(1, rounds.size(), rounds.toString());
    assertEquals("none", value(output, "FFMPEG_PROGRAM_ON_PATH "), "the child's PATH holds no ffmpeg program");
    assertEquals(String.valueOf(!jcodec), value(output, "FFMPEG_ON_CLASS_PATH "), "FFmpeg's jars on the child's class path");
    long childMaxHeap = Long.parseLong(value(output, "CHILD_MAX_HEAP_BYTES "));
    String decodes = value(output, "VIDEO_DECODES ");
    Map<String, String> decodeTasks = decodeTasks(decodes);
    assertEquals((long) files.size(), decodeTasks.values().stream().mapToLong(Long::parseLong).sum(), "one decode per video: " + decodes);
    if (!"video-light".equals(cell)) {
      // the cell's claim is TASKS decodes at once at this heap: a balance that prepped the videos one at a time would hold far less
      assertEquals(TASKS, decodeTasks.size(), "the videos are decoded by " + TASKS + " tasks: " + decodes);
      assertTrue(decodes.startsWith("max_in_flight=" + TASKS + " "), TASKS + " decodes run at once: " + decodes);
    }
    if (admittedHeapMib > 0) {
      assertEquals(admittedHeapMib * MIB, childMaxHeap, "the start check read exactly the computed heap in the child");
    }

    Dataset<Row> table = spark().read().format("hudi").load(table());
    Map<String, Long> framesPerFile = counts(table, functions.col(EmbeddingTableSchema.SOURCE_PATH_SHA));
    Map<String, Long> expectedPerFile = new TreeMap<>();
    files.forEach(file -> expectedPerFile.put(file.sha, (long) expected.frames));
    assertEquals(expectedPerFile, framesPerFile, "every kept frame of every video is a row");
    long rows = table.count();
    assertEquals(0L, table.filter(functions.col(EmbeddingStreamers.MODEL_COLUMN).isNull()).count(), "every row has its embedding");
    assertEquals(Collections.singletonMap(EmbeddingTableSchema.MODALITY_IMAGE, rows),
        counts(table, functions.col(EmbeddingTableSchema.MODALITY)), "every row is a frame");
    assertEquals(Collections.singletonMap(jcodec ? JcodecVideoFramePreprocessor.NAME : VideoFramePreprocessor.INPUT_LOGIC, rows),
        counts(table, functions.col(EmbeddingTableSchema.AUX).getItem(EmbeddingTableSchema.AUX_INPUT_LOGIC)),
        "every frame is the expected preprocessor's");
    Map<String, Long> storage = counts(table, functions.col(EmbeddingTableSchema.PREPARED_IMAGE).getField(HoodieSchema.Blob.TYPE));
    long payloadFiles = payloadFiles();
    if (store) {
      assertEquals(Collections.singletonMap(HoodieSchema.Blob.OUT_OF_LINE, rows), storage, "every frame is stored out of line");
      assertEquals(rows, payloadFiles, "one payload file per frame");
    }

    long peak = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES "));
    List<String> report = new ArrayList<>();
    report.add("MEMORY_CELL " + cell + " heap=" + heap + " child_max_heap_bytes=" + childMaxHeap + " rounds=" + rounds.size()
        + " videos=" + files.size() + " video_bytes=" + bytes.length + " frames_per_video=" + expected.frames + " rows=" + rows
        + " frame_jpeg_bytes=" + expected.minBytes + ".." + expected.maxBytes + " prepared_image=" + storage + " payload_files="
        + payloadFiles + " decodes=" + decodes.replace(' ', '/') + " heap_pool_peaks_sum_bytes=" + peak + " seconds=" + seconds
        + MemoryCellJvm.gcFields(childLog, output));
    if (admittedHeapMib > 0) {
      report.add("MEMORY_CELL " + cell + " computed_smallest_admitted_heap_mib=" + admittedHeapMib + " fixed_term_bytes="
          + EmbeddingTableConfig.DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES + " max_pixels=" + MAX_PIXELS + " task_slots=" + TASKS);
    }
    report.forEach(System.out::println);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /**
   * Length cells: the heap of a round of 4 concurrent FFmpeg decodes must not grow with the length of the videos. Each cell runs
   * two children one after the other, at the same heap and master, with {@code video-ffmpeg}'s job and video (1920x1080 FFmpeg
   * MPEG-4, 2 fps, a new picture every second), {@value #COPIES} copies decoded at once by the {@value #TASKS} tasks, each run
   * into a table and an events table of its own: first {@value #COPIES} copies of a SHORT video, then of a LONG one. Each run is
   * graded as {@code video-ffmpeg} is (the round, the 4 decodes at once, every kept frame of every copy a row with its
   * embedding); then the long video keeps as many more frames as it is longer, both runs collected garbage, and the live heap of
   * the long run is at most {@value #LENGTH_DELTA_MIB} MiB above the short run's.
   *
   * <p>The live heap is {@link PostGcHeap}'s {@code POST_FULL_GC_HEAP_PEAK_BYTES}: the children ask for a full collection every
   * {@value #LENGTH_SAMPLE_PERIOD_MS} ms, and the largest heap left after those collections is the largest live heap the samples
   * saw. The peak over every collection ({@code POST_GC_HEAP_PEAK_BYTES}) is reported too, not graded: after a young collection
   * the old generation still holds the garbage promoted since the last full one, which grows with the run's length alone.
   * <ul>
   *   <li>{@code video-length-light}: 20 s against 60 s: the plumbing in about a minute, and the noise of the difference when the
   *   inputs are small;</li>
   *   <li>{@code video-length}: 2 min against 10 min ({@code video-ffmpeg}'s video). A 10-minute video keeps about 600 frames of
   *   about 100 KB of JPEG (60 MB; 2 minutes: 12 MB) and decodes 1200 pictures of 6 MB (1920x1080x3). A task that kept its
   *   video's frames until the video ends would hold about 48 MB more in the long run, 190 MB for the four; one that kept a
   *   decoded picture per second of video, gigabytes. {@value #LENGTH_DELTA_MIB} MiB is below what ONE task keeping its
   *   frames would add, and above the round's own noise: Spark's and Hudi's per-round state is the same in both runs (one
   *   commit of 4 files, the same tasks), the rows differ (about 480 against 2400, a few hundred bytes each on the heap while
   *   they are written), and a sample may catch the 4 decoders at different points of their bounded working set (the frame
   *   heap check budgets 6 pictures and 16 MiB per decode).</li>
   * </ul>
   */
  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"video-length-light", "video-length"})
  public void heapDoesNotGrowWithVideoLength(String cell) throws Exception {
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dvideo.memory.cells)");
    boolean light = "video-length-light".equals(cell);
    int shortSeconds = light ? 20 : 2 * 60;
    int longSeconds = light ? 60 : 10 * 60;
    Path work = localPath(basePath()).resolve("cell-" + cell);
    Files.createDirectories(work);
    long generateStart = System.nanoTime();
    Path shortVideo = VideoMemoryCellFixtures.ffmpegMp4(work.resolve("short.mp4"), shortSeconds, FPS, 1920, 1080, Scene.MOSAIC);
    Path longVideo = VideoMemoryCellFixtures.ffmpegMp4(work.resolve("long.mp4"), longSeconds, FPS, 1920, 1080, Scene.MOSAIC);
    long generateSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - generateStart);
    String heap = System.getProperty("video.memory.heap", REFERENCE_HEAP);
    Path reports = Paths.get("target", "memory-cells").toAbsolutePath();
    Files.createDirectories(reports);
    List<String> childJvm = Collections.singletonList("-D" + PostGcHeap.SAMPLE_PROPERTY + "=" + LENGTH_SAMPLE_PERIOD_MS);
    VideoRun shortRun = runAndGradeLength(cell + "-short", shortVideo, heap, childJvm, reports);
    VideoRun longRun = runAndGradeLength(cell + "-long", longVideo, heap, childJvm, reports);

    int ratio = longSeconds / shortSeconds;
    assertTrue(shortRun.frames > 1, "the short video keeps several frames: " + shortRun.frames);
    assertTrue(Math.abs(longRun.frames - (long) ratio * shortRun.frames) <= ratio,
        "the kept frames scale with the video's length: " + shortRun.frames + " against " + longRun.frames);
    for (VideoRun run : Arrays.asList(shortRun, longRun)) {
      assertTrue(run.postGcCount > 0, run.label + " collected garbage: " + run.postGcCount);
      assertTrue(run.postFullGcCount > 0, run.label + " ran its sampling full collections: " + run.postFullGcCount);
    }
    long delta = longRun.postFullGcPeak - shortRun.postFullGcPeak;
    List<String> report = new ArrayList<>();
    report.add(shortRun.summary());
    report.add(longRun.summary());
    report.add("MEMORY_CELL " + cell + " short_seconds=" + shortSeconds + " long_seconds=" + longSeconds + " sample_period_ms="
        + LENGTH_SAMPLE_PERIOD_MS + " post_full_gc_heap_peak_delta_bytes=" + delta + " post_gc_heap_peak_delta_bytes="
        + (longRun.postGcPeak - shortRun.postGcPeak) + " bound_bytes=" + LENGTH_DELTA_MIB * MIB + " generate_seconds="
        + generateSeconds);
    report.forEach(System.out::println);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
    assertTrue(delta <= LENGTH_DELTA_MIB * MIB, "the live heap of the long video's round is at most " + LENGTH_DELTA_MIB
        + " MiB above the short video's one: " + longRun.postFullGcPeak + " - " + shortRun.postFullGcPeak + " = " + delta);
  }

  /** One graded child run of a length cell. */
  private static final class VideoRun {
    private String label;
    private String heap;
    private long videoBytes;
    private long frames;
    private long rows;
    private String decodes;
    private long heapPoolPeaks;
    private long postGcPeak;
    private long postGcCount;
    private long postFullGcPeak;
    private long postFullGcCount;
    private long childSeconds;
    private String gcFields;

    private String summary() {
      return "MEMORY_CELL " + label + " heap=" + heap + " videos=" + COPIES + " video_bytes=" + videoBytes + " frames_per_video="
          + frames + " rows=" + rows + " decodes=" + decodes.replace(' ', '/') + " heap_pool_peaks_sum_bytes=" + heapPoolPeaks
          + " post_gc_heap_peak_bytes=" + postGcPeak + " post_gc_count=" + postGcCount + " post_full_gc_heap_peak_bytes="
          + postFullGcPeak + " post_full_gc_count=" + postFullGcCount + " seconds=" + childSeconds + gcFields;
    }
  }

  /**
   * Announces {@value #COPIES} copies of {@code video} in one events commit of an events table of their own, runs the
   * {@code video-ffmpeg} job over them into a table of its own in a child at {@code heap} (with {@code childJvm} among its JVM
   * options), and grades it as {@code video-ffmpeg} is graded. {@code label} names the run's folders, log and heap dump.
   */
  private VideoRun runAndGradeLength(String label, Path video, String heap, List<String> childJvm, Path reports) throws Exception {
    BlobJobHarness objects = BlobJobHarness.under(localPath(basePath()).resolve("source-" + label))
        .install(spark().sparkContext().hadoopConfiguration());
    String table = table("table-" + label);
    Path work = localPath(basePath()).resolve("cell-" + label);
    Files.createDirectories(work);
    byte[] bytes = Files.readAllBytes(video);
    List<FixtureFile> files = new ArrayList<>();
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    for (int i = 0; i < COPIES; i++) {
      FixtureFile file = object("cells/" + label + "/video-" + i + ".mp4", bytes);
      files.add(file);
      events.add(objects.put(keyOf(file), file.bytes));
    }
    objects.announce(spark(), events);

    TypedProperties props = cellProps(false, objects, table);
    FrameStats expected = keptFrames(new FfmpegKeyframeVideoFrameSource(), video, files.get(0), props);
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + label);
    }
    long start = System.nanoTime();
    Path childLog = reports.resolve(label + "-child.log");
    List<String> output = runChild(label, heap, childJvm, propsFile, false, reports, childLog);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog);
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(1, rounds.size(), rounds.toString());
    assertEquals("none", value(output, "FFMPEG_PROGRAM_ON_PATH "), "the child's PATH holds no ffmpeg program");
    String decodes = value(output, "VIDEO_DECODES ");
    Map<String, String> decodeTasks = decodeTasks(decodes);
    assertEquals((long) COPIES, decodeTasks.values().stream().mapToLong(Long::parseLong).sum(), "one decode per video: " + decodes);
    assertEquals(TASKS, decodeTasks.size(), "the videos are decoded by " + TASKS + " tasks: " + decodes);
    assertTrue(decodes.startsWith("max_in_flight=" + TASKS + " "), TASKS + " decodes run at once: " + decodes);

    Dataset<Row> rows = spark().read().format("hudi").load(table);
    Map<String, Long> expectedPerFile = new TreeMap<>();
    files.forEach(file -> expectedPerFile.put(file.sha, (long) expected.frames));
    assertEquals(expectedPerFile, counts(rows, functions.col(EmbeddingTableSchema.SOURCE_PATH_SHA)),
        "every kept frame of every video is a row");
    assertEquals(0L, rows.filter(functions.col(EmbeddingStreamers.MODEL_COLUMN).isNull()).count(), "every row has its embedding");

    VideoRun run = new VideoRun();
    run.label = label;
    run.heap = heap;
    run.videoBytes = bytes.length;
    run.frames = expected.frames;
    run.rows = rows.count();
    run.decodes = decodes;
    run.heapPoolPeaks = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES "));
    run.postGcPeak = Long.parseLong(value(output, "POST_GC_HEAP_PEAK_BYTES "));
    run.postGcCount = Long.parseLong(value(output, "POST_GC_COUNT "));
    run.postFullGcPeak = Long.parseLong(value(output, "POST_FULL_GC_HEAP_PEAK_BYTES "));
    run.postFullGcCount = Long.parseLong(value(output, "POST_FULL_GC_COUNT "));
    run.childSeconds = seconds;
    run.gcFields = MemoryCellJvm.gcFields(childLog, output);
    return run;
  }

  /**
   * The cell's job: {@code prep_and_compute} with the dummy multimodal model, the source objects above the blob inline threshold,
   * the input store's inline threshold at the 80 KiB cap, prep of {@code mp4} only (the JCodec preprocessor when {@code jcodec}),
   * frames up to {@link #MAX_PIXELS}, the default fixed term, DISK_ONLY persists, the default balance (one video per prep task),
   * {@value #TASKS} compute tasks, the preprocessor wrapped to count its decodes.
   */
  private TypedProperties cellProps(boolean jcodec) {
    return cellProps(jcodec, objects(), table());
  }

  /** {@link #cellProps(boolean)} into {@code table} over the objects and events table of {@code objects}. */
  private TypedProperties cellProps(boolean jcodec, BlobJobHarness objects, String table) {
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table, EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, ParquetInlineCap.MAX_BYTES,
        EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(ParquetInlineCap.MAX_BYTES),
        model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER,
        model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, "text,image",
        HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), objects.eventsPath());
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    // the child installs the bucket file system over the same objects
    props.setProperty(BucketFolderFileSystem.ROOT, objects.objectsRoot().toAbsolutePath().toString());
    props.setProperty(HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    props.remove(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key());
    props.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), String.valueOf(TASKS));
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4");
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), String.valueOf(MAX_PIXELS));
    // the job's own preprocessor, unchanged, with its decodes counted (VIDEO_DECODES)
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), jcodec
        ? CountingJcodecVideoFramePreprocessor.class.getName() : CountingFfmpegVideoFramePreprocessor.class.getName());
    return props;
  }

  /**
   * The smallest executor heap, in whole MiB, that the video frame heap start check admits for the job {@code props} with
   * {@value #TASKS} task slots and Spark's default reserved memory: searched with {@link VideoFrameHeapCheck#check} itself over an
   * executor conf ({@code spark.executor.memory}, no explicit G1 region size, so the region is G1's default for that heap, as in
   * the child). In local mode the check reads the child's max heap instead, which is its {@code -Xmx} under G1 (the cell checks
   * it). The heap one MiB below is refused.
   */
  static long smallestAdmittedHeapMib(TypedProperties props) {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    long ceiling = 64 * 1024;
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(config, executor(ceiling)), "the job fits some heap");
    long low = 1;
    long high = ceiling;
    while (low < high) {
      long middle = (low + high) / 2;
      if (admits(config, middle)) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    long smallest = low;
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(config, executor(smallest - 1)));
    return smallest;
  }

  private static boolean admits(EmbeddingTableConfig config, long heapMib) {
    try {
      VideoFrameHeapCheck.check(config, executor(heapMib));
      return true;
    } catch (HoodieValidationException e) {
      return false;
    }
  }

  /** An executor of {@value #TASKS} cores and {@code heapMib} of heap, Spark's default reserved memory. */
  private static SparkConf executor(long heapMib) {
    return new SparkConf(false).set("spark.master", "yarn").set("spark.executor.cores", String.valueOf(TASKS))
        .set("spark.executor.memory", heapMib + "m");
  }

  /** The kept frames of one video and the smallest and largest frame JPEG. */
  private static final class FrameStats {
    private int frames;
    private long minBytes = Long.MAX_VALUE;
    private long maxBytes;
  }

  /** The frames {@code source} keeps for {@code video} under {@code props}, read straight from the file, one at a time. */
  private static FrameStats keptFrames(FrameExtractionStrategy source, Path video, FixtureFile file, TypedProperties props)
      throws IOException {
    FrameStats stats = new FrameStats();
    try (ClosableIterator<ExtractedFrame> frames = source.frames(VideoClips.supplier(video),
        new FileMeta(file.sha, file.extension, Files.size(video)), props)) {
      while (frames.hasNext()) {
        long size;
        try (InputStream in = frames.next().getImage().open()) {
          size = in.readAllBytes().length;
        }
        stats.frames++;
        stats.minBytes = Math.min(stats.minBytes, size);
        stats.maxBytes = Math.max(stats.maxBytes, size);
      }
    }
    return stats;
  }

  /** Rows of {@code table} by the value of {@code column}, as text. */
  private static Map<String, Long> counts(Dataset<Row> table, Column column) {
    Map<String, Long> counts = new TreeMap<>();
    for (Row row : table.groupBy(column).count().collectAsList()) {
      counts.put(String.valueOf(row.get(0)), row.getLong(1));
    }
    return counts;
  }

  /** The payload files under the table's blob folder, the checksum files left out. */
  private long payloadFiles() throws IOException {
    Path blob = localPath(table()).resolve(org.apache.hudi.utilities.unstructured.storage.ManagedBlobPaths.FOLDER);
    if (!Files.exists(blob)) {
      return 0;
    }
    try (Stream<Path> files = Files.walk(blob)) {
      return files.filter(Files::isRegularFile).filter(file -> file.getFileName().toString().endsWith(".bin")).count();
    }
  }

  /** The {@code tasks=<partition>=<decodes>,...} part of a {@code VIDEO_DECODES} value, partition to decodes. */
  private static Map<String, String> decodeTasks(String decodes) {
    Map<String, String> tasks = new TreeMap<>();
    String list = decodes.substring(decodes.indexOf("tasks=") + "tasks=".length());
    for (String entry : list.split(",")) {
      if (!entry.isEmpty()) {
        String[] kv = entry.split("=");
        tasks.put(kv[0], kv[1]);
      }
    }
    return tasks;
  }

  private static String value(List<String> output, String prefix) {
    return output.stream().filter(line -> line.startsWith(prefix)).map(line -> line.substring(prefix.length()))
        .reduce((first, last) -> last).orElseThrow(() -> new AssertionError("no " + prefix.trim() + " line"));
  }

  /**
   * Runs {@link VideoRoundMemoryChild} at {@code heap} under G1 (with {@code childJvm} among its JVM options), with a heap dump
   * on an exhausted heap, its class path the
   * tests' (without the FFmpeg jars when {@code withoutFfmpeg}) in an argument file, its {@code PATH} without any folder holding
   * an {@code ffmpeg} program; its output is copied to {@code log} and returned.
   */
  private static List<String> runChild(String cell, String heap, List<String> childJvm, File propsFile, boolean withoutFfmpeg,
                                       Path reports, Path log) throws IOException, InterruptedException {
    List<String> classPath = withoutFfmpeg ? NoFfmpegClassPath.testClassPathWithoutFfmpeg() : NoFfmpegClassPath.testClassPath();
    Path argFile = propsFile.toPath().resolveSibling("child-jvm.args");
    // the class path is far longer than one command line argument may be: the JVM reads it from an argument file
    Files.write(argFile, Arrays.asList("-cp", "\"" + String.join(File.pathSeparator, classPath).replace("\\", "\\\\") + "\""),
        StandardCharsets.UTF_8);
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(MemoryCellJvm.heapOptions(heap, log));
    command.add("-XX:+ExitOnOutOfMemoryError");
    command.add("-XX:+HeapDumpOnOutOfMemoryError");
    command.add("-XX:HeapDumpPath=" + reports.resolve(cell + "-first-oom.hprof"));
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens") || argument.startsWith("--add-exports")) {
        command.add(argument);
      }
    }
    command.addAll(childJvm);
    command.add("@" + argFile);
    command.add(VideoRoundMemoryChild.class.getName());
    command.add(propsFile.getAbsolutePath());
    command.add("1");
    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
    builder.environment().put("PATH", pathWithoutFfmpeg(System.getenv("PATH")));
    Process child = builder.start();
    if (!child.waitFor(CELL_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      child.destroyForcibly();
      throw new AssertionError("the cell's child did not finish in " + CELL_TIMEOUT_MINUTES + " minutes (TIMEOUT), see " + log);
    }
    List<String> output = Files.readAllLines(log, StandardCharsets.UTF_8);
    output.stream().filter(line -> line.startsWith("ROUND ") || line.startsWith("CHILD_MAX_HEAP_BYTES ")
        || line.startsWith("FFMPEG_") || line.startsWith("HEAP_POOL_PEAKS_SUM_BYTES ") || line.startsWith("POST_")
        || line.startsWith("RESULT "))
        .forEach(line -> System.out.println("child: " + line));
    System.out.println("child exit " + child.exitValue());
    // the child prints RESULT ok before stopping Spark: a shutdown that throws or exhausts the heap fails the cell too
    assertEquals(0, child.exitValue(), "the cell's child exited nonzero, see " + log.toAbsolutePath());
    return output;
  }

  /** {@code path} (a {@code PATH} value) without the folders that hold an {@code ffmpeg} program. */
  private static String pathWithoutFfmpeg(String path) {
    if (path == null) {
      return "";
    }
    return Arrays.stream(path.split(File.pathSeparator))
        .filter(folder -> !folder.isEmpty() && VideoRoundMemoryChild.ffmpegPrograms(folder).isEmpty())
        .collect(Collectors.joining(File.pathSeparator));
  }
}
