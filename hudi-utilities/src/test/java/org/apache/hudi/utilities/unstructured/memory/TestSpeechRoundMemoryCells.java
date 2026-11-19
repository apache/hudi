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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobFileNames;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.SpeakerLabels;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;

import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Memory cells of the speech path of the single-writer embedding round: videos with an audio track, announced in ONE events commit,
 * through the embedding job over the unstructured data source on S3 ({@code prep_and_compute}, audio on: the default video preprocessor writes
 * each video's frames and then its speech rows, the real FFmpeg audio segment source cutting the track, the in-heap
 * {@link SegmentCountingTranscriber} saying one utterance per segment, the real transcript packer making one row per segment at
 * {@code chunk.max.tokens} = {@value #CHUNK_MAX_TOKENS} with no overlap; the dummy multimodal model embedding frames and speech;
 * every persist DISK_ONLY) in a child JVM ({@link SpeechRoundMemoryChild}) whose heap is the reference heap ({@value #REFERENCE_HEAP},
 * the embedding cells' one, unless {@code -Dspeech.memory.heap} says otherwise), local[4], {@value #TASKS} prep tasks. The videos
 * are generated at test time ({@link SpeechCellFixtures}: 64x48 at 2 fps, a 16 kHz AAC track of 29 s bursts and 1 s silences) and
 * stored out of line (the source rows' inline threshold is {@value #VIDEO_INLINE_MAX_BYTES} bytes), so prep streams each video
 * from its object. The frame-heap start check applies, with {@code prep.video.max.pixels} = 640x480 (the fixtures are 64x48).
 *
 * <p>A cell passes when the child ends its one round without an exhausted heap and with no error event, every segment was
 * transcribed exactly once, and the table holds, for every video, its frame row(s) and exactly one speech row per burst (row
 * {@code k} carries segment {@code k} alone, its text and its span), every row embedded. Each cell writes its report
 * ({@code MEMORY_CELL} lines) and its child's log under {@code target/memory-cells/}; an exhausted heap leaves
 * {@code <cell>-first-oom.hprof} there.
 *
 * <p>Gated: a cell runs only when named in {@code -Dspeech.memory.cells} (a comma list), so the class costs nothing in the
 * ordinary test runs. Cells:
 * <ul>
 *   <li>{@code speech-light}: two videos of 90 s (3 segments each): the speech path end to end in seconds;</li>
 *   <li>{@code speech-long}: four videos of 2 h (240 segments each) prepared at once by the {@value #TASKS} tasks (asserted from
 *   the tasks' segment times: each task transcribed its first segment before any task its last). A 2 h track is 230 MB of 16 kHz
 *   16-bit PCM, four of them 922 MB, so the round finishes at {@value #REFERENCE_HEAP} only if each task holds one segment (at most
 *   1 MiB) at a time, never a track: the heap itself bounds the in-task peak whatever the track length;</li>
 *   <li>{@code speech-many}: sixteen videos of 5 min (10 segments each) in the one events commit, four per task one after
 *   another: a task's audio implementations, built once on its first video, and its per-video state must not grow with the
 *   videos it has prepared.</li>
 *   <li>{@code speech-length-light} and {@code speech-length}: the same round over short tracks and then over long ones, in two
 *   children; the live heap must not grow with the tracks' length ({@link #heapDoesNotGrowWithTrackLength}).</li>
 * </ul>
 */
public class TestSpeechRoundMemoryCells extends EmbeddingRoundTestBase {

  static final String REFERENCE_HEAP = TestEmbeddingRoundMemoryCells.REFERENCE_HEAP;
  private static final long CELL_TIMEOUT_MINUTES = 15;
  /** Prep tasks: the child's local[4] slots, one video per task at a time. */
  private static final int TASKS = 4;
  /** Spark 3.5's Parquet caps inline values at 80 KiB: the videos are stored out of line, their frames inline. */
  private static final long VIDEO_INLINE_MAX_BYTES = 80L * 1024;
  /** The largest frame a video may declare: the fixtures' 64x48 frames are far below it, and the frame-heap check admits it. */
  private static final long MAX_PIXELS = 640L * 480;
  /** One utterance of {@link SegmentCountingTranscriber} (41 tokens with its label) fits a chunk; two do not. */
  private static final int CHUNK_MAX_TOKENS = 64;
  private static final int GENERATOR_THREADS = 4;
  /** The length cells' bound on the growth of the live heap from the short tracks to the long ones, see the length cells. */
  private static final long LENGTH_DELTA_MIB = 32;
  /** The length cells' period of sampling full collections: a light run of under a minute still sees dozens. */
  private static final long LENGTH_SAMPLE_PERIOD_MS = 1000;

  private static List<String> selectedCells() {
    String cells = System.getProperty("speech.memory.cells", "");
    return Arrays.stream(cells.split(",")).map(String::trim).filter(cell -> !cell.isEmpty()).collect(Collectors.toList());
  }

  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"speech-light", "speech-long", "speech-many"})
  public void cellPassesAtTheReferenceHeap(String cell) throws Exception {
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dspeech.memory.cells)");
    int videos;
    int seconds;
    switch (cell) {
      case "speech-light":
        videos = 2;
        seconds = 90;
        break;
      case "speech-long":
        videos = 4;
        seconds = 2 * 60 * 60;
        break;
      case "speech-many":
        videos = 16;
        seconds = 5 * 60;
        break;
      default:
        throw new IllegalArgumentException(cell);
    }
    String heap = System.getProperty("speech.memory.heap", REFERENCE_HEAP);
    Path reports = Paths.get("target", "memory-cells").toAbsolutePath();
    Files.createDirectories(reports);
    SpeechRun run = runAndGrade(cell, objects(), table(), videos, seconds, heap, Collections.emptyList(), reports);
    List<String> report = new ArrayList<>();
    report.add(run.summary());
    report.add("MEMORY_CELL " + cell + " speech_tasks=" + run.speechTasks);
    report.forEach(System.out::println);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /**
   * Length cells: the speech heap of a round must not grow with the length of its tracks. Each cell runs two children one after
   * the other, at the same heap and master, each over {@value #TASKS} videos prepared at once by the {@value #TASKS} tasks (one
   * video per task) into a table and an events table of its own: first {@value #TASKS} SHORT tracks, then {@value #TASKS} LONG
   * ones, otherwise the same job ({@code speech-long}'s settings). Each run is graded as the other speech cells are; then the long
   * run transcribed exactly as many more segments as its tracks are longer, both runs collected garbage, and the live heap of the
   * long run is at most {@link #lengthDeltaBytes} above the short run's.
   *
   * <p>The live heap is {@link PostGcHeap}'s {@code POST_FULL_GC_HEAP_PEAK_BYTES}: the children run under G1 and ask for a full
   * collection every {@value #LENGTH_SAMPLE_PERIOD_MS} ms, and the largest heap left after those collections is the largest live heap
   * the samples saw. The peak over every collection ({@code POST_GC_HEAP_PEAK_BYTES}) is reported too, not graded: after a young
   * collection the old generation still holds the garbage promoted since the last full one, which grows with the run's length
   * alone.
   * <ul>
   *   <li>{@code speech-length-light}: 90 s tracks (3 segments each, so the check that the tasks transcribe at once has a
   *   first and a last segment to compare) against 4.5 min tracks (9 segments each): the plumbing in about a minute and the
   *   noise of the difference when the inputs are small. It checks the plumbing only: its transcriptions take tens of
   *   milliseconds, shorter than the sampling period, so its delta does not sample the speech stage and is no length claim;</li>
   *   <li>{@code speech-length}: 10 min tracks (20 segments each) against 2 h tracks (240 segments each), the {@code speech-long}
   *   fixture. Each run holds 4 tracks at once; a 2 h track is 230 MB of 16 kHz 16-bit mono PCM (10 min: 19 MB) and 58 MB of its
   *   64 kbit/s AAC (10 min: 5 MB), so a task that buffered its whole decoded track, or even its whole compressed object, would
   *   hold over 50 MB more in the long run per task, over 200 MB for the four. {@value #LENGTH_DELTA_MIB} MiB is below what
   *   ONE task buffering its compressed track would add, and well above the round's own noise (Spark's and Hudi's per-round
   *   state is the same in both runs: one commit of 4 files, the same number of tasks; what differs is the number of rows, 80
   *   against 960 speech rows, a few hundred KB).</li>
   * </ul>
   */
  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"speech-length-light", "speech-length"})
  public void heapDoesNotGrowWithTrackLength(String cell) throws Exception {
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dspeech.memory.cells)");
    boolean light = "speech-length-light".equals(cell);
    int shortSeconds = light ? 90 : 10 * 60;
    int longSeconds = light ? 270 : 2 * 60 * 60;
    String heap = System.getProperty("speech.memory.heap", REFERENCE_HEAP);
    Path reports = Paths.get("target", "memory-cells").toAbsolutePath();
    Files.createDirectories(reports);
    List<String> childJvm = Collections.singletonList("-D" + PostGcHeap.SAMPLE_PROPERTY + "=" + LENGTH_SAMPLE_PERIOD_MS);
    SpeechRun shortRun = runAndGrade(cell + "-short", lengthObjects("short"), table("embedding_table_short"), TASKS,
        shortSeconds, heap, childJvm, reports);
    SpeechRun longRun = runAndGrade(cell + "-long", lengthObjects("long"), table("embedding_table_long"), TASKS,
        longSeconds, heap, childJvm, reports);

    assertEquals(shortRun.transcribed * (longSeconds / shortSeconds), longRun.transcribed,
        "the segments scale with the tracks' length: " + shortRun.transcribed + " against " + longRun.transcribed);
    for (SpeechRun run : Arrays.asList(shortRun, longRun)) {
      assertTrue(run.postGcCount > 0, run.label + " collected garbage: " + run.postGcCount);
      assertTrue(run.postFullGcCount > 0, run.label + " ran its sampling full collections: " + run.postFullGcCount);
    }
    long delta = longRun.postFullGcPeak - shortRun.postFullGcPeak;
    List<String> report = new ArrayList<>();
    report.add(shortRun.summary());
    report.add(longRun.summary());
    report.add("MEMORY_CELL " + cell + " short_seconds=" + shortSeconds + " long_seconds=" + longSeconds + " sample_period_ms="
        + LENGTH_SAMPLE_PERIOD_MS + " post_full_gc_heap_peak_delta_bytes=" + delta + " post_gc_heap_peak_delta_bytes="
        + (longRun.postGcPeak - shortRun.postGcPeak) + " bound_bytes=" + lengthDeltaBytes());
    report.forEach(System.out::println);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
    assertTrue(delta <= lengthDeltaBytes(), "the live heap of the long tracks' round is at most " + LENGTH_DELTA_MIB
        + " MiB above the short tracks' one: " + longRun.postFullGcPeak + " - " + shortRun.postFullGcPeak + " = " + delta);
  }

  /** The largest growth of the live heap the length cells accept from the short tracks to the long ones. */
  private static long lengthDeltaBytes() {
    return LENGTH_DELTA_MIB * 1024L * 1024L;
  }

  /** The objects and events table of one run of a length cell, under {@code basePath()/source-<run>}. */
  private BlobJobHarness lengthObjects(String run) {
    return BlobJobHarness.under(localPath(basePath()).resolve("source-" + run)).install(spark().sparkContext().hadoopConfiguration());
  }

  /** One graded child run of the speech cells. */
  private static final class SpeechRun {
    private String label;
    private String heap;
    private int rounds;
    private int videos;
    private int seconds;
    private long totalBytes;
    private long transcribed;
    private long frames;
    private int rows;
    private String speechTasks;
    private long heapPoolPeaks;
    private long postGcPeak;
    private long postGcCount;
    private long postFullGcPeak;
    private long postFullGcCount;
    private long generateSeconds;
    private long childSeconds;
    private String gcFields;

    private String summary() {
      return "MEMORY_CELL " + label + " heap=" + heap + " rounds=" + rounds + " videos=" + videos + " seconds_per_video="
          + seconds + " video_bytes=" + totalBytes + " segments=" + transcribed + " frame_rows=" + frames + " rows=" + rows
          + " heap_pool_peaks_sum_bytes=" + heapPoolPeaks + " post_gc_heap_peak_bytes=" + postGcPeak + " post_gc_count="
          + postGcCount + " post_full_gc_heap_peak_bytes=" + postFullGcPeak + " post_full_gc_count=" + postFullGcCount
          + " generate_seconds=" + generateSeconds + " seconds=" + childSeconds + gcFields;
    }
  }

  /**
   * Generates {@code videos} tracks of {@code seconds} into {@code objects}' bucket, announces them in one events commit, runs
   * the speech job into {@code table} in a child at {@code heap} (with {@code childJvm} before its class path), and grades the
   * child's report and the table as every speech cell does. {@code label} names the run's work folder, log and heap dump.
   */
  private SpeechRun runAndGrade(String label, BlobJobHarness objects, String table, int videos, int seconds, String heap,
                                List<String> childJvm, Path reports) throws Exception {
    int bursts = SpeechCellFixtures.bursts(seconds);
    Path work = localPath(basePath()).resolve("cell-" + label);
    Files.createDirectories(work);

    // the videos are written straight into the bucket (never held on the heap) and announced in one events commit
    long generateStart = System.nanoTime();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < videos; i++) {
      keys.add("cells/" + label + "/video-" + i + ".mp4");
    }
    ExecutorService generators = Executors.newFixedThreadPool(GENERATOR_THREADS);
    try {
      List<Future<Path>> written = new ArrayList<>();
      for (int i = 0; i < videos; i++) {
        Path file = objects.file(keys.get(i));
        long seed = i;
        written.add(generators.submit(() -> SpeechCellFixtures.track(file, seconds, seed)));
      }
      for (Future<Path> future : written) {
        future.get();
      }
    } finally {
      generators.shutdownNow();
    }
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    long totalBytes = 0;
    for (String key : keys) {
      long size = Files.size(objects.file(key));
      totalBytes += size;
      events.add(S3EventsTableFixture.created(BlobJobHarness.BUCKET, key, size, BlobJobHarness.EVENT_TIME));
    }
    objects.announce(spark(), events);
    long generateSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - generateStart);

    TypedProperties props = cellProps(objects, table);
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + label);
    }

    long start = System.nanoTime();
    Path childLog = reports.resolve(label + "-child.log");
    List<String> output = runChild(heap, childJvm, propsFile, 2, childLog, reports.resolve(label + "-first-oom.hprof"));
    long childSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    // graded on the child's own report and the committed table
    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog);
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(1, rounds.size(), rounds.toString());
    assertTrue(rounds.get(0).contains(" errors=0 "), "no video failed: " + rounds.get(0));

    Map<Integer, long[]> tasks = speechTasks(value(output, "SPEECH_TASKS "));
    long transcribed = tasks.values().stream().mapToLong(task -> task[0]).sum();
    assertEquals((long) videos * bursts, transcribed, "every segment is transcribed exactly once: " + value(output, "SPEECH_TASKS "));
    assertEquals(Math.min(videos, TASKS), tasks.size(), "the videos are spread over the prep tasks: " + value(output, "SPEECH_TASKS "));
    if (videos % TASKS == 0) {
      for (long[] task : tasks.values()) {
        assertEquals((long) videos / TASKS * bursts, task[0], "each task prepares as many videos: " + value(output, "SPEECH_TASKS "));
      }
      long lastFirst = tasks.values().stream().mapToLong(task -> task[1]).max().getAsLong();
      long firstLast = tasks.values().stream().mapToLong(task -> task[2]).min().getAsLong();
      assertTrue(lastFirst < firstLast, "the tasks transcribe at once: " + value(output, "SPEECH_TASKS "));
    }

    Map<String, Row> rows = rows(table);
    Map<String, List<Row>> speechBySha = new TreeMap<>();
    Map<String, Long> framesBySha = new TreeMap<>();
    for (Row row : rows.values()) {
      assertTrue(vector(row) != null, "every row has its embedding: " + row);
      String sha = row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA);
      if (EmbeddingTableSchema.MODALITY_TEXT.equals(row.getAs(EmbeddingTableSchema.MODALITY))) {
        speechBySha.computeIfAbsent(sha, s -> new ArrayList<>()).add(row);
      } else {
        assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, row.getAs(EmbeddingTableSchema.MODALITY), row.toString());
        framesBySha.merge(sha, 1L, Long::sum);
      }
    }
    for (String key : keys) {
      String sha = BlobFileNames.sourcePathSha(BlobJobHarness.sourcePath(key));
      assertTrue(framesBySha.getOrDefault(sha, 0L) >= 1, key + " has its frame row");
      List<Row> speech = speechBySha.getOrDefault(sha, new ArrayList<>());
      assertEquals(bursts, speech.size(), key + " has one speech row per burst");
      speech.sort(Comparator.comparingLong(row -> ((Number) row.getAs(EmbeddingTableSchema.CHUNK_ID)).longValue()));
      long previousEnd = -1;
      for (int k = 0; k < speech.size(); k++) {
        Row row = speech.get(k);
        scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
        assertEquals(k, ((Number) row.getAs(EmbeddingTableSchema.CHUNK_ID)).longValue(), key + " speech rows are dense");
        assertEquals(String.valueOf(k), aux.apply(AudioRows.AUX_FIRST_SEGMENT), key + " row " + k + " starts at segment " + k);
        assertEquals(String.valueOf(k), aux.apply(AudioRows.AUX_LAST_SEGMENT), key + " row " + k + " is segment " + k + " alone");
        String text = row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
        assertTrue(text.startsWith(SpeakerLabels.line(SpeakerLabels.FILE_SPEAKER, "segment " + k + " ")), key + " row " + k + ": " + text);
        long startMs = Long.parseLong(aux.apply(AudioRows.AUX_START_MS));
        long endMs = Long.parseLong(aux.apply(AudioRows.AUX_END_MS));
        assertTrue(previousEnd <= startMs && startMs < endMs, key + " row " + k + " spans [" + startMs + ", " + endMs + ") after "
            + previousEnd);
        previousEnd = endMs;
      }
    }
    long frames = framesBySha.values().stream().mapToLong(Long::longValue).sum();
    assertEquals((long) videos * bursts + frames, rows.size(), "the table holds the frames and the speech rows only");

    SpeechRun run = new SpeechRun();
    run.label = label;
    run.heap = heap;
    run.rounds = rounds.size();
    run.videos = videos;
    run.seconds = seconds;
    run.totalBytes = totalBytes;
    run.transcribed = transcribed;
    run.frames = frames;
    run.rows = rows.size();
    run.speechTasks = value(output, "SPEECH_TASKS ");
    run.heapPoolPeaks = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES "));
    run.postGcPeak = Long.parseLong(value(output, "POST_GC_HEAP_PEAK_BYTES "));
    run.postGcCount = Long.parseLong(value(output, "POST_GC_COUNT "));
    run.postFullGcPeak = Long.parseLong(value(output, "POST_FULL_GC_HEAP_PEAK_BYTES "));
    run.postFullGcCount = Long.parseLong(value(output, "POST_FULL_GC_COUNT "));
    run.generateSeconds = generateSeconds;
    run.childSeconds = childSeconds;
    run.gcFields = MemoryCellJvm.gcFields(childLog, output);
    return run;
  }

  /**
   * The cell's job into {@code table} over {@code objects}: {@code prep_and_compute} with the dummy multimodal model, audio on
   * with {@link SegmentCountingTranscriber}, one speech row per segment, mp4 only, {@value #TASKS} prep tasks, DISK_ONLY persists.
   */
  private TypedProperties cellProps(BlobJobHarness objects, String table) {
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table, EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, VIDEO_INLINE_MAX_BYTES,
        model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER,
        AudioConfigKeys.ENABLED.key(), "true",
        AudioConfigKeys.TRANSCRIBER_CLASS.key(), SegmentCountingTranscriber.class.getName(),
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), String.valueOf(CHUNK_MAX_TOKENS),
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0",
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4",
        EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), String.valueOf(MAX_PIXELS),
        EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), String.valueOf(TASKS),
        HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), objects.eventsPath());
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    // the child installs the bucket file system over the same objects
    props.setProperty(BucketFolderFileSystem.ROOT, objects.objectsRoot().toAbsolutePath().toString());
    props.setProperty(HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    return props;
  }

  /** {@code SPEECH_TASKS} as partition to {segments, first ms, last ms}. */
  private static Map<Integer, long[]> speechTasks(String line) {
    Map<Integer, long[]> tasks = new TreeMap<>();
    for (String entry : line.split(",")) {
      String[] partitionAndStats = entry.split("=");
      String[] segmentsAndTimes = partitionAndStats[1].split("[@-]");
      tasks.put(Integer.parseInt(partitionAndStats[0]), new long[] {Long.parseLong(segmentsAndTimes[0]),
          Long.parseLong(segmentsAndTimes[1]), Long.parseLong(segmentsAndTimes[2])});
    }
    return tasks;
  }

  private static String value(List<String> output, String prefix) {
    return output.stream().filter(line -> line.startsWith(prefix)).map(line -> line.substring(prefix.length()))
        .reduce((first, last) -> last).orElseThrow(() -> new AssertionError("no " + prefix.trim() + " line"));
  }

  /**
   * Runs {@link SpeechRoundMemoryChild} at {@code heap}, with {@code childJvm} among its JVM options; its output is copied to
   * {@code log} and returned. An exhausted heap dumps to {@code heapDump}, a fixed name, so a later attempt does not overwrite the
   * first dump.
   */
  private static List<String> runChild(String heap, List<String> childJvm, File propsFile, int maxRounds, Path log, Path heapDump)
      throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(MemoryCellJvm.heapOptions(heap, log));
    command.add("-XX:+ExitOnOutOfMemoryError");
    command.add("-XX:+HeapDumpOnOutOfMemoryError");
    command.add("-XX:HeapDumpPath=" + heapDump);
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens") || argument.startsWith("--add-exports")) {
        command.add(argument);
      }
    }
    command.addAll(childJvm);
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(SpeechRoundMemoryChild.class.getName());
    command.add(propsFile.getAbsolutePath());
    command.add(String.valueOf(maxRounds));
    Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!child.waitFor(CELL_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      child.destroyForcibly();
      throw new AssertionError("the cell's child did not finish in " + CELL_TIMEOUT_MINUTES + " minutes (TIMEOUT), see " + log);
    }
    List<String> output = Files.readAllLines(log, StandardCharsets.UTF_8);
    output.stream().filter(line -> line.startsWith("ROUND ") || line.startsWith("SPEECH_TASKS ")
        || line.startsWith("HEAP_POOL_PEAKS_SUM_BYTES ") || line.startsWith("POST_") || line.startsWith("RESULT "))
        .forEach(line -> System.out.println("child: " + line));
    System.out.println("child exit " + child.exitValue());
    // the child prints RESULT ok before stopping Spark: a shutdown that throws or exhausts the heap fails the cell too
    assertEquals(0, child.exitValue(), "the cell's child exited nonzero, see " + log.toAbsolutePath());
    return output;
  }
}
