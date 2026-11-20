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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.sources.blob.BlobInlineCeiling;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.JsonLinesErrorTableWriter;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One memory cell's MERGE_ON_READ blob rounds, in a child JVM whose heap is the cell's heap: reads the job's properties
 * from a file, writes {@code fileCount} objects of {@code fileSize} bytes (each at most the inline threshold, so every version is
 * stored INLINE) with one fixed modification time, announces them in one events commit and runs one round of the blob job (stock
 * HoodieStreamer over the unstructured data source on S3 and the blob quarantine transformer, MERGE_ON_READ). Each further round re-announces the
 * same events in a new events commit: the same versions, so every record key is updated and its inline bytes land in log blocks.
 * After each round it prints {@code ROUND <n> commit=<instant> files=<write stats> log_files=<write stats of log files>
 * updates=<updated records> writes=<written records>}. Then, at the same heap, it reads the snapshot (base files merged with the
 * log blocks) and computes in Spark, per row, the blob's storage type and the SHA-256 of {@code read_blob(blob)}, compared with
 * the SHA-256 of the bytes written: {@code SNAPSHOT rows=<n> inline=<INLINE rows> sha_mismatches=<n>}. Last the sum of the heap
 * pools' peaks ({@code HEAP_POOL_PEAKS_SUM_BYTES <n>}, an upper bound of the peak heap), the heap held after its garbage
 * collections ({@link PostGcHeap}: {@code POST_GC_HEAP_PEAK_BYTES <n>} and the lines after it) and {@code RESULT ok}. A
 * failure prints its stack and exits 1; an exhausted heap ends the JVM ({@code -XX:+ExitOnOutOfMemoryError}).
 *
 * <p>Arguments: the properties file, the table base path, the objects root, the events table path, the file count, the file
 * size, the rounds, the table type, and optionally the mode and its argument:
 * <ul>
 *   <li>{@value #MODE_ROUNDS} (the default): the rounds above.</li>
 *   <li>{@value #MODE_REFUSED}: prints {@code INLINE_CEILING ceiling=<this JVM's ceiling> parquet_cap=<n|none> configured=<n>
 *   threshold=<the threshold the round uses> ...}, writes and announces the objects, then runs one round that the inline ceiling
 *   should refuse at the source's fetch. Prints {@code REFUSED <the HoodieValidationException's message>} (or {@code none} when
 *   the round was not refused; a failure of another kind fails the child), {@code TASKS started=<Spark tasks started from the
 *   round's start>} and {@code TIMELINE instants=<n> completed=<n>} of the blob table; no snapshot.</li>
 *   <li>{@value #MODE_CEILING4}: sets the inline threshold to the largest one this JVM's ceiling admits (bounded by the Parquet
 *   inline cap on a Parquet without the per-column statistics switch), the object size to {@value #NEAR_CEILING_MARGIN_BYTES}
 *   bytes below it, and, when the argument is a byte budget above 0, the object count to fill it; the count is rounded up to a
 *   multiple of {@value #WRITE_TASKS} and the inserts split into {@value #WRITE_TASKS} buckets of that many objects
 *   (hoodie.copyonwrite.insert.auto.split off), so the write stage runs four tasks that each write near-threshold inline values.
 *   Prints {@code INLINE_CEILING ...} as above, the rounds and snapshot as above, and, per write stage (a job's last stage named
 *   {@code collect at StreamSync} in a round), {@code WRITE_STAGE phase=<write n> stage=<id> tasks=<n> max_in_flight=<n>}, then
 *   {@code WRITE_TASKS stages=<n> tasks=<n> max_in_flight=<n>} over them.</li>
 *   <li>{@value #MODE_EVENTS}: the argument is the count of objects announced and then deleted before the first round, one in
 *   every count/argument objects. After the round prints {@code EVENTS announced=<n> deleted=<n> present=<n> inserts=<n>
 *   writes=<n> quarantined=<error table events> gone_since_announced=<of them> put_seconds=<n> announce_seconds=<n> round_seconds=<n>} (the job needs an error table,
 *   hoodie.errortable.base.path, read with JsonLinesErrorTableWriter); its snapshot is counted in Spark, so the driver holds no
 *   row and no digest per object: {@code SNAPSHOT rows=<n> inline=<n> null_blobs=<n> full_length=<rows whose read_blob is the
 *   object size> seconds=<n>}.</li>
 * </ul>
 *
 * <p>System properties: {@code memory.cell.master} the Spark master (default {@code local[4]}; the partition counts stay 4, so
 * {@code local[1]} runs the same tasks one at a time); {@code memory.cell.dump} {@code <phase>:<millis>:<path>} takes one live
 * heap dump to {@code path} that many milliseconds after the first task of the phase's stage starts, where the phase is
 * {@code write<n>} (round n's write stage, {@code collect at StreamSync}) or {@code read} (the snapshot query's collect): a
 * picture of what the tasks hold when the stage does not exhaust the heap. The snapshot read takes every {@code hoodie.memory.*} property of the job.
 */
public final class BlobRoundMemoryChild {

  /** The default mode: the rounds of the {@code mor-inline} cells. */
  static final String MODE_ROUNDS = "rounds";
  /** One round whose inline threshold is above the inline ceiling: refused on the driver. */
  static final String MODE_REFUSED = "refused";
  /** One round at the largest threshold the inline ceiling admits, written by four tasks at once. */
  static final String MODE_CEILING4 = "ceiling4";
  /** One round over many small objects, some announced and then deleted before the round. */
  static final String MODE_EVENTS = "events";
  /** The tasks of the write stage of the {@value #MODE_CEILING4} mode, the slots of {@code local[4]}. */
  static final int WRITE_TASKS = 4;
  /** How far below the threshold the objects of the {@value #MODE_CEILING4} mode are. */
  static final int NEAR_CEILING_MARGIN_BYTES = 64;
  private static final long LISTENER_DRAIN_MILLIS = 30_000L;
  /** The modification time of every object, so each object has one version across the rounds. */
  static final Instant MODIFICATION_TIME = Instant.parse("2026-01-02T09:00:00Z");

  private BlobRoundMemoryChild() {
  }

  /** The key of the cell's {@code i}-th object. */
  static String objectKey(int i) {
    return "cells/mor/f-" + i + ".pdf";
  }

  /** The phase the child is in: {@code write<n>} during round n, {@code read} during the snapshot read. */
  private static volatile String phase = "";

  public static void main(String[] args) throws Exception {
    TypedProperties props = new TypedProperties();
    try (InputStream in = new FileInputStream(args[0])) {
      props.load(in);
    }
    String table = args[1];
    String objectsRoot = args[2];
    String eventsPath = args[3];
    int fileCount = Integer.parseInt(args[4]);
    int fileSize = Integer.parseInt(args[5]);
    int rounds = Integer.parseInt(args[6]);
    HoodieTableType tableType = HoodieTableType.valueOf(args[7]);
    String mode = args.length > 8 ? args[8] : MODE_ROUNDS;
    long modeArgument = args.length > 9 ? Long.parseLong(args[9]) : 0L;
    PostGcHeap postGcHeap = PostGcHeap.install();
    SparkConf conf = new SparkConf()
        .setMaster(System.getProperty("memory.cell.master", "local[4]"))
        .setAppName("blob-memory-cell")
        .set("spark.ui.enabled", "false")
        .set("spark.default.parallelism", "4")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        // read_blob is a function of the Hudi session extension
        .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    String dump = System.getProperty("memory.cell.dump", "");
    if (!dump.isEmpty()) {
      spark.sparkContext().addSparkListener(new PhaseHeapDump(dump));
    }
    TaskCounter tasks = null;
    if (!MODE_ROUNDS.equals(mode)) {
      tasks = new TaskCounter();
      spark.sparkContext().addSparkListener(tasks);
    }
    BlobJobHarness harness = new BlobJobHarness(Paths.get(objectsRoot), eventsPath).install(spark.sparkContext().hadoopConfiguration());
    int exit = 0;
    try {
      if (MODE_REFUSED.equals(mode) || MODE_CEILING4.equals(mode)) {
        // the ceiling of this JVM's own heap and slots, as the source's driver check computes it
        long ceiling = BlobInlineCeiling.onDriver(spark.sparkContext().getConf(), BlobInlineCeiling.fraction(props)).ceilingBytes();
        boolean statisticsSwitch = HoodieBaseParquetWriter.supportsColumnStatisticsSwitch();
        if (MODE_CEILING4.equals(mode)) {
          // the largest threshold the ceiling admits, bounded by the Parquet inline cap on a Parquet without the switch
          long threshold = statisticsSwitch ? ceiling : Math.min(ceiling, ParquetInlineCap.MAX_BYTES);
          props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(threshold));
          fileSize = (int) (threshold - NEAR_CEILING_MARGIN_BYTES);
          if (modeArgument > 0) {
            // a byte budget: as many objects as fill it, a multiple of the four write tasks
            fileCount = (int) ((modeArgument + fileSize - 1) / fileSize);
          }
          fileCount = (fileCount + WRITE_TASKS - 1) / WRITE_TASKS * WRITE_TASKS;
          // four insert buckets of the same count, so the write stage runs four tasks of near-threshold values
          props.setProperty(HoodieCompactionConfig.COPY_ON_WRITE_AUTO_SPLIT_INSERTS.key(), "false");
          props.setProperty(HoodieCompactionConfig.COPY_ON_WRITE_INSERT_SPLIT_SIZE.key(), String.valueOf(fileCount / WRITE_TASKS));
        }
        System.out.println("INLINE_CEILING ceiling=" + ceiling + " parquet_cap=" + (statisticsSwitch ? "none" : ParquetInlineCap.MAX_BYTES)
            + " configured=" + props.getString(BlobTableConfig.INLINE_MAX_BYTES.key()) + " threshold="
            + BlobTableConfig.inlineMaxBytes(props) + " files=" + fileCount + " file_size=" + fileSize);
      }
      int missingEvery = MODE_EVENTS.equals(mode) && modeArgument > 0 ? (int) (fileCount / modeArgument) : 0;
      // only the expected digests stay on the heap, never the objects' bytes; the events cells keep none (see the snapshot below)
      Map<String, String> expectedSha = new HashMap<>();
      List<S3EventsTableFixture.Event> events = new ArrayList<>();
      List<String> missing = new ArrayList<>();
      long putStart = System.nanoTime();
      for (int i = 0; i < fileCount; i++) {
        String key = objectKey(i);
        byte[] bytes = BlobJobHarness.fileBytes(key, fileSize);
        events.add(harness.put(key, bytes));
        harness.touch(key, MODIFICATION_TIME);
        if (missingEvery > 0 && i % missingEvery == 0 && missing.size() < modeArgument) {
          missing.add(key);
        }
        if (!MODE_EVENTS.equals(mode)) {
          expectedSha.put(BlobJobHarness.versionKey(BlobJobHarness.sourcePath(key), MODIFICATION_TIME.toEpochMilli(), fileSize),
              BlobJobHarness.sha256Hex(bytes));
        }
      }
      if (MODE_REFUSED.equals(mode)) {
        harness.announce(spark, events);
        refuse(spark, jsc, props, table, tableType, tasks);
        printHeapPeak(postGcHeap);
        System.out.println("RESULT ok");
        spark.stop();
        System.exit(0);
      }
      long putSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - putStart);
      long announceSeconds = 0;
      String previous = null;
      for (int n = 1; n <= rounds; n++) {
        // the first round inserts every version; each later one re-announces the same versions, an update of every record key
        long announceStart = System.nanoTime();
        harness.announce(spark, events);
        announceSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - announceStart);
        if (n == 1) {
          // announced, then gone before the round: the round finds them missing when it reads them
          for (String key : missing) {
            Files.delete(harness.file(key));
          }
        }
        phase = "write" + n;
        long roundStart = System.nanoTime();
        HoodieStreamer streamer = new HoodieStreamer(BlobJobHarness.streamerConfig(table, tableType), jsc, Option.of(props));
        try {
          streamer.sync();
        } finally {
          streamer.shutdownGracefully();
        }
        long roundSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - roundStart);
        HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(table)
            .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
        HoodieInstant commit = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
        if (commit.requestedTime().equals(previous)) {
          throw new IllegalStateException("round " + n + " committed nothing");
        }
        previous = commit.requestedTime();
        List<HoodieWriteStat> stats = meta.getActiveTimeline().readCommitMetadata(commit).getWriteStats();
        long logFiles = stats.stream().filter(stat -> FSUtils.isLogFile(stat.getPath())).count();
        long updates = stats.stream().mapToLong(HoodieWriteStat::getNumUpdateWrites).sum();
        long writes = stats.stream().mapToLong(HoodieWriteStat::getNumWrites).sum();
        System.out.println("ROUND " + n + " commit=" + commit.requestedTime() + " files=" + stats.size() + " log_files=" + logFiles
            + " updates=" + updates + " writes=" + writes);
        if (MODE_EVENTS.equals(mode)) {
          long inserts = stats.stream().mapToLong(HoodieWriteStat::getNumInserts).sum();
          List<Map<String, Object>> quarantined = JsonLinesErrorTableWriter.read(
              props.getString(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key()));
          long gone = quarantined.stream().map(event -> String.valueOf(((Map<?, ?>) event.get("payload")).get("error")))
              .filter(error -> error.contains("gone since announced")).count();
          System.out.println("EVENTS announced=" + events.size() + " deleted=" + missing.size() + " present="
              + (events.size() - missing.size()) + " inserts=" + inserts + " writes=" + writes + " quarantined=" + quarantined.size()
              + " gone_since_announced=" + gone + " put_seconds=" + putSeconds + " announce_seconds=" + announceSeconds
              + " round_seconds=" + roundSeconds);
        }
      }
      if (tasks != null) {
        spark.sparkContext().listenerBus().waitUntilEmpty(LISTENER_DRAIN_MILLIS);
        tasks.printWriteStages();
      }

      // the snapshot query merges the base files with the log blocks; the bytes are read and digested inside the tasks
      phase = "read";
      Map<String, String> readOptions = new HashMap<>();
      props.stringPropertyNames().stream().filter(key -> key.startsWith("hoodie.memory."))
          .forEach(key -> readOptions.put(key, props.getString(key)));
      System.out.println("READ_OPTIONS " + readOptions);
      spark.read().format("hudi").options(readOptions).load(table).createOrReplaceTempView("blob_memory_cell_snapshot");
      if (MODE_EVENTS.equals(mode)) {
        // counted in Spark: the driver holds no row per object. read_blob may only appear in a projection, so the counts are
        // summed over the projected rows' RDD rather than by a SQL aggregate
        long snapshotStart = System.nanoTime();
        int expectedLength = fileSize;
        long[] counts = spark.sql("select blob.type, blob is null, length(read_blob(blob)) from blob_memory_cell_snapshot").javaRDD()
            .map(row -> new long[] {1L, HoodieSchema.Blob.INLINE.equals(row.getString(0)) ? 1L : 0L, row.getBoolean(1) ? 1L : 0L,
                !row.isNullAt(2) && row.getInt(2) == expectedLength ? 1L : 0L})
            .fold(new long[4], (a, b) -> new long[] {a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]});
        System.out.println("SNAPSHOT rows=" + counts[0] + " inline=" + counts[1] + " null_blobs=" + counts[2]
            + " full_length=" + counts[3] + " seconds=" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - snapshotStart));
      } else {
        List<Row> digests = spark.sql("select _hoodie_record_key, blob.type, sha2(read_blob(blob), 256) from blob_memory_cell_snapshot")
            .collectAsList();
        long inline = 0;
        long mismatches = 0;
        for (Row row : digests) {
          if (HoodieSchema.Blob.INLINE.equals(row.getString(1))) {
            inline++;
          }
          if (!row.getString(2).equals(expectedSha.get(row.getString(0)))) {
            mismatches++;
          }
        }
        System.out.println("SNAPSHOT rows=" + digests.size() + " inline=" + inline + " sha_mismatches=" + mismatches);
      }
      printHeapPeak(postGcHeap);
      System.out.println("RESULT ok");
    } catch (Throwable t) {
      t.printStackTrace(System.out);
      System.out.println("RESULT failed");
      exit = 1;
    } finally {
      spark.stop();
    }
    System.exit(exit);
  }

  /**
   * {@code HEAP_POOL_PEAKS_SUM_BYTES <n>}: the sum of the heap pools' peaks, an upper bound of the peak heap; then
   * {@code postGcHeap}'s lines, the heap held after the garbage collections.
   */
  private static void printHeapPeak(PostGcHeap postGcHeap) throws InterruptedException {
    long peak = 0;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        peak += pool.getPeakUsage().getUsed();
      }
    }
    System.out.println("HEAP_POOL_PEAKS_SUM_BYTES " + peak);
    postGcHeap.print(System.out);
  }

  /**
   * One round expected to be refused by the inline ceiling: counts the Spark tasks started from the round's start, prints
   * {@code REFUSED <message>} (the ceiling's refusal found in the failure's cause chain, or {@code none}), {@code TASKS started=<n>}
   * and {@code TIMELINE instants=<n> completed=<n>} of the blob table (0 when the round left no table).
   */
  private static void refuse(SparkSession spark, JavaSparkContext jsc, TypedProperties props, String table, HoodieTableType tableType,
                             TaskCounter tasks) throws Exception {
    spark.sparkContext().listenerBus().waitUntilEmpty(LISTENER_DRAIN_MILLIS);
    tasks.armed = true;
    phase = "write1";
    String refusal = "none";
    try {
      HoodieStreamer streamer = new HoodieStreamer(BlobJobHarness.streamerConfig(table, tableType), jsc, Option.of(props));
      try {
        streamer.sync();
      } finally {
        streamer.shutdownGracefully();
      }
    } catch (Throwable t) {
      Throwable validation = t;
      while (validation != null && !(validation instanceof HoodieValidationException)) {
        validation = validation.getCause() == validation ? null : validation.getCause();
      }
      if (validation == null) {
        throw t;
      }
      refusal = validation.getMessage();
      System.out.println("REFUSED_BY " + t.getClass().getName());
    }
    spark.sparkContext().listenerBus().waitUntilEmpty(LISTENER_DRAIN_MILLIS);
    tasks.armed = false;
    System.out.println("REFUSED " + refusal);
    System.out.println("TASKS started=" + tasks.startedWhileArmed.get());
    long instants = 0;
    long completed = 0;
    if (Files.isDirectory(Paths.get(new URI(table)).resolve(".hoodie"))) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(table)
          .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
      instants = meta.getActiveTimeline().countInstants();
      completed = meta.getActiveTimeline().filterCompletedInstants().countInstants();
    }
    System.out.println("TIMELINE instants=" + instants + " completed=" + completed);
  }

  /**
   * Counts Spark tasks: every task started while {@link #armed}, and per write stage (a job's last stage named
   * "collect at StreamSync" during a {@code write<n>} phase, the stage that runs the write handles) its task count and
   * the most of its tasks running at once.
   */
  private static final class TaskCounter extends SparkListener {
    private volatile boolean armed;
    private final AtomicInteger startedWhileArmed = new AtomicInteger();
    private final Set<Integer> finalStages = ConcurrentHashMap.newKeySet();
    private final Map<Integer, String> stageNames = new ConcurrentHashMap<>();
    private final Map<Integer, String> stagePhases = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> stageTasks = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> maxInFlight = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      int last = -1;
      scala.collection.Iterator<Object> ids = jobStart.stageIds().iterator();
      while (ids.hasNext()) {
        last = Math.max(last, (Integer) ids.next());
      }
      finalStages.add(last);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted submitted) {
      stageNames.put(submitted.stageInfo().stageId(), submitted.stageInfo().name());
      stagePhases.put(submitted.stageInfo().stageId(), phase);
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart start) {
      if (armed) {
        startedWhileArmed.incrementAndGet();
      }
      stageTasks.computeIfAbsent(start.stageId(), id -> new AtomicInteger()).incrementAndGet();
      int running = inFlight.computeIfAbsent(start.stageId(), id -> new AtomicInteger()).incrementAndGet();
      maxInFlight.computeIfAbsent(start.stageId(), id -> new AtomicInteger()).accumulateAndGet(running, Math::max);
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd end) {
      inFlight.computeIfAbsent(end.stageId(), id -> new AtomicInteger()).decrementAndGet();
    }

    /**
     * Prints {@code WRITE_STAGE phase=<phase> stage=<id> tasks=<n> max_in_flight=<n>} per write stage, then
     * {@code WRITE_TASKS stages=<n> tasks=<n> max_in_flight=<n>} over them all.
     */
    void printWriteStages() {
      int stages = 0;
      int taskCount = 0;
      int max = 0;
      for (Integer stage : new TreeSet<>(stageNames.keySet())) {
        if (finalStages.contains(stage) && stageNames.get(stage).contains(PhaseHeapDump.WRITE_STAGE)
            && stagePhases.getOrDefault(stage, "").startsWith("write")) {
          int stageTaskCount = stageTasks.getOrDefault(stage, new AtomicInteger()).get();
          int stageMax = maxInFlight.getOrDefault(stage, new AtomicInteger()).get();
          System.out.println("WRITE_STAGE phase=" + stagePhases.get(stage) + " stage=" + stage + " tasks=" + stageTaskCount
              + " max_in_flight=" + stageMax);
          stages++;
          taskCount += stageTaskCount;
          max = Math.max(max, stageMax);
        }
      }
      System.out.println("WRITE_TASKS stages=" + stages + " tasks=" + taskCount + " max_in_flight=" + max);
    }
  }

  /**
   * Takes one live heap dump {@code millis} milliseconds after the first task of the phase's stage starts while the child is in that
   * phase: the round's write stage ({@code collect at StreamSync}) for {@code write<n>}, the snapshot query's collect in this class
   * for {@code read}. Spec: {@code <phase>:<millis>:<path>}. One dump per cell: a rerun of the cell finds the first attempt's dump
   * and takes none.
   */
  private static final class PhaseHeapDump extends SparkListener {
    /** The stage of a round that runs the write handles. */
    static final String WRITE_STAGE = "collect at StreamSync";
    /** The snapshot read's own stage: the collect this class makes, not the listing stages before it. */
    private static final String READ_STAGE = "at BlobRoundMemoryChild";
    private final String target;
    private final long millis;
    private final String path;
    private final Map<Integer, String> stageNames = new ConcurrentHashMap<>();
    private final AtomicBoolean taken = new AtomicBoolean();

    PhaseHeapDump(String spec) {
      String[] parts = spec.split(":", 3);
      this.target = parts[0];
      this.millis = Long.parseLong(parts[1]);
      this.path = parts[2];
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted submitted) {
      stageNames.put(submitted.stageInfo().stageId(), submitted.stageInfo().name());
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart start) {
      String name = stageNames.getOrDefault(start.stageId(), "");
      boolean stage = name.contains("read".equals(target) ? READ_STAGE : WRITE_STAGE);
      if (target.equals(phase) && stage && taken.compareAndSet(false, true)) {
        if (Files.exists(Paths.get(path))) {
          // a surefire rerun of the cell: the first attempt's dump is the one kept
          System.out.println("PHASE_DUMP skipped, " + path + " exists from an earlier attempt");
          return;
        }
        String stageName = name;
        Thread thread = new Thread(() -> {
          try {
            // by design: the heap is dumped a fixed delay into the phase, so the sleep is the point in time, not a wait
            Thread.sleep(millis);
            System.out.println("PHASE_DUMP start phase=" + target + " stage=" + start.stageId() + " (" + stageName + ") at "
                + Instant.now());
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).dumpHeap(path, true);
            System.out.println("PHASE_DUMP done " + path + " at " + Instant.now());
          } catch (Exception e) {
            System.out.println("PHASE_DUMP failed " + e);
          }
        }, "phase-heap-dump");
        thread.setDaemon(true);
        thread.start();
      }
    }
  }
}
