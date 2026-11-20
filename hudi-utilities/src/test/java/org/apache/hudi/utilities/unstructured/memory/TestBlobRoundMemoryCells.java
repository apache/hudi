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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Memory cells of the MERGE_ON_READ blob round. Each cell runs the blob job (stock HoodieStreamer over the unstructured data source on S3 and
 * the blob quarantine transformer, MERGE_ON_READ, as {@link BlobJobHarness#streamerConfig} builds it) in a child JVM
 * ({@link BlobRoundMemoryChild}) whose heap is {@link #CELL_HEAP} (unless {@code -Dblob.memory.heap} says otherwise), local[4]. Every object is at most the inline threshold, so every version carries
 * its bytes INLINE: the first round inserts them into base files, then two rounds re-announce the same versions, so each updates
 * every record key and writes its inline bytes into log blocks. At the same heap the child then reads the snapshot, which merges
 * each base file with its two log layers, and digests every row's {@code read_blob} bytes. The cells pressure the write of inline
 * bytes into log blocks and the snapshot read that merges them. A cell passes when the child ends without an exhausted heap, the
 * first round updates nothing, each later round updates every record key and writes only log files (base files on the COW
 * cell), the timeline holds three completed commits, and the snapshot holds every version INLINE with the bytes written. Each cell writes its report
 * ({@code MEMORY_CELL} line) and its child's log under {@code target/memory-cells/}. Every persist is DISK_ONLY, as in the embedding
 * cells.
 *
 * <p>The cells are a micro benchmark at an explicit small heap, so the job's memory bounds are scaled to it: a log block
 * ({@code hoodie.logfile.data.block.max.size}, 256 MiB by default) and the snapshot read's merge buffer
 * ({@code hoodie.memory.compaction.max.size}, otherwise derived with a 100 MiB floor) are each {@value #MEMORY_BOUND_BYTES} bytes,
 * and the heap is the reference heap ({@value #REFERENCE_HEAP}, the embedding cells' one) plus that bound. At the defaults a
 * writing task holds its file group's updates below the block size and a reading task its file group's log records below the
 * merge buffer, each several times a task's share of the reference heap. Two more stock sizes are scaled the same way: the
 * log streams' buffer ({@code hoodie.memory.dfs.buffer.max.size}, 16 MiB by default) is {@value #STREAM_BUFFER_BYTES} bytes,
 * since a reading task holds two such streams per log file (doubled again on the local file system); and the Parquet row
 * group ({@code hoodie.parquet.block.size}, 120 MiB by default) is {@value #ROW_GROUP_BYTES} bytes, since a base file read
 * holds a whole row group of the inline bytes column. The base files keep their size. A COW update's merge buffer
 * ({@code hoodie.memory.merge.max.size}, a fixed 1 GiB by default) is {@value #MEMORY_BOUND_BYTES} bytes as well: the merge
 * handle holds a file group's incoming updates below it.
 *
 * <p>The inline threshold is {@value #INLINE_MAX_BYTES} bytes and an object {@value #FILE_SIZE} bytes, unless the threshold the
 * round uses is lower (a Parquet without per-column statistics, before 1.14, uses at most 81920): then an object is that
 * threshold, so every version stays INLINE.
 *
 * <p>Gated: a cell runs only when named in {@code -Dblob.memory.cells} (a comma list), so the class costs nothing in the
 * ordinary test runs (it starts no Spark of its own). Cells:
 * <ul>
 *   <li>{@code mor-inline-light}: {@value #LIGHT_FILES} objects, three rounds;</li>
 *   <li>{@code mor-inline}: {@value #FULL_BYTES_PER_ROUND} bytes of objects per round (6554 objects at the 81920-byte threshold,
 *   1074 at {@value #FILE_SIZE} bytes), three rounds;</li>
 *   <li>{@code cow-inline}: the {@code mor-inline} objects on a COPY_ON_WRITE table: each update round rewrites the base files.
 *   A manual cell: CI does not run it (the heavy job leaves it out of its list to stay within its time budget); run it with
 *   {@code -Dblob.memory.cells=cow-inline};</li>
 *   <li>{@code inline-ceiling-refused}: a threshold above the inline ceiling, refused on the driver before any task
 *   ({@link #inlineCeilingRefusedCell});</li>
 *   <li>{@code inline-ceiling-4-light}, {@code inline-ceiling-4} (heavy): four write tasks at once at the largest threshold the
 *   ceiling admits ({@link #inlineCeilingFourTasksCell});</li>
 *   <li>{@code events-light}, {@code events-100k} (heavy): one round over many small objects, some deleted before the round and
 *   quarantined ({@link #eventsCell}).</li>
 * </ul>
 * Those last cells run one round each and keep the rest of this setup (heap, memory bounds, DISK_ONLY).
 *
 * <p>Experiment knobs (system properties, all optional): {@code -Dblob.memory.props=<key>=<value>,...} overrides job properties
 * (hoodie.* keys; the child's Spark conf is fixed) after the cell's own; the snapshot read takes the hoodie.memory.* ones (its
 * merge buffer is hoodie.memory.compaction.max.size: the reader replaces hoodie.memory.merge.max.size with it); {@code -Dmemory.cell.master} is the child's Spark master (default {@code local[4]});
 * {@code -Dblob.memory.dump=<phase>:<millis>} has the child take one live heap dump in that phase ({@code write<n>} or
 * {@code read}, see {@link BlobRoundMemoryChild}) next to its log; {@code -Dblob.memory.events=<announced>:<deleted>} replaces the
 * object counts of the events cells.
 */
class TestBlobRoundMemoryCells {

  static final String REFERENCE_HEAP = TestEmbeddingRoundMemoryCells.REFERENCE_HEAP;
  /** The log block size and the read's and the write's merge buffers of the cells, scaled to the small heap. */
  static final long MEMORY_BOUND_BYTES = 15L << 20;
  /** The log streams' buffer of the cells, scaled to the small heap. */
  static final long STREAM_BUFFER_BYTES = 1L << 20;
  /** The Parquet row group of the cells, scaled to the small heap. */
  static final long ROW_GROUP_BYTES = 16L << 20;
  /** The reference heap plus {@link #MEMORY_BOUND_BYTES}. */
  static final String CELL_HEAP = (Long.parseLong(REFERENCE_HEAP.replace("m", "")) + (MEMORY_BOUND_BYTES >> 20)) + "m";
  private static final long CELL_TIMEOUT_MINUTES = 15;
  private static final long INLINE_MAX_BYTES = 524288;
  private static final int FILE_SIZE = 500000;
  private static final int LIGHT_FILES = 96;
  /** The inline bytes {@code mor-inline} writes per round, about the reference heap: the object count follows the object size. */
  private static final long FULL_BYTES_PER_ROUND = 512L << 20;
  /** The insert, then two re-announcements: the last snapshot merges a base file and two log layers. */
  private static final int ROUNDS = 3;
  /** The heap fraction of {@code inline-ceiling-refused}: its ceiling at the cell heap and local[4] falls below 80 KiB. */
  private static final double REFUSED_HEAP_FRACTION = 1.0 / 1024;
  private static final int REFUSED_FILES = 8;
  private static final int CEILING4_LIGHT_FILES = 8;
  private static final int EVENTS_FILE_SIZE = 1024;
  private static final int EVENTS_LIGHT_FILES = 2000;
  private static final int EVENTS_LIGHT_MISSING = 20;
  private static final int EVENTS_FULL_FILES = 100_000;
  private static final int EVENTS_FULL_MISSING = 1000;

  @TempDir
  Path tempDir;

  private static boolean selected(String cell) {
    String cells = System.getProperty("blob.memory.cells", "");
    return Arrays.stream(cells.split(",")).map(String::trim).anyMatch(cell::equals);
  }

  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"mor-inline-light", "mor-inline", "cow-inline"})
  void cellPassesAtTheReferenceHeap(String cell) throws Exception {
    // a report of an earlier run must not stand for this one
    Files.deleteIfExists(Paths.get("target", "memory-cells", cell + ".txt"));
    assumeTrue(selected(cell), "memory cell " + cell + " not selected (-Dblob.memory.cells)");
    Path work = tempDir.resolve("cell-" + cell);
    BlobJobHarness harness = BlobJobHarness.under(work);
    String table = work.resolve("table").toUri().toString();
    TypedProperties props = cellProperties(harness, table, INLINE_MAX_BYTES, Collections.emptyMap());
    HoodieTableType tableType = cell.startsWith("cow-") ? HoodieTableType.COPY_ON_WRITE : HoodieTableType.MERGE_ON_READ;
    int fileSize = (int) Math.min(FILE_SIZE, BlobTableConfig.inlineMaxBytes(props));
    int fileCount = cell.endsWith("-light") ? LIGHT_FILES : (int) ((FULL_BYTES_PER_ROUND + fileSize - 1) / fileSize);
    File propsFile = storeProperties(work, props, cell);

    String heap = System.getProperty("blob.memory.heap", CELL_HEAP);
    long start = System.nanoTime();
    Path childLog = Paths.get("target", "memory-cells", cell + "-child.log");
    Files.createDirectories(childLog.getParent());
    List<String> arguments = Arrays.asList(propsFile.getAbsolutePath(), table, harness.objectsRoot().toAbsolutePath().toString(),
        harness.eventsPath(), String.valueOf(fileCount), String.valueOf(fileSize), String.valueOf(ROUNDS), tableType.name());
    List<String> output = runChild(heap, arguments, childLog);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    // graded on the child's own report and the committed timeline
    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog.toAbsolutePath());
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(ROUNDS, rounds.size(), rounds.toString());
    assertEquals("0", field(rounds.get(0), "updates"), "the first round inserts every version: " + rounds.get(0));
    for (String round : rounds.subList(1, rounds.size())) {
      assertEquals(String.valueOf(fileCount), field(round, "updates"), "a re-announcement updates every record key: " + round);
      String expectedLogFiles = tableType == HoodieTableType.MERGE_ON_READ ? field(round, "files") : "0";
      assertEquals(expectedLogFiles, field(round, "log_files"), "an update round writes log files on MOR only: " + round);
    }
    assertEquals("SNAPSHOT rows=" + fileCount + " inline=" + fileCount + " sha_mismatches=0", value(output, "SNAPSHOT "),
        "the snapshot holds every version INLINE with the bytes written");
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(table)
        .setConf(HadoopFSUtils.getStorageConf(new Configuration())).build();
    HoodieTimeline commits = tableType == HoodieTableType.MERGE_ON_READ ? meta.getActiveTimeline().getDeltaCommitTimeline()
        : meta.getActiveTimeline().getCommitTimeline();
    assertEquals(ROUNDS, commits.filterCompletedInstants().countInstants(),
        "one completed " + (tableType == HoodieTableType.MERGE_ON_READ ? "deltacommit" : "commit") + " per round");

    long peak = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES ").substring("HEAP_POOL_PEAKS_SUM_BYTES ".length()));
    List<String> report = Collections.singletonList("MEMORY_CELL " + cell + " heap=" + heap + " master="
        + System.getProperty("memory.cell.master", "local[4]") + " rounds=" + rounds.size() + " files="
        + fileCount + " inline_bytes_per_round=" + ((long) fileCount * fileSize) + " heap_pool_peaks_sum_bytes=" + peak
        + " seconds=" + seconds + MemoryCellJvm.gcFields(childLog, output));
    report.forEach(System.out::println);
    Path reports = Paths.get("target", "memory-cells");
    Files.createDirectories(reports);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /**
   * Cell {@code inline-ceiling-refused} (light, {@link #CELL_HEAP}, local[4]): a job whose inline threshold is above the inline
   * ceiling of the child's own heap and task slots, the ceiling enforced (the default), is refused on the driver at the source's
   * fetch, before any Spark task of the round. The child writes and announces {@value #REFUSED_FILES} objects of the threshold's
   * size and runs one round. Asserts: the child ends normally at the cell heap; the threshold it prints is above the ceiling it
   * computes from its own conf; the round failed with the ceiling's refusal text, which names the inline max bytes key, the
   * threshold, the ceiling and "checked on the driver at job start"; no Spark task started from the round's start (a SparkListener
   * counting onTaskStart); the blob table's timeline holds no instant, so nothing was committed.
   *
   * <p>At the default heap fraction (1/64) the ceiling at the cell heap and local[4] is about 1.2 MB, far above
   * {@link ParquetInlineCap#MAX_BYTES} (80 KiB), the largest threshold a round uses on a Parquet without the per-column statistics
   * switch (the Spark 3.5 builds): there no configured threshold exceeds the ceiling at any heap Spark starts with at four slots.
   * So the cell lowers the fraction to {@value #REFUSED_HEAP_FRACTION}: the ceiling is then about 74 KB, below the 80 KiB
   * threshold the cell configures, on either Parquet.
   */
  @Test
  void inlineCeilingRefusedCell() throws Exception {
    String cell = "inline-ceiling-refused";
    // a report of an earlier run must not stand for this one
    Files.deleteIfExists(Paths.get("target", "memory-cells", cell + ".txt"));
    assumeTrue(selected(cell), "memory cell " + cell + " not selected (-Dblob.memory.cells)");
    Path work = tempDir.resolve("cell-" + cell);
    BlobJobHarness harness = BlobJobHarness.under(work);
    String table = work.resolve("table").toUri().toString();
    long threshold = ParquetInlineCap.MAX_BYTES;
    TypedProperties props = cellProperties(harness, table, threshold,
        Collections.singletonMap(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), String.valueOf(REFUSED_HEAP_FRACTION)));
    CellRun run = runCell(cell, work, props, table, REFUSED_FILES, (int) threshold, BlobRoundMemoryChild.MODE_REFUSED, 0);

    assertTrue(run.output.contains("RESULT ok"), "the child failed at " + run.heap + ", see " + run.log.toAbsolutePath());
    String ceilingLine = value(run.output, "INLINE_CEILING ");
    long ceiling = Long.parseLong(field(ceilingLine, "ceiling"));
    assertEquals(String.valueOf(threshold), field(ceilingLine, "threshold"), ceilingLine);
    assertTrue(threshold > ceiling, "the threshold is above the child's own ceiling: " + ceilingLine);
    String refusal = value(run.output, "REFUSED ");
    assertTrue(refusal.contains(BlobTableConfig.INLINE_MAX_BYTES.key() + "=" + threshold + " is above the per-task inline ceiling "
        + ceiling + " bytes (checked on the driver at job start)"), refusal);
    assertEquals("TASKS started=0", value(run.output, "TASKS "), "no Spark task ran in the refused round");
    assertEquals("TIMELINE instants=0 completed=0", value(run.output, "TIMELINE "), "the refused round left no instant");

    writeReport(cell, run, "inline_max_bytes=" + threshold + " ceiling=" + ceiling + " refused=true tasks_started=0");
  }

  /**
   * Cells {@code inline-ceiling-4} (heavy, CI only) and {@code inline-ceiling-4-light} (light), {@link #CELL_HEAP}, local[4]:
   * four write tasks at once, each writing inline values just under the largest inline threshold the ceiling admits at the cell
   * heap. The child computes the ceiling from its own conf (default fraction, ceiling enforced) and sets the threshold to it,
   * bounded by {@link ParquetInlineCap#MAX_BYTES} on a Parquet without the per-column statistics switch: on the Spark 3.5 builds
   * the ceiling (about 1.2 MB) is unreachable, so the threshold there is the 80 KiB cap, min(ceiling, cap). Objects are
   * {@value BlobRoundMemoryChild#NEAR_CEILING_MARGIN_BYTES} bytes below the threshold, so all INLINE; the inserts are split into
   * four buckets so the write stage runs four tasks. The light cell writes {@value #CEILING4_LIGHT_FILES} objects, the heavy cell
   * {@value #FULL_BYTES_PER_ROUND} bytes of them (6560 at the 80 KiB cap), one round of a MERGE_ON_READ table. Asserts: the child
   * ends at the cell heap; the threshold is min(ceiling, cap) as the child printed them; one completed deltacommit inserting every
   * object; the snapshot holds every row INLINE with the SHA-256 of the bytes written; the most write tasks running at once is 4
   * (a SparkListener counting the in-flight tasks of the write stage). The inserts go to the buckets by record key hash, so a
   * bucket of the light cell can be empty; the heavy cell's buckets hold about 1640 objects each.
   */
  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"inline-ceiling-4-light", "inline-ceiling-4"})
  void inlineCeilingFourTasksCell(String cell) throws Exception {
    // a report of an earlier run must not stand for this one
    Files.deleteIfExists(Paths.get("target", "memory-cells", cell + ".txt"));
    assumeTrue(selected(cell), "memory cell " + cell + " not selected (-Dblob.memory.cells)");
    Path work = tempDir.resolve("cell-" + cell);
    BlobJobHarness harness = BlobJobHarness.under(work);
    String table = work.resolve("table").toUri().toString();
    // the child replaces the threshold, the object size and, for the heavy cell, the object count
    TypedProperties props = cellProperties(harness, table, INLINE_MAX_BYTES, Collections.emptyMap());
    boolean light = cell.endsWith("-light");
    CellRun run = runCell(cell, work, props, table, light ? CEILING4_LIGHT_FILES : 0, 0, BlobRoundMemoryChild.MODE_CEILING4,
        light ? 0 : FULL_BYTES_PER_ROUND);

    assertTrue(run.output.contains("RESULT ok"), "the child failed at " + run.heap + ", see " + run.log.toAbsolutePath());
    String ceilingLine = value(run.output, "INLINE_CEILING ");
    long ceiling = Long.parseLong(field(ceilingLine, "ceiling"));
    String cap = field(ceilingLine, "parquet_cap");
    long threshold = Long.parseLong(field(ceilingLine, "threshold"));
    int files = Integer.parseInt(field(ceilingLine, "files"));
    int fileSize = Integer.parseInt(field(ceilingLine, "file_size"));
    assertEquals("none".equals(cap) ? ceiling : Math.min(ceiling, Long.parseLong(cap)), threshold,
        "the threshold is the largest the ceiling admits, within the Parquet inline cap: " + ceilingLine);
    assertEquals(threshold - BlobRoundMemoryChild.NEAR_CEILING_MARGIN_BYTES, fileSize, ceilingLine);
    assertEquals(0, files % BlobRoundMemoryChild.WRITE_TASKS, ceilingLine);
    String round = value(run.output, "ROUND ");
    assertEquals(String.valueOf(files), field(round, "writes"), "the round inserts every object: " + round);
    assertEquals("0", field(round, "updates"), round);
    assertEquals("SNAPSHOT rows=" + files + " inline=" + files + " sha_mismatches=0", value(run.output, "SNAPSHOT "),
        "the snapshot holds every object INLINE with the bytes written");
    String writeTasks = value(run.output, "WRITE_TASKS ");
    assertEquals(String.valueOf(BlobRoundMemoryChild.WRITE_TASKS), field(writeTasks, "max_in_flight"),
        "four write tasks hold near-threshold inline values at once: " + writeTasks);
    assertEquals(1, completedDeltaCommits(table), "one completed deltacommit");

    writeReport(cell, run, "ceiling=" + ceiling + " parquet_cap=" + cap + " inline_max_bytes=" + threshold + " files=" + files
        + " file_size=" + fileSize + " write_tasks_max_in_flight=" + field(writeTasks, "max_in_flight"));
  }

  /**
   * Cells {@code events-100k} (heavy, CI only) and {@code events-light} (light), {@link #CELL_HEAP}, local[4]: one round of a
   * MERGE_ON_READ table over many small announced objects ({@value #EVENTS_FILE_SIZE} bytes each, INLINE), some of which are
   * announced and then deleted from the objects root before the round. The round finds those missing when it reads them: the
   * row builder keeps the row without a blob (BlobRowBuilder#buildRow), the quarantine transformer routes it ("gone since
   * announced", BlobQuarantineTransformer) to the error table, and the round commits the rest. The job has an error table
   * (without one the first such row fails the round), and its per-round object cap
 * (hoodie.streamer.source.cloud.data.max.files.per.sync, 10,000 by default) is the announced count, so one round takes every
 * object. The heavy cell announces {@value #EVENTS_FULL_FILES} objects, of which
   * {@value #EVENTS_FULL_MISSING} are deleted; the light one {@value #EVENTS_LIGHT_FILES}, {@value #EVENTS_LIGHT_MISSING} deleted.
   * It pressures the driver and the source with many events and many failure records in one round. Asserts: the child ends at the
   * cell heap; one completed deltacommit whose inserts and writes equal the present objects; the error table holds one event per
   * deleted object, each "gone since announced"; the snapshot holds the present objects, all INLINE, with no null blob and each
   * {@code read_blob} the object size (counted in Spark: the driver keeps no digest per object at this count). Duration: the
   * child took 27 s at 2,000 objects and 32 s at 20,000 on an 8-core arm64 machine, so about 0.3 ms per object over a fixed
   * 26 s: about a minute at 100,000.
   */
  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"events-light", "events-100k"})
  void eventsCell(String cell) throws Exception {
    // a report of an earlier run must not stand for this one
    Files.deleteIfExists(Paths.get("target", "memory-cells", cell + ".txt"));
    assumeTrue(selected(cell), "memory cell " + cell + " not selected (-Dblob.memory.cells)");
    Path work = tempDir.resolve("cell-" + cell);
    BlobJobHarness harness = BlobJobHarness.under(work);
    String table = work.resolve("table").toUri().toString();
    TypedProperties props = cellProperties(harness, table, INLINE_MAX_BYTES, Collections.emptyMap());
    BlobJobHarness.withErrorTable(props, work.resolve("errors").toString());
    boolean light = cell.endsWith("-light");
    int files = light ? EVENTS_LIGHT_FILES : EVENTS_FULL_FILES;
    int missing = light ? EVENTS_LIGHT_MISSING : EVENTS_FULL_MISSING;
    String counts = System.getProperty("blob.memory.events", "");
    if (!counts.isEmpty()) {
      // an experiment knob: <announced>:<deleted>, e.g. to measure how the round scales with the event count
      files = Integer.parseInt(counts.split(":")[0]);
      missing = Integer.parseInt(counts.split(":")[1]);
    }
    // one round takes every announced object: the stock cap is 10,000 objects per round
    props.setProperty(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), String.valueOf(files));
    CellRun run = runCell(cell, work, props, table, files, EVENTS_FILE_SIZE, BlobRoundMemoryChild.MODE_EVENTS, missing);

    assertTrue(run.output.contains("RESULT ok"), "the child failed at " + run.heap + ", see " + run.log.toAbsolutePath());
    int present = files - missing;
    String events = value(run.output, "EVENTS ");
    assertEquals(String.valueOf(files), field(events, "announced"), events);
    assertEquals(String.valueOf(missing), field(events, "deleted"), events);
    assertEquals(String.valueOf(present), field(events, "inserts"), "the round inserts the present objects: " + events);
    assertEquals(String.valueOf(present), field(events, "writes"), "the round writes the present objects: " + events);
    assertEquals(String.valueOf(missing), field(events, "quarantined"), "one error event per deleted object: " + events);
    assertEquals(String.valueOf(missing), field(events, "gone_since_announced"), "each as gone since announced: " + events);
    String snapshot = value(run.output, "SNAPSHOT ");
    assertEquals("SNAPSHOT rows=" + present + " inline=" + present + " null_blobs=0 full_length=" + present,
        snapshot.substring(0, snapshot.indexOf(" seconds=")), "the snapshot holds the present objects INLINE with their bytes");
    assertEquals(1, completedDeltaCommits(table), "one completed deltacommit");

    writeReport(cell, run, "announced=" + files + " deleted=" + missing + " written=" + field(events, "writes") + " quarantined="
        + field(events, "quarantined") + " file_size=" + EVENTS_FILE_SIZE + " put_seconds=" + field(events, "put_seconds")
        + " announce_seconds=" + field(events, "announce_seconds") + " round_seconds=" + field(events, "round_seconds")
        + " snapshot_seconds=" + field(snapshot, "seconds"));
  }

  /**
   * The blob job's properties of a cell: the job file, the table, the inline threshold, every persist DISK_ONLY and the memory
   * bounds scaled to the small heap, then {@code cellOverrides}, then {@code -Dblob.memory.props}.
   */
  private static TypedProperties cellProperties(BlobJobHarness harness, String table, long inlineMaxBytes,
                                                Map<String, String> cellOverrides) throws IOException {
    TypedProperties props = harness.jobProperties(inlineMaxBytes);
    props.setProperty("hoodie.base.path", table);
    // every persist DISK_ONLY, the memory cells' reference (TestEmbeddingRoundMemoryCells)
    props.setProperty(HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    // the SIMPLE index persists the input records, inline bytes included
    props.setProperty(HoodieIndexConfig.SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    // the memory bounds scaled to the small heap (the child passes the merge buffer to its snapshot read)
    props.setProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_MAX_SIZE.key(), String.valueOf(MEMORY_BOUND_BYTES));
    props.setProperty(HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION.key(), String.valueOf(MEMORY_BOUND_BYTES));
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(MEMORY_BOUND_BYTES));
    props.setProperty(HoodieCommonConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(), String.valueOf(STREAM_BUFFER_BYTES));
    props.setProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), String.valueOf(ROW_GROUP_BYTES));
    cellOverrides.forEach(props::setProperty);
    for (String override : System.getProperty("blob.memory.props", "").split(",")) {
      if (!override.trim().isEmpty()) {
        String[] kv = override.trim().split("=", 2);
        if (kv.length != 2 || kv[0].isEmpty()) {
          throw new IllegalArgumentException("-Dblob.memory.props takes <key>=<value> pairs separated by commas, got '" + override + "'");
        }
        props.setProperty(kv[0], kv[1]);
      }
    }
    return props;
  }

  /** Stores {@code props} as {@code work/job.properties} for the child. */
  private static File storeProperties(Path work, TypedProperties props, String cell) throws IOException {
    Files.createDirectories(work);
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + cell);
    }
    return propsFile;
  }

  /** One child run of a single-round cell: its heap, log, output and duration. */
  private static final class CellRun {
    private final String heap;
    private final Path log;
    private final List<String> output;
    private final long seconds;

    CellRun(String heap, Path log, List<String> output, long seconds) {
      this.heap = heap;
      this.log = log;
      this.output = output;
      this.seconds = seconds;
    }
  }

  /** Runs one MERGE_ON_READ round of the child in {@code mode} at the cell heap. */
  private static CellRun runCell(String cell, Path work, TypedProperties props, String table, int fileCount, int fileSize, String mode,
                                 long modeArgument) throws IOException, InterruptedException {
    BlobJobHarness harness = BlobJobHarness.under(work);
    File propsFile = storeProperties(work, props, cell);
    String heap = System.getProperty("blob.memory.heap", CELL_HEAP);
    long start = System.nanoTime();
    Path childLog = Paths.get("target", "memory-cells", cell + "-child.log");
    Files.createDirectories(childLog.getParent());
    List<String> arguments = Arrays.asList(propsFile.getAbsolutePath(), table, harness.objectsRoot().toAbsolutePath().toString(),
        harness.eventsPath(), String.valueOf(fileCount), String.valueOf(fileSize), "1", HoodieTableType.MERGE_ON_READ.name(), mode,
        String.valueOf(modeArgument));
    List<String> output = runChild(heap, arguments, childLog);
    return new CellRun(heap, childLog, output, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
  }

  /** The completed deltacommits of the MERGE_ON_READ table at {@code table}. */
  private static int completedDeltaCommits(String table) {
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(table)
        .setConf(HadoopFSUtils.getStorageConf(new Configuration())).build();
    return meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants();
  }

  /**
   * Writes the cell's report {@code MEMORY_CELL <cell> heap=... master=... <details> heap_pool_peaks_sum_bytes=... seconds=...},
   * then {@link MemoryCellJvm#gcFields}.
   */
  private static void writeReport(String cell, CellRun run, String details) throws IOException {
    long peak = Long.parseLong(value(run.output, "HEAP_POOL_PEAKS_SUM_BYTES ").substring("HEAP_POOL_PEAKS_SUM_BYTES ".length()));
    List<String> report = Collections.singletonList("MEMORY_CELL " + cell + " heap=" + run.heap + " master="
        + System.getProperty("memory.cell.master", "local[4]") + " " + details + " heap_pool_peaks_sum_bytes=" + peak
        + " seconds=" + run.seconds + MemoryCellJvm.gcFields(run.log, run.output));
    report.forEach(System.out::println);
    Path reports = Paths.get("target", "memory-cells");
    Files.createDirectories(reports);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /** The last line of {@code output} starting with {@code prefix}, whole. */
  private static String value(List<String> output, String prefix) {
    return output.stream().filter(line -> line.startsWith(prefix)).reduce((first, last) -> last)
        .orElseThrow(() -> new AssertionError("no " + prefix.trim() + " line"));
  }

  /** The value of {@code name=<value>} in a {@code ROUND} (or another {@code name=<value>}) line. */
  private static String field(String line, String name) {
    return Arrays.stream(line.split(" ")).filter(token -> token.startsWith(name + "=")).map(token -> token.substring(name.length() + 1))
        .findFirst().orElseThrow(() -> new AssertionError("no " + name + " in " + line));
  }

  /** Runs {@link BlobRoundMemoryChild} at {@code heap} with {@code arguments}; its output is copied to {@code log} and returned. */
  private static List<String> runChild(String heap, List<String> arguments, Path log) throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(MemoryCellJvm.heapOptions(heap, log));
    command.add("-XX:+ExitOnOutOfMemoryError");
    // a heap dump of the first exhausted attempt; a surefire rerun does not overwrite it
    command.add("-XX:+HeapDumpOnOutOfMemoryError");
    String cell = log.getFileName().toString().replace("-child.log", "");
    command.add("-XX:HeapDumpPath=" + log.toAbsolutePath().getParent().resolve(cell + "-first-oom.hprof"));
    command.add("-Dmemory.cell.master=" + System.getProperty("memory.cell.master", "local[4]"));
    String dump = System.getProperty("blob.memory.dump", "");
    if (!dump.isEmpty()) {
      command.add("-Dmemory.cell.dump=" + dump + ":" + log.toAbsolutePath().getParent().resolve(cell + "-" + dump.split(":")[0]
          + ".hprof"));
    }
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens") || argument.startsWith("--add-exports")) {
        command.add(argument);
      }
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(BlobRoundMemoryChild.class.getName());
    command.addAll(arguments);
    Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!child.waitFor(CELL_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      child.destroyForcibly();
      throw new AssertionError("the cell's child did not finish in " + CELL_TIMEOUT_MINUTES + " minutes (TIMEOUT), see " + log);
    }
    List<String> output = Files.readAllLines(log, StandardCharsets.UTF_8);
    output.stream().filter(line -> line.startsWith("ROUND ") || line.startsWith("SNAPSHOT ")
        || line.startsWith("HEAP_POOL_PEAKS_SUM_BYTES ") || line.startsWith("RESULT ") || line.startsWith("INLINE_CEILING ")
        || line.startsWith("REFUSED") || line.startsWith("TASKS ") || line.startsWith("TIMELINE ") || line.startsWith("WRITE_")
        || line.startsWith("EVENTS "))
        .forEach(line -> System.out.println("child: " + line));
    System.out.println("child exit " + child.exitValue());
    // the child prints RESULT ok before stopping Spark: a shutdown that throws or exhausts the heap fails the cell too
    assertEquals(0, child.exitValue(), "the cell's child exited nonzero, see " + log.toAbsolutePath());
    return output;
  }
}
