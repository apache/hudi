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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobFileNames;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobRowBuilder;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Memory cells of the embedding round. Each cell announces objects in an S3 events table, then runs the embedding
 * job over the unstructured data source on S3
 * ({@code prep_and_compute}, the dummy model through {@link PartitionCountingEmbeddingClient}, every persist DISK_ONLY) in a
 * child JVM whose heap is the reference heap ({@value #REFERENCE_HEAP} unless {@code -Dembedding.memory.heap} says otherwise),
 * local[4]. A cell passes when the child ends its rounds without an exhausted heap and the table holds every file of the
 * fixture. Each cell writes its report ({@code MEMORY_CELL} lines) and its child's log under {@code target/memory-cells/}.
 *
 * <p>Gated: a cell runs only when named in {@code -Dembedding.memory.cells} (a comma list), so the class costs nothing in the
 * ordinary test runs. Cells:
 * <ul>
 *   <li>{@code small}: eight 3-page PDFs in one events commit, one round;</li>
 *   <li>{@code 4instants}: after a setup round, four events commits of 25 text files each with {@code num_instants=2} (the stock
 *   default is 5): four rounds, each bounded by that setting to exactly one NEW events commit, which its checkpoint names with its
 *   last key (a
 *   keyed checkpoint counts its own, fully read commit as the first of the {@code num_instants}, so 1 would read nothing);</li>
 *   <li>{@code skew-light} and {@code skew}: one large text PDF (about 2 MB, or 80 MB) and 63 one-chunk text files in ONE events
 *   commit, compute parallelism 4: the prepared rows are skewed (the large file's rows are at least 90% of them, all prepared in
 *   one task), and the compute shuffle spreads the rows by a hash of their keys over the 4 tasks: every task embeds rows, and
 *   none more than half again the mean;</li>
 *   <li>{@code 1k}: 1000 text files of 4 chunks in one events commit, one round;</li>
 *   <li>{@code prep-only}: the {@code skew} fixture under {@code prep_only}: the large file's rows are at least 90% of the rows
 *   (prep reads each file in one task) and are written with no compute stage, into a table with no model column;</li>
 *   <li>{@code pdf-over-heap} (CI only: it writes a {@value #PDF_OVER_HEAP_BULK_MIB} MiB file) and its light sibling
 *   {@code pdf-over-heap-light} ({@value #PDF_OVER_HEAP_LIGHT_BULK_MIB} MiB, the same layout and path): ONE PDF, larger than the
 *   child's heap in the heavy cell, goes through the out-of-line read, prep and embedding at the reference heap. See
 *   {@link #PDF_OVER_HEAP_BULK_MIB}.</li>
 * </ul>
 */
public class TestEmbeddingRoundMemoryCells extends EmbeddingRoundTestBase {

  static final String REFERENCE_HEAP = "576m";
  private static final long CELL_TIMEOUT_MINUTES = 15;
  private static final int COMPUTE_PARALLELISM = 4;

  /**
   * Bulk of the {@code pdf-over-heap} PDF, in MiB. It pressures the claim that a PDF's prep needs memory independent of the
   * file's size: the file ({@link MemoryCellFixtures#writePdfWithUnreferencedBulk}, {@value #PDF_OVER_HEAP_PAGES} text pages and
   * one {@value #PDF_OVER_HEAP_BULK_MIB} MiB binary stream that no page refers to, about 768 MiB in all) is larger than the
   * child's whole heap ({@value #REFERENCE_HEAP}, {@code Runtime.maxMemory()} at most that), so any stage that held it on the heap
   * would end the child. On its way: the source keeps an object above the inline threshold out of line, a reference with no
   * bytes; prep reads it through {@code BlobBytesSource}'s range stream and {@code PdfBoxPageSource} copies that stream through
   * one 64 KiB buffer to a staged file (PdfBoxPageSource.java:175-199) and loads it from that file with PDFBox's scratch on disk
   * (PdfBoxPageSource.java:136-146). PDFBox resolves objects from the exact xref only when they are reached, and nothing reaches
   * the bulk stream: the decode guard walks only each page's /Contents and the forms and fonts of its /Resources
   * (StreamDecodeGuard.java:167-186, 201-228), and the stripper extracts one page's content (PdfBoxPageSource.java:257-260). The
   * file is above the PDF max input bytes default (512 MiB), so the cell raises that key to
   * {@value #PDF_OVER_HEAP_MAX_INPUT_BYTES} in its job (outside the benchmarked envelope, which the chunker logs); the light cell
   * sets the same key so both run the same code path.
   *
   * <p>Asserted: one round and one commit; the PDF's text chunks are in the table, each with an embedding; the PDF row's stored
   * size equals the fixture's and, in the heavy cell, exceeds the child's {@code Runtime.maxMemory()} (both printed); the source
   * row the job's own {@link BlobRowBuilder} builds for the object is OUT_OF_LINE; the child reports RESULT ok.
   */
  static final int PDF_OVER_HEAP_BULK_MIB = 768;
  /** Bulk of {@code pdf-over-heap-light}: the same layout, small enough to run anywhere. */
  static final int PDF_OVER_HEAP_LIGHT_BULK_MIB = 20;
  static final int PDF_OVER_HEAP_PAGES = 4;
  private static final int PDF_OVER_HEAP_LINES_PER_PAGE = 50;
  /** The PDF max input bytes of both PDF cells: 1 GiB, above the heavy file. */
  static final long PDF_OVER_HEAP_MAX_INPUT_BYTES = 1L << 30;

  private static List<String> selectedCells() {
    String cells = System.getProperty("embedding.memory.cells", "");
    return Arrays.stream(cells.split(",")).map(String::trim).filter(cell -> !cell.isEmpty()).collect(Collectors.toList());
  }

  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"small", "4instants", "skew-light", "skew", "1k", "prep-only", "pdf-over-heap-light", "pdf-over-heap"})
  public void cellPassesAtTheReferenceHeap(String cell) throws Exception {
    // a report of an earlier run must not stand for this one
    Files.deleteIfExists(Paths.get("target", "memory-cells", cell + ".txt"));
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dembedding.memory.cells)");
    Path work = localPath(basePath()).resolve("cell-" + cell);
    Files.createDirectories(work);
    List<FixtureFile> files = new ArrayList<>();
    TypedProperties extra = new TypedProperties();
    int expectedRounds = 1;
    // the greatest key of each events commit after the setup one: what each 4instants round's checkpoint names
    List<String> lastKeys = new ArrayList<>();
    String bigSha = null;
    // the over-heap PDF: written straight into the bucket, never read into this JVM
    String pdfKey = null;
    long pdfBytes = 0;
    String mode = "prep-only".equals(cell) ? EmbeddingTableConfig.MODE_PREP_ONLY : EmbeddingTableConfig.MODE_PREP_AND_COMPUTE;
    switch (cell) {
      case "small":
        for (int i = 0; i < 8; i++) {
          files.add(object("cells/small/doc-" + i + ".pdf", PdfFixtures.prose(i, 3, 12)));
        }
        announce(files.toArray(new FixtureFile[0]));
        break;
      case "4instants":
        // a table's first round reads every events commit (stock READ_UPTO_LATEST_COMMIT ignores num_instants): a setup round
        // in this JVM takes the first one, then each of the child's rounds takes exactly one new one: the stock incremental query
        // counts the checkpoint's own commit (read to its last key) as the first of num_instants, so 2 reads exactly the next
        List<FixtureFile> setup = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
          setup.add(object("cells/4instants/setup-" + i + "." + TEXT_EXTENSION, MemoryCellFixtures.pipeText("setup file " + i, 4)));
        }
        announce(setup.toArray(new FixtureFile[0]));
        files.addAll(setup);
        EmbeddingStreamers.round(spark(), cellProps(mode, new TypedProperties()));
        for (int commit = 0; commit < 4; commit++) {
          List<FixtureFile> batch = new ArrayList<>();
          for (int i = 0; i < 25; i++) {
            batch.add(object("cells/4instants/c" + commit + "-" + i + "." + TEXT_EXTENSION,
                MemoryCellFixtures.pipeText("commit " + commit + " file " + i, 4)));
          }
          announce(batch.toArray(new FixtureFile[0]));
          files.addAll(batch);
          lastKeys.add(batch.stream().map(EmbeddingRoundTestBase::keyOf).max(String::compareTo).get());
        }
        extra.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "2");
        expectedRounds = 4;
        break;
      case "skew-light":
      case "skew":
      case "prep-only":
        Path big = work.resolve("big.pdf");
        MemoryCellFixtures.writeTextPdf(big, "skew-light".equals(cell) ? 2L << 20 : 80L << 20, 100, 7L);
        FixtureFile bigFile = object("cells/skew/big.pdf", Files.readAllBytes(big));
        bigSha = bigFile.sha;
        files.add(bigFile);
        for (int i = 0; i < 63; i++) {
          files.add(object("cells/skew/tiny-" + i + "." + TEXT_EXTENSION, MemoryCellFixtures.pipeText("tiny " + i, 1)));
        }
        announce(files.toArray(new FixtureFile[0]));
        if (EmbeddingTableConfig.MODE_PREP_AND_COMPUTE.equals(mode)) {
          extra.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), String.valueOf(COMPUTE_PARALLELISM));
        }
        break;
      case "pdf-over-heap-light":
      case "pdf-over-heap":
        pdfKey = "cells/pdf-over-heap/large.pdf";
        BlobJobHarness bucket = objects();
        Path pdf = bucket.file(pdfKey);
        Files.createDirectories(pdf.getParent());
        long fixtureStart = System.nanoTime();
        pdfBytes = MemoryCellFixtures.writePdfWithUnreferencedBulk(pdf, PDF_OVER_HEAP_PAGES, PDF_OVER_HEAP_LINES_PER_PAGE,
            ("pdf-over-heap".equals(cell) ? PDF_OVER_HEAP_BULK_MIB : PDF_OVER_HEAP_LIGHT_BULK_MIB) * (1L << 20), 11L);
        System.out.println("pdf fixture " + pdfBytes + " bytes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fixtureStart)
            + " ms");
        bucket.announce(spark(), Collections.singletonList(
            S3EventsTableFixture.created(BlobJobHarness.BUCKET, pdfKey, pdfBytes, BlobJobHarness.EVENT_TIME)));
        extra.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), String.valueOf(PDF_OVER_HEAP_MAX_INPUT_BYTES));
        break;
      case "1k":
        for (int i = 0; i < 1000; i++) {
          files.add(object("cells/1k/f-" + i + "." + TEXT_EXTENSION, MemoryCellFixtures.pipeText("file " + i, 4)));
        }
        announce(files.toArray(new FixtureFile[0]));
        break;
      default:
        throw new IllegalArgumentException(cell);
    }

    TypedProperties props = cellProps(mode, extra);
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + cell);
    }

    String heap = System.getProperty("embedding.memory.heap", REFERENCE_HEAP);
    long start = System.nanoTime();
    Path childLog = Paths.get("target", "memory-cells", cell + "-child.log");
    Files.createDirectories(childLog.getParent());
    List<String> output = runChild(heap, propsFile, expectedRounds + 1, childLog);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    // graded on the child's own report and the committed table
    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog.toAbsolutePath());
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(expectedRounds, rounds.size(), rounds.toString());
    Map<String, Row> rows = rows(table());
    long filesCommitted = rows.values().stream().map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)).distinct().count();
    assertEquals(files.size() + (pdfKey == null ? 0 : 1), filesCommitted, "every file of the fixture is in the table");
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      List<String> columns = Arrays.asList(rows.values().iterator().next().schema().fieldNames());
      assertFalse(columns.contains(EmbeddingStreamers.MODEL_COLUMN), "a prep_only table has no model column: " + columns);
      assertTrue(rows.values().stream().allMatch(row -> row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK) != null),
          "every row has its prepared chunk");
    } else {
      assertTrue(rows.values().stream().allMatch(row -> vector(row) != null), "every row has its embedding");
    }
    long peak = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES "));
    List<String> report = new ArrayList<>();
    report.add("MEMORY_CELL " + cell + " heap=" + heap + " master=" + System.getProperty("memory.cell.master", "local[4]")
        + " rounds=" + rounds.size() + " files=" + filesCommitted + " rows="
        + rows.size() + " heap_pool_peaks_sum_bytes=" + peak + " seconds=" + seconds + MemoryCellJvm.gcFields(childLog, output));

    if ("4instants".equals(cell)) {
      List<HoodieInstant> eventsCommits = EmbeddingStreamers.commits(objects().eventsPath(), storage());
      for (int n = 0; n < rounds.size(); n++) {
        HoodieInstant eventsCommit = eventsCommits.get(n + 1);
        assertTrue(rounds.get(n).endsWith("checkpoint=" + eventsCommit.requestedTime() + "#" + lastKeys.get(n)),
            "round " + (n + 1) + " covers exactly events commit " + eventsCommit + ": " + rounds.get(n));
        assertTrue(rounds.get(n).contains(" rows=100 "), rounds.get(n));
      }
    }
    if (bigSha != null) {
      String sha = bigSha;
      long bigRows = rows.values().stream().filter(row -> sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))).count();
      double skew = (double) bigRows / rows.size();
      assertTrue(skew >= 0.9, "pre-shuffle skew " + skew + ": the large file's rows are prepared in one task");
      if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
        report.add("MEMORY_CELL " + cell + " pre_shuffle_skew=" + skew);
      } else {
        Map<Integer, Long> counts = new TreeMap<>();
        for (String entry : value(output, "COMPUTE_PARTITIONS ").split(",")) {
          String[] kv = entry.split("=");
          counts.merge(Integer.parseInt(kv[0]), Long.parseLong(kv[1]), Long::sum);
        }
        report.add("MEMORY_CELL " + cell + " pre_shuffle_skew=" + skew + " compute_tasks=" + counts);
        assertEquals(COMPUTE_PARALLELISM, counts.size(), counts.toString());
        assertEquals((long) rows.size(), counts.values().stream().mapToLong(Long::longValue).sum());
        // a hash spread: balanced in expectation, so each task holds rows and none more than half again the mean
        double mean = (double) rows.size() / COMPUTE_PARALLELISM;
        assertTrue(Collections.min(counts.values()) > 0, counts.toString());
        assertTrue(Collections.max(counts.values()) <= 1.5 * mean, counts.toString());
      }
    }
    if (pdfKey != null) {
      report.add(checkOverHeapPdf(cell, props, pdfKey, pdfBytes, rows, Long.parseLong(value(output, "MAX_HEAP_BYTES "))));
    }
    report.forEach(System.out::println);
    Path reports = Paths.get("target", "memory-cells");
    Files.createDirectories(reports);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /**
   * The over-heap PDF cells' own checks (see {@link #PDF_OVER_HEAP_BULK_MIB}); returns their report line.
   */
  private String checkOverHeapPdf(String cell, TypedProperties props, String pdfKey, long pdfBytes, Map<String, Row> rows,
                                  long childMaxHeap) {
    String sourcePath = BlobJobHarness.sourcePath(pdfKey);
    String sha = BlobFileNames.sourcePathSha(sourcePath);
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size(), "one commit");
    List<Row> pdfRows = rows.values().stream().filter(row -> sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)))
        .collect(Collectors.toList());
    assertTrue(pdfRows.size() > 0, "the PDF's text chunks are in the table");
    for (Row row : pdfRows) {
      assertEquals(EmbeddingTableSchema.MODALITY_TEXT, row.getAs(EmbeddingTableSchema.MODALITY), row.toString());
      String chunk = row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
      assertTrue(chunk != null && !chunk.trim().isEmpty(), "a prepared text chunk: " + row);
      assertEquals(EmbeddingStreamers.MODEL_DIMENSIONS, vector(row).length, "the chunk's embedding: " + row);
      assertEquals(pdfBytes, (long) row.<Long>getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE), "the stored size is the fixture's");
    }
    // the row the job's source builds for the object, from the same job properties: a reference, no bytes
    Row sourceRow = new BlobRowBuilder(props, storage())
        .build(Collections.singletonList(new CloudObjectMetadata(sourcePath, pdfBytes)).iterator()).next();
    // the builder's rows carry no schema: the blob and its type are read at their positions in the blob table schema
    StructField blobField = BlobTableSchema.schema().apply(BlobTableSchema.BLOB);
    Row blob = sourceRow.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB));
    String blobType = blob.getString(((StructType) blobField.dataType()).fieldIndex(HoodieSchema.Blob.TYPE));
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blobType, "the source keeps the PDF out of line");
    if ("pdf-over-heap".equals(cell)) {
      assertTrue(pdfBytes > childMaxHeap, "the PDF (" + pdfBytes + " bytes) is larger than the child's heap (" + childMaxHeap + ")");
    }
    return "MEMORY_CELL " + cell + " pdf_bytes=" + pdfBytes + " child_max_heap_bytes=" + childMaxHeap + " over_heap="
        + (pdfBytes > childMaxHeap) + " pdf_rows=" + pdfRows.size() + " blob=" + blobType;
  }

  /**
   * The cell's job in {@code mode}: under {@code prep_and_compute} the counting client; DISK_ONLY persists, the default balance,
   * then {@code extra}.
   */
  private TypedProperties cellProps(String mode, TypedProperties extra) {
    TypedProperties props = props(table(), mode);
    // the child installs the bucket file system over the same objects
    props.setProperty(BucketFolderFileSystem.ROOT, objects().objectsRoot().toAbsolutePath().toString());
    if (EmbeddingTableConfig.MODE_PREP_AND_COMPUTE.equals(mode)) {
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
          PartitionCountingEmbeddingClient.class.getName());
    }
    props.setProperty(HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    props.remove(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key());
    // the cells hold PDFs and text files only: without video in the allow-list the video frame heap check does not apply
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf," + TEXT_EXTENSION);
    props.putAll(extra);
    return props;
  }

  private static String value(List<String> output, String prefix) {
    return output.stream().filter(line -> line.startsWith(prefix)).map(line -> line.substring(prefix.length()))
        .reduce((first, last) -> last).orElseThrow(() -> new AssertionError("no " + prefix.trim() + " line"));
  }

  /** Runs {@link EmbeddingRoundMemoryChild} at {@code heap}; its output is copied to {@code log} and returned. */
  private static List<String> runChild(String heap, File propsFile, int maxRounds, Path log) throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(MemoryCellJvm.heapOptions(heap, log));
    command.add("-XX:+ExitOnOutOfMemoryError");
    // a heap dump of the first exhausted attempt next to the log; -Dmemory.cell.master sets the child's Spark master
    command.add("-XX:+HeapDumpOnOutOfMemoryError");
    command.add("-XX:HeapDumpPath=" + log.toAbsolutePath().getParent().resolve(log.getFileName().toString().replace("-child.log", "")
        + "-first-oom.hprof"));
    command.add("-Dmemory.cell.master=" + System.getProperty("memory.cell.master", "local[4]"));
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens") || argument.startsWith("--add-exports")) {
        command.add(argument);
      }
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(EmbeddingRoundMemoryChild.class.getName());
    command.add(propsFile.getAbsolutePath());
    command.add(String.valueOf(maxRounds));
    Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!child.waitFor(CELL_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      child.destroyForcibly();
      throw new AssertionError("the cell's child did not finish in " + CELL_TIMEOUT_MINUTES + " minutes (TIMEOUT), see " + log);
    }
    List<String> output = Files.readAllLines(log, StandardCharsets.UTF_8);
    output.stream().filter(line -> line.startsWith("ROUND ") || line.startsWith("COMPUTE_PARTITIONS ")
        || line.startsWith("HEAP_POOL_PEAKS_SUM_BYTES ") || line.startsWith("MAX_HEAP_BYTES ") || line.startsWith("RESULT ")).forEach(line -> System.out.println("child: " + line));
    System.out.println("child exit " + child.exitValue());
    // the child prints RESULT ok before stopping Spark: a shutdown that throws or exhausts the heap fails the cell too
    assertEquals(0, child.exitValue(), "the cell's child exited nonzero, see " + log.toAbsolutePath());
    return output;
  }
}
