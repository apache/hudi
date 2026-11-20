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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.input.PayloadDestination;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.CountingObjectFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prep iterator's read ahead ({@link EmbeddingTableConfig#PREP_PREFETCH_MAX_BYTES}): the next file's object is read on a
 * background thread while the current file is prepared; the rows and side outputs are those of a run without it, in the same
 * order; a read ahead that fails hands the file back to the task, which opens it as before; closing the iterator stops a read in
 * flight.
 */
public class TestPreprocessingIteratorPrefetch {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private static final String PREFETCH_THREAD = "embedding-prep-prefetch-";

  @TempDir
  Path temp;

  private String embeddingTable;
  private boolean useEtagStatus;
  private BlobBytesSource sourceOverride;
  private boolean useStatusFailures;

  @BeforeEach
  void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
  }

  @AfterEach
  void clear() {
    SourceOpenHooks.clear();
    CountingObjectFileSystem.clear();
    StatusFailureFileSystem.missing = false;
    StatusFailureFileSystem.FAILURES.set(0);
  }

  /**
   * A mixed corpus gives the same rows and side outputs, in the same order, with the read ahead and without it: images (one whose
   * header the reader parses twice), text read as a stream, a file larger than the bound, a missing object, an object longer and
   * one shorter than listed, a row without a blob, an extension no preprocessor takes, an inline file and a ranged reference.
   */
  @Test
  void rowsAndSideOutputsAreThoseOfARunWithoutReadAhead() throws IOException {
    List<Row> corpus = corpus();
    Run without = run(props(0L), corpus);
    Run with = run(props(100_000L), corpus);

    assertEquals(without.rows, with.rows);
    assertEquals(without.sideOutputs, with.sideOutputs);
    assertEquals(9, with.rows.size(), with.rows.toString());
    assertEquals(6, with.sideOutputs.size(), with.sideOutputs.toString());
    assertTrue(with.sideOutputs.stream().anyMatch(error -> error.startsWith("sha-longer prep text") && error.contains("changed")),
        with.sideOutputs.toString());
    assertTrue(with.prefetchOpens > 0, "files were read ahead");
    assertEquals(0, without.prefetchOpens, "nothing is read ahead at 0");
  }

  /**
   * A file read ahead is opened on the read-ahead thread and then prepared with no further storage access; the first file and a
   * file larger than the bound are opened by the task itself.
   */
  @Test
  void theNextFileIsReadOnTheReadAheadThread() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    byte[] big = ImageFixtures.image("bmp", 300, 200);
    Row[] rows = {
        objectRow("sha-1", "png", object("one.png", png), png.length),
        objectRow("sha-2", "png", object("two.png", png), png.length),
        objectRow("sha-big", "bmp", object("big.bmp", big), big.length),
        objectRow("sha-3", "png", object("three.png", png), png.length)};
    Map<String, List<String>> openThreads = recordOpenThreads("one.png", "two.png", "big.bmp", "three.png");

    List<Row> out = drain(iterator(props(100_000L), Arrays.asList(rows)));

    assertEquals(4, out.size());
    assertTrue(openThreads.get("one.png").stream().noneMatch(t -> t.startsWith(PREFETCH_THREAD)), openThreads.toString());
    assertEquals(1, openThreads.get("two.png").size(), "read ahead once, then served from memory: " + openThreads);
    assertTrue(openThreads.get("two.png").get(0).startsWith(PREFETCH_THREAD), openThreads.toString());
    assertTrue(openThreads.get("big.bmp").stream().noneMatch(t -> t.startsWith(PREFETCH_THREAD)), "larger than the bound");
    assertTrue(openThreads.get("three.png").get(0).startsWith(PREFETCH_THREAD), openThreads.toString());
    assertTrue(CountingObjectFileSystem.counts(path("two.png")).statuses.get() >= 3,
        "prefetch checks before and after reading; prep checks again before consuming buffered bytes");
  }

  @Test
  void confirmedReadAheadVersionChangeMustNotBecomeAFreshUnversionedRead() throws IOException {
    byte[] bytes = ImageFixtures.image("png", 30, 20);
    Row row = objectRow("sha-change", "png", object("changed.png", bytes), bytes.length);
    java.util.concurrent.atomic.AtomicBoolean changed = new java.util.concurrent.atomic.AtomicBoolean(true);
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())) {
      @Override
      public BlobBytesSource withoutOpenRetry() {
        return this;
      }

      @Override
      public void checkVersion(Row blob, long modificationTime, long length) throws IOException {
        if (changed.get()) {
          throw new org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException("changed during read ahead");
        }
      }

      @Override
      public InputStream open(Row blob, long length) {
        return new java.io.ByteArrayInputStream(bytes);
      }
    };
    try (SourcePrefetcher prefetcher = new SourcePrefetcher(source, 100_000L)) {
      prefetcher.start(row, bytes.length, bytes.length);
      assertThrows(org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException.class,
          () -> prefetcher.take(row));
      changed.set(false);
      prefetcher.start(row, bytes.length, bytes.length);
      assertArrayEquals(bytes, prefetcher.take(row).get().bytes(), "an unchanged read ahead remains usable");
    }
  }

  @Test
  void suppressedReadAheadVersionChangeMustNotBecomeAFallback() throws IOException {
    byte[] bytes = ImageFixtures.image("png", 30, 20);
    Row row = objectRow("sha-change", "png", object("changed.png", bytes), bytes.length);
    for (boolean changed : new boolean[] {false, true}) {
      BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())) {
        @Override
        public BlobBytesSource withoutOpenRetry() {
          return this;
        }

        @Override
        public void checkVersion(Row blob, long modificationTime, long length) {
          // The overwrite is detected by the post-read status check when the stream closes.
        }

        @Override
        public InputStream open(Row blob, long length) {
          return new InputStream() {
            @Override
            public int read() throws IOException {
              throw new IOException("interrupted object read");
            }

            @Override
            public void close() throws IOException {
              if (changed) {
                throw new org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException("changed at close");
              }
            }
          };
        }
      };
      try (SourcePrefetcher prefetcher = new SourcePrefetcher(source, 100_000L)) {
        prefetcher.start(row, bytes.length, bytes.length);
        if (changed) {
          assertThrows(org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException.class,
              () -> prefetcher.take(row));
        } else {
          assertFalse(prefetcher.take(row).isPresent(), "a transient read failure still permits fallback");
        }
      }
    }
  }

  /** A read ahead that fails hands the file back to the task, which opens it itself, exactly as without a read ahead. */
  @Test
  void failedReadAheadHandsTheFileBackToTheTask() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {
        objectRow("sha-1", "png", object("first.png", png), png.length),
        objectRow("sha-2", "png", object("flaky.png", png), png.length)};
    AtomicInteger attempts = new AtomicInteger();
    SourceOpenHooks.beforeOpen(local("flaky.png").toAbsolutePath().toString(), () -> {
      if (attempts.incrementAndGet() == 1) {
        throw new IOException("injected 503 Slow Down");
      }
    });

    List<Row> out = drain(iterator(props(100_000L), Arrays.asList(rows)));

    assertEquals(2, out.size());
    assertEquals(2, attempts.get(), "the failed read ahead, then the task's own open");
  }

  /** A storage failure the task meets itself still fails the round, after the rows of the files before it. */
  @Test
  void persistentStorageFailureStillFailsTheRound() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {
        objectRow("sha-1", "png", object("good.png", png), png.length),
        objectRow("sha-2", "png", object("throttled.png", png), png.length)};
    SourceOpenHooks.beforeOpen(local("throttled.png").toAbsolutePath().toString(), () -> {
      throw new IOException("injected 503 Slow Down");
    });
    PreprocessingIterator iterator = iterator(props(100_000L), Arrays.asList(rows));
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
    RuntimeException failed = assertThrows(RuntimeException.class, iterator::hasNext);
    assertNotNull(PrepFailures.roundFailure(failed), failed.toString());
    iterator.close();
  }

  /** Closing the iterator interrupts a read ahead in flight. */
  @Test
  void closingStopsAReadInFlight() throws Exception {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {
        objectRow("sha-1", "png", object("now.png", png), png.length),
        objectRow("sha-2", "png", object("slow.png", png), png.length)};
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    SourceOpenHooks.beforeOpen(local("slow.png").toAbsolutePath().toString(), () -> {
      started.countDown();
      try {
        // by design: simulates a slow read that the iterator's close interrupts; not a wait
        Thread.sleep(30_000L);
      } catch (InterruptedException e) {
        interrupted.countDown();
        throw new IOException("interrupted", e);
      }
    });
    PreprocessingIterator iterator = iterator(props(100_000L), Arrays.asList(rows));
    assertNotNull(iterator.next());
    assertTrue(started.await(10, TimeUnit.SECONDS), "the next file is being read ahead");
    iterator.close();
    assertTrue(interrupted.await(10, TimeUnit.SECONDS), "closing interrupted the read in flight");
    assertFalse(iterator.hasNext());
  }

  /**
   * A close on another thread (Spark's task-completion listener) while the task waits for a read ahead ends that wait: the read is
   * interrupted and the task is not left blocked on it.
   */
  @Test
  void closingOnAnotherThreadEndsAWaitForAReadAhead() throws Exception {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {
        objectRow("sha-1", "png", object("first.png", png), png.length),
        objectRow("sha-2", "png", object("stuck.png", png), png.length)};
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    SourceOpenHooks.beforeOpen(local("stuck.png").toAbsolutePath().toString(), () -> {
      if (!Thread.currentThread().getName().startsWith(PREFETCH_THREAD)) {
        return;
      }
      started.countDown();
      try {
        // by design: simulates a slow read that the iterator's close interrupts; not a wait
        Thread.sleep(30_000L);
      } catch (InterruptedException e) {
        interrupted.countDown();
        throw new IOException("interrupted", e);
      }
    });
    PreprocessingIterator iterator = iterator(props(100_000L), Arrays.asList(rows));
    assertNotNull(iterator.next());
    assertTrue(started.await(10, TimeUnit.SECONDS), "the next file is being read ahead");
    Thread caller = Thread.currentThread();
    Thread closer = new Thread(() -> {
      // closes once the caller is blocked in hasNext, waiting for the read ahead
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (caller.getState() != Thread.State.WAITING && caller.getState() != Thread.State.TIMED_WAITING
          && System.nanoTime() < deadline) {
        Thread.onSpinWait();
      }
      iterator.close();
    });
    closer.start();
    long start = System.nanoTime();
    iterator.hasNext();
    long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    closer.join(10_000L);
    assertTrue(interrupted.await(10, TimeUnit.SECONDS), "the read in flight was interrupted");
    assertTrue(waitedMs < 10_000L, "the task stopped waiting once the iterator closed: " + waitedMs + " ms");
  }

  /** The bound is a byte count in [0, one array]. */
  @Test
  void theBoundIsValidated() {
    assertEquals(EmbeddingTableConfig.DEFAULT_PREP_PREFETCH_MAX_BYTES, EmbeddingTableConfig.from(props()).prepPrefetchMaxBytes());
    EmbeddingTableConfig negative = EmbeddingTableConfig.from(props(-1L));
    assertThrows(IllegalArgumentException.class, negative::prepPrefetchMaxBytes);
    EmbeddingTableConfig overOneArray = EmbeddingTableConfig.from(props(EmbeddingTableConfig.MAX_PREP_PREFETCH_MAX_BYTES + 1));
    assertThrows(IllegalArgumentException.class, overOneArray::prepPrefetchMaxBytes);
  }

  private List<Row> corpus() throws IOException {
    byte[] png = ImageFixtures.image("png", 97, 41);
    byte[] jpeg = ImageFixtures.image("jpeg", 120, 80);
    byte[] tiff = ImageFixtures.singleStripTiff(200, 100);
    byte[] big = ImageFixtures.image("bmp", 300, 200);
    byte[] text = "alpha beta|gamma delta|epsilon".getBytes(StandardCharsets.UTF_8);
    byte[] broken = ("one|" + TextPipePreprocessor.BROKEN).getBytes(StandardCharsets.UTF_8);
    byte[] twice = metadataHeavyJpeg();
    List<Row> rows = new ArrayList<>();
    rows.add(objectRow("sha-png", "png", object("a.png", png), png.length));
    rows.add(objectRow("sha-text", "txt", object("b.txt", text), text.length));
    rows.add(objectRow("sha-twice", "jpg", object("c.jpg", twice), twice.length));
    rows.add(objectRow("sha-missing", "png", path("gone.png"), 1_000L));
    rows.add(objectRow("sha-big", "bmp", object("d.bmp", big), big.length));
    rows.add(goneRow("sha-gone", "png"));
    rows.add(objectRow("sha-unknown", "xyz", object("e.xyz", png), png.length));
    rows.add(objectRow("sha-broken", "txt", object("f.txt", broken), broken.length));
    rows.add(objectRow("sha-longer", "txt", object("g.txt", text), text.length - 5L));
    rows.add(objectRow("sha-shorter", "jpg", object("h.jpg", Arrays.copyOf(jpeg, 60)), jpeg.length));
    rows.add(inlineRow("sha-inline", "jpg", jpeg));
    rows.add(rangedRow("sha-ranged", "png", object("i.bin", concat(new byte[17], png)), 17L, png.length));
    rows.add(objectRow("sha-tiff", "tif", object("j.tif", tiff), tiff.length));
    return rows;
  }

  /**
   * Videos read ahead while their frames are written out of line in the background: two clips in one partition, every frame
   * above the inline threshold (0). The second clip is read on the read-ahead thread; the frames (their stored bytes) and side
   * outputs are those of a run without the read ahead, each clip is opened once in all (a failed read ahead would open it again
   * on the task), and every payload file a row references exists when the iterator first reports its end.
   */
  @Test
  void videosReadAheadWithBackgroundFrameWritesMatchARunWithoutReadAhead() throws IOException {
    byte[] first = resource("/video/h264-aac-speech-12s.mp4");
    byte[] second = resource("/video/h264-aac-bframes-12s.mp4");
    List<Row> corpus = Arrays.asList(
        objectRow("sha-v1", "mp4", object("v1.mp4", first), first.length),
        objectRow("sha-v2", "mp4", object("v2.mp4", second), second.length));

    StoredRun without = storedRun(videoProps(0L), corpus);
    StoredRun with = storedRun(videoProps(8L * 1024 * 1024), corpus);

    assertEquals(without.frames, with.frames);
    assertEquals(without.sideOutputs, with.sideOutputs);
    assertTrue(with.frames.size() >= 2, with.frames.toString());
    assertTrue(with.frames.stream().allMatch(frame -> frame.startsWith(HoodieSchema.Blob.OUT_OF_LINE)), with.frames.toString());
    assertEquals(0, without.prefetchOpens, "nothing is read ahead at 0");
    assertEquals(1, with.prefetchOpens, "the second clip is read ahead");
    assertEquals(Arrays.asList(1, 1), with.opensPerClip, "each clip is opened once, the second only by the read ahead");
    assertEquals(Arrays.asList(1, 1), without.opensPerClip);
    assertEquals(Collections.emptyList(), with.missingAtEnd);
    assertEquals(Collections.emptyList(), without.missingAtEnd);
  }

  /** A JPEG under 1 MiB whose header the JDK reader reads twice, more than 1 MiB served in all. */
  private static byte[] metadataHeavyJpeg() {
    byte[] jpeg = ImageFixtures.image("jpeg", 64, 48);
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    out.write(jpeg, 0, 2);
    java.util.Random random = new java.util.Random(1);
    for (int i = 0; i < 9; i++) {
      out.write(0xFF);
      out.write(0xE2);
      out.write(0xFF);
      out.write(0xFF);
      byte[] data = new byte[65533];
      random.nextBytes(data);
      for (int b = 0; b < data.length; b++) {
        if (data[b] == (byte) 0xFF) {
          data[b] = 0;
        }
      }
      out.write(data, 0, data.length);
    }
    out.write(jpeg, 2, jpeg.length - 2);
    return out.toByteArray();
  }

  /** The rows, side outputs and read-ahead opens of one drained run over {@code rows}. */
  private static final class Run {
    private final List<String> rows = new ArrayList<>();
    private final List<String> sideOutputs = new ArrayList<>();
    private int prefetchOpens;
  }

  /** The frames (each its blob type and the SHA-256 of its stored bytes), side outputs and read-ahead opens of one drained run. */
  private static final class StoredRun {
    private final List<String> frames = new ArrayList<>();
    private final List<String> sideOutputs = new ArrayList<>();
    private final List<String> missingAtEnd = new ArrayList<>();
    private final List<Integer> opensPerClip = new ArrayList<>();
    private int prefetchOpens;
  }

  /** {@link #run} with the input store writing out-of-line payloads under the embedding table. */
  private StoredRun storedRun(TypedProperties props, List<Row> rows) throws IOException {
    AtomicInteger prefetchOpens = new AtomicInteger();
    List<AtomicInteger> opens = new ArrayList<>();
    for (Row row : rows) {
      String path = ((Row) row.getAs(BlobColumns.BLOB)).getStruct(2).getString(0);
      AtomicInteger clipOpens = new AtomicInteger();
      opens.add(clipOpens);
      SourceOpenHooks.beforeOpen(localOf(path).toAbsolutePath().toString(), () -> {
        clipOpens.incrementAndGet();
        if (Thread.currentThread().getName().startsWith(PREFETCH_THREAD)) {
          prefetchOpens.incrementAndGet();
        }
      });
    }
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    if (useEtagStatus) {
      conf.set("fs.countfs.impl", org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.class.getName());
    }
    if (useStatusFailures) {
      conf.set("fs.countfs.impl", StatusFailureFileSystem.class.getName());
    }
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf),
        useStatusFailures ? new TransientRetry(2, 0) : TransientRetry.NONE)
        .withReadahead(BlobBytesSource.PREP_SOURCE_READAHEAD_BYTES);
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    PayloadDestination destination = new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable, props);
    PreprocessingIterator iterator = new PreprocessingIterator(
        rows.iterator(),
        EmbeddingTableConfig.from(props),
        "001",
        source,
        new PreprocessorRegistry(props),
        new WordCountTokenizer(),
        counters,
        null,
        destination);
    StoredRun run = new StoredRun();
    List<Row> prepared = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        prepared.add(iterator.next());
      }
      // the iterator has reported its end: every payload file it references is written
      for (Row row : prepared) {
        Row image = (Row) row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.PREPARED_IMAGE));
        byte[] bytes;
        // the prepared image is a blob value: type, inline data, reference (path first)
        if (HoodieSchema.Blob.OUT_OF_LINE.equals(image.getString(0))) {
          String path = image.getStruct(2).getString(0);
          Path file = temp.resolve("embedding").resolve(path);
          if (!Files.exists(file)) {
            run.missingAtEnd.add(path);
            continue;
          }
          bytes = Files.readAllBytes(file);
        } else {
          bytes = (byte[]) image.get(1);
        }
        run.frames.add(image.getString(0) + " " + row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.SOURCE_PATH_SHA))
            + " " + sha256(bytes));
      }
    } finally {
      iterator.close();
    }
    for (PrepSideOutput entry : iterator.getSideOutputs()) {
      run.sideOutputs.add(entry.getSourcePathSha() + " " + entry.getStage() + " " + entry.getModality() + " " + entry.getError());
    }
    run.prefetchOpens = prefetchOpens.get();
    for (AtomicInteger clipOpens : opens) {
      run.opensPerClip.add(clipOpens.get());
    }
    SourceOpenHooks.clear();
    return run;
  }

  /** Video prep only (mp4), every frame stored out of line, reading at most {@code prefetchMaxBytes} ahead. */
  private TypedProperties videoProps(long prefetchMaxBytes) {
    TypedProperties props = props(prefetchMaxBytes);
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4");
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), "0");
    // no timeline server here: the files are written without markers
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    return props;
  }

  private static byte[] resource(String name) throws IOException {
    try (InputStream in = TestPreprocessingIteratorPrefetch.class.getResourceAsStream(name)) {
      assertNotNull(in, name);
      return in.readAllBytes();
    }
  }

  private static String sha256(byte[] bytes) {
    try {
      StringBuilder hex = new StringBuilder();
      for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private Run run(TypedProperties props, List<Row> rows) {
    AtomicInteger prefetchOpens = new AtomicInteger();
    for (Row row : rows) {
      Object blob = row.getAs(BlobColumns.BLOB);
      if (blob instanceof Row && HoodieSchema.Blob.OUT_OF_LINE.equals(((Row) blob).getAs(HoodieSchema.Blob.TYPE))) {
        String path = ((Row) blob).getStruct(2).getString(0);
        SourceOpenHooks.beforeOpen(localOf(path).toAbsolutePath().toString(), () -> {
          if (Thread.currentThread().getName().startsWith(PREFETCH_THREAD)) {
            prefetchOpens.incrementAndGet();
          }
        });
      }
    }
    PreprocessingIterator iterator = iterator(props, rows);
    Run run = new Run();
    try {
      while (iterator.hasNext()) {
        run.rows.add(iterator.next().toString());
      }
    } finally {
      iterator.close();
    }
    for (PrepSideOutput entry : iterator.getSideOutputs()) {
      run.sideOutputs.add(entry.getSourcePathSha() + " " + entry.getStage() + " " + entry.getModality() + " " + entry.getError());
    }
    run.prefetchOpens = prefetchOpens.get();
    SourceOpenHooks.clear();
    return run;
  }

  @Test
  void imageInputLimitPreventsSpeculativeReads() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("limited", "png", object("limited.png", png), png.length)};
    TypedProperties properties = props(100_000L);
    properties.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "1");
    PreprocessingIterator iterator = iterator(properties, Arrays.asList(rows));
    assertEquals(1, drain(iterator).size());
    assertEquals(1, iterator.getSideOutputs().size());
    assertEquals(0, CountingObjectFileSystem.counts(path("limited.png")).opens.get());
  }

  @Test
  void pdfInputLimitPreventsSpeculativeReads() throws IOException {
    byte[] pdf = new byte[128];
    Row[] rows = {objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("limited", "pdf", object("limited.pdf", pdf), pdf.length)};
    TypedProperties properties = props(100_000L);
    properties.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "txt,pdf");
    properties.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "1");
    PreprocessingIterator iterator = iterator(properties, Arrays.asList(rows));
    assertEquals(1, drain(iterator).size());
    assertEquals(1, iterator.getSideOutputs().size());
    assertEquals(0, CountingObjectFileSystem.counts(path("limited.pdf")).opens.get());
  }

  @Test
  void completedCacheRetainsItsObservedEtag() throws Exception {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Row[] rows = {objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("changed", "png", object("changed.png", png), png.length)};
    useEtagStatus = true;
    org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "initial";
    org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.hidden = false;
    org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.unavailableEtag = false;
    PreprocessingIterator iterator = iterator(props(100_000L), Arrays.asList(rows));
    try {
      assertNotNull(iterator.next());
      java.lang.reflect.Field field = PreprocessingIterator.class.getDeclaredField("prefetcher");
      field.setAccessible(true);
      Object prefetcher = field.get(iterator);
      field = SourcePrefetcher.class.getDeclaredField("pending");
      field.setAccessible(true);
      ((java.util.concurrent.Future<?>) field.get(prefetcher)).get(5, TimeUnit.SECONDS);
      org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "replacement";
      assertFalse(iterator.hasNext());
      assertEquals(1, iterator.getSideOutputs().size());
      assertTrue(iterator.getSideOutputs().get(0).getError().contains("changed"));
    } finally {
      iterator.close();
      org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "initial";
    }
  }

  @Test
  void videoInputLimitPreventsSpeculativeReads() throws IOException {
    byte[] clip = new byte[128];
    Row[] rows = {objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("limited", "mp4", object("limited.mp4", clip), clip.length)};
    TypedProperties properties = props(100_000L);
    properties.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "txt,mp4");
    properties.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(), "1");
    PreprocessingIterator iterator = iterator(properties, Arrays.asList(rows));
    assertEquals(1, drain(iterator).size());
    assertEquals(1, iterator.getSideOutputs().size());
    assertEquals(0, CountingObjectFileSystem.counts(path("limited.mp4")).opens.get());
  }

  @Test
  void speculativePdfSourceChangeKeepsForegroundDiagnostics() throws IOException {
    byte[] pdf = new byte[128];
    Row[] rows = {objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("changed", "pdf", object("changed.pdf", pdf), pdf.length)};
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    sourceOverride = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf)) {
      @Override
      public BlobBytesSource withoutOpenRetry() {
        return this;
      }

      @Override
      public void checkVersion(Row blob, long modificationTime, long length) throws IOException {
        if (length == 128L) {
          throw new org.apache.hudi.utilities.unstructured.embedding.source.BlobSourceChangedException("source changed");
        }
        super.checkVersion(blob, modificationTime, length);
      }
    };
    TypedProperties properties = props(0);
    properties.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "txt,pdf");
    Run foreground = run(properties, Arrays.asList(rows));
    assertEquals(1, foreground.rows.size());
    assertEquals(1, foreground.sideOutputs.size());
    assertTrue(foreground.sideOutputs.get(0).contains("SOURCE_IO"));
    properties.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "100000");
    Run speculative = run(properties, Arrays.asList(rows));
    assertEquals(foreground.rows, speculative.rows);
    assertEquals(foreground.sideOutputs, speculative.sideOutputs);
  }

  @Test
  void repeatedImageHeaderUsesTheCache() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    byte[] jpeg = metadataHeavyJpeg();
    Row[] rows = {objectRow("first", "png", object("first.png", png), png.length),
        objectRow("cached", "jpg", object("cached.jpg", jpeg), jpeg.length)};
    Map<String, List<String>> threads = recordOpenThreads("first.png", "cached.jpg");
    assertEquals(2, drain(iterator(props(1024L * 1024), Arrays.asList(rows))).size());
    assertEquals(1, threads.get("cached.jpg").size());
    assertTrue(threads.get("cached.jpg").get(0).startsWith(PREFETCH_THREAD));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(longs = {0L, 100000L})
  void closedImageViewDoesNotCheckLaterReplacements(long prefetchBytes) throws Exception {
    useEtagStatus = true;
    org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "initial";
    PreprocessingIterator iterator = iterator(props(prefetchBytes), statusRows());
    try {
      assertNotNull(iterator.next());
      assertNotNull(iterator.next());
      org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "replacement";
      assertFalse(iterator.hasNext());
      assertTrue(iterator.getSideOutputs().isEmpty());
    } finally {
      iterator.close();
      org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.etag = "initial";
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(longs = {0L, 100000L})
  void presentObjectStatusFailureStillFailsTheRound(long prefetchBytes) throws Exception {
    useStatusFailures = true;
    try (PreprocessingIterator iterator = iterator(props(prefetchBytes), statusRows())) {
      assertNotNull(iterator.next());
      awaitReadAhead(iterator, prefetchBytes);
      StatusFailureFileSystem.missing = true;
      RuntimeException failure = assertThrows(RuntimeException.class, iterator::hasNext);
      assertNotNull(PrepFailures.roundFailure(failure));
      assertTrue(iterator.getSideOutputs().isEmpty());
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(longs = {0L, 100000L})
  void cachedStatusUsesForegroundRetries(long prefetchBytes) throws Exception {
    useStatusFailures = true;
    try (PreprocessingIterator iterator = iterator(props(prefetchBytes), statusRows())) {
      assertNotNull(iterator.next());
      awaitReadAhead(iterator, prefetchBytes);
      StatusFailureFileSystem.FAILURES.set(1);
      assertNotNull(iterator.next());
      assertFalse(iterator.hasNext());
      assertTrue(iterator.getSideOutputs().isEmpty());
      assertEquals(0, StatusFailureFileSystem.FAILURES.get());
    }
  }

  @Test
  void customImagePreprocessorKeepsTheSharedHeaderCutoff() throws IOException {
    byte[] bytes = new byte[2048];
    Arrays.fill(bytes, (byte) 'a');
    List<Row> rows = Arrays.asList(
        objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("custom", "img", object("custom.img", bytes), bytes.length));
    TypedProperties properties = props(100_000L);
    properties.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "txt,img");
    properties.setProperty(EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES.key(), "1024");
    properties.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "img"
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX, CustomImagePreprocessor.class.getName());
    Map<String, List<String>> threads = recordOpenThreads("first.txt", "custom.img");
    assertEquals(2, drain(iterator(properties, rows)).size());
    assertTrue(threads.get("custom.img").stream().noneMatch(name -> name.startsWith(PREFETCH_THREAD)));
  }

  public static class CustomImagePreprocessor extends TextPipePreprocessor {
    @Override
    public SourceKind sourceKind() {
      return SourceKind.IMAGE;
    }
  }

  public static class StatusFailureFileSystem
      extends org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem {
    static volatile boolean missing;
    static final AtomicInteger FAILURES = new AtomicInteger();

    @Override
    public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path file) throws IOException {
      if ("changed.png".equals(file.getName())) {
        if (missing) {
          throw new java.io.FileNotFoundException("status unavailable for a present object");
        }
        if (FAILURES.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
          throw new IOException("temporary status failure");
        }
      }
      return super.getFileStatus(file);
    }

    @Override
    public boolean exists(org.apache.hadoop.fs.Path file) throws IOException {
      return "changed.png".equals(file.getName()) || super.exists(file);
    }
  }

  private List<Row> statusRows() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    return Arrays.asList(objectRow("first", "txt", object("first.txt", "control".getBytes(StandardCharsets.UTF_8)), 7),
        objectRow("changed", "png", object("changed.png", png), png.length));
  }

  private static void awaitReadAhead(PreprocessingIterator iterator, long bytes) throws Exception {
    if (bytes == 0) {
      return;
    }
    java.lang.reflect.Field field = PreprocessingIterator.class.getDeclaredField("prefetcher");
    field.setAccessible(true);
    Object prefetcher = field.get(iterator);
    field = SourcePrefetcher.class.getDeclaredField("pending");
    field.setAccessible(true);
    ((java.util.concurrent.Future<?>) field.get(prefetcher)).get(5, TimeUnit.SECONDS);
  }

  private static List<Row> drain(PreprocessingIterator iterator) {
    List<Row> rows = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    } finally {
      iterator.close();
    }
    return rows;
  }

  private Map<String, List<String>> recordOpenThreads(String... keys) {
    Map<String, List<String>> threads = new ConcurrentHashMap<>();
    for (String key : keys) {
      List<String> names = java.util.Collections.synchronizedList(new ArrayList<>());
      threads.put(key, names);
      SourceOpenHooks.beforeOpen(local(key).toAbsolutePath().toString(), () -> names.add(Thread.currentThread().getName()));
    }
    return threads;
  }

  private PreprocessingIterator iterator(TypedProperties props, List<Row> rows) {
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    if (useEtagStatus) {
      conf.set("fs.countfs.impl", org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem.class.getName());
    }
    if (useStatusFailures) {
      conf.set("fs.countfs.impl", StatusFailureFileSystem.class.getName());
    }
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf),
        useStatusFailures ? new TransientRetry(2, 0) : TransientRetry.NONE)
        .withReadahead(BlobBytesSource.PREP_SOURCE_READAHEAD_BYTES);
    if (sourceOverride != null) {
      source = sourceOverride;
    }
    return new PreprocessingIterator(
        rows.iterator(),
        EmbeddingTableConfig.from(props),
        "001",
        source,
        new PreprocessorRegistry(props),
        new WordCountTokenizer(),
        counters,
        null);
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        TextPipePreprocessor.class.getName());
    return props;
  }

  private TypedProperties props(long prefetchMaxBytes) {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), Long.toString(prefetchMaxBytes));
    return props;
  }

  private Path local(String key) {
    return temp.resolve("buckets").resolve("bucket").resolve(key);
  }

  private Path localOf(String path) {
    return local(path.substring(path.lastIndexOf('/') + 1));
  }

  private static String path(String key) {
    return CountingObjectFileSystem.path("bucket", key);
  }

  private String object(String key, byte[] bytes) throws IOException {
    Path file = local(key);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    Files.setLastModifiedTime(file, FileTime.fromMillis(1L));
    return path(key);
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] both = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, both, first.length, second.length);
    return both;
  }

  private static Row objectRow(String sha, String extension, String path, long size) {
    return sourceRow(sha, extension, size, outOfLine(path, null, null));
  }

  private static Row rangedRow(String sha, String extension, String path, long offset, long length) {
    return sourceRow(sha, extension, offset + length, outOfLine(path, offset, length));
  }

  private static Row inlineRow(String sha, String extension, byte[] bytes) {
    return sourceRow(sha, extension, bytes.length, new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, bytes, null}, BLOB));
  }

  private static Row goneRow(String sha, String extension) {
    return sourceRow(sha, extension, 10L, null);
  }

  private static Row outOfLine(String path, Long offset, Long length) {
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {path, offset, length, false}, REFERENCE)}, BLOB);
  }

  private static Row sourceRow(String sha, String extension, long size, Row blob) {
    return new GenericRowWithSchema(new Object[] {sha, extension, size, 1L, blob, "s3://bucket/" + sha + "." + extension,
        sha + "." + extension}, SOURCE_ROW);
  }
}
