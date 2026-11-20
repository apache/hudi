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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.sources.blob.BlobInlineCeiling;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriterTestExtension;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests of {@link PreparedInputs}: the {@code prepared_image} value of a payload, INLINE up to the threshold and
 * OUT_OF_LINE above it, the payload written to its {@link PayloadDestination} at the final reference
 * {@code .hoodie/.blob/[<partition>/]<fileId>_<writeToken>_<instantTime>.bin} and the inline field null; the input rules, the
 * size check and the per-task ceiling; and that row preparation writes nothing but those payload files (the write itself:
 * {@link TestPayloadWriter}).
 */
@ExtendWith(OutOfLineWriterTestExtension.class)
public class TestPreparedInputs {

  private static final String SHA = "0123456789abcdef0123456789abcdef01234567";
  private static final String JPEG = ImageContentTypes.JPEG;
  private static final String PNG = ImageContentTypes.PNG;
  private static final long THRESHOLD = 16;
  private static final String INSTANT = "20261018120000000";

  @TempDir
  Path tempDir;

  /** Fixtures returned by inputs() live until teardown; task-local try scopes retain their own ownership. */
  private final List<PreparedInputs> ownedInputs = new ArrayList<>();

  @AfterEach
  void unsetTaskContext() {
    try {
      ownedInputs.forEach(PreparedInputs::close);
    } finally {
      ownedInputs.clear();
      TaskContext$.MODULE$.unset();
    }
  }

  @Test
  void fixtureCleanupStopsItsWriteWorkersAfterAwaitWrites() throws Exception {
    Set<Thread> before = Thread.getAllStackTraces().keySet();
    PreparedInputs inputs = inputs(THRESHOLD);
    try {
      prepare(inputs, row(99), "p", JPEG, bytes((int) THRESHOLD + 1, 1));
      List<Thread> workers = Thread.getAllStackTraces().keySet().stream()
          .filter(thread -> !before.contains(thread) && thread.getName().startsWith("hudi-out-of-line-write-"))
          .collect(Collectors.toList());
      assertFalse(workers.isEmpty(), "the completed write leaves workers owned by this fixture");
      unsetTaskContext();
      for (Thread worker : workers) {
        worker.join(1000);
        assertFalse(worker.isAlive(), "fixture cleanup left " + worker.getName());
      }
    } finally {
      inputs.close();
    }
  }

  @Test
  void explicitCloseStopsItsWriteWorkersAfterAwaitWrites() throws Exception {
    Set<Thread> before = Thread.getAllStackTraces().keySet();
    PreparedInputs inputs = inputs(THRESHOLD);
    try {
      prepare(inputs, row(99), "p", JPEG, bytes((int) THRESHOLD + 1, 1));
      List<Thread> workers = Thread.getAllStackTraces().keySet().stream()
          .filter(thread -> !before.contains(thread) && thread.getName().startsWith("hudi-out-of-line-write-"))
          .collect(Collectors.toList());
      assertFalse(workers.isEmpty(), "the completed write leaves workers owned by this fixture");
      inputs.close();
      for (Thread worker : workers) {
        worker.join(1000);
        assertFalse(worker.isAlive(), "fixture cleanup left " + worker.getName());
      }
    } finally {
      inputs.close();
    }
  }

  // ---- (1) inline vs out of line at the threshold and at the ceiling ----

  @Test
  void inlineAtThresholdOutOfLineAboveWithTheBytesInThePayloadFile() throws IOException {
    PreparedInputs inputs = inputs(THRESHOLD);
    byte[] atThreshold = bytes(16, 1);
    Row inline = prepare(inputs, row(0), "p1", JPEG, atThreshold);
    assertEquals(HoodieSchema.Blob.INLINE, inline.getAs(HoodieSchema.Blob.TYPE));
    assertArrayEquals(atThreshold, inline.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    assertNull(inline.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE));

    byte[] above = bytes(17, 2);
    Row outOfLine = prepare(inputs, row(1), "p1", JPEG, above);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, outOfLine.getAs(HoodieSchema.Blob.TYPE));
    // the payload is written as the row is prepared; the row holds the reference only
    assertNull(outOfLine.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    assertArrayEquals(above, stored(outOfLine));
    Row reference = outOfLine.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    assertOpaquePath(reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH), "p1");
    assertEquals(0L, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET));
    assertEquals(17L, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH));
    assertTrue((Boolean) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED));
    assertEquals(PreparedInputs.BLOB_STRUCT, outOfLine.schema());
    assertEquals(PreparedInputs.REFERENCE_STRUCT, reference.schema());
  }

  @Test
  void ceilingAcceptsThresholdAtCeilingAndRefusesAbove() {
    SparkConf conf = new SparkConf().set("spark.master", "local[2]").set("spark.testing.reservedMemory", "0");
    BlobInlineCeiling ceiling = BlobInlineCeiling.onDriver(conf, EmbeddingInputConfig.DEFAULT_INLINE_MAX_HEAP_FRACTION);
    long atCeiling = ceiling.ceilingBytes();
    assertTrue(atCeiling > 0, "a local[2] JVM has a positive ceiling");
    assertDoesNotThrow(() -> EmbeddingInputConfig.checkCeiling(atCeiling, ceiling, "in the test"));
    HoodieValidationException refused = assertThrows(HoodieValidationException.class,
        () -> EmbeddingInputConfig.checkCeiling(atCeiling + 1, ceiling, "in the test"));
    assertTrue(refused.getMessage().contains(EmbeddingInputConfig.INLINE_MAX_BYTES.key() + "=" + (atCeiling + 1)), refused.getMessage());
    assertTrue(refused.getMessage().contains(String.valueOf(atCeiling)), refused.getMessage());
    assertFalse(refused.getMessage().contains("hoodie.blob.table."), "names the embedding keys only: " + refused.getMessage());

    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(atCeiling));
    assertDoesNotThrow(() -> EmbeddingInputConfig.checkOnDriver(props, conf));
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(atCeiling + 1));
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      assertThrows(HoodieValidationException.class, () -> EmbeddingInputConfig.checkOnDriver(props, conf));
    } else {
      // without the per-column statistics switch the ceiling checks the value used, capped to 81,920
      assertTrue(atCeiling > ParquetInlineCap.MAX_BYTES, "a local[2] JVM's ceiling is above the cap: " + atCeiling);
      assertDoesNotThrow(() -> EmbeddingInputConfig.checkOnDriver(props, conf));
    }
    // a larger fraction raises the ceiling: the key is read from the props, never clamped
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), String.valueOf(2.0 / 64));
    assertDoesNotThrow(() -> EmbeddingInputConfig.checkOnDriver(props, conf));
  }

  @Test
  void configDefaultsAndRefusals() {
    TypedProperties props = new TypedProperties();
    // 1 MiB, or 80 KiB (the most used there) on a Parquet without per-column statistics
    assertEquals(HoodieBaseParquetWriter.supportsColumnStatisticsSwitch() ? 1024L * 1024L : 80L * 1024, EmbeddingInputConfig.inlineMaxBytes(props));
    assertEquals(1.0 / 64, EmbeddingInputConfig.inlineMaxHeapFraction(props));
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), "-1");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingInputConfig.inlineMaxBytes(props));
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(Integer.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> EmbeddingInputConfig.inlineMaxBytes(props));
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), "0");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingInputConfig.inlineMaxHeapFraction(props));
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), "1.5");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingInputConfig.inlineMaxHeapFraction(props));
  }

  /**
   * Inside a Spark task the threshold is checked against the task's own ceiling before the first payload, and refused, never
   * clamped; the payload is not even opened. Outside a task the same threshold is not checked here (the driver checks it at job
   * start, {@link EmbeddingInputConfig#checkOnDriver}).
   */
  @Test
  void prepareInATaskRefusesAThresholdAboveTheTaskCeiling() {
    String huge = String.valueOf(Integer.MAX_VALUE - 8);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), huge);
    int[] opens = {0};
    BlobBytesSupplier payload = () -> {
      opens[0]++;
      return new ByteArrayInputStream(bytes(64, 3));
    };
    // outside a task: no task ceiling to check, the 64 bytes are inline under the huge threshold
    assertEquals(HoodieSchema.Blob.INLINE, own(new PreparedInputs(props, INSTANT, destination())).prepare(row(0), null, JPEG, payload, 64)
        .getAs(HoodieSchema.Blob.TYPE));
    assertEquals(1, opens[0]);

    SparkEnv priorEnv = SparkEnv.get();
    SparkEnv env = mock(SparkEnv.class);
    // no reserve, so the task ceiling is a positive share of this JVM's heap whatever its size
    when(env.conf()).thenReturn(new SparkConf(false).set("spark.master", "local[4]").set("spark.testing.reservedMemory", "0"));
    SparkEnv.set(env);
    TaskContext$.MODULE$.setTaskContext(mock(TaskContext.class));
    try (PreparedInputs inTask = new PreparedInputs(props, INSTANT, destination())) {
      if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
        String fileId = row(0);
        HoodieValidationException refused = assertThrows(HoodieValidationException.class,
            () -> inTask.prepare(fileId, null, JPEG, payload, 64));
        assertTrue(refused.getMessage().contains(EmbeddingInputConfig.INLINE_MAX_BYTES.key() + "=" + huge
            + " is above the per-task inline ceiling"), refused.getMessage());
        assertTrue(refused.getMessage().contains("in the task before its first stored input"), refused.getMessage());
        assertEquals(1, opens[0], "the refused payload is never read");
      } else {
        // without the per-column statistics switch the huge threshold is capped to 81,920, under the task ceiling
        assertEquals(HoodieSchema.Blob.INLINE, inTask.prepare(row(0), null, JPEG, payload, 64).getAs(HoodieSchema.Blob.TYPE));
        assertEquals(2, opens[0]);
      }
      // a threshold under the task ceiling passes the same check
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, inputs(THRESHOLD).prepare(row(0), null, JPEG, payload, 64)
          .getAs(HoodieSchema.Blob.TYPE));
    } finally {
      TaskContext$.MODULE$.unset();
      SparkEnv.set(priorEnv);
    }
  }

  // ---- (2) path shape, uniqueness and the name rule ----

  @Test
  void pathShapeIsAManagedRelativeReferenceAndNamesAreUnique() {
    byte[] content = bytes(40, 3);
    PreparedInputs inputs = inputs(THRESHOLD);
    Set<String> paths = new HashSet<>();
    List<String> partitions = Arrays.asList("a", "b", null);
    for (String partition : partitions) {
      for (long chunkId : new long[] {0, 1, 10, 1_000_000_000_000L - 1}) {
        Row blob = prepare(inputs, row(chunkId), partition, JPEG, content);
        Row reference = blob.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
        String path = reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
        assertOpaquePath(path, partition);
        assertEquals(0L, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET));
        assertEquals((long) content.length, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH));
        assertTrue((Boolean) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED));
        assertTrue(paths.add(path), "every payload gets a fresh name: " + path);
      }
    }
    assertEquals(12, paths.size());
  }

  @Test
  void refusesInputsOutsideTheNameRule() {
    PreparedInputs inputs = inputs(THRESHOLD);
    byte[] content = bytes(4, 4);
    String fileId = row(0);
    // Row identity never constrains the independently generated managed file name.
    assertDoesNotThrow(() -> prepare(inputs, "opaque identity/with separators", null, JPEG, content));
    assertThrows(IllegalArgumentException.class, () -> prepare(inputs, fileId, null, "image/webp", content));
    assertThrows(IllegalArgumentException.class, () -> prepare(inputs, fileId, null, null, content));
    assertThrows(IllegalArgumentException.class, () -> inputs.prepare(fileId, null, JPEG, null, 4));
    assertThrows(IllegalArgumentException.class, () -> inputs.prepare(fileId, null, JPEG, () -> new ByteArrayInputStream(content), -1));
    for (String partition : new String[] {"../x", "a/../b", "/2026", "2026/", "a//b", "./a", "a/."}) {
      byte[] outOfLine = bytes(32, 4);
      assertThrows(IllegalArgumentException.class, () -> prepare(inputs, fileId, partition, JPEG, outOfLine), partition);
      // the rule applies to every row, inline or not
      byte[] inline = bytes(8, 4);
      assertThrows(IllegalArgumentException.class, () -> prepare(inputs, fileId, partition, JPEG, inline), "inline " + partition);
    }
    for (String partition : new String[] {"2026/10/14", "p", "dt=2026-10-14/region=us"}) {
      Row blob = prepare(inputs, fileId, partition, JPEG, bytes(32, 4));
      assertOpaquePath(((Row) blob.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE)).getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH), partition);
    }
  }

  /**
   * The five image types a standalone image is stored in as its source bytes each prepare inline and out of line; an
   * out-of-line reference is named with the type's extension and its payload file holds the exact bytes.
   */
  @Test
  void preparesEveryImageTypeInlineAndOutOfLine() throws IOException {
    Map<String, String> extensions = new LinkedHashMap<>();
    extensions.put("image/jpeg", "jpg");
    extensions.put("image/png", "png");
    extensions.put("image/gif", "gif");
    extensions.put("image/bmp", "bmp");
    extensions.put("image/tiff", "tif");
    assertEquals(new ArrayList<>(extensions.keySet()), new ArrayList<>(ImageContentTypes.stored()));
    PreparedInputs inputs = inputs(THRESHOLD);
    int seed = 0;
    for (Map.Entry<String, String> type : extensions.entrySet()) {
      byte[] small = bytes((int) THRESHOLD, seed++);
      Row inline = prepare(inputs, row(seed), null, type.getKey(), small);
      assertEquals(HoodieSchema.Blob.INLINE, inline.getAs(HoodieSchema.Blob.TYPE), type.getKey());
      assertArrayEquals(small, inline.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD), type.getKey());
      byte[] large = bytes((int) THRESHOLD + 1, seed++);
      Row outOfLine = prepare(inputs, row(seed), null, type.getKey(), large);
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, outOfLine.getAs(HoodieSchema.Blob.TYPE), type.getKey());
      String path = ((Row) outOfLine.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE))
          .<String>getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
      assertOpaquePath(path, null);
      assertNull(outOfLine.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD), type.getKey());
      assertArrayEquals(large, stored(outOfLine), type.getKey());
    }
  }

  // ---- (3) the payload read ----

  /** The payload is opened once per preparation; a replay writes the same bytes to a fresh file. */
  @Test
  void readsThePayloadOnceAndAllocatesFreshStorage() throws IOException {
    byte[] content = bytes(300_000, 17);
    int[] opens = {0};
    BlobBytesSupplier payload = () -> {
      opens[0]++;
      return new ByteArrayInputStream(content);
    };
    PreparedInputs inputs = inputs(THRESHOLD);
    Row first = inputs.prepare(row(11), "p", JPEG, payload, content.length);
    assertEquals(1, opens[0]);
    Row replay = inputs.prepare(row(11), "p", JPEG, payload, content.length);
    assertEquals(2, opens[0], "one open per prepare");
    assertEquals(first.getString(0), replay.getString(0));
    assertNotEquals(first.getStruct(2).getString(0), replay.getStruct(2).getString(0));
    inputs.awaitWrites();
    assertArrayEquals(content, stored(replay));
    assertArrayEquals(content, stored(first));
  }

  /**
   * A payload read that fails part way (a reset connection) is an interruption ({@link HoodieIOException}) and leaves nothing
   * behind; preparing the same payload afterwards preserves the bytes of a clean prepare.
   */
  @Test
  void readFailingPartWayIsAnInterruptionAndItsRetryPreservesBytes() throws IOException {
    byte[] content = bytes(300_000, 19);
    PreparedInputs inputs = inputs(THRESHOLD);
    Row clean = inputs.prepare(row(13), "p", JPEG, () -> new ByteArrayInputStream(content), content.length);
    String fileId = row(13);
    HoodieIOException interrupted = assertThrows(HoodieIOException.class, () -> inputs.prepare(fileId, "p", JPEG,
        () -> new ResetAfter(new ByteArrayInputStream(content), 100_000), content.length));
    assertTrue(interrupted.getCause() instanceof IOException, String.valueOf(interrupted.getCause()));
    Row retry = inputs.prepare(row(13), "p", JPEG, () -> new ByteArrayInputStream(content), content.length);
    assertNotEquals(clean.getStruct(2).getString(0), retry.getStruct(2).getString(0));
    inputs.awaitWrites();
    assertArrayEquals(content, stored(retry));
  }

  /** A payload that reads back shorter or longer than its declared size is refused (a content failure of its file). */
  @Test
  void refusesAPayloadOfAnotherSize() {
    byte[] content = bytes(1000, 18);
    PreparedInputs inputs = inputs(THRESHOLD);
    for (long declared : new long[] {content.length - 1, content.length + 1}) {
      String fileId = row(12);
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
          () -> inputs.prepare(fileId, "p", JPEG, () -> new ByteArrayInputStream(content), declared));
      assertTrue(refused.getMessage().contains("declares " + declared), refused.getMessage());
    }
    // inline too
    String fileId = row(12);
    IllegalArgumentException inline = assertThrows(IllegalArgumentException.class,
        () -> inputs.prepare(fileId, "p", JPEG, () -> new ByteArrayInputStream(bytes(8, 1)), 9));
    assertTrue(inline.getMessage().contains("declares 9"), inline.getMessage());
  }

  /**
   * A payload that runs past its declared length (here 10 MiB more) is refused after reading one byte past the length:
   * the read is bounded by the declared size, never by the stream.
   */
  @Test
  void longerPayloadStopsOneBytePastItsLength() {
    int length = 4 * 64 * 1024;
    long[] served = {0};
    BlobBytesSupplier growing = () -> new InputStream() {
      @Override
      public int read() {
        return served[0]++ < length + 10L * 1024 * 1024 ? 7 : -1;
      }

      @Override
      public int read(byte[] buffer, int offset, int len) {
        long left = length + 10L * 1024 * 1024 - served[0];
        if (left <= 0) {
          return -1;
        }
        int n = (int) Math.min(len, left);
        Arrays.fill(buffer, offset, offset + n, (byte) 7);
        served[0] += n;
        return n;
      }
    };
    PreparedInputs inputs = inputs(THRESHOLD);
    String fileId = row(16);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> inputs.prepare(fileId, "p", JPEG, growing, length));
    assertTrue(refused.getMessage().contains("more than " + length), refused.getMessage());
    assertEquals(length + 1L, served[0], "the read stopped one byte past the length");
  }

  /**
   * A payload above the inline threshold is streamed to its file through the writer's bounded buffer, never read
   * whole into one array; a short or long payload and a failing read keep their classification on that path and leave no file.
   */
  @Test
  void outOfLinePayloadIsStreamedThroughABoundedBuffer() throws IOException {
    // above what is read whole and written in the background: streamed
    int length = OutOfLineWritePool.MAX_PENDING_BYTES + 5 * PayloadWriter.COPY_BUFFER_BYTES + 3;
    byte[] content = bytes(length, 23);
    int[] largestRead = {0};
    BlobBytesSupplier spied = () -> new FilterInputStream(new ByteArrayInputStream(content)) {
      @Override
      public int read(byte[] buffer, int offset, int len) throws IOException {
        largestRead[0] = Math.max(largestRead[0], len);
        return super.read(buffer, offset, len);
      }
    };
    PreparedInputs inputs = inputs(THRESHOLD);
    Row outOfLine = inputs.prepare(row(17), "p", JPEG, spied, length);
    assertArrayEquals(content, stored(outOfLine));
    assertTrue(largestRead[0] <= PayloadWriter.COPY_BUFFER_BYTES,
        "one read asked for " + largestRead[0] + " bytes: the payload was buffered whole");
    List<String> before = payloadFiles();
    for (long declared : new long[] {length - 1, length + 1}) {
      String fileId = row(17);
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
          () -> inputs.prepare(fileId, "p", JPEG, () -> new ByteArrayInputStream(content), declared));
      assertTrue(refused.getMessage().contains("declares " + declared), refused.getMessage());
    }
    String fileId = row(17);
    HoodieIOException interrupted = assertThrows(HoodieIOException.class, () -> inputs.prepare(fileId, "p", JPEG,
        () -> new ResetAfter(new ByteArrayInputStream(content), 2 * PayloadWriter.COPY_BUFFER_BYTES), length));
    assertTrue(interrupted.getCause() instanceof IOException, String.valueOf(interrupted.getCause()));
    assertEquals(before, payloadFiles(), "a payload that fails to read leaves no file");
  }

  // ---- (4) storage, executors ----

  /**
   * Row preparation writes nothing but the payload files of its out-of-line values: an inline value creates no file,
   * an out-of-line one exactly its referenced file under the destination's base, and nothing relative to the working directory.
   */
  @Test
  void prepareWritesOnlyTheOutOfLinePayloads() throws IOException {
    PreparedInputs inputs = inputs(THRESHOLD);
    List<String> references = new ArrayList<>();
    for (String partition : Arrays.asList(null, "p")) {
      prepare(inputs, row(0), partition, JPEG, bytes(8, 1));
      Row outOfLine = prepare(inputs, row(1), partition, PNG, bytes(400, 2));
      references.add(((Row) outOfLine.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE))
          .<String>getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH));
    }
    for (String reference : references) {
      assertFalse(Files.exists(Paths.get(reference)), "nothing relative to the working directory: " + reference);
    }
    references.sort(null);
    assertEquals(references, payloadFiles(), "the out-of-line payload files only");
  }

  @Test
  void preparedInputsIsSerializableForExecutors() throws Exception {
    PreparedInputs inputs = inputs(THRESHOLD);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(inputs);
    }
    PreparedInputs copy;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      copy = own((PreparedInputs) in.readObject());
    }
    byte[] small = bytes((int) THRESHOLD, 11);
    byte[] large = bytes(100, 11);
    // the threshold travels with the copy
    assertEquals(HoodieSchema.Blob.INLINE, prepare(copy, row(2), null, JPEG, small).getAs(HoodieSchema.Blob.TYPE));
    Row outOfLine = prepare(copy, row(3), null, JPEG, large);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, outOfLine.getAs(HoodieSchema.Blob.TYPE));
    assertArrayEquals(large, stored(outOfLine));
  }

  private static void assertOpaquePath(String path, String partition) {
    String directory = ".hoodie/.blob/" + (partition == null || partition.isEmpty() ? "" : partition + "/");
    assertTrue(path.startsWith(directory), path);
    String name = path.substring(directory.length());
    assertTrue(name.matches("[0-9a-f-]{36}-0_\\d+-\\d+-\\d+_\\d+\\.bin"), path);
  }

  /** A stream whose reads fail like a reset connection once {@code limit} bytes were read. */
  private static class ResetAfter extends FilterInputStream {
    private final long limit;
    private long read;

    ResetAfter(InputStream in, long limit) {
      super(in);
      this.limit = limit;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (read >= limit) {
        throw new IOException("Connection reset");
      }
      int n = super.read(buffer, offset, (int) Math.min(len, limit - read));
      read += Math.max(n, 0);
      return n;
    }
  }

  /** A row's name as prep gives it: its record key fields. */
  private static String row(long chunkId) {
    return "source_path_sha=" + SHA + " source_modification_time=1761868800000 source_file_size=4096 modality=image chunk_id="
        + chunkId;
  }

  private PreparedInputs own(PreparedInputs inputs) {
    ownedInputs.add(inputs);
    return inputs;
  }

  private PreparedInputs inputs(long threshold) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(threshold));
    return own(new PreparedInputs(props, INSTANT, destination()));
  }

  /** The payloads' destination: an embedding table base under the test folder. */
  private PayloadDestination destination() {
    return new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), base().toUri().toString(),
        new TypedProperties());
  }

  private Path base() {
    return tempDir.resolve("embedding");
  }

  /** The bytes of the payload file an out-of-line value references. */
  private byte[] stored(Row outOfLine) throws IOException {
    String path = ((Row) outOfLine.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE)).getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
    return Files.readAllBytes(base().resolve(path));
  }

  /** Prepares {@code content} and waits for its payload file, if any, to be written. */
  private static Row prepare(PreparedInputs inputs, String fileId, String partition, String contentType, byte[] content) {
    Row prepared = inputs.prepare(fileId, partition, contentType, () -> new ByteArrayInputStream(content), content.length);
    inputs.awaitWrites();
    return prepared;
  }

  /** Every regular file under the test folder, relative to the destination's base, the local file system's checksums left out. */
  private List<String> payloadFiles() throws IOException {
    try (Stream<Path> paths = Files.walk(tempDir)) {
      return paths.filter(Files::isRegularFile).filter(path -> !path.getFileName().toString().endsWith(".crc"))
          .map(path -> base().relativize(path).toString()).sorted().collect(Collectors.toList());
    }
  }

  private static byte[] bytes(int length, int seed) {
    byte[] out = new byte[length];
    for (int i = 0; i < length; i++) {
      out[i] = (byte) (seed * 31 + i);
    }
    return out;
  }
}
