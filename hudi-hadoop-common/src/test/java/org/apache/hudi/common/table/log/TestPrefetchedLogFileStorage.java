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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PrefetchedLogFileStorage}.
 */
public class TestPrefetchedLogFileStorage {

  /**
   * Reads a multi-block log file through the prefetched storage and checks the blocks match a read through the
   * file system.
   */
  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  void testReadMultipleBlocks(HoodieLogBlockType dataBlockType, @TempDir Path tempDir)
      throws IOException, InterruptedException, URISyntaxException {
    StoragePath dir = new StoragePath(tempDir.toUri().toString());
    HoodieStorage storage = HoodieStorageUtils.getStorage(dir.toString(), HoodieTestUtils.getDefaultStorageConf());
    HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(dir)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("prefetch")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    writer.appendBlock(HoodieCommonTestHarness.getDataBlock(dataBlockType, SchemaTestUtil.generateTestRecords(0, 100), header));
    List<Pair<DeleteRecord, Long>> deletes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      deletes.add(Pair.of(DeleteRecord.create("key" + i, "partition"), (long) i));
    }
    writer.appendBlock(new HoodieDeleteBlock(deletes, header));
    writer.appendBlock(HoodieCommonTestHarness.getDataBlock(dataBlockType, SchemaTestUtil.generateTestRecords(100, 50), header));
    writer.close();

    StoragePath logPath = writer.getLogFile().getPath();
    HoodieLogFile logFile = new HoodieLogFile(logPath, storage.getPathInfo(logPath).getLength());
    List<String> expected = readBlocks(storage, logFile);
    assertEquals(3, expected.size());

    // The whole file is fetched into memory; the reader still has to find the end of the file after the last block
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(logFile), Long.MAX_VALUE)) {
      assertEquals(expected, readBlocks(prefetched, logFile));
    }
    // A fetcher reads each file whole, at its known size, into the buffer it is handed
    List<Integer> fetchedSizes = new ArrayList<>();
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(logFile), Long.MAX_VALUE, (path, buffer) -> {
          fetchedSizes.add(buffer.length);
          try (SeekableDataInputStream in = storage.openSeekable(path, 4096, false)) {
            in.readFully(buffer, 0, buffer.length);
          }
        })) {
      assertEquals(expected, readBlocks(prefetched, logFile));
    }
    assertEquals(Collections.singletonList((int) logFile.getFileSize()), fetchedSizes);
    // A file over the budget is read through the wrapped storage
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(logFile), 1)) {
      assertEquals(expected, readBlocks(prefetched, logFile));
    }
  }

  private static List<String> readBlocks(HoodieStorage storage, HoodieLogFile logFile) throws IOException {
    List<String> blocks = new ArrayList<>();
    try (HoodieLogFileReader reader = new HoodieLogFileReader(storage, logFile, getSimpleSchema(), 4096)) {
      while (reader.hasNext()) {
        blocks.add(describe(reader.next()));
      }
    }
    return blocks;
  }

  private static List<String> readBlocksInReverse(HoodieStorage storage, HoodieLogFile logFile) throws IOException {
    List<String> blocks = new ArrayList<>();
    try (HoodieLogFileReader reader = new HoodieLogFileReader(storage, logFile, getSimpleSchema(), 4096, true)) {
      while (reader.hasPrev()) {
        blocks.add(describe(reader.prev()));
      }
    } catch (Exception e) {
      blocks.add(e.getClass().getSimpleName());
    }
    return blocks;
  }

  private static String describe(HoodieLogBlock block) throws IOException {
    int count = 0;
    if (block instanceof HoodieDeleteBlock) {
      count = ((HoodieDeleteBlock) block).getRecordsToDelete().length;
    } else if (block instanceof HoodieDataBlock) {
      try (ClosableIterator<HoodieRecord<Object>> records =
               ((HoodieDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
        while (records.hasNext()) {
          records.next();
          count++;
        }
      }
    }
    return block.getBlockType() + ":" + count;
  }

  /**
   * A log file with a partially written block between valid blocks reads the same forward and in reverse from memory
   * as from the file system. The file is deleted after the first read, so the later reads can only come from memory.
   */
  @Test
  void testCorruptBlockAndReverseReadFromMemory(@TempDir Path tempDir) throws IOException, InterruptedException, URISyntaxException {
    StoragePath dir = new StoragePath(tempDir.toUri().toString());
    HoodieStorage storage = HoodieStorageUtils.getStorage(dir.toString(), HoodieTestUtils.getDefaultStorageConf());
    HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(dir)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("valid")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    writer.appendBlock(HoodieCommonTestHarness.getDataBlock(HoodieLogBlockType.AVRO_DATA_BLOCK, SchemaTestUtil.generateTestRecords(0, 100), header));
    writer.appendBlock(new HoodieDeleteBlock(Collections.singletonList(Pair.of(DeleteRecord.create("key0", "partition"), 0L)), header));
    writer.close();
    byte[] valid = readAll(storage.open(writer.getLogFile().getPath()));

    // Valid blocks, then a block whose declared size runs past its content, then the valid blocks again
    ByteArrayOutputStream composed = new ByteArrayOutputStream();
    composed.write(valid);
    DataOutputStream partial = new DataOutputStream(composed);
    partial.write(HoodieLogFormat.MAGIC);
    partial.writeLong(474);
    partial.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    partial.writeInt(HoodieLogFormat.INLINE_LOG_FORMAT_VERSION);
    partial.writeLong(400);
    partial.write(getUTF8Bytes("partial-content"));
    partial.flush();
    composed.write(valid);
    StoragePath path = new StoragePath(dir, "composed.log");
    try (OutputStream out = storage.create(path, false)) {
      out.write(composed.toByteArray());
    }
    HoodieLogFile logFile = new HoodieLogFile(path, (long) composed.size());
    List<String> forward = readBlocks(storage, logFile);
    List<String> reverse = readBlocksInReverse(storage, logFile);
    assertEquals(Arrays.asList("AVRO_DATA_BLOCK:100", "DELETE_BLOCK:1", "CORRUPT_BLOCK:0", "AVRO_DATA_BLOCK:100", "DELETE_BLOCK:1"), forward);
    assertEquals(Arrays.asList("DELETE_BLOCK:1", "AVRO_DATA_BLOCK:100", "CorruptedLogFileException"), reverse);

    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(storage, Collections.singletonList(logFile), Long.MAX_VALUE)) {
      assertEquals(forward, readBlocks(prefetched, logFile));
      assertTrue(storage.deleteFile(path));
      assertEquals(forward, readBlocks(prefetched, logFile));
      assertEquals(reverse, readBlocksInReverse(prefetched, logFile));
    }
  }

  /**
   * The fetcher is handed each file within the remaining budget at its known size, and later reads of those files
   * come from memory, whether the caller opens the qualified path (as the log reader does) or the path it listed.
   */
  @Test
  void testFetcherWithinBudget(@TempDir Path tempDir) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(tempDir.toUri().toString(), HoodieTestUtils.getDefaultStorageConf());
    // a and b exist only in the fetcher; c is over the remaining budget and exists only on disk
    StoragePath a = new StoragePath(tempDir.resolve("a.log").toString());
    StoragePath b = new StoragePath(tempDir.toUri().toString(), "b.log");
    StoragePath c = new StoragePath(tempDir.toUri().toString(), "c.log");
    byte[] onDisk = bytes(30, 7);
    try (OutputStream out = storage.create(c, false)) {
      out.write(onDisk);
    }
    List<HoodieLogFile> logFiles = Arrays.asList(
        new HoodieLogFile(a, 10L), new HoodieLogFile(b, 20L), new HoodieLogFile(c, 30L),
        new HoodieLogFile(new StoragePath(tempDir.toUri().toString(), "unknown.log"), -1L),
        new HoodieLogFile(new StoragePath(tempDir.toUri().toString(), "empty.log"), 0L));
    Map<String, Integer> fetchedSizes = new ConcurrentHashMap<>();
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(storage, logFiles, 35, (path, buffer) -> {
      fetchedSizes.put(path.getName(), buffer.length);
      System.arraycopy(bytes(buffer.length, path.getName().charAt(0)), 0, buffer, 0, buffer.length);
    })) {
      assertArrayEquals(bytes(10, 'a'), readAll(prefetched.openSeekable(FSUtils.makeQualified(storage, a), 4096, true)));
      assertArrayEquals(bytes(10, 'a'), readAll(prefetched.open(a)));
      assertArrayEquals(bytes(20, 'b'), readAll(prefetched.openSeekable(b, 4096, true)));
      assertArrayEquals(onDisk, readAll(prefetched.openSeekable(c, 4096, true)));
    }
    Map<String, Integer> expectedSizes = new HashMap<>();
    expectedSizes.put("a.log", 10);
    expectedSizes.put("b.log", 20);
    assertEquals(expectedSizes, fetchedSizes);
  }

  /**
   * A failed fetch surfaces as an IOException naming the file when the file is opened, rather than hanging the read.
   */
  @Test
  void testFailedFetch(@TempDir Path tempDir) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(tempDir.toUri().toString(), HoodieTestUtils.getDefaultStorageConf());
    StoragePath ioFailure = new StoragePath(tempDir.toUri().toString(), "io.log");
    StoragePath runtimeFailure = new StoragePath(tempDir.toUri().toString(), "runtime.log");
    IOException ioCause = new IOException("injected read failure");
    RuntimeException runtimeCause = new UncheckedIOException(new IOException("injected engine failure"));
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(storage,
        Arrays.asList(new HoodieLogFile(ioFailure, 10L), new HoodieLogFile(runtimeFailure, 10L)), Long.MAX_VALUE, (path, buffer) -> {
          if (path.equals(ioFailure)) {
            throw ioCause;
          }
          throw runtimeCause;
        })) {
      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        IOException seekable = assertThrows(IOException.class, () -> prefetched.openSeekable(ioFailure, 4096, true));
        assertTrue(seekable.getMessage().contains(ioFailure.toString()), seekable.getMessage());
        assertSame(ioCause, seekable.getCause());
        assertSame(ioCause, assertThrows(IOException.class, () -> prefetched.open(ioFailure)).getCause());
        assertSame(runtimeCause, assertThrows(IOException.class, () -> prefetched.openSeekable(runtimeFailure, 4096, true)).getCause());
      });
    }
  }

  /**
   * Seeks and reads at and past the end of a prefetched file behave as they do on the file system, which the log
   * reader relies on to find the end of the file and truncated blocks. A seek past the end throws, as on HDFS.
   */
  @Test
  void testEndOfFileMatchesFileSystem(@TempDir Path tempDir) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(tempDir.toUri().toString(), HoodieTestUtils.getDefaultStorageConf());
    StoragePath path = new StoragePath(tempDir.toUri().toString(), "eof.log");
    try (OutputStream out = storage.create(path, false)) {
      out.write(bytes(100, 3));
    }
    List<String> expected;
    try (SeekableDataInputStream in = storage.openSeekable(path, 4096, true)) {
      expected = endOfFileBehavior(in);
    }
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(new HoodieLogFile(path, 100L)), Long.MAX_VALUE);
         SeekableDataInputStream in = prefetched.openSeekable(path, 4096, true)) {
      assertEquals(expected, endOfFileBehavior(in));
      assertThrows(EOFException.class, () -> in.seek(101));
      assertThrows(EOFException.class, () -> in.seek(-1));
    }
  }

  private static List<String> endOfFileBehavior(SeekableDataInputStream in) {
    List<String> outcomes = new ArrayList<>();
    outcomes.add(outcome(() -> {
      in.seek(60);
      in.readFully(new byte[40]);
      return in.getPos();
    }));
    outcomes.add(outcome(() -> (long) in.read()));
    outcomes.add(outcome(() -> {
      in.seek(100);
      return in.getPos();
    }));
    outcomes.add(outcome(() -> (long) in.read()));
    outcomes.add(outcome(() -> {
      in.seek(90);
      in.readFully(new byte[20]);
      return in.getPos();
    }));
    outcomes.add(outcome(() -> {
      in.seek(96);
      return in.readLong();
    }));
    return outcomes;
  }

  private interface IOSupplier {
    long get() throws IOException;
  }

  private static String outcome(IOSupplier action) {
    try {
      return String.valueOf(action.get());
    } catch (IOException e) {
      return e.getClass().getSimpleName();
    }
  }

  private static byte[] bytes(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (seed + i);
    }
    return bytes;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    try (InputStream stream = in) {
      return FileIOUtils.readAsByteArray(stream);
    }
  }
}
