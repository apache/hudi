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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeader;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderReader;
import org.apache.hudi.utilities.unstructured.embedding.image.SeekableBlobImageInputStream;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BlobBytesSource#openSeekable}: random access to INLINE and OUT_OF_LINE blob bytes, restricted to the referenced range,
 * with the row checks, the open retry and the failure classification of {@link BlobBytesSource#open}.
 */
class TestBlobBytesSourceSeekable {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();

  @TempDir
  Path tempDir;

  @AfterEach
  void clearHooks() {
    SourceOpenHooks.clear();
  }

  @Test
  void inlineSeekAndRead() throws IOException {
    byte[] data = sequence(200);
    try (SeekableBlob seekable = source().openSeekable(inline(data))) {
      assertEquals(200, seekable.length());
      assertArrayEquals(data, readAll(seekable));
      seekable.seek(150);
      assertEquals(150, seekable.getPos());
      assertArrayEquals(Arrays.copyOfRange(data, 150, 200), readAll(seekable));
      seekable.seek(200);
      assertEquals(-1, seekable.read(new byte[4], 0, 4), "a seek to the end reads nothing");
      assertThrows(BlobTruncatedException.class, () -> seekable.seek(201));
      assertThrows(BlobTruncatedException.class, () -> seekable.seek(-1));
    }
  }

  @Test
  void outOfLineRangeIsExact() throws IOException {
    byte[] file = sequence(300);
    String path = write("ranged.bin", file);
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, 10L, 100L))) {
      assertEquals(100, seekable.length());
      assertEquals(0, seekable.getPos(), "positions are relative to the range's first byte");
      assertArrayEquals(Arrays.copyOfRange(file, 10, 110), readAll(seekable));
      assertEquals(-1, seekable.read(new byte[4], 0, 4), "reads stop at the range's end, not the file's");
      seekable.seek(50);
      byte[] one = new byte[1];
      assertEquals(1, seekable.read(one, 0, 1));
      assertEquals(file[60], one[0]);
      seekable.seek(0);
      assertArrayEquals(Arrays.copyOfRange(file, 10, 110), readAll(seekable));
      assertThrows(BlobTruncatedException.class, () -> seekable.seek(101));
    }
  }

  @Test
  void outOfLineWithoutARangeIsTheWholeFile() throws IOException {
    byte[] file = sequence(120);
    String path = write("whole.bin", file);
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, null, null))) {
      assertEquals(120, seekable.length(), "the object's own length");
      assertArrayEquals(file, readAll(seekable));
    }
  }

  @Test
  void truncatedOutOfLineFileFailsInsteadOfReturningAShortBlob() throws IOException {
    String path = write("short.bin", sequence(40));
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, 0L, 100L))) {
      BlobTruncatedException truncated = assertThrows(BlobTruncatedException.class, () -> readAll(seekable));
      assertTrue(SourceFailures.isContentFailure(truncated), "the file's own fault");
    }
    // an offset beyond the object's end is its shortness, at open or at the first read
    BlobTruncatedException beyond = assertThrows(BlobTruncatedException.class, () -> {
      try (SeekableBlob seekable = source().openSeekable(outOfLine(path, 80L, 10L))) {
        readAll(seekable);
      }
    });
    assertTrue(SourceFailures.isContentFailure(beyond));
  }

  @Test
  void missingFileIsAContentFailure() {
    String path = tempDir.resolve("gone.bin").toUri().toString();
    BlobBytesSource source = source();
    Row whole = outOfLine(path, null, null);
    Exception missing = assertThrows(Exception.class, () -> source.openSeekable(whole));
    assertTrue(SourceFailures.isContentFailure(missing), missing.toString());
    Row range = outOfLine(path, 0L, 4L);
    Exception missingRange = assertThrows(Exception.class, () -> source.openSeekable(range));
    assertTrue(SourceFailures.isContentFailure(missingRange), missingRange.toString());
  }

  /**
   * A row that names no readable range (null, INLINE without data, an unknown type, OUT_OF_LINE without a reference, an empty or
   * relative path, a range {@code read_blob()} refuses) is a {@link BlobReferenceException} of both readers, before any read.
   */
  @Test
  void malformedRowsAreRefusedByBothReaders() throws IOException {
    String path = write("x.bin", sequence(64));
    Row[] malformed = {
        null,
        new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, null, null}, BLOB),
        new GenericRowWithSchema(new Object[] {"ELSEWHERE", null, null}, BLOB),
        new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, null}, BLOB),
        outOfLine("", 0L, 4L),
        outOfLine("x.bin", 0L, 4L),
        outOfLine(path, -1L, 4L),
        outOfLine(path, 0L, -5L),
        outOfLine(path, 5L, null),
        outOfLine(path, null, 4L)};
    for (Row blob : malformed) {
      assertThrows(BlobReferenceException.class, () -> source().openSeekable(blob).close(), "openSeekable " + blob);
      assertThrows(BlobReferenceException.class, () -> source().open(blob).close(), "open " + blob);
    }
  }

  /** A transient failure to open is retried in the round as {@link BlobBytesSource#open} retries it; the third try reads. */
  @Test
  void transientOpenFailureIsRetried() throws IOException {
    byte[] file = sequence(50);
    Path local = bucketFile("retried.bin", file);
    AtomicInteger attempts = new AtomicInteger();
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      if (attempts.incrementAndGet() < 3) {
        throw new IOException("injected 503 Slow Down");
      }
    });
    try (SeekableBlob seekable = bucketSource(new TransientRetry(3, 0L)).openSeekable(outOfLine(bucketPath("retried.bin"), 5L, 20L))) {
      assertArrayEquals(Arrays.copyOfRange(file, 5, 25), readAll(seekable));
    }
    assertEquals(3, attempts.get());
  }

  /**
   * Retries end at the limit with the storage failure (checked, or unchecked as the storage reports it), which is no content
   * failure; an access denial is never retried.
   */
  @Test
  void exhaustedRetriesAndAccessDenialsFailWithTheStorageFailure() throws IOException {
    Path local = bucketFile("denied.bin", sequence(50));
    AtomicInteger attempts = new AtomicInteger();
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      attempts.incrementAndGet();
      throw new IOException("injected connection reset");
    });
    BlobBytesSource source = bucketSource(new TransientRetry(2, 0L));
    Row deniedObject = outOfLine(bucketPath("denied.bin"), null, null);
    Exception exhausted = assertThrows(Exception.class, () -> source.openSeekable(deniedObject));
    assertFalse(SourceFailures.isContentFailure(exhausted), exhausted.toString());
    assertEquals(2, attempts.get());

    attempts.set(0);
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      attempts.incrementAndGet();
      throw new AccessDeniedException(local.toString(), null, "injected 403");
    });
    Exception denied = assertThrows(Exception.class, () -> source.openSeekable(deniedObject));
    assertFalse(SourceFailures.isContentFailure(denied), denied.toString());
    assertFalse(BlobBytesSource.isTransient(denied));
    assertEquals(1, attempts.get(), "an access denial is not retried");

    // a FileNotFoundException of a file that is there is an access denial, not the file's absence
    attempts.set(0);
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      attempts.incrementAndGet();
      throw new FileNotFoundException("injected unreadable object");
    });
    Row deniedRange = outOfLine(bucketPath("denied.bin"), 0L, 10L);
    Exception unreadable = assertThrows(Exception.class, () -> source.openSeekable(deniedRange));
    assertFalse(SourceFailures.isContentFailure(unreadable), unreadable.toString());
    assertEquals(1, attempts.get());
  }

  @Test
  void imageIoReadsTheHeaderThroughTheSeekableStream() throws IOException {
    byte[] png = ImageFixtures.image("png", 321, 123);
    byte[] file = new byte[png.length + 7];
    System.arraycopy(png, 0, file, 7, png.length);
    String path = write("image.bin", file);
    try (SeekableBlobImageInputStream in = new SeekableBlobImageInputStream(
        source().openSeekable(outOfLine(path, 7L, (long) png.length)))) {
      ImageHeader header = ImageHeaderReader.read(in);
      assertEquals("png", header.getFormat());
      assertEquals(321, header.getWidth());
      assertEquals(123, header.getHeight());
      assertEquals(png.length, in.length());
    }
  }

  private BlobBytesSource source() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private BlobBytesSource bucketSource(TransientRetry retry) {
    Configuration conf = new Configuration();
    BucketFolderFileSystem.install(conf, tempDir.resolve("buckets"));
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(conf), retry);
  }

  private Path bucketFile(String key, byte[] bytes) throws IOException {
    Path file = BucketFolderFileSystem.file(tempDir.resolve("buckets"), "bucket", key);
    Files.createDirectories(file.getParent());
    return Files.write(file, bytes);
  }

  private static String bucketPath(String key) {
    return BucketFolderFileSystem.SCHEME + "://bucket/" + key;
  }

  private String write(String name, byte[] bytes) throws IOException {
    return Files.write(tempDir.resolve(name), bytes).toUri().toString();
  }

  private static Row inline(byte[] data) {
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, data, null}, BLOB);
  }

  private static Row outOfLine(String path, Long offset, Long length) {
    Row reference = new GenericRowWithSchema(new Object[] {path, offset, length, false}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  private static byte[] sequence(int size) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i * 7 + 3);
    }
    return bytes;
  }

  private static byte[] readAll(SeekableBlob seekable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[16];
    int n;
    while ((n = seekable.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }
}
