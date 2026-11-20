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
import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.unstructured.embedding.source.fixtures.EtagStatusFileSystem;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data prep's reads of a source object whose length the listing already has: {@link BlobBytesSource#openSeekable(Row, long)} and
 * {@link BlobBytesSource#open(Row, long)} send no status request and read exactly the bytes the length-less opens read; a source
 * {@link BlobBytesSource#withReadahead with a read-ahead} sets it on every stream it opens; bytes read ahead are served as the
 * storage blob serves them.
 */
class TestBlobBytesSourceKnownLength {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();

  @TempDir
  Path temp;

  @AfterEach
  void clear() {
    CountingObjectFileSystem.clear();
    SourceOpenHooks.clear();
  }

  /** The length-less open asks for the object's status (a HEAD on an object store); the known-length open does not. */
  @Test
  void knownLengthSendsNoStatusRequest() throws IOException {
    byte[] file = sequence(70_000);
    String path = object("whole.bin", file);
    BlobBytesSource source = source();

    try (SeekableBlob seekable = source.openSeekable(outOfLine(path, null, null))) {
      assertArrayEquals(file, readAll(seekable));
    }
    assertEquals(1, CountingObjectFileSystem.counts(path).statuses.get(), "the length-less open asks for the length");

    CountingObjectFileSystem.clear();
    try (SeekableBlob seekable = source.openSeekable(outOfLine(path, null, null), file.length)) {
      assertEquals(file.length, seekable.length());
      assertArrayEquals(file, readAll(seekable));
      seekable.seek(65_000);
      assertArrayEquals(Arrays.copyOfRange(file, 65_000, 70_000), readAll(seekable));
    }
    try (InputStream in = source.open(outOfLine(path, null, null), file.length)) {
      assertArrayEquals(file, readAll(in));
    }
    assertEquals(0, CountingObjectFileSystem.counts(path).statuses.get(), "no status request with the listed length");
    assertEquals(2, CountingObjectFileSystem.counts(path).opens.get());
  }

  /** A ranged reference reads its range: the listed length is the object's, not the range's, and is not used. */
  @Test
  void rangedReferenceReadsItsRange() throws IOException {
    byte[] file = sequence(300);
    String path = object("ranged.bin", file);
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, 10L, 100L), file.length)) {
      assertEquals(100, seekable.length());
      assertArrayEquals(Arrays.copyOfRange(file, 10, 110), readAll(seekable));
    }
    try (InputStream in = source().open(outOfLine(path, 10L, 100L), file.length)) {
      assertArrayEquals(Arrays.copyOfRange(file, 10, 110), readAll(in));
    }
    assertEquals(0, CountingObjectFileSystem.counts(path).statuses.get());
  }

  /** An INLINE blob is its data, whatever length is passed. */
  @Test
  void anInlineBlobIsItsData() throws IOException {
    byte[] data = sequence(50);
    Row inline = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, data, null}, BLOB);
    try (SeekableBlob seekable = source().openSeekable(inline, 999L)) {
      assertEquals(50, seekable.length());
      assertArrayEquals(data, readAll(seekable));
    }
  }

  /** A missing object is the file's own failure with or without a listed length. */
  @Test
  void missingObjectIsAContentFailure() {
    String path = CountingObjectFileSystem.path("bucket", "gone.bin");
    Exception seekable = assertThrows(Exception.class, () -> readSeekable(path));
    assertTrue(SourceFailures.isContentFailure(seekable), seekable.toString());
    Exception stream = assertThrows(Exception.class, () -> readStream(path));
    assertTrue(SourceFailures.isContentFailure(stream), stream.toString());
  }

  /** An object shorter than its listed length is short, as a ranged reference shorter than its range is. */
  @Test
  void anObjectShorterThanItsListedLengthIsTruncated() throws IOException {
    String path = object("short.bin", sequence(40));
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, null, null), 100L)) {
      BlobTruncatedException truncated = assertThrows(BlobTruncatedException.class, () -> readAll(seekable));
      assertTrue(SourceFailures.isContentFailure(truncated));
    }
  }

  /** A transient failure of a known-length open is retried as the length-less open's is. */
  @Test
  void transientOpenFailureIsRetried() throws IOException {
    byte[] file = sequence(64);
    String path = object("retried.bin", file);
    int[] attempts = {0};
    SourceOpenHooks.beforeOpen(local("retried.bin").toAbsolutePath().toString(), () -> {
      if (++attempts[0] == 1) {
        throw new IOException("injected 503 Slow Down");
      }
    });
    BlobBytesSource source = new BlobBytesSource(storageConf(), new TransientRetry(2, 0L));
    try (SeekableBlob seekable = source.openSeekable(outOfLine(path, null, null), file.length)) {
      assertArrayEquals(file, readAll(seekable));
    }
    assertEquals(2, attempts[0]);
  }

  /** A source with a read-ahead sets it on every stream it opens; the default source leaves the file system's own. */
  @Test
  void theReadaheadIsSetOnEveryOpenedStream() throws IOException {
    byte[] file = sequence(5_000);
    String path = object("readahead.bin", file);
    try (SeekableBlob seekable = source().openSeekable(outOfLine(path, null, null), file.length)) {
      assertArrayEquals(file, readAll(seekable));
    }
    assertEquals(-1L, CountingObjectFileSystem.counts(path).readahead.get(), "no read-ahead by default");

    BlobBytesSource prep = source().withReadahead(BlobBytesSource.PREP_SOURCE_READAHEAD_BYTES);
    try (SeekableBlob seekable = prep.openSeekable(outOfLine(path, null, null), file.length)) {
      assertArrayEquals(file, readAll(seekable), "the read-ahead changes no byte");
    }
    assertEquals(1024L * 1024, CountingObjectFileSystem.counts(path).readahead.get());
    CountingObjectFileSystem.clear();
    try (InputStream in = prep.open(outOfLine(path, 0L, 100L))) {
      assertArrayEquals(Arrays.copyOf(file, 100), readAll(in));
    }
    assertEquals(1024L * 1024, CountingObjectFileSystem.counts(path).readahead.get(), "the stream open too");
    BlobBytesSource source = source();
    assertThrows(IllegalArgumentException.class, () -> source.withReadahead(-1L));
  }

  /** A stream that takes no read-ahead (a local file) reads as before. */
  @Test
  void streamWithoutReadaheadReadsAsBefore() throws IOException {
    byte[] file = sequence(3_000);
    String path = Files.write(temp.resolve("local.bin"), file).toUri().toString();
    BlobBytesSource prep = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()))
        .withReadahead(BlobBytesSource.PREP_SOURCE_READAHEAD_BYTES);
    try (SeekableBlob seekable = prep.openSeekable(outOfLine(path, null, null), file.length)) {
      assertArrayEquals(file, readAll(seekable));
    }
  }

  /** Bytes read ahead are the storage blob's: same length, bytes, seeks and refusals, with no storage access. */
  @Test
  void bytesReadAheadAreServedAsTheStorageBlobServesThem() throws IOException {
    byte[] file = sequence(1_000);
    String path = object("ahead.bin", file);
    Row row = outOfLine(path, null, null);
    try (SeekableBlob stored = source().openSeekable(row, file.length);
         SeekableBlob ahead = source().openSeekable(row, file.clone())) {
      assertEquals(stored.length(), ahead.length());
      assertArrayEquals(readAll(stored), readAll(ahead));
      stored.seek(990);
      ahead.seek(990);
      assertEquals(stored.getPos(), ahead.getPos());
      assertArrayEquals(readAll(stored), readAll(ahead));
      assertEquals(-1, ahead.read(new byte[4], 0, 4));
      BlobTruncatedException storedRefusal = assertThrows(BlobTruncatedException.class, () -> stored.seek(1_001));
      BlobTruncatedException aheadRefusal = assertThrows(BlobTruncatedException.class, () -> ahead.seek(1_001));
      assertEquals(storedRefusal.getMessage(), aheadRefusal.getMessage());
    }
    assertEquals(1, CountingObjectFileSystem.counts(path).opens.get(), "only the storage blob opened the object");
    Row inline = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, file, null}, BLOB);
    BlobBytesSource source = source();
    assertThrows(IllegalArgumentException.class, () -> source.openSeekable(inline, file));
    Row ranged = outOfLine(path, 0L, 10L);
    assertThrows(IllegalArgumentException.class, () -> source.openSeekable(ranged, file));
    Row unnamed = outOfLine("", null, null);
    assertThrows(BlobReferenceException.class, () -> source.openSeekable(unnamed, file));
  }

  /** Only S3A is opened with a file status; any other file system, and this Hadoop without openFile, opens as before. */
  @Test
  void onlyS3aIsOpenedWithAFileStatus() throws IOException {
    assertFalse(SourceObjectOpener.isS3A(LocalFileSystem.class));
    assertFalse(SourceObjectOpener.isS3A(CountingObjectFileSystem.class));
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    String path = object("plain.bin", sequence(10));
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
    FileSystem fs = hadoopPath.getFileSystem(conf);
    assertNull(SourceObjectOpener.openWithKnownLength(fs, hadoopPath, 10L, 4096));
  }

  @Test
  void overwriteBetweenCheckedStatusAndReadIsReportedAsChanged() throws IOException {
    String path = object("changed.bin", new byte[] {1, 2, 3});
    Row blob = outOfLine(path, null, null);
    BlobBytesSource source = source();
    source.checkVersion(blob, 0L, 3L);
    long before = Files.getLastModifiedTime(local("changed.bin")).toMillis();
    Files.write(local("changed.bin"), new byte[] {4, 5, 6});
    Files.setLastModifiedTime(local("changed.bin"), java.nio.file.attribute.FileTime.fromMillis(before + 2000));
    assertThrows(BlobSourceChangedException.class, () -> {
      try (InputStream in = source.open(blob, 3L)) {
        readAll(in);
      }
    });
  }

  @Test
  void unchangedObjectAfterCheckedStatusKeepsItsBytes() throws IOException {
    String path = object("unchanged.bin", new byte[] {1, 2, 3});
    Row blob = outOfLine(path, null, null);
    BlobBytesSource source = source();
    source.checkVersion(blob, 0L, 3L);
    try (InputStream in = source.open(blob, 3L)) {
      assertArrayEquals(new byte[] {1, 2, 3}, readAll(in));
    }
  }

  public static class EtagStatus extends org.apache.hadoop.fs.FileStatus {
    private final String etag;

    public EtagStatus(org.apache.hadoop.fs.FileStatus status, String etag) {
      super(status.getLen(), status.isDirectory(), status.getReplication(), status.getBlockSize(), status.getModificationTime(),
          status.getPath());
      this.etag = etag;
    }

    public String getETag() {
      return etag;
    }
  }

  public static class EtagFileSystem extends CountingObjectFileSystem {
    static String etag = "initial";

    @Override
    public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
      return new EtagStatus(super.getFileStatus(path), etag);
    }
  }

  @Test
  void changedEtagIsDetectedEvenWhenMtimeAndSizeAreEqual() throws IOException {
    String path = object("etag.bin", new byte[] {1, 2, 3});
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    conf.set("fs.countfs.impl", EtagFileSystem.class.getName());
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf));
    Row blob = outOfLine(path, null, null);
    EtagFileSystem.etag = "initial";
    source.checkVersion(blob, 0L, 3L);
    try (InputStream unchanged = source.open(blob, 3L)) {
      assertArrayEquals(new byte[] {1, 2, 3}, readAll(unchanged));
    }
    source.checkVersion(blob, 0L, 3L);
    EtagFileSystem.etag = "replacement";
    assertThrows(BlobSourceChangedException.class, () -> {
      try (SeekableBlob changed = source.openSeekable(blob, 3L)) {
        readAll(changed);
      }
    });
  }

  @ParameterizedTest
  @CsvSource({"false,false,false", "true,false,false", "false,true,false", "true,true,false",
      "false,false,true", "true,false,true", "false,true,true", "true,true,true"})
  void standardHadoopEtagIsCheckedAfterRead(boolean seekable, boolean hidden, boolean nullVersion) throws IOException {
    String path = object("standard-etag.bin", new byte[] {1, 2, 3});
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    conf.set("fs.countfs.impl", EtagStatusFileSystem.class.getName());
    EtagStatusFileSystem.hidden = hidden;
    EtagStatusFileSystem.versionId = nullVersion ? "null" : null;
    EtagStatusFileSystem.unavailableEtag = false;
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf));
    Row blob = outOfLine(path, null, null);
    EtagStatusFileSystem.etag = "initial";
    source.checkVersion(blob, 0L, 3L);
    try (InputStream unchanged = source.open(blob, 3L)) {
      assertArrayEquals(new byte[] {1, 2, 3}, readAll(unchanged));
    }
    source.checkVersion(blob, 0L, 3L);
    EtagStatusFileSystem.etag = "replacement";
    assertThrows(BlobSourceChangedException.class, () -> {
      if (seekable) {
        try (SeekableBlob changed = source.openSeekable(blob, 3L)) {
          readAll(changed);
        }
      } else {
        try (InputStream changed = source.open(blob, 3L)) {
          readAll(changed);
        }
      }
    });
  }

  @Test
  void unavailableOptionalEtagKeepsVersionChecks() throws IOException {
    String path = object("optional-etag.bin", new byte[] {1, 2, 3});
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    conf.set("fs.countfs.impl", EtagStatusFileSystem.class.getName());
    EtagStatusFileSystem.hidden = false;
    EtagStatusFileSystem.versionId = "version-a";
    EtagStatusFileSystem.unavailableEtag = true;
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(conf));
    Row blob = outOfLine(path, null, null);
    try {
      source.checkVersion(blob, 0L, 3L);
      try (InputStream unchanged = source.open(blob, 3L)) {
        assertArrayEquals(new byte[] {1, 2, 3}, readAll(unchanged));
      }
      source.checkVersion(blob, 0L, 3L);
      EtagStatusFileSystem.versionId = "version-b";
      assertThrows(BlobSourceChangedException.class, () -> {
        try (InputStream changed = source.open(blob, 3L)) {
          readAll(changed);
        }
      });
    } finally {
      EtagStatusFileSystem.unavailableEtag = false;
      EtagStatusFileSystem.versionId = null;
    }
  }

  private BlobBytesSource source() {
    return new BlobBytesSource(storageConf());
  }

  private void readSeekable(String path) throws IOException {
    try (SeekableBlob blob = source().openSeekable(outOfLine(path, null, null), 10L)) {
      readAll(blob);
    }
  }

  private void readStream(String path) throws IOException {
    try (InputStream in = source().open(outOfLine(path, null, null), 10L)) {
      readAll(in);
    }
  }

  private org.apache.hudi.storage.StorageConfiguration<?> storageConf() {
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    return HadoopFSUtils.getStorageConf(conf);
  }

  private Path local(String key) {
    return temp.resolve("buckets").resolve("bucket").resolve(key);
  }

  private String object(String key, byte[] bytes) throws IOException {
    Path file = local(key);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    return CountingObjectFileSystem.path("bucket", key);
  }

  private static Row outOfLine(String path, Long offset, Long length) {
    Row reference = new GenericRowWithSchema(new Object[] {path, offset, length, false}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  private static byte[] sequence(int n) {
    byte[] bytes = new byte[n];
    for (int i = 0; i < n; i++) {
      bytes[i] = (byte) (i * 31 + 7);
    }
    return bytes;
  }

  private static byte[] readAll(SeekableBlob blob) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int n;
    while ((n = blob.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int n;
    while ((n = in.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }
}
