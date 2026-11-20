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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class TestBlobRowBuilder {

  @TempDir
  Path source;
  @TempDir
  Path table;

  private BlobRowBuilder builder() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "16");

    props.setProperty("hoodie.datasource.write.recordkey.field", BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
    return new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private CloudObjectMetadata writeSource(String name, byte[] bytes) throws Exception {
    Path file = source.resolve(name);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    return object(file, bytes.length);
  }

  /** The object as the selection hands it over: its URL as decoded text and the size of its event. */
  private static CloudObjectMetadata object(Path file, long size) {
    return new CloudObjectMetadata(BlobTestPaths.canonicalPath(file.toUri()), size);
  }

  private static byte[] bytes(int size, int fill) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) fill);
    return bytes;
  }

  private static Object column(Row row, String name) {
    return row.get(BlobTableSchema.schema().fieldIndex(name));
  }

  /** Every regular file under the table base path. */
  private long tableFiles() throws Exception {
    try (Stream<Path> walk = Files.walk(table)) {
      return walk.filter(Files::isRegularFile).count();
    }
  }

  @Test
  public void inlineRowCarriesItsBytesAndSourcePathSha() throws Exception {
    CloudObjectMetadata meta = writeSource("tiny.pdf", bytes(8, 7));
    Row row = builder().buildRow(meta);
    String fid = BlobFileNames.sourcePathSha(meta.getPath());
    assertEquals(fid, column(row, BlobTableSchema.SOURCE_PATH_SHA));
    Row blob = (Row) column(row, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.INLINE, blob.getString(0));
    assertArrayEquals(bytes(8, 7), (byte[]) blob.get(1));
    assertEquals(0L, tableFiles());
  }

  /** The version tag is the object's own: the status call's modification time and length, not the event's. */
  @Test
  public void rowCarriesTheStatusCallsModificationTimeAndSize() throws Exception {
    CloudObjectMetadata meta = writeSource("tiny.pdf", bytes(8, 7));
    Path file = source.resolve("tiny.pdf");
    Files.setLastModifiedTime(file, FileTime.from(Instant.parse("2026-01-02T10:00:00Z")));
    Row row = builder().buildRow(new CloudObjectMetadata(meta.getPath(), 999L));
    assertEquals(1767348000000L, column(row, BlobTableSchema.SOURCE_MODIFICATION_TIME));
    assertEquals(8L, column(row, BlobTableSchema.SOURCE_FILE_SIZE));
    assertEquals(BlobFileNames.sourcePathSha(meta.getPath()), column(row, BlobTableSchema.SOURCE_PATH_SHA));
    assertEquals("tiny.pdf", column(row, BlobTableSchema.SOURCE_FILE_NAME));
    assertEquals("pdf", column(row, BlobTableSchema.SOURCE_FILE_EXTENSION));
  }

  @Test
  public void zeroByteObjectIsInline() throws Exception {
    Row row = builder().buildRow(writeSource("empty.pdf", new byte[0]));
    Row blob = (Row) column(row, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.INLINE, blob.getString(0));
    assertEquals(0, ((byte[]) blob.get(1)).length);
  }

  @Test
  public void unreadableSourceFailsTheRound() throws Exception {
    // RawLocalFileSystem reports a file it may not open as FileNotFoundException ("Permission denied"); the file is there,
    // so it is not a gone file, and an unreadable source may hit every file: the round fails, nothing is quarantined
    CloudObjectMetadata meta = writeSource("tiny.pdf", bytes(10, 8));
    Path file = Paths.get(java.net.URI.create(meta.getPath()));
    Set<PosixFilePermission> before = Files.getPosixFilePermissions(file);
    Files.setPosixFilePermissions(file, Collections.emptySet());
    try {
      assumeFalse(Files.isReadable(file), "running as a user that reads any file (root)");
      assertThrows(HoodieIOException.class, () -> builder().buildRow(meta));
    } finally {
      Files.setPosixFilePermissions(file, before);
    }
  }

  /** The storage API must address the exact decoded key, rather than its normalized neighbour. */
  @ParameterizedTest
  @ValueSource(strings = {"a//b.pdf", "a/./b.pdf", "a/c/../b.pdf"})
  public void normalizedSourcePathsAreRefused(String relative) throws Exception {
    Files.createDirectories(source.resolve("a"));
    Files.write(source.resolve("a/b.pdf"), bytes(8, 1));
    String path = source.toUri().toString() + relative;
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> builder().buildRow(new CloudObjectMetadata(path, 8)));
    assertTrue(refused.getMessage().contains("normalized"), refused.getMessage());
  }

  /** Paths from the selector are decoded once; literal characters must not be decoded again. */
  @ParameterizedTest
  @ValueSource(strings = {"space name.pdf", "literal%2F.pdf", "plus+name.pdf", "query?name.pdf", "fragment#name.pdf", "text-λ.pdf"})
  public void decodedSourcePathsKeepTheirIdentity(String name) throws Exception {
    CloudObjectMetadata file = writeSource(name, bytes(8, 1));
    Row row = builder().buildRow(file);
    assertEquals(file.getPath(), column(row, BlobTableSchema.SOURCE_FILE_PATH));
    assertEquals(BlobFileNames.sourcePathSha(file.getPath()), column(row, BlobTableSchema.SOURCE_PATH_SHA));
    assertArrayEquals(bytes(8, 1), (byte[]) ((Row) column(row, BlobTableSchema.BLOB)).get(1));
  }

  @Test
  public void theRowCarriesNoIngestStatusColumns() throws Exception {
    Row row = builder().buildRow(writeSource("tiny.pdf", bytes(8, 7)));
    assertEquals(BlobTableSchema.schema().size(), row.size());
    for (String absent : new String[] {"mtime", "ingest_status", "ingest_error", "ingest_time", "checksum"}) {
      assertFalse(Arrays.asList(BlobTableSchema.schema().fieldNames()).contains(absent), absent);
    }
  }

  /**
   * A file above the inline threshold stays where it is: the row references the object's own URI, as listed, with managed=false,
   * no offset or length; nothing is read and nothing is written under the table.
   */
  @Test
  public void largeObjectIsAnUnmanagedReferenceToItsSource() throws Exception {
    CloudObjectMetadata meta = writeSource("large.mp4", bytes(100, 1));
    Row row = builder().buildRow(meta);
    Row blob = (Row) column(row, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    org.junit.jupiter.api.Assertions.assertNull(blob.get(1));
    assertEquals(org.apache.spark.sql.RowFactory.create(meta.getPath(), null, null, false), blob.getStruct(2));
    assertEquals(0L, tableFiles());
  }

  /** A source path without a scheme cannot be referenced: a reader would resolve it against the table. */
  @Test
  public void schemeLessSourcePathIsRefused() throws Exception {
    CloudObjectMetadata meta = writeSource("plain.mp4", bytes(100, 1));
    String schemeLess = new java.net.URI(meta.getPath()).getPath();
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> builder().buildRow(new CloudObjectMetadata(schemeLess, 100L)));
    assertTrue(refused.getMessage().contains("requires managed=true"), refused.getMessage());
    assertEquals(0L, tableFiles());
  }

  @Test
  public void currentSizeAndAllBytesWinWhenEventSizeGrewOrShrank() throws Exception {
    for (int eventSize : new int[] {4, 12}) {
      CloudObjectMetadata file = writeSource("changed" + eventSize + ".pdf", bytes(8, 9));
      Row row = builder().buildRow(new CloudObjectMetadata(file.getPath(), eventSize));
      assertEquals(8L, column(row, BlobTableSchema.SOURCE_FILE_SIZE));
      Row blob = (Row) column(row, BlobTableSchema.BLOB);
      assertEquals(HoodieSchema.Blob.INLINE, blob.getString(0));
      assertArrayEquals(bytes(8, 9), (byte[]) blob.get(1));
    }
  }

  @Test
  public void currentSizeDecidesStorageAcrossTheThreshold() throws Exception {
    CloudObjectMetadata grown = writeSource("grown.mp4", bytes(20, 4));
    Row large = builder().buildRow(new CloudObjectMetadata(grown.getPath(), 8L));
    assertEquals(20L, column(large, BlobTableSchema.SOURCE_FILE_SIZE));
    Row largeBlob = (Row) column(large, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, largeBlob.getString(0));
    assertEquals(org.apache.spark.sql.RowFactory.create(grown.getPath(), null, null, false), largeBlob.getStruct(2));
    CloudObjectMetadata shrunk = writeSource("shrunk.pdf", bytes(8, 3));
    Row small = builder().buildRow(new CloudObjectMetadata(shrunk.getPath(), 20L));
    assertEquals(8L, column(small, BlobTableSchema.SOURCE_FILE_SIZE));
    Row smallBlob = (Row) column(small, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.INLINE, smallBlob.getString(0));
    assertArrayEquals(bytes(8, 3), (byte[]) smallBlob.get(1));
  }

  /**
   * The object is overwritten with a larger one between the status call and the read: the row would be tagged with the old
   * (modification time, size) and hold the first bytes of the new object. The read fails as any other failed read does.
   */
  @Test
  public void objectOverwrittenLargerAfterTheStatusCallFailsTheRead() throws Exception {
    assertOverwriteAfterStatusFailsTheRead(14);
  }

  /** The object is overwritten with a smaller one between the status call and the read: the read comes up short and fails. */
  @Test
  public void objectOverwrittenSmallerAfterTheStatusCallFailsTheRead() throws Exception {
    assertOverwriteAfterStatusFailsTheRead(4);
  }

  @Test
  public void objectOverwrittenAtTheSameSizeAfterTheStatusCallFailsTheRead() throws Exception {
    assertOverwriteAfterStatusFailsTheRead(10);
  }

  private void assertOverwriteAfterStatusFailsTheRead(int newSize) throws Exception {
    Path file = source.resolve("overwritten.pdf");
    Files.write(file, bytes(10, 1));
    Files.setLastModifiedTime(file, FileTime.fromMillis(1000L));
    OverwriteAfterStatusFileSystem.overwriteAfterNextStatus(file, bytes(newSize, 2));
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "16");
    Configuration conf = new Configuration();
    conf.set("fs." + OverwriteAfterStatusFileSystem.SCHEME + ".impl", OverwriteAfterStatusFileSystem.class.getName());
    conf.setBoolean("fs." + OverwriteAfterStatusFileSystem.SCHEME + ".impl.disable.cache", true);
    BlobRowBuilder builder = new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(conf));
    CloudObjectMetadata meta = new CloudObjectMetadata(OverwriteAfterStatusFileSystem.SCHEME + "://" + file.toAbsolutePath().toUri().getPath(), 10L);
    assertThrows(HoodieIOException.class, () -> builder.buildRow(meta));
  }

  /** TEST-ONLY local file system that replaces a file's content right after its next status call. */
  public static class OverwriteAfterStatusFileSystem extends RawLocalFileSystem {
    static final String SCHEME = "overwritefs";
    private static final Map<String, byte[]> PENDING = new ConcurrentHashMap<>();

    static void overwriteAfterNextStatus(Path file, byte[] content) {
      PENDING.put(file.toAbsolutePath().toString(), content);
    }

    @Override
    public URI getUri() {
      return URI.create(SCHEME + ":///");
    }

    @Override
    public String getScheme() {
      return SCHEME;
    }

    @Override
    public FileStatus getFileStatus(org.apache.hadoop.fs.Path f) throws IOException {
      FileStatus status = super.getFileStatus(f);
      byte[] content = PENDING.remove(pathToFile(f).getAbsolutePath());
      if (content != null) {
        Files.write(pathToFile(f).toPath(), content);
      }
      return status;
    }
  }
}
