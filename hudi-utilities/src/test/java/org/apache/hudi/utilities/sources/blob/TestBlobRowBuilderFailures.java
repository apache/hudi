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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.unstructured.SourceCauses;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Missing and unreadable sources in the row builder, which stats before it reads and keys rows by source path. */
class TestBlobRowBuilderFailures {

  @TempDir
  Path temp;

  private Configuration conf;

  @BeforeEach
  void setUp() {
    conf = new Configuration();
    BlobUnhappyFileSystem.install(conf, temp.resolve("buckets"), "healthy");
  }

  private BlobRowBuilder builder() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "16");
    props.setProperty("hoodie.datasource.write.recordkey.field", BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
    return new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(conf));
  }

  private String put(int size) throws Exception {
    Path source = BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", "docs/a.pdf");
    Files.createDirectories(source.getParent());
    Files.write(source, bytes(size));
    return "bucketfs://bucket/docs/a.pdf";
  }

  private static byte[] bytes(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 7);
    return bytes;
  }

  private static CloudObjectMetadata object(String path, long size) {
    return new CloudObjectMetadata(path, size);
  }

  private static Object column(Row row, String name) {
    return row.get(BlobTableSchema.schema().fieldIndex(name));
  }

  /** The row of an object that is gone: the identity columns, the version tag the builder could observe, and no blob. */
  private void assertNullBlobIdentity(Row row, String source, long size, long modificationTime) {
    String sha = BlobFileNames.sourcePathSha(source);
    assertEquals(BlobTableSchema.schema().size(), row.size());
    assertEquals(sha, column(row, BlobTableSchema.SOURCE_PATH_SHA));
    assertEquals(source, column(row, BlobTableSchema.SOURCE_FILE_PATH));
    assertEquals(size, column(row, BlobTableSchema.SOURCE_FILE_SIZE));
    assertEquals(modificationTime, column(row, BlobTableSchema.SOURCE_MODIFICATION_TIME));
    assertNull(column(row, BlobTableSchema.BLOB));
    for (String removed : new String[] {"source_path", "checksum", "ingest_time"}) {
      assertFalse(Arrays.asList(BlobTableSchema.schema().fieldNames()).contains(removed), removed);
    }
  }

  @Test
  void confirmedMissingAtInitialStatProducesNullBlobWithPathIdentity() {
    String source = "bucketfs://bucket/docs/gone.pdf";
    Row row = builder().buildRow(object(source, 10L));
    // nothing was observed: the announced size and no modification time
    assertNullBlobIdentity(row, source, 10L, 0L);
    assertEquals(0, BlobUnhappyFileSystem.OPENS.get(), "stat established absence before any open");
  }

  @Test
  void confirmedMissingAfterSuccessfulStatProducesNullBlobWithPathIdentity() throws Exception {
    String source = put(8);
    long observed = Files.getLastModifiedTime(BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", "docs/a.pdf")).toMillis();
    conf.set(BlobUnhappyFileSystem.MODE, "open-missing");
    // the status call succeeded: the row keeps the version it observed, as the error record will name it
    Row row = builder().buildRow(object(source, 999L));
    assertNullBlobIdentity(row, source, 8L, observed);
    assertEquals(1, BlobUnhappyFileSystem.OPENS.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"status-denied", "status-unknown", "open-denied", "open-unknown", "access-denied"})
  void presentOrUnconfirmedAbsenceFailsTheRoundInsteadOfProducingNullBlob(String mode) throws Exception {
    String source = put(8);
    conf.set(BlobUnhappyFileSystem.MODE, mode);
    BlobRowBuilder builder = builder();
    CloudObjectMetadata object = object(source, 8L);
    assertThrows(HoodieException.class, () -> builder.buildRow(object));
    Path file = BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", "docs/a.pdf");
    assertArrayEquals(bytes(8), Files.readAllBytes(file), "a platform failure must not mutate source bytes");
  }

  /** A missing-file wrapper is no proof of absence when it contains an access denial, even if exists answers false. */
  @ParameterizedTest
  @ValueSource(strings = {"status", "open"})
  void nestedDenialWithFalseExistenceRecheckFailsTheRound(String stage) throws Exception {
    Path file = BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", NestedDenialFileSystem.DENIED_FILE);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes(8));
    NestedDenialFileSystem.install(conf, temp.resolve("buckets"), stage);
    String source = "bucketfs://bucket/" + NestedDenialFileSystem.DENIED_FILE;

    HoodieException failure = assertThrows(HoodieException.class, () -> builder().buildRow(object(source, 8L)));

    assertTrue(SourceCauses.isCausedBy(failure, AccessDeniedException.class), "the denial remains available to diagnose the failed round");
    assertArrayEquals(bytes(8), Files.readAllBytes(file), "a platform failure must not mutate source bytes");
  }

  @Test
  void realPresentUnreadableSourceFailsTheRound() throws Exception {
    Path file = temp.resolve("unreadable.pdf");
    Files.write(file, bytes(8));
    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
    Files.setPosixFilePermissions(file, Collections.emptySet());
    try {
      assumeFalse(Files.isReadable(file), "root can read files with no permission bits");
      BlobRowBuilder builder = builder();
      CloudObjectMetadata object = object(file.toUri().toString(), 8L);
      assertThrows(HoodieException.class, () -> builder.buildRow(object));
    } finally {
      Files.setPosixFilePermissions(file, permissions);
    }
  }

  /** The current object's bytes are inlined whatever size its event announced. */
  @ParameterizedTest
  @ValueSource(ints = {4, 12, 24})
  void currentInlineBytesWinRegardlessOfEventSize(int eventSize) throws Exception {
    String source = put(8);
    Row row = builder().buildRow(object(source, eventSize));
    assertEquals(8L, column(row, BlobTableSchema.SOURCE_FILE_SIZE));
    assertEquals(BlobFileNames.sourcePathSha(source), column(row, BlobTableSchema.SOURCE_PATH_SHA));
    Row blob = (Row) column(row, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.INLINE, blob.getString(0));
    assertArrayEquals(bytes(8), (byte[]) blob.get(1));
  }

  @Test
  void currentLargeObjectIsAnUnmanagedReferenceAtItsCurrentSize() throws Exception {
    String source = put(24);
    Row row = builder().buildRow(object(source, 8L));
    assertEquals(24L, column(row, BlobTableSchema.SOURCE_FILE_SIZE));
    Row blob = (Row) column(row, BlobTableSchema.BLOB);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    assertNull(blob.get(1));
    Row reference = blob.getStruct(2);
    assertEquals(source, reference.getString(0));
    assertNull(reference.get(1));
    assertNull(reference.get(2));
    assertEquals(false, reference.getBoolean(3));
    assertEquals(0, BlobUnhappyFileSystem.OPENS.get(), "large inputs are not opened by row preparation");
  }
}
