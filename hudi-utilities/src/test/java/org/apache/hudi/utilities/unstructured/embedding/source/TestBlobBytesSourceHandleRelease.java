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
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Every storage stream {@link BlobBytesSource} opens is closed: by the caller once it holds the stream or blob, and by the source
 * itself on each path where the caller never receives it (a failed seek to the range, a retried open).
 */
class TestBlobBytesSourceHandleRelease {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();

  @TempDir
  Path tempDir;

  private Path file;

  @BeforeEach
  void writeFile() throws IOException {
    HandleCountingFileSystem.reset();
    file = tempDir.resolve("object.bin");
    Files.write(file, sequence(300));
  }

  @AfterEach
  void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  void mainPathStreamsAndBlobsAreReleasedOnClose() throws IOException {
    BlobBytesSource source = source(TransientRetry.NONE);
    try (InputStream in = source.open(outOfLine(HandleCountingFileSystem.url(file), null, null))) {
      assertEquals(300, readAll(in).length);
    }
    try (InputStream in = source.open(outOfLine(HandleCountingFileSystem.url(file), 100L, 50L))) {
      assertArrayEquals(Arrays.copyOfRange(sequence(300), 100, 150), readAll(in));
    }
    try (SeekableBlob blob = source.openSeekable(outOfLine(HandleCountingFileSystem.url(file), 10L, 200L))) {
      blob.seek(150);
      assertEquals(50, blob.read(new byte[64], 0, 64));
    }
    // the video frame source's stream over a seekable blob, closed twice (a grabber that failed to start closes it too)
    SeekableBlobInputStream video = new SeekableBlobInputStream(source.openSeekable(outOfLine(HandleCountingFileSystem.url(file), null, null)));
    video.mark(0);
    assertEquals(300, readAll(video).length);
    video.close();
    video.close();
    assertEquals(4, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "storage streams left open by the main path");
  }

  @Test
  void truncatedRangeReadIsReleasedByTheCaller() throws IOException {
    BlobBytesSource source = source(TransientRetry.NONE);
    try (InputStream in = source.open(outOfLine(HandleCountingFileSystem.url(file), 200L, 500L))) {
      assertThrows(BlobTruncatedException.class, () -> readAll(in));
    }
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "storage streams left open by a truncated range");
  }

  @Test
  void failedSeekToTheRangeReleasesTheStreamTheCallerNeverReceives() {
    HandleCountingFileSystem.inject(file, HandleCountingFileSystem.Fault.SEEK, 2);
    BlobBytesSource source = source(TransientRetry.NONE);
    assertThrows(IOException.class, () -> source.open(outOfLine(HandleCountingFileSystem.url(file), 100L, 50L)));
    assertThrows(IOException.class, () -> source.openSeekable(outOfLine(HandleCountingFileSystem.url(file), 100L, 50L)));
    assertEquals(2, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "storage streams left open by a failed seek");
  }

  @Test
  void retriedOpenReleasesEveryFailedAttempt() throws IOException {
    // two transient seek failures, then the third attempt opens
    HandleCountingFileSystem.inject(file, HandleCountingFileSystem.Fault.SEEK, 2);
    BlobBytesSource source = source(new TransientRetry(3, 0L));
    try (InputStream in = source.open(outOfLine(HandleCountingFileSystem.url(file), 100L, 50L))) {
      assertEquals(50, readAll(in).length);
    }
    HandleCountingFileSystem.inject(file, HandleCountingFileSystem.Fault.SEEK, 2);
    try (SeekableBlob blob = source.openSeekable(outOfLine(HandleCountingFileSystem.url(file), 100L, 50L))) {
      assertEquals(50, blob.read(new byte[64], 0, 64));
    }
    assertEquals(6, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "storage streams left open by retried opens");
  }

  @Test
  void failedReadOfAnOpenedStreamIsReleasedByTheCaller() throws IOException {
    HandleCountingFileSystem.inject(file, HandleCountingFileSystem.Fault.READ, 1);
    BlobBytesSource source = source(TransientRetry.NONE);
    try (InputStream in = source.open(outOfLine(HandleCountingFileSystem.url(file), null, null))) {
      assertThrows(IOException.class, () -> readAll(in));
    }
    HandleCountingFileSystem.inject(file, HandleCountingFileSystem.Fault.READ, 1);
    try (SeekableBlobInputStream in = new SeekableBlobInputStream(
        source.openSeekable(outOfLine(HandleCountingFileSystem.url(file), null, null)))) {
      assertThrows(BlobReadException.class, () -> readAll(in));
    }
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "storage streams left open by a failed read");
  }

  private BlobBytesSource source(TransientRetry retry) {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(HandleCountingFileSystem.install(new Configuration())), retry);
  }

  private static Row outOfLine(String path, Long offset, Long length) {
    Row reference = new GenericRowWithSchema(new Object[] {path, offset, length, false}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[37];
    for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static byte[] sequence(int size) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }
}
