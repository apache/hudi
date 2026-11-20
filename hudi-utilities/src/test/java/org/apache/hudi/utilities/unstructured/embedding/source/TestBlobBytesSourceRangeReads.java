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
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.util.FileIOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BlobBytesSource} over OUT_OF_LINE blobs in local files: an absolute reference is read whole, or only its
 * {@code [offset, offset + length)} range; a range the stored object cannot fill, or a file that is gone, is the file's content
 * failure; a malformed reference is refused before any read.
 */
public class TestBlobBytesSourceRangeReads {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final byte[] DIGITS = "0123456789".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path temp;

  @Test
  void readsTheWholeObjectWithoutARange() throws Exception {
    String path = write("whole.bin", DIGITS);
    try (InputStream in = source().open(outOfLine(path, null, null))) {
      assertArrayEquals(DIGITS, FileIOUtils.readAsByteArray(in));
    }
  }

  @Test
  void readsOnlyTheRange() throws Exception {
    String path = write("range.bin", DIGITS);
    try (InputStream in = source().open(outOfLine(path, 2L, 5L))) {
      assertArrayEquals("23456".getBytes(StandardCharsets.UTF_8), FileIOUtils.readAsByteArray(in));
    }
    try (InputStream in = source().open(outOfLine(path, 0L, 3L))) {
      assertEquals('0', in.read());
      assertEquals('1', in.read());
      assertEquals('2', in.read());
      assertEquals(-1, in.read(), "the stream ends with the range");
    }
  }

  @Test
  void zeroLengthReadReturnsZero() throws Exception {
    String path = write("zero.bin", DIGITS);
    try (InputStream in = source().open(outOfLine(path, 4L, 2L))) {
      byte[] buffer = new byte[4];
      assertEquals(0, in.read(buffer, 0, 0), "inside the range");
      assertEquals(2, in.read(buffer, 0, 4));
      assertEquals(0, in.read(buffer, 0, 0), "after the range: the InputStream contract answers 0 to a zero-length read");
      assertEquals(-1, in.read(buffer, 0, 4));
    }
  }

  @Test
  void rangePastTheEndOfTheObjectFailsOnRead() throws Exception {
    String path = write("short.bin", DIGITS);
    try (InputStream in = source().open(outOfLine(path, 8L, 5L))) {
      BlobTruncatedException truncated = assertThrows(BlobTruncatedException.class, () -> FileIOUtils.readAsByteArray(in));
      assertTrue(SourceFailures.isContentFailure(truncated));
      assertTrue(truncated.getMessage().contains("3 more bytes"), truncated.getMessage());
    }
  }

  @Test
  void offsetPastTheEndOfTheObjectFailsAtOpen() throws Exception {
    String path = write("offset.bin", DIGITS);
    BlobTruncatedException truncated = assertThrows(BlobTruncatedException.class, () -> source().open(outOfLine(path, 20L, 1L)));
    assertTrue(SourceFailures.isContentFailure(truncated));
    assertTrue(truncated.getMessage().contains("holds 10 bytes, before the recorded offset 20"), truncated.getMessage());
  }

  @Test
  void missingFileIsAContentFailure() {
    String path = temp.resolve("gone.bin").toUri().toString();
    for (Row blob : new Row[] {outOfLine(path, null, null), outOfLine(path, 2L, 3L)}) {
      Exception missing = assertThrows(Exception.class, () -> source().open(blob));
      assertTrue(ExceptionUtil.isCausedBy(missing, FileNotFoundException.class), String.valueOf(missing));
      assertTrue(SourceFailures.isContentFailure(missing), String.valueOf(missing));
      assertFalse(BlobBytesSource.isTransient(missing), "a file that is gone is never retried");
    }
  }

  @Test
  void refusesAMalformedReferenceBeforeAnyRead() throws Exception {
    String path = write("present.bin", DIGITS);
    Row[] malformed = {
        outOfLine("present.bin", null, null),
        outOfLine("dir/present.bin", 0L, 3L),
        outOfLine("", null, null),
        outOfLine(null, null, null),
        outOfLine(path, 2L, null),
        outOfLine(path, null, 3L),
        outOfLine(path, -1L, 3L),
        outOfLine(path, 0L, -3L)};
    BlobBytesSource source = source();
    for (Row blob : malformed) {
      BlobReferenceException refused = assertThrows(BlobReferenceException.class, () -> source.open(blob), String.valueOf(blob));
      assertTrue(SourceFailures.isContentFailure(refused));
    }
    assertTrue(assertThrows(BlobReferenceException.class, () -> source.resolve("present.bin")).getMessage()
        .contains("reads an object only by its absolute path"));
  }

  private static Row outOfLine(String path, Long offset, Long length) {
    StructType reference = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    Object[] referenceValues = new Object[reference.fields().length];
    referenceValues[reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH)] = path;
    referenceValues[reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET)] = offset;
    referenceValues[reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH)] = length;
    Object[] values = new Object[BLOB.fields().length];
    values[BLOB.fieldIndex(HoodieSchema.Blob.TYPE)] = HoodieSchema.Blob.OUT_OF_LINE;
    values[BLOB.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)] = new GenericRowWithSchema(referenceValues, reference);
    return new GenericRowWithSchema(values, BLOB);
  }

  private static BlobBytesSource source() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private String write(String name, byte[] bytes) throws Exception {
    Path file = temp.resolve(name);
    Files.write(file, bytes);
    return file.toUri().toString();
  }
}
