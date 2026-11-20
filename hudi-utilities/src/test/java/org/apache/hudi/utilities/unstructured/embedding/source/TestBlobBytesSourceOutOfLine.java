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
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
 * {@link BlobBytesSource} over OUT_OF_LINE blobs: an absolute reference (the source object itself, as prep points an object above
 * the inline ceiling at it) is opened as it is, whole or in its range. Without a table base path a relative reference has no table
 * to resolve against and is refused before any read; with one (compute reading the embedding table's {@code prepared_image}) a
 * managed relative reference is opened under the base as {@code read_blob()} opens it, while one escaping the base, is refused.
 */
public class TestBlobBytesSourceOutOfLine {

  private static final StructType BLOB = (StructType) BlobType.dataType();

  @TempDir
  Path temp;

  private static Row outOfLine(String path, Long offset, Long length, Boolean managed) {
    StructType reference = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {path, offset, length, managed}, reference)}, BLOB);
  }

  private static BlobBytesSource source() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private BlobBytesSource sourceAt(Path tableBase) {
    return new BlobBytesSource(tableBase.toUri().toString(), HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private String write(String name, byte[] bytes) throws Exception {
    Path file = temp.resolve(name);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    return file.toUri().toString();
  }

  @ParameterizedTest
  @ValueSource(strings = {"oss", "OSS", "OsS"})
  void unavailableConnectorIsAPlatformFailure(String scheme) {
    IOException failure = assertThrows(IOException.class,
        () -> source().open(outOfLine(scheme + "://bucket/a.pdf", null, null, false)));
    assertFalse(SourceFailures.isContentFailure(failure));
    assertFalse(BlobBytesSource.isTransient(failure));
  }

  @Test
  void unknownSchemeIsAReferenceFailure() {
    BlobReferenceException failure = assertThrows(BlobReferenceException.class,
        () -> source().open(outOfLine("unknownscheme://bucket/a.pdf", null, null, false)));
    assertTrue(SourceFailures.isContentFailure(failure));
  }

  @Test
  void opensAnAbsoluteReferenceWholeWithoutARange() throws Exception {
    byte[] bytes = "%PDF-1.7 the source object".getBytes(StandardCharsets.UTF_8);
    String absolute = write("landing/a.pdf", bytes);

    assertEquals(new StoragePath(absolute), source().resolve(absolute));
    try (InputStream in = source().open(outOfLine(absolute, null, null, false))) {
      assertArrayEquals(bytes, FileIOUtils.readAsByteArray(in));
    }
  }

  @Test
  void opensTheRangeOfAnAbsoluteReference() throws Exception {
    byte[] bytes = "0123456789".getBytes(StandardCharsets.UTF_8);
    String absolute = write("landing/b.bin", bytes);

    try (InputStream in = source().open(outOfLine(absolute, 2L, 5L, true))) {
      assertArrayEquals("23456".getBytes(StandardCharsets.UTF_8), FileIOUtils.readAsByteArray(in));
    }
  }

  @Test
  void refusesARelativeOrEmptyReference() throws Exception {
    write(".hoodie/.blob/pdf/a.bin", new byte[] {1});
    BlobBytesSource source = source();
    for (String relative : new String[] {".hoodie/.blob/pdf/a.bin", "../outside.bin", "a.bin"}) {
      assertThrows(BlobReferenceException.class, () -> source.resolve(relative), relative);
      assertThrows(BlobReferenceException.class, () -> source.open(outOfLine(relative, null, null, true)), relative);
      BlobReferenceException unmanaged = assertThrows(BlobReferenceException.class,
          () -> source.open(outOfLine(relative, null, null, false)), relative);
      assertTrue(unmanaged.getMessage().contains("reads an object only by its absolute path"), unmanaged.getMessage());
      BlobReferenceException seekable = assertThrows(BlobReferenceException.class,
          () -> source.openSeekable(outOfLine(relative, null, null, true)), relative);
      assertTrue(seekable.getMessage().contains("reads an object only by its absolute path"), seekable.getMessage());
    }
    assertThrows(BlobReferenceException.class, () -> source.resolve(""));
  }

  /**
   * With a table base path, a managed relative reference (as the input store writes a video frame into the embedding table's
   * {@code .hoodie/.blob} folder) resolves under the base and is read whole or in its range, as a stream and for random access;
   * a null {@code managed} counts as managed here (read_blob() would take it as unmanaged; the input store always writes true).
   */
  @Test
  void opensAManagedRelativeReferenceUnderTheTableBase() throws Exception {
    byte[] bytes = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    write("table/.hoodie/.blob/p1/frame-0.bin", bytes);
    Path table = temp.resolve("table");
    String relative = ".hoodie/.blob/p1/frame-0.bin";
    BlobBytesSource source = sourceAt(table);

    assertEquals(new StoragePath(new StoragePath(table.toUri().toString()), relative), source.resolve(relative));
    for (Boolean managed : new Boolean[] {true, false, null}) {
      try (InputStream in = source.open(outOfLine(relative, null, null, managed))) {
        assertArrayEquals(bytes, FileIOUtils.readAsByteArray(in), "whole, managed=" + managed);
      }
      try (InputStream in = source.open(outOfLine(relative, 3L, 6L, managed))) {
        assertArrayEquals("345678".getBytes(StandardCharsets.UTF_8), FileIOUtils.readAsByteArray(in), "range, managed=" + managed);
      }
      try (SeekableBlob seekable = source.openSeekable(outOfLine(relative, null, null, managed))) {
        assertEquals(bytes.length, seekable.length());
        assertArrayEquals(bytes, readAll(seekable), "seekable whole, managed=" + managed);
      }
      try (SeekableBlob seekable = source.openSeekable(outOfLine(relative, 3L, 6L, managed))) {
        assertEquals(6L, seekable.length());
        assertArrayEquals("345678".getBytes(StandardCharsets.UTF_8), readAll(seekable), "seekable range, managed=" + managed);
        seekable.seek(4L);
        assertArrayEquals("78".getBytes(StandardCharsets.UTF_8), readAll(seekable), "seek inside the range, managed=" + managed);
      }
    }
  }

  /** A table base path does not change how an absolute reference is opened. */
  @Test
  void opensAnAbsoluteReferenceWithATableBase() throws Exception {
    byte[] bytes = "0123456789".getBytes(StandardCharsets.UTF_8);
    String absolute = write("landing/c.bin", bytes);
    BlobBytesSource source = sourceAt(temp.resolve("table"));

    assertEquals(new StoragePath(absolute), source.resolve(absolute));
    try (InputStream in = source.open(outOfLine(absolute, 2L, 5L, false))) {
      assertArrayEquals("23456".getBytes(StandardCharsets.UTF_8), FileIOUtils.readAsByteArray(in));
    }
    try (SeekableBlob seekable = source.openSeekable(outOfLine(absolute, null, null, true))) {
      assertArrayEquals(bytes, readAll(seekable));
    }
  }

  /** A managed relative reference whose {@code ..} segments leave the table base is refused, even when the file it names exists. */
  @Test
  void refusesAManagedRelativeReferenceEscapingTheTableBase() throws Exception {
    write("outside.bin", new byte[] {1, 2, 3});
    write("table/.hoodie/.blob/a.bin", new byte[] {4});
    BlobBytesSource source = sourceAt(temp.resolve("table"));
    for (String escaping : new String[] {"../outside.bin", ".hoodie/../../outside.bin"}) {
      BlobReferenceException resolved = assertThrows(BlobReferenceException.class, () -> source.resolve(escaping), escaping);
      assertTrue(resolved.getMessage().contains("outside the table base path"), resolved.getMessage());
      assertThrows(BlobReferenceException.class, () -> source.open(outOfLine(escaping, null, null, true)), escaping);
      assertThrows(BlobReferenceException.class, () -> source.openSeekable(outOfLine(escaping, 0L, 3L, null)), escaping);
    }
  }

  private static byte[] readAll(SeekableBlob seekable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4];
    for (int n = seekable.read(buffer, 0, buffer.length); n >= 0; n = seekable.read(buffer, 0, buffer.length)) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }
}
