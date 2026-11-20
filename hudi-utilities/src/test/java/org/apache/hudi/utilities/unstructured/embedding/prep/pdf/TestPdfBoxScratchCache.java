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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.apache.pdfbox.io.RandomAccess;
import org.apache.pdfbox.io.RandomAccessStreamCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PdfBoxScratchCache} over PDFBox's real scratch file: a local disk failure beneath PDFBox is the platform's, PDFBox's own
 * storage cap stays the file's failure, and data written reads back unchanged.
 */
class TestPdfBoxScratchCache {

  private static final int PAGE = 4096;

  @TempDir
  Path dir;

  @Test
  void writtenDataReadsBack() throws IOException {
    PdfBoxScratchCache scratch = new PdfBoxScratchCache(PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(dir.toFile(), 1L << 20));
    byte[] data = new byte[3 * PAGE + 17];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    try (RandomAccessStreamCache cache = scratch.create(); RandomAccess buffer = cache.createBuffer()) {
      buffer.write(data);
      buffer.seek(0);
      byte[] read = new byte[data.length];
      buffer.readFully(read);
      assertArrayEquals(data, read);
      assertTrue(buffer.isEOF());
    }
    assertNull(scratch.failure());
  }

  @Test
  void storageCapStaysTheFilesFailure() throws IOException {
    // the cap is max.input.bytes: what the file asks PDFBox to keep beyond it is the file's failure, never the platform's.
    // PDFBox grows its scratch file 16 pages at a time and checks the cap before each growth, so 2 pages cap it at 16.
    PdfBoxScratchCache scratch = new PdfBoxScratchCache(PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(dir.toFile(), 2 * PAGE));
    try (RandomAccessStreamCache cache = scratch.create(); RandomAccess buffer = cache.createBuffer()) {
      IOException e = assertThrows(IOException.class, () -> buffer.write(new byte[20 * PAGE]));
      assertTrue(e.getMessage().contains("Maximum allowed scratch file memory exceeded"), e.toString());
    }
    assertNull(scratch.failure());
  }

  @Test
  void scratchDirGoneBeforeTheFirstBufferIsAPlatformFailure() throws IOException {
    Path scratchDir = Files.createDirectory(dir.resolve("scratch"));
    PdfBoxScratchCache scratch = new PdfBoxScratchCache(PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(scratchDir.toFile(), 1L << 20));
    try (RandomAccessStreamCache cache = scratch.create()) {
      Files.delete(scratchDir);                       // PDFBox creates its temp file with the first buffer
      PrepPlatformException e = assertThrows(PrepPlatformException.class, cache::createBuffer);
      assertTrue(e.getCause() instanceof NoSuchFileException, String.valueOf(e.getCause()));
      assertSame(e, scratch.failure());
    }
  }

  @Test
  void cacheThatCannotBeCreatedIsAPlatformFailure() {
    // PDFBox's document would swallow an IOException here and fall back to an unbounded heap cache
    Path missing = dir.resolve("missing");
    PdfBoxScratchCache scratch = new PdfBoxScratchCache(PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(missing.toFile(), 1L << 20));
    PrepPlatformException e = assertThrows(PrepPlatformException.class, scratch::create);
    assertSame(e, scratch.failure());
    assertFalse(Files.exists(missing));
  }

  @Test
  void injectedDiskFullIsAPlatformFailureAndTheFirstIsKept() throws IOException {
    FailingScratchCache failing = new FailingScratchCache();
    PdfBoxScratchCache scratch = new PdfBoxScratchCache(failing.create(dir.toFile(), 1L << 20));
    try (RandomAccessStreamCache cache = scratch.create(); RandomAccess buffer = cache.createBuffer()) {
      buffer.write(1);
      failing.arm();
      PrepPlatformException first = assertThrows(PrepPlatformException.class, () -> buffer.write(2));
      assertThrows(PrepPlatformException.class, () -> buffer.write(new byte[8], 0, 8));
      assertTrue(first.getCause().getMessage().contains(FailingScratchCache.NO_SPACE), String.valueOf(first.getCause()));
      assertSame(first, scratch.failure());
    }
  }
}
