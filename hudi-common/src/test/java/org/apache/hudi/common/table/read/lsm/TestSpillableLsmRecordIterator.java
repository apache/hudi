/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class TestSpillableLsmRecordIterator {

  @TempDir
  Path tempDir;

  @Test
  void testSpillAndReadBackSequentially() throws IOException {
    List<BufferedRecord<String>> records = Arrays.asList(
        new BufferedRecord<>("key1", 1, null, null, null),
        new BufferedRecord<>("key2", 2, null, null, null),
        new BufferedRecord<>("key3", 3, null, null, null));

    SpillableLsmRecordIterator<String> iterator = new SpillableLsmRecordIterator<>(
        ClosableIterator.wrap(records.iterator()), new DefaultSerializer<>(), null, tempDir.toString());

    assertEquals(1, spillFileCount());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(records.get(0), iterator.next());
    assertEquals(records.get(1), iterator.next());
    assertEquals(records.get(2), iterator.next());
    assertFalse(iterator.hasNext());

    iterator.close();
    assertEquals(0, spillFileCount());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSpillFailurePreservesSourceCloseFailureAsSuppressed(boolean closeError) throws IOException {
    Path spillBaseFile = Files.createTempFile(tempDir, "spill-base", ".tmp");
    Throwable closeFailure = sourceCloseFailure(closeError);

    HoodieIOException exception = assertThrows(HoodieIOException.class, () -> new SpillableLsmRecordIterator<>(
        closeFailingIterator(closeFailure), new DefaultSerializer<>(), null, spillBaseFile.toString()));

    assertSame(closeFailure, exception.getCause().getSuppressed()[0]);
  }

  @Test
  void testEmptySpillAndIdempotentClose() throws IOException {
    SpillableLsmRecordIterator<String> iterator = new SpillableLsmRecordIterator<>(
        ClosableIterator.wrap(java.util.Collections.emptyIterator()), new DefaultSerializer<>(), null, tempDir.toString());

    assertFalse(iterator.hasNext());
    assertThrows(java.util.NoSuchElementException.class, iterator::next);
    iterator.close();
    iterator.close();
    assertEquals(0, spillFileCount());
  }

  @Test
  void testMissingSpillFileIsReportedAsReadFailure() throws IOException {
    SpillableLsmRecordIterator<String> iterator = new SpillableLsmRecordIterator<>(
        ClosableIterator.wrap(java.util.Collections.singletonList(
            new BufferedRecord<String>("key", 1, null, null, null)).iterator()),
        new DefaultSerializer<>(), null, tempDir.toString());
    Path spillFile;
    try (Stream<Path> paths = Files.list(tempDir)) {
      spillFile = paths.findFirst().get();
    }
    Files.delete(spillFile);

    assertThrows(HoodieIOException.class, iterator::hasNext);
    iterator.close();
  }

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void testSuccessfulSpillCleansUpOnSourceCloseFailure(boolean empty, boolean closeError) throws IOException {
    Throwable closeFailure = sourceCloseFailure(closeError);
    List<BufferedRecord<String>> records = empty ? Collections.emptyList()
        : Collections.singletonList(new BufferedRecord<>("key", 1, null, null, null));
    ClosableIterator<BufferedRecord<String>> sourceIterator = spy(ClosableIterator.wrap(records.iterator()));
    doThrow(closeFailure).when(sourceIterator).close();

    assertSame(closeFailure, assertThrows(closeFailure.getClass(), () -> new SpillableLsmRecordIterator<>(
        sourceIterator, new DefaultSerializer<>(), null, tempDir.toString())));
    assertEquals(0, spillFileCount());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSourceCloseFailurePreservesSpillCleanupFailureAsSuppressed(boolean closeError) {
    Throwable closeFailure = sourceCloseFailure(closeError);
    ClosableIterator<BufferedRecord<String>> sourceIterator = closeFailingIterator(closeFailure);
    doAnswer(invocation -> {
      replaceSpillFileWithNonEmptyDirectory();
      throw closeFailure;
    }).when(sourceIterator).close();

    Throwable exception = assertThrows(closeFailure.getClass(), () -> new SpillableLsmRecordIterator<>(
        sourceIterator, new DefaultSerializer<>(), null, tempDir.toString()));

    assertSame(closeFailure, exception);
    assertEquals(1, exception.getSuppressed().length);
    assertTrue(exception.getSuppressed()[0] instanceof HoodieIOException);
    assertTrue(exception.getSuppressed()[0].getCause() instanceof DirectoryNotEmptyException);
  }

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void testSpillErrorPreservesCleanupAndSourceCloseFailures(boolean cleanupFails, boolean closeFails) throws IOException {
    AssertionError spillFailure = new AssertionError("spill failed");
    AssertionError closeFailure = new AssertionError("source close failed");
    ClosableIterator<BufferedRecord<String>> sourceIterator = spy(ClosableIterator.wrap(Collections.emptyIterator()));
    doAnswer(invocation -> {
      assertEquals(1, spillFileCount());
      if (cleanupFails) {
        replaceSpillFileWithNonEmptyDirectory();
      }
      throw spillFailure;
    }).when(sourceIterator).hasNext();
    if (closeFails) {
      doThrow(closeFailure).when(sourceIterator).close();
    }

    AssertionError exception = assertThrows(AssertionError.class, () -> new SpillableLsmRecordIterator<>(
        sourceIterator, new DefaultSerializer<>(), null, tempDir.toString()));

    assertSame(spillFailure, exception);
    Throwable[] suppressed = exception.getSuppressed();
    assertEquals((cleanupFails ? 1 : 0) + (closeFails ? 1 : 0), suppressed.length);
    if (cleanupFails) {
      assertTrue(suppressed[0] instanceof HoodieIOException);
      assertTrue(suppressed[0].getCause() instanceof DirectoryNotEmptyException);
    }
    if (closeFails) {
      assertSame(closeFailure, suppressed[suppressed.length - 1]);
    }
    verify(sourceIterator).close();
    assertEquals(cleanupFails ? 1 : 0, spillFileCount());
  }

  private void replaceSpillFileWithNonEmptyDirectory() throws IOException {
    Path spillFile;
    try (Stream<Path> paths = Files.list(tempDir)) {
      spillFile = paths.findFirst().get();
    }
    // A non-empty directory makes deletion fail reliably without relying on filesystem permissions.
    Files.delete(spillFile);
    Files.createDirectory(spillFile);
    Files.createFile(spillFile.resolve("child"));
  }

  private static Throwable sourceCloseFailure(boolean error) {
    return error ? new AssertionError("source close failed") : new RuntimeException("source close failed");
  }

  private long spillFileCount() throws IOException {
    try (Stream<Path> paths = Files.list(tempDir)) {
      return paths.count();
    }
  }

  private ClosableIterator<BufferedRecord<String>> closeFailingIterator(Throwable closeFailure) {
    ClosableIterator<BufferedRecord<String>> sourceIterator = spy(ClosableIterator.wrap(Collections.emptyIterator()));
    doThrow(closeFailure).when(sourceIterator).close();
    return sourceIterator;
  }
}
