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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  void testSpillFailurePreservesSourceCloseFailureAsSuppressed() throws IOException {
    Path spillBaseFile = Files.createTempFile(tempDir, "spill-base", ".tmp");
    RuntimeException closeFailure = new RuntimeException("source close failed");

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

  @Test
  void testSuccessfulSpillPropagatesSourceCloseFailure() {
    RuntimeException closeFailure = new RuntimeException("source close failed");

    assertSame(closeFailure, assertThrows(RuntimeException.class, () -> new SpillableLsmRecordIterator<>(
        closeFailingIterator(closeFailure), new DefaultSerializer<>(), null, tempDir.toString())));
  }

  private long spillFileCount() throws IOException {
    try (Stream<Path> paths = Files.list(tempDir)) {
      return paths.count();
    }
  }

  private ClosableIterator<BufferedRecord<String>> closeFailingIterator(RuntimeException closeFailure) {
    return new ClosableIterator<BufferedRecord<String>>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public BufferedRecord<String> next() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        throw closeFailure;
      }
    };
  }
}
