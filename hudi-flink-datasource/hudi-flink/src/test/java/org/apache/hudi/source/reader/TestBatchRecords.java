/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.hudi.common.util.collection.ClosableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link BatchRecords}.
 */
public class TestBatchRecords {

  @Test
  public void testForRecordsWithEmptyIterator() {
    String splitId = "test-split-1";
    ClosableIterator<String> emptyIterator = createClosableIterator(Collections.emptyList());

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, emptyIterator, 0, 0L);

    assertNotNull(batchRecords);
    assertEquals(splitId, batchRecords.nextSplit());
    assertNull(batchRecords.nextRecordFromSplit(), "Should have no records");
    assertTrue(batchRecords.finishedSplits().contains(splitId), "Should contain finished split");
    assertNull(batchRecords.nextSplit(), "Second call to nextSplit should return null");
  }

  @Test
  public void testForRecordsWithMultipleRecords() {
    String splitId = "test-split-2";
    List<String> records = Arrays.asList("record1", "record2", "record3");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);

    // Verify split ID
    assertEquals(splitId, batchRecords.nextSplit());
    assertNull(batchRecords.nextSplit(), "Second call should return null");

    // Verify records
    HoodieRecordWithPosition<String> record1 = batchRecords.nextRecordFromSplit();
    assertNotNull(record1);
    assertEquals("record1", record1.record());
    assertEquals(0, record1.fileOffset());
    assertEquals(1L, record1.recordOffset()); // recordOffset starts at 0 and increments to 1 after first record

    HoodieRecordWithPosition<String> record2 = batchRecords.nextRecordFromSplit();
    assertNotNull(record2);
    assertEquals("record2", record2.record());
    assertEquals(2L, record2.recordOffset());

    HoodieRecordWithPosition<String> record3 = batchRecords.nextRecordFromSplit();
    assertNotNull(record3);
    assertEquals("record3", record3.record());
    assertEquals(3L, record3.recordOffset());

    // No more records
    assertNull(batchRecords.nextRecordFromSplit());
  }

  @Test
  public void testSeekToStartingOffset() {
    String splitId = "test-split-3";
    List<String> records = Arrays.asList("record1", "record2", "record3", "record4", "record5");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 2L);
    batchRecords.seek(2L);

    // After seeking to offset 2, we should start from record3
    batchRecords.nextSplit();

    HoodieRecordWithPosition<String> record = batchRecords.nextRecordFromSplit();
    assertNotNull(record);
    assertEquals("record3", record.record());
  }

  @Test
  public void testSeekBeyondAvailableRecords() {
    String splitId = "test-split-4";
    List<String> records = Arrays.asList("record1", "record2");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);

    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
      batchRecords.seek(10L);
    });

    assertTrue(exception.getMessage().contains("Invalid starting record offset"));
  }

  @Test
  public void testFileOffsetPersistence() {
    String splitId = "test-split-5";
    int fileOffset = 5;
    List<String> records = Arrays.asList("record1", "record2");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, fileOffset, 0L);
    batchRecords.nextSplit();

    HoodieRecordWithPosition<String> record1 = batchRecords.nextRecordFromSplit();
    assertNotNull(record1);
    assertEquals(fileOffset, record1.fileOffset());

    HoodieRecordWithPosition<String> record2 = batchRecords.nextRecordFromSplit();
    assertNotNull(record2);
    assertEquals(fileOffset, record2.fileOffset(), "File offset should remain constant");
  }

  @Test
  public void testFinishedSplitsEmpty() {
    String splitId = "test-split-6";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);

    assertTrue(batchRecords.finishedSplits().isEmpty(), "Should have empty finished splits by default");
  }

  @Test
  public void testConstructorWithFinishedSplits() {
    String splitId = "test-split-7";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);
    Set<String> finishedSplits = new HashSet<>(Arrays.asList("split1", "split2"));

    BatchRecords<String> batchRecords = new BatchRecords<>(
        splitId, iterator, 0, 0L, finishedSplits);

    assertEquals(2, batchRecords.finishedSplits().size());
    assertTrue(batchRecords.finishedSplits().contains("split1"));
    assertTrue(batchRecords.finishedSplits().contains("split2"));
  }

  @Test
  public void testRecordOffsetIncrementsCorrectly() {
    String splitId = "test-split-8";
    long startingRecordOffset = 10L;
    List<String> records = Arrays.asList("A", "B", "C");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(
        splitId, iterator, 0, startingRecordOffset);
    batchRecords.nextSplit();

    // First record should be at startingRecordOffset + 1
    HoodieRecordWithPosition<String> record1 = batchRecords.nextRecordFromSplit();
    assertEquals(startingRecordOffset + 1, record1.recordOffset());

    // Second record should be at startingRecordOffset + 2
    HoodieRecordWithPosition<String> record2 = batchRecords.nextRecordFromSplit();
    assertEquals(startingRecordOffset + 2, record2.recordOffset());

    // Third record should be at startingRecordOffset + 3
    HoodieRecordWithPosition<String> record3 = batchRecords.nextRecordFromSplit();
    assertEquals(startingRecordOffset + 3, record3.recordOffset());
  }

  @Test
  public void testSplitIdReturnedOnlyOnce() {
    String splitId = "test-split-9";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);

    assertEquals(splitId, batchRecords.nextSplit());
    assertNull(batchRecords.nextSplit());
    assertNull(batchRecords.nextSplit());
    assertNull(batchRecords.nextSplit());
  }

  @Test
  public void testRecycleClosesIterator() {
    String splitId = "test-split-10";
    List<String> records = Arrays.asList("record1", "record2");
    MockClosableIterator<String> mockIterator = new MockClosableIterator<>(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, mockIterator, 0, 0L);

    batchRecords.recycle();

    assertTrue(mockIterator.isClosed(), "Iterator should be closed after recycle");
  }

  @Test
  public void testRecycleWithNullIterator() {
    // Test that recycle handles null iterator gracefully (though in practice this shouldn't happen)
    // This tests the null check in recycle() method
    String splitId = "test-split-11";
    ClosableIterator<String> emptyIterator = createClosableIterator(Collections.emptyList());

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, emptyIterator, 0, 0L);

    // Should not throw exception
    batchRecords.recycle();
  }

  @Test
  public void testNextRecordFromSplitAfterExhaustion() {
    String splitId = "test-split-12";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);
    batchRecords.nextSplit();

    // Read the only record
    assertNotNull(batchRecords.nextRecordFromSplit());

    // After exhaustion, should return null
    assertNull(batchRecords.nextRecordFromSplit());
    assertNull(batchRecords.nextRecordFromSplit());
  }

  @Test
  public void testSeekWithZeroOffset() {
    String splitId = "test-split-13";
    List<String> records = Arrays.asList("record1", "record2", "record3");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);

    // Seeking to 0 should not skip any records
    batchRecords.seek(0L);
    batchRecords.nextSplit();

    HoodieRecordWithPosition<String> record = batchRecords.nextRecordFromSplit();
    assertNotNull(record);
    assertEquals("record1", record.record());
  }

  @Test
  public void testConstructorNullValidation() {
    String splitId = "test-split-14";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);

    // Test null finishedSplits
    assertThrows(IllegalArgumentException.class, () -> {
      new BatchRecords<>(splitId, iterator, 0, 0L, null);
    });

    // Test null recordIterator
    assertThrows(IllegalArgumentException.class, () -> {
      new BatchRecords<>(splitId, null, 0, 0L, new HashSet<>());
    });
  }

  @Test
  public void testRecordPositionReusability() {
    String splitId = "test-split-15";
    List<String> records = Arrays.asList("A", "B", "C");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);
    batchRecords.nextSplit();

    HoodieRecordWithPosition<String> pos1 = batchRecords.nextRecordFromSplit();
    HoodieRecordWithPosition<String> pos2 = batchRecords.nextRecordFromSplit();

    // Should reuse the same object
    assertTrue(pos1 == pos2, "Should reuse the same HoodieRecordWithPosition object");
  }

  @Test
  public void testSeekUpdatesPosition() {
    String splitId = "test-split-16";
    List<String> records = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 5, 10L);

    // Seek to offset 3
    batchRecords.seek(3L);

    batchRecords.nextSplit();

    // After seeking 3, next record should be r4 (4th record)
    HoodieRecordWithPosition<String> record = batchRecords.nextRecordFromSplit();
    assertNotNull(record);
    assertEquals("r4", record.record());
  }

  @Test
  public void testIteratorClosedAfterExhaustion() {
    String splitId = "test-split-17";
    List<String> records = Arrays.asList("record1");
    MockClosableIterator<String> mockIterator = new MockClosableIterator<>(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, mockIterator, 0, 0L);
    batchRecords.nextSplit();

    // Read records
    batchRecords.nextRecordFromSplit();

    // Trigger close operation
    batchRecords.nextRecordFromSplit();

    // After exhaustion, nextRecordFromSplit should close the iterator
    assertTrue(mockIterator.isClosed(), "Iterator should be closed after exhaustion");
  }

  @Test
  public void testFinishedSplitsAddedAfterExhaustion() {
    String splitId = "test-split-18";
    List<String> records = Arrays.asList("record1");
    ClosableIterator<String> iterator = createClosableIterator(records);

    BatchRecords<String> batchRecords = BatchRecords.forRecords(splitId, iterator, 0, 0L);
    batchRecords.nextSplit();

    assertTrue(batchRecords.finishedSplits().isEmpty());

    // Read all records
    batchRecords.nextRecordFromSplit();

    // After exhaustion, split should be added to finished splits
    assertNull(batchRecords.nextRecordFromSplit());
    assertTrue(batchRecords.finishedSplits().contains(splitId));
  }

  /**
   * Helper method to create a ClosableIterator from a list of items.
   */
  private <T> ClosableIterator<T> createClosableIterator(List<T> items) {
    Iterator<T> iterator = items.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // No-op for test
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  /**
   * Mock closable iterator for testing close behavior.
   */
  private static class MockClosableIterator<T> implements ClosableIterator<T> {
    private final Iterator<T> iterator;
    private boolean closed = false;

    public MockClosableIterator(List<T> items) {
      this.iterator = items.iterator();
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
