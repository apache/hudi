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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link HoodieSourceSplitReader}.
 */
public class TestHoodieSourceSplitReader {

  private SourceReaderContext mockContext;
  private int subtaskIndex = 0;

  @BeforeEach
  public void setUp() {
    mockContext = Mockito.mock(SourceReaderContext.class);
    when(mockContext.getIndexOfSubtask()).thenReturn(subtaskIndex);
  }

  @Test
  public void testFetchWithNoSplits() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testFetchWithSingleSplit() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2", "record3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    SplitsAddition<HoodieSourceSplit> splitsChange = new SplitsAddition<>(Collections.singletonList(split));
    reader.handleSplitsChanges(splitsChange);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertEquals(split.splitId(), result.nextSplit());
  }

  @Test
  public void testFetchWithMultipleSplits() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");

    SplitsAddition<HoodieSourceSplit> splitsChange =
        new SplitsAddition<>(Arrays.asList(split1, split2, split3));
    reader.handleSplitsChanges(splitsChange);

    // Fetch first split
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result1 = reader.fetch();
    assertNotNull(result1);
    assertEquals(split1.splitId(), result1.nextSplit());

    // Fetch second split
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result2 = reader.fetch();
    assertNotNull(result2);
    assertEquals(split2.splitId(), result2.nextSplit());

    // Fetch third split
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result3 = reader.fetch();
    assertNotNull(result3);
    assertEquals(split3.splitId(), result3.nextSplit());

    // No more splits
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result4 = reader.fetch();
    assertNotNull(result4);
    assertNull(result4.nextSplit());
  }

  @Test
  public void testHandleSplitsChangesWithComparator() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    // Comparator that sorts by file ID in reverse order
    SerializableComparator<HoodieSourceSplit> comparator =
        (s1, s2) -> s2.getFileId().compareTo(s1.getFileId());

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, comparator);

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");

    // Add splits in forward order
    SplitsAddition<HoodieSourceSplit> splitsChange =
        new SplitsAddition<>(Arrays.asList(split1, split2, split3));
    reader.handleSplitsChanges(splitsChange);

    // Should fetch in reverse order due to comparator
    assertEquals(split3.splitId(), reader.fetch().nextSplit());
    assertEquals(split2.splitId(), reader.fetch().nextSplit());
    assertEquals(split1.splitId(), reader.fetch().nextSplit());
  }

  @Test
  public void testAddingSplitsInMultipleBatches() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    // First batch
    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));

    // Second batch
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split2, split3)));

    // Verify all splits can be fetched
    assertEquals(split1.splitId(), reader.fetch().nextSplit());
    assertEquals(split2.splitId(), reader.fetch().nextSplit());
    assertEquals(split3.splitId(), reader.fetch().nextSplit());
    assertNull(reader.fetch().nextSplit());
  }

  @Test
  public void testClose() throws Exception {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
    reader.fetch();

    // Close should not throw exception
    reader.close();

    // After close, fetching should work but return empty results
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testWakeUp() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    // wakeUp is a no-op, should not throw any exception
    reader.wakeUp();
  }

  @Test
  public void testPauseOrResumeSplits() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");

    // pauseOrResumeSplits is currently a no-op, should not throw exception
    reader.pauseOrResumeSplits(
        Collections.singletonList(split1),
        Collections.singletonList(split2)
    );
  }

  @Test
  public void testReaderFunctionCalledCorrectly() throws IOException {
    List<String> testData = Arrays.asList("A", "B", "C");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    reader.fetch();

    assertEquals(1, readerFunction.getReadCount());
    assertEquals(split, readerFunction.getLastReadSplit());
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "base-path-" + splitNum,
        Option.of(Collections.emptyList()),
        "/test/table",
        "/test/partition",
        "20231201000000",
        "read_optimized",
        fileId
    );
  }

  /**
   * Test implementation of SplitReaderFunction.
   */
  private static class TestSplitReaderFunction implements SplitReaderFunction<String> {
    private final List<String> testData;
    private int readCount = 0;
    private HoodieSourceSplit lastReadSplit = null;

    public TestSplitReaderFunction() {
      this(Collections.emptyList());
    }

    public TestSplitReaderFunction(List<String> testData) {
      this.testData = testData;
    }

    @Override
    public RecordsWithSplitIds<HoodieRecordWithPosition<String>> read(HoodieSourceSplit split) {
      readCount++;
      lastReadSplit = split;
      ClosableIterator<String> iterator = createClosableIterator(testData);
      return BatchRecords.forRecords(
          split.splitId(),
          iterator,
          split.getFileOffset(),
          split.getRecordOffset()
      );
    }

    public int getReadCount() {
      return readCount;
    }

    public HoodieSourceSplit getLastReadSplit() {
      return lastReadSplit;
    }

    private ClosableIterator<String> createClosableIterator(List<String> items) {
      Iterator<String> iterator = items.iterator();
      return new ClosableIterator<String>() {
        @Override
        public void close() {
          // No-op
        }

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public String next() {
          return iterator.next();
        }
      };
    }
  }
}
