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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link HoodieBatchReader} interface and its implementations.
 */
public class TestBatchReader {

  @Test
  public void testBatchReaderInterface() throws Exception {
    // Test that custom BatchReader implementations work correctly
    CustomBatchReader<String> customReader = new CustomBatchReader<>(5);

    List<String> data = createTestData(20);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        customReader.batch(split, createClosableIterator(data));

    int totalRecords = 0;
    int batchCount = 0;

    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      assertNotNull(batch);

      HoodieRecordWithPosition<String> record;
      while ((record = batch.nextRecordFromSplit()) != null) {
        totalRecords++;
      }
      batchCount++;
    }

    assertEquals(20, totalRecords);
    assertEquals(4, batchCount); // 20 records / 5 per batch = 4 batches

    batchIterator.close();
  }

  @Test
  public void testDefaultBatchReaderImplementation() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 50);

    HoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config);

    List<String> data = createTestData(150);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    int batchCount = 0;
    List<Integer> batchSizes = new ArrayList<>();

    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      int batchSize = 0;

      while (batch.nextRecordFromSplit() != null) {
        batchSize++;
      }

      batchSizes.add(batchSize);
      batchCount++;
    }

    assertEquals(3, batchCount); // 150 / 50 = 3 batches
    assertEquals(50, batchSizes.get(0));
    assertEquals(50, batchSizes.get(1));
    assertEquals(50, batchSizes.get(2));

    batchIterator.close();
  }

  @Test
  public void testBatchReaderWithDifferentDataTypes() throws Exception {
    // Test with Integer type
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);

    HoodieBatchReader<Integer> intBatchReader = new DefaultHoodieBatchReader<>(config);

    List<Integer> intData = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      intData.add(i);
    }

    HoodieSourceSplit split = createTestSplit(0);
    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<Integer>>> batchIterator =
        intBatchReader.batch(split, createClosableIterator(intData));

    int totalSum = 0;
    int recordCount = 0;

    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<Integer>> batch = batchIterator.next();
      HoodieRecordWithPosition<Integer> record;

      while ((record = batch.nextRecordFromSplit()) != null) {
        totalSum += record.record();
        recordCount++;
      }
    }

    assertEquals(25, recordCount);
    assertEquals(300, totalSum); // Sum of 0..24 = 300

    batchIterator.close();
  }

  @Test
  public void testBatchReaderSerialization() {
    // BatchReader interface extends Serializable
    Configuration config = new Configuration();
    HoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config);

    // Verify it's serializable
    assertTrue(batchReader instanceof java.io.Serializable);
  }

  @Test
  public void testBatchReaderWithEmptyIterator() throws Exception {
    Configuration config = new Configuration();
    HoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config);

    HoodieSourceSplit split = createTestSplit(0);
    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(Collections.emptyList()));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testMultipleBatchReadersOnSameSplit() throws Exception {
    Configuration config1 = new Configuration();
    config1.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);

    Configuration config2 = new Configuration();
    config2.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 20);

    HoodieBatchReader<String> batchReader1 = new DefaultHoodieBatchReader<>(config1);
    HoodieBatchReader<String> batchReader2 = new DefaultHoodieBatchReader<>(config2);

    List<String> data = createTestData(100);

    // Use first reader
    HoodieSourceSplit split1 = createTestSplit(0);
    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> iter1 =
        batchReader1.batch(split1, createClosableIterator(data));

    int batches1 = 0;
    while (iter1.hasNext()) {
      iter1.next();
      batches1++;
    }
    assertEquals(10, batches1); // 100 / 10 = 10 batches
    iter1.close();

    // Use second reader on different split
    HoodieSourceSplit split2 = createTestSplit(0);
    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> iter2 =
        batchReader2.batch(split2, createClosableIterator(data));

    int batches2 = 0;
    while (iter2.hasNext()) {
      iter2.next();
      batches2++;
    }
    assertEquals(5, batches2); // 100 / 20 = 5 batches
    iter2.close();
  }

  @Test
  public void testBatchReaderPreservesRecordPosition() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 5);

    HoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config);

    List<String> data = createTestData(10);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    List<String> allRecords = new ArrayList<>();

    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      HoodieRecordWithPosition<String> record;

      while ((record = batch.nextRecordFromSplit()) != null) {
        allRecords.add(record.record());
      }
    }

    // Verify order is preserved
    assertEquals(data, allRecords);

    batchIterator.close();
  }

  // Helper methods

  private List<String> createTestData(int count) {
    List<String> data = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      data.add("record-" + i);
    }
    return data;
  }

  private HoodieSourceSplit createTestSplit(long consumed) {
    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        "base-path",
        Option.of(Collections.emptyList()),
        "/test/table",
        "/test/partition",
        "read_optimized",
        "file-1"
    );
    for (long i = 0; i < consumed; i++) {
      split.consume();
    }
    return split;
  }

  private <T> ClosableIterator<T> createClosableIterator(List<T> items) {
    Iterator<T> iterator = items.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // No-op
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
   * Custom BatchReader implementation for testing the interface.
   */
  private static class CustomBatchReader<T> implements HoodieBatchReader<T> {
    private final int batchSize;

    public CustomBatchReader(int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<T>>> batch(
        HoodieSourceSplit split, ClosableIterator<T> inputIterator) {
      return new CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<T>>>() {
        @Override
        public boolean hasNext() {
          return inputIterator.hasNext();
        }

        @Override
        public RecordsWithSplitIds<HoodieRecordWithPosition<T>> next() {
          List<T> batch = new ArrayList<>(batchSize);
          int count = 0;

          while (inputIterator.hasNext() && count < batchSize) {
            batch.add(inputIterator.next());
            split.consume();
            count++;
          }

          return HoodieBatchRecords.forRecords(
              split.splitId(),
              ClosableIterator.wrap(batch.iterator()),
              split.getFileOffset(),
              split.getConsumed() - count);
        }

        @Override
        public void close() throws IOException {
          inputIterator.close();
        }
      };
    }
  }
}
