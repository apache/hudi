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

package org.apache.hudi.source.reader.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.source.reader.function.AbstractSplitReaderFunction.NO_LIMIT;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link AbstractSplitReaderFunction}, specifically the
 * {@code limitIterator()} and {@code getHadoopConf()} logic introduced
 * with the limit push-down feature.
 */
public class TestAbstractSplitReaderFunction {

  @TempDir
  File tempDir;

  private Configuration conf;

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
  }

  // -------------------------------------------------------------------------
  //  Minimal concrete subclass to exercise AbstractSplitReaderFunction
  // -------------------------------------------------------------------------

  /**
   * Thin subclass used only in tests. Exposes {@code limitIterator} and
   * {@code getHadoopConf} as package-accessible helpers.
   */
  private static class TestableFunction extends AbstractSplitReaderFunction {

    TestableFunction(Configuration conf, long limit) {
      super(conf, Collections.emptyList(), limit, false);
    }

    /** Delegate test access to the protected method. */
    ClosableIterator<RowData> wrapWithLimit(ClosableIterator<RowData> iterator, long limit) {
      return limitIterator(iterator, limit);
    }

    org.apache.hadoop.conf.Configuration hadoopConf() {
      return getHadoopConf();
    }

    @Override
    public RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> read(HoodieSourceSplit split) {
      throw new UnsupportedOperationException("Not used in unit tests");
    }

    @Override
    public void close() {
      // no-op
    }
  }

  // -------------------------------------------------------------------------
  //  Simple in-memory ClosableIterator helper
  // -------------------------------------------------------------------------

  private static ClosableIterator<RowData> iteratorOf(int count) {
    List<RowData> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      rows.add(mock(RowData.class));
    }
    return new ClosableIterator<RowData>() {
      int index = 0;

      @Override
      public boolean hasNext() {
        return index < rows.size();
      }

      @Override
      public RowData next() {
        return rows.get(index++);
      }

      @Override
      public void close() {
        // no-op for basic helper
      }
    };
  }

  // -------------------------------------------------------------------------
  //  limitIterator — core limit enforcement
  // -------------------------------------------------------------------------

  @Test
  public void testLimitIteratorEnforcesLimit() {
    TestableFunction fn = new TestableFunction(conf, 3);
    ClosableIterator<RowData> source = iteratorOf(10);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 3);

    int count = 0;
    while (limited.hasNext()) {
      limited.next();
      count++;
    }
    assertEquals(3, count, "Should stop after the limit");
    assertFalse(limited.hasNext(), "hasNext() must be false after limit is reached");
  }

  @Test
  public void testLimitIteratorWithLimitEqualToDataSize() {
    TestableFunction fn = new TestableFunction(conf, 5);
    ClosableIterator<RowData> source = iteratorOf(5);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 5);

    int count = 0;
    while (limited.hasNext()) {
      limited.next();
      count++;
    }
    assertEquals(5, count, "Should return all records when limit == data size");
  }

  @Test
  public void testLimitIteratorWithLimitExceedingDataSize() {
    TestableFunction fn = new TestableFunction(conf, 100);
    ClosableIterator<RowData> source = iteratorOf(5);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 100);

    int count = 0;
    while (limited.hasNext()) {
      limited.next();
      count++;
    }
    assertEquals(5, count, "Should return all records when limit > data size");
  }

  @Test
  public void testLimitIteratorWithLimitOne() {
    TestableFunction fn = new TestableFunction(conf, 1);
    ClosableIterator<RowData> source = iteratorOf(50);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 1);

    assertTrue(limited.hasNext());
    limited.next();
    assertFalse(limited.hasNext(), "hasNext() must be false after the single-record limit");
  }

  @Test
  public void testLimitIteratorHasNextReturnsFalseImmediatelyWithZeroLimit() {
    // limitIterator(iter, 0): currentReadCount (0) < limit (0) is false → always returns false
    TestableFunction fn = new TestableFunction(conf, 0);
    ClosableIterator<RowData> source = iteratorOf(10);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 0);

    assertFalse(limited.hasNext(), "With limit=0, hasNext() must be false from the start");
  }

  @Test
  public void testLimitIteratorNextIncrementsCounterCorrectly() {
    // Verify next() is counted: after exactly N calls, hasNext() goes false.
    TestableFunction fn = new TestableFunction(conf, 4);
    ClosableIterator<RowData> source = iteratorOf(10);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 4);

    for (int i = 0; i < 4; i++) {
      assertTrue(limited.hasNext(), "hasNext() should be true before limit is reached (call " + (i + 1) + ")");
      limited.next();
    }
    assertFalse(limited.hasNext(), "hasNext() must be false after exactly limit records are read");
  }

  @Test
  public void testLimitIteratorReturnsSameRecordsAsSource() {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(mock(RowData.class));
    }
    ClosableIterator<RowData> source = new ClosableIterator<RowData>() {
      int index = 0;

      @Override
      public boolean hasNext() {
        return index < rows.size();
      }

      @Override
      public RowData next() {
        return rows.get(index++);
      }

      @Override
      public void close() {
      }
    };

    TestableFunction fn = new TestableFunction(conf, 3);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 3);

    List<RowData> result = new ArrayList<>();
    while (limited.hasNext()) {
      result.add(limited.next());
    }

    assertEquals(3, result.size());
    assertSame(rows.get(0), result.get(0), "First record identity should match");
    assertSame(rows.get(1), result.get(1), "Second record identity should match");
    assertSame(rows.get(2), result.get(2), "Third record identity should match");
  }

  // -------------------------------------------------------------------------
  //  limitIterator — close() delegation
  // -------------------------------------------------------------------------

  @Test
  public void testLimitIteratorCloseDelegatestoInnerIterator() {
    AtomicBoolean closed = new AtomicBoolean(false);
    ClosableIterator<RowData> source = new ClosableIterator<RowData>() {

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public RowData next() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };

    TestableFunction fn = new TestableFunction(conf, 5);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 5);
    limited.close();

    assertTrue(closed.get(), "close() must delegate to the inner iterator");
  }

  @Test
  public void testLimitIteratorCloseCanBeCalledBeforeExhausting() {
    AtomicBoolean closed = new AtomicBoolean(false);
    ClosableIterator<RowData> source = new ClosableIterator<RowData>() {
      int count = 0;

      @Override
      public boolean hasNext() {
        return count < 100;
      }

      @Override
      public RowData next() {
        count++;
        return mock(RowData.class);
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };

    TestableFunction fn = new TestableFunction(conf, 10);
    ClosableIterator<RowData> limited = fn.wrapWithLimit(source, 10);

    // Read a few, then close early
    limited.next();
    limited.next();
    limited.close();

    assertTrue(closed.get(), "Closing mid-iteration must still delegate close() to inner iterator");
  }

  // -------------------------------------------------------------------------
  //  getHadoopConf — lazy initialization
  // -------------------------------------------------------------------------

  @Test
  public void testGetHadoopConfReturnsNonNull() {
    TestableFunction fn = new TestableFunction(conf, NO_LIMIT);
    assertNotNull(fn.hadoopConf(), "getHadoopConf() must return a non-null Configuration");
  }

  @Test
  public void testGetHadoopConfReturnsSameInstanceOnRepeatedCalls() {
    TestableFunction fn = new TestableFunction(conf, NO_LIMIT);
    org.apache.hadoop.conf.Configuration first = fn.hadoopConf();
    org.apache.hadoop.conf.Configuration second = fn.hadoopConf();
    assertSame(first, second, "getHadoopConf() must return the same cached instance on subsequent calls");
  }
}
