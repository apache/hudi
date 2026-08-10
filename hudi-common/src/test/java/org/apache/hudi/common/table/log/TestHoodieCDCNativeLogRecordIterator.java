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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.util.collection.ClosableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCDCNativeLogRecordIterator {

  @Test
  public void testIteratesAcrossFilesAndClosesExhaustedIterators() {
    Map<String, TrackingClosableIterator<String>> fileIterators = new LinkedHashMap<>();
    fileIterators.put("file1", new TrackingClosableIterator<>(Collections.singletonList("i:key1:before1:after1")));
    fileIterators.put("file2", new TrackingClosableIterator<>(Collections.emptyList()));
    fileIterators.put("file3", new TrackingClosableIterator<>(Arrays.asList("u:key2:before2:after2", "d:key3:before3:null")));

    HoodieCDCNativeLogRecordIterator<String> iterator = new HoodieCDCNativeLogRecordIterator<>(
        fileIterators.keySet().iterator(),
        fileIterators::get,
        accessor());

    assertTrue(iterator.hasNext());
    HoodieCDCLogRecord<String> insertRecord = iterator.next();
    assertEquals("i", insertRecord.getOperation());
    assertEquals("key1", insertRecord.getRecordKey());
    assertEquals("after1", insertRecord.getEngineImage(3, 2));
    assertTrue(insertRecord.isNative());

    assertTrue(iterator.hasNext());
    HoodieCDCLogRecord<String> updateRecord = iterator.next();
    assertEquals("u", updateRecord.getOperation());
    assertEquals("key2", updateRecord.getRecordKey());
    assertEquals("before2", updateRecord.getEngineImage(2, 2));

    assertTrue(iterator.hasNext());
    HoodieCDCLogRecord<String> deleteRecord = iterator.next();
    assertEquals("d", deleteRecord.getOperation());
    assertEquals("key3", deleteRecord.getRecordKey());
    assertNull(deleteRecord.getEngineImage(3, 2));

    assertFalse(iterator.hasNext());
    assertTrue(fileIterators.get("file1").isClosed());
    assertTrue(fileIterators.get("file2").isClosed());
    assertTrue(fileIterators.get("file3").isClosed());
  }

  @Test
  public void testCloseClosesCurrentIterator() {
    Map<String, TrackingClosableIterator<String>> fileIterators = new LinkedHashMap<>();
    fileIterators.put("file1", new TrackingClosableIterator<>(Arrays.asList("i:key1:before1:after1", "u:key2:before2:after2")));

    HoodieCDCNativeLogRecordIterator<String> iterator = new HoodieCDCNativeLogRecordIterator<>(
        fileIterators.keySet().iterator(),
        fileIterators::get,
        accessor());

    assertTrue(iterator.hasNext());
    iterator.close();

    assertTrue(fileIterators.get("file1").isClosed());
  }

  @Test
  public void testRejectsNullNativeRecordIterator() {
    HoodieCDCNativeLogRecordIterator<String> iterator = new HoodieCDCNativeLogRecordIterator<>(
        Collections.singletonList("file1").iterator(),
        cdcFile -> null,
        accessor());

    assertThrows(IllegalStateException.class, iterator::hasNext);
  }

  @Test
  public void testEmptyIteratorThrowsOnNext() {
    HoodieCDCLogRecordIterator<String> iterator = HoodieCDCLogRecordIterator.empty();

    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  private static HoodieCDCEngineRecordAccessor<String> accessor() {
    return new HoodieCDCEngineRecordAccessor<String>() {
      @Override
      public String getOperation(String record) {
        return split(record)[0];
      }

      @Override
      public String getRecordKey(String record) {
        return split(record)[1];
      }

      @Override
      public String getImage(String record, int ordinal, int imageArity) {
        String image = split(record)[ordinal];
        return "null".equals(image) ? null : image;
      }
    };
  }

  private static String[] split(String record) {
    return record.split(":");
  }

  private static class TrackingClosableIterator<T> implements ClosableIterator<T> {
    private final Iterator<T> iterator;
    private boolean closed;

    private TrackingClosableIterator(Iterable<T> records) {
      this.iterator = records.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }

    @Override
    public void close() {
      closed = true;
    }

    private boolean isClosed() {
      return closed;
    }
  }
}
