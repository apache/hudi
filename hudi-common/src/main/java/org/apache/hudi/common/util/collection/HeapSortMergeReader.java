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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static org.apache.hudi.common.util.collection.SortedAppendOnlyExternalSpillableMap.DEFAULT_PROGRESS_LOG_INTERVAL_NUM;

public class HeapSortMergeReader<K, V> implements SortMergeReader<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(HeapSortMergeReader.class);

  private final HoodieTimer readTimer = HoodieTimer.create();

  private final List<SortedEntryReader<Pair<K, V>>> readers;

  private final Comparator<V> comparator;

  private final CombineFunc<K, V, V> combineFunc;

  private final PriorityQueue<Element<Pair<K, V>>> minHeap;

  private Option<Aggregator<K, V, V>> aggregator = Option.empty();

  private long timeTakenToReadAllRecords;
  // TODO: add merge function for combine same key records

  private boolean init = false;
  private boolean closed = false;
  private boolean isEOF = false;

  private long readRawCount = 0;
  private long readCombinedCount = 0;

  public HeapSortMergeReader(List<SortedEntryReader<Pair<K, V>>> readers, Comparator<V> comparator, CombineFunc<K, V, V> combineFunc) {
    this.readers = readers;
    this.comparator = comparator;
    this.combineFunc = combineFunc;
    this.minHeap = new PriorityQueue<>(Comparator.comparing(e -> e.head.getValue(), comparator));
  }

  private void init() {
    readTimer.startTimer();
    for (SortedEntryReader<Pair<K, V>> reader : readers) {
      while (true) {
        Iterator<Pair<K, V>> iter = reader.readBatch();
        if (iter == null) {
          try {
            reader.close();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to close reader", e);
          }
          break;
        }
        if (!iter.hasNext()) {
          reader.releaseBatch();
        } else {
          Pair<K, V> record = iter.next();
          minHeap.offer(Element.of(record, iter, reader));
          break;
        }
      }
    }
    init = true;
  }

  public Pair<K, V> tick() {
    while (!minHeap.isEmpty()) {
      Element<Pair<K, V>> polled = minHeap.poll();
      Pair<K, V> record = polled.head;
      try {
        if (polled.update()) {
          // element still has next record, update it in heap
          minHeap.offer(polled);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to update element", e);
      }
      if (++readRawCount % DEFAULT_PROGRESS_LOG_INTERVAL_NUM == 0) {
        // Log every 1_000_000 records
        LOG.info("HeapSortMergeReader reading progress: ------------ read {} entries, {} combined entries ------------", readRawCount, readCombinedCount);
      }

      if (aggregator.isEmpty()) {
        aggregator = Option.of(Aggregator.init(combineFunc, record.getKey(), record.getValue()));
        continue;
      }
      Pair<K, V> output = aggregator.get().combine(record.getKey(), record.getValue()).orElse(null);
      if (output != null) {
        return output;
      }
    }

    if (aggregator.isEmpty()) {
      return null;
    }
    Pair<K, V> current = aggregator.get().current();
    aggregator = Option.empty();
    return current;
  }

  @Override
  public boolean hasNext() {
    if (!init) {
      init();
    }
    if (isEOF) {
      return false;
    }
    boolean hasNext = !minHeap.isEmpty() || aggregator.isPresent();
    if (!hasNext) {
      // reach end
      isEOF = true;
      timeTakenToReadAllRecords = readTimer.endTimer();
      LOG.info("HeapSortMergeReader read all {} records and get {} combined records in {} ms", readRawCount, readCombinedCount, timeTakenToReadAllRecords);
    }
    return hasNext;
  }

  @Override
  public V next() {
    if (!hasNext()) {
      return null;
    }
    Pair<K, V> tick = tick();
    readCombinedCount++;
    return tick.getValue();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    LOG.info("HeapSortMergeReader closed, read {} records and get {} combined records in {} ms", readRawCount, readCombinedCount, timeTakenToReadAllRecords);
    try {
      for (SortedEntryReader reader : readers) {
        reader.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close reader", e);
    }
    minHeap.clear();
    aggregator = Option.empty();
    closed = true;
  }

  @Override
  public long getCombinedRecordsNum() {
    return readCombinedCount;
  }

  private static final class Element<R> {
    private R head;
    private Iterator<R> iterator;
    private SortedEntryReader<R> reader;

    private Element(R head, Iterator<R> iterator, SortedEntryReader<R> reader) {
      this.head = head;
      this.iterator = iterator;
      this.reader = reader;
    }

    public boolean update() throws IOException {
      while (true) {
        if (iterator.hasNext()) {
          R next = iterator.next();
          if (next != null) {
            head = next;
            return true;
          }
        }
        // check out to next batch
        reader.releaseBatch();
        iterator = reader.readBatch();
        if (iterator == null) {
          reader.close();
          return false;
        }
      }
    }

    public static <R> Element<R> of(R head, Iterator<R> iterator, SortedEntryReader<R> reader) {
      return new Element<>(head, iterator, reader);
    }
  }
}