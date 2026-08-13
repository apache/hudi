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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.collection.ClosableIterator;

import lombok.extern.slf4j.Slf4j;

/**
 * Counts the records a record index shard lookup yields, and reports one
 * {@link RecordIndexShardLookupStats} when the underlying iterator is exhausted or closed.
 *
 * <p>Counting on exhaustion rather than eagerly is what keeps the lookup lazy. Callers are expected
 * to consume the iterator fully — every caller in this repository does, via a collect or a forEach.
 * A caller that abandons it early under-reports hits; it never over-reports, and never fails.
 *
 * @param <T> element type of the wrapped lookup result.
 */
@Slf4j
class RecordIndexLookupStatsCollectingIterator<T> implements ClosableIterator<T> {

  private final ClosableIterator<T> delegate;
  private final RecordIndexLookupStatsCollector collector;
  private final int shardIndex;
  private final String fileGroupId;
  private final long keysSubmitted;
  private final long logFilesRead;
  private final long bytesInShard;
  private final long startMillis;

  private long hits;
  private boolean reported;

  RecordIndexLookupStatsCollectingIterator(ClosableIterator<T> delegate,
                                           RecordIndexLookupStatsCollector collector,
                                           int shardIndex,
                                           String fileGroupId,
                                           long keysSubmitted,
                                           long logFilesRead,
                                           long bytesInShard,
                                           long startMillis) {
    this.delegate = delegate;
    this.collector = collector;
    this.shardIndex = shardIndex;
    this.fileGroupId = fileGroupId;
    this.keysSubmitted = keysSubmitted;
    this.logFilesRead = logFilesRead;
    this.bytesInShard = bytesInShard;
    this.startMillis = startMillis;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = delegate.hasNext();
    if (!hasNext) {
      report();
    }
    return hasNext;
  }

  @Override
  public T next() {
    T next = delegate.next();
    hits++;
    return next;
  }

  @Override
  public void close() {
    try {
      report();
    } finally {
      delegate.close();
    }
  }

  /**
   * Emits the stats exactly once, whichever of exhaustion or close happens first.
   */
  private void report() {
    if (reported) {
      return;
    }
    reported = true;
    try {
      collector.collect(new RecordIndexShardLookupStats(shardIndex, fileGroupId, keysSubmitted,
          Math.min(hits, keysSubmitted), logFilesRead, bytesInShard,
          System.currentTimeMillis() - startMillis));
    } catch (Exception e) {
      // Instrumentation must never fail a write.
      log.warn("Failed to collect record index lookup stats for shard {} (file group {})",
          shardIndex, fileGroupId, e);
    }
  }
}
