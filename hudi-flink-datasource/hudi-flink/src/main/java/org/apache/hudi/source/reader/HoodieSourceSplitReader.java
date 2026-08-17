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

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkStreamReadMetrics;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * The split reader of Hoodie source.
 *
 * <p>Each call to {@link #fetch()} returns one bounded minibatch of the currently open split, so a
 * single split spans multiple {@code fetch()} calls. When the open split is exhausted its resources
 * are closed on this (split-fetcher) thread and a finish signal ({@code finishedSplits()}) is
 * returned so Flink's {@code SourceReaderBase} can advance / reach end-of-input. All record reading
 * and resource teardown for a split therefore happen on the same thread, which removes the
 * cross-thread teardown race a live-iterator batch would be exposed to.
 *
 * @param <T> record type
 */
@Slf4j
public class HoodieSourceSplitReader<T> implements SplitReader<HoodieRecordWithPosition<T>, HoodieSourceSplit> {
  // Upper bound on the number of records materialized per fetch() call (one minibatch). Kept as a
  // fixed constant for now (mirrors RecordIterators.DEFAULT_BATCH_SIZE); it can be promoted to a
  // Flink option later if a tunable per-fetch bound is ever needed.
  private static final int DEFAULT_MINI_BATCH_SIZE = 2048;

  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final Queue<HoodieSourceSplit> splits;
  private final FlinkStreamReadMetrics readerMetrics;
  private final SplitReaderFunction<T> readerFunction;
  private final Option<RecordLimiter> recordLimiter;
  private transient HoodieSourceSplit currentSplit;
  // Set by wakeUp() (possibly from another thread) to stop the in-flight minibatch drain promptly.
  // Reset at the start of every fetch(), so it only ever means "a wakeUp() landed during THIS fetch".
  private volatile boolean wokenUp;

  public HoodieSourceSplitReader(
      String tableName,
      SourceReaderContext context,
      SerializableSupplier<SplitReaderFunction<T>> readerFunctionSupplier,
      SerializableComparator<HoodieSourceSplit> splitComparator,
      Option<RecordLimiter> recordLimiter) {
    this.splitComparator = splitComparator;
    this.splits = new ArrayDeque<>();
    // Flink can start a new fetcher before the previous idle fetcher finishes closing. Supply a
    // fresh stateful cursor for each split reader so the old fetcher's close cannot affect the new one.
    this.readerFunction = readerFunctionSupplier.get();
    this.recordLimiter = recordLimiter;
    this.readerMetrics = new FlinkStreamReadMetrics(context.metricGroup(), tableName);
    this.readerMetrics.registerMetrics();
  }

  @Override
  public RecordsWithSplitIds<HoodieRecordWithPosition<T>> fetch() throws IOException {
    // A wakeUp() only needs to unblock an in-progress fetch(); Flink's SplitFetcher drives shutdown
    // off its own 'closed' flag (set before wakeUp() and checked before the next fetch()), not off a
    // lasting wakeUp effect. Start each cycle from a clean flag so the drain below reacts only to a
    // wakeUp that lands during THIS fetch.
    wokenUp = false;
    if (currentSplit == null) {
      // Limit already satisfied: drain any remaining locally-queued splits as immediately finished
      // so that Flink's SourceReaderBase can reach end-of-input cleanly.
      if (recordLimiter.map(RecordLimiter::isLimitReached).orElse(false)) {
        return drainRemainingAsSplitsFinished();
      }
      HoodieSourceSplit nextSplit = splits.poll();
      if (nextSplit == null) {
        // return an empty result, which will lead to split fetch to be idle.
        // SplitFetcherManager will then close idle fetcher.
        return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
      }
      currentSplit = nextSplit;
      readerFunction.open(currentSplit);
    }

    // Read the next bounded minibatch of the open split, unless the global limit is already reached.
    if (!recordLimiter.map(RecordLimiter::isLimitReached).orElse(false)) {
      BatchRecords<T> batch = readerFunction.readBatch(currentSplit, DEFAULT_MINI_BATCH_SIZE, () -> wokenUp);
      if (batch != null) {
        // Partial (woken) or full minibatch; the split is not finished either way.
        return recordLimiter.map(rl -> rl.wrap(batch)).orElse(batch);
      }
      if (wokenUp) {
        // Woken before any record was buffered: return promptly WITHOUT finishing or closing the
        // split, so it resumes on the next fetch(), or the fetcher observes shutdown and closes it
        // on this (split-fetcher) thread. This branch must stay inside the !isLimitReached block:
        // a wakeUp coinciding with the limit-reached path below must still finish the split.
        return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
      }
    }

    // Split exhausted (or the limit was reached mid-split): close its resources on this
    // (split-fetcher) thread first, then emit the finish signal so SourceReaderBase can advance.
    readerFunction.closeCurrentSplit();
    return finishSplit();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<HoodieSourceSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          String.format("Unsupported split change: %s", splitsChange.getClass()));
    }

    if (splitComparator != null) {
      List<HoodieSourceSplit> newSplits = new ArrayList<>(splitsChange.splits());
      newSplits.sort(splitComparator);
      log.info("Add {} splits to reader: {}", newSplits.size(), newSplits);
      splits.addAll(newSplits);
    } else {
      log.info("Add {} splits to reader", splitsChange.splits().size());
      splits.addAll(splitsChange.splits());
    }
  }

  @Override
  public void wakeUp() {
    // Flink calls this (while holding SplitFetcher.lock) to unblock a fetch() that is draining a
    // minibatch, e.g. on shutdown. Keep it a non-blocking plain volatile write; the drain loop in
    // readBatch polls the flag between records and returns promptly. The actual resource teardown
    // still happens on the split-fetcher thread via close()/closeCurrentSplit().
    wokenUp = true;
  }

  /**
   * SourceSplitReader only reads splits sequentially. When waiting for watermark alignment
   * the SourceOperator will stop processing and recycling the fetched batches. Based on this the
   * {@code pauseOrResumeSplits} is left empty.
   * @param splitsToPause splits to pause
   * @param splitsToResume splits to resume
   */
  @Override
  public void pauseOrResumeSplits(
      Collection<HoodieSourceSplit> splitsToPause,
      Collection<HoodieSourceSplit> splitsToResume) {
  }

  @Override
  public void close() throws Exception {
    readerFunction.close();
  }

  private RecordsWithSplitIds<HoodieRecordWithPosition<T>> finishSplit() {
    RecordsWithSplitIds<HoodieRecordWithPosition<T>> records = BatchRecords.lastBatchRecords(currentSplit.splitId());
    currentSplit = null;
    return records;
  }

  /**
   * Returns a batch that immediately marks all locally-queued splits as finished, allowing
   * Flink's SourceReaderBase to reach end-of-input without reading any more records.
   */
  private RecordsWithSplitIds<HoodieRecordWithPosition<T>> drainRemainingAsSplitsFinished() {
    Set<String> finishedIds = new HashSet<>();
    HoodieSourceSplit split;
    while ((split = splits.poll()) != null) {
      finishedIds.add(split.splitId());
    }
    return new RecordsBySplits<>(Collections.emptyMap(), finishedIds);
  }
}
