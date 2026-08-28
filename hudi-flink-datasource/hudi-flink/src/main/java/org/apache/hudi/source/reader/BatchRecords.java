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

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link RecordsWithSplitIds} backed by a materialized, bounded minibatch of
 * records for a single split.
 *
 * <p>The records are already drained (and copied) on the split-fetcher thread before this batch is
 * enqueued, so {@link #nextRecordFromSplit()} only walks an in-memory list and holds no live I/O
 * resource. Record reading and resource teardown therefore stay on the same (split-fetcher) thread,
 * which removes the cross-thread teardown race a live-iterator batch would be exposed to. The
 * underlying iterator and its I/O resources are owned and closed by the reader function (see
 * {@code AbstractSplitReaderFunction#closeCurrentSplit}).
 *
 * @param <T> record type
 */
public class BatchRecords<T> implements RecordsWithSplitIds<HoodieRecordWithPosition<T>> {
  private String splitId;
  private String nextSplitId;
  private final List<T> records;
  private final Set<String> finishedSplits;
  private final HoodieRecordWithPosition<T> recordAndPosition;

  // points to the current read position within the records list
  private int index;

  BatchRecords(
      String splitId,
      List<T> records,
      int fileOffset,
      long startingRecordOffset,
      Set<String> finishedSplits) {
    ValidationUtils.checkArgument(
        finishedSplits != null, "finishedSplits can be empty but not null");
    ValidationUtils.checkArgument(
        records != null, "records can be empty but not null");

    this.splitId = splitId;
    this.nextSplitId = splitId;
    this.records = records;
    this.finishedSplits = finishedSplits;
    this.recordAndPosition = new HoodieRecordWithPosition<>();
    this.recordAndPosition.set(null, fileOffset, startingRecordOffset);
    this.index = 0;
  }

  @Nullable
  @Override
  public String nextSplit() {
    if (splitId.equals(nextSplitId)) {
      // set the nextSplitId to null to indicate no more splits
      // this class only contains records for one split
      nextSplitId = null;
      return splitId;
    } else {
      return nextSplitId;
    }
  }

  @Nullable
  @Override
  public HoodieRecordWithPosition<T> nextRecordFromSplit() {
    if (index < records.size()) {
      recordAndPosition.record(records.get(index));
      index++;
      return recordAndPosition;
    } else {
      return null;
    }
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  @Override
  public void recycle() {
    // No-op: the minibatch is fully materialized, so there is no live iterator or I/O resource to
    // release here. The underlying reader is owned and closed by the reader function on the
    // split-fetcher thread (AbstractSplitReaderFunction#closeCurrentSplit).
  }

  public static <T> BatchRecords<T> forRecords(
      String splitId, List<T> records, int fileOffset, long startingRecordOffset) {
    return new BatchRecords<>(splitId, records, fileOffset, startingRecordOffset, Set.of());
  }

  public static <T> RecordsWithSplitIds<HoodieRecordWithPosition<T>> lastBatchRecords(String splitId) {
    // Pre-populate finishedSplits with splitId so that FetchTask calls splitFinishedCallback
    // immediately after enqueueing the batch. This removes the split from
    // SplitFetcher.assignedSplits, causing the fetcher to idle and invoke
    // elementsQueue.notifyAvailable(), which is required to drive the END_OF_INPUT signal
    // in SourceReaderBase for bounded (batch) reads.
    return new RecordsBySplits<>(Collections.emptyMap(), Set.of(splitId));
  }
}
