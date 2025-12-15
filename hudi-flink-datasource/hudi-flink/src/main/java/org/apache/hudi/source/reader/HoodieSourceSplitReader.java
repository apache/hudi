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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * The split reader of Hoodie source.
 *
 * @param <T> record type
 */
public class HoodieSourceSplitReader<T> implements SplitReader<HoodieRecordWithPosition<T>, HoodieSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSourceSplitReader.class);

  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final SplitReaderFunction<T> readerFunction;
  private final int indexOfSubTask;
  private final Queue<HoodieSourceSplit> splits;

  private HoodieSourceSplit currentSplit;
  private String currentSplitId;

  public HoodieSourceSplitReader(
      SourceReaderContext context,
      SplitReaderFunction<T> readerFunction,
      SerializableComparator<HoodieSourceSplit> splitComparator) {
    this.splitComparator = splitComparator;
    this.readerFunction = readerFunction;
    this.indexOfSubTask = context.getIndexOfSubtask();
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<HoodieRecordWithPosition<T>> fetch() throws IOException {
    HoodieSourceSplit nextSplit = splits.poll();
    if (nextSplit != null) {
      currentSplit = nextSplit;
      currentSplitId = nextSplit.splitId();
      return readerFunction.read(currentSplit);
    } else {
      // return an empty result, which will lead to split fetch to be idle.
      // SplitFetcherManager will then close idle fetcher.
      return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
    }
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
      LOG.info("Add {} splits to reader: {}", newSplits.size(), newSplits);
      splits.addAll(newSplits);
    } else {
      LOG.info("Add {} splits to reader", splitsChange.splits().size());
      splits.addAll(splitsChange.splits());
    }
  }

  @Override
  public void wakeUp() {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    currentSplitId = null;
  }

  @Override
  public void pauseOrResumeSplits(
      Collection<HoodieSourceSplit> splitsToPause,
      Collection<HoodieSourceSplit> splitsToResume) {
    // SourceSplitReader only reads splits sequentially. When waiting for watermark alignment
    // the SourceOperator will stop processing and recycling the fetched batches. This exhausts the
    // {@link ArrayPoolDataIteratorBatcher#pool} and the `currentReader.next()` call will be
    // blocked even without split-level watermark alignment. Based on this the
    // `pauseOrResumeSplits` and the `wakeUp` are left empty.
  }
}
