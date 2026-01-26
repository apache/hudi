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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Default batch reader implementation.
 *
 * @param <T> record type
 */
public class DefaultHoodieBatchReader<T> implements HoodieBatchReader<T> {

  private final int batchSize;

  public DefaultHoodieBatchReader(Configuration configuration) {
    this.batchSize = configuration.get(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT);
    ValidationUtils.checkArgument(batchSize > 0, "source.fetch-batch-record-count can only be positive.");
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<T>>> batch(
      HoodieSourceSplit split, ClosableIterator<T> inputIterator) {
    return new ListBatchIterator(split, inputIterator);
  }

  private class ListBatchIterator implements CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<T>>> {
    private final ClosableIterator<T> inputIterator;
    private final HoodieSourceSplit split;
    private long consumed;

    ListBatchIterator(HoodieSourceSplit split, ClosableIterator<T> inputIterator) {
      this.inputIterator = inputIterator;
      this.split = split;
      this.consumed = split.getConsumed();
      seek();
    }

    @Override
    public boolean hasNext() {
      return inputIterator.hasNext();
    }

    @Override
    public RecordsWithSplitIds<HoodieRecordWithPosition<T>> next() {
      if (!inputIterator.hasNext()) {
        throw new NoSuchElementException();
      }

      final List<T> batch = new ArrayList<>(batchSize);
      int recordCount = 0;
      while (inputIterator.hasNext() && recordCount < batchSize) {
        T nextRecord = inputIterator.next();
        consumed++;
        batch.add(nextRecord);
        recordCount++;
      }

      return HoodieBatchRecords.forRecords(
          split.splitId(), ClosableIterator.wrap(batch.iterator()), split.getFileOffset(), consumed - recordCount);
    }

    @Override
    public void close() throws IOException {
      if (inputIterator != null) {
        inputIterator.close();
      }
    }

    private void seek() {
      for (long i = 0; i < split.getConsumed(); ++i) {
        if (inputIterator.hasNext()) {
          inputIterator.next();
        } else {
          throw new IllegalStateException(
              String.format(
                  "Invalid starting record offset %d for split %s",
                  split.getConsumed(),
                  split.splitId()));
        }
      }
    }
  }
}
