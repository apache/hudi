/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;

class UnmergedFileGroupRecordBuffer<T> extends FileGroupRecordBuffer<T> {

  private final Deque<HoodieLogBlock> currentInstantLogBlocks;
  private final HoodieReadStats readStats;
  private ClosableIterator<T> recordIterator;

  UnmergedFileGroupRecordBuffer(
      HoodieReaderContext<T> readerContext,
      HoodieTableMetaClient hoodieTableMetaClient,
      RecordMergeMode recordMergeMode,
      PartialUpdateMode partialUpdateMode,
      IteratorMode iteratorMode,
      TypedProperties props,
      HoodieReadStats readStats) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, iteratorMode, props, Collections.emptyList(), null);
    this.readStats = readStats;
    this.currentInstantLogBlocks = new ArrayDeque<>();
  }

  @Override
  protected boolean doHasNext() {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Output from base file first.
    if (baseFileIterator.hasNext()) {
      nextRecord = bufferedRecordConverter.convert(readerContext.seal(baseFileIterator.next()));
      return true;
    }

    while ((recordIterator == null || !recordIterator.hasNext()) && !currentInstantLogBlocks.isEmpty()) {
      HoodieLogBlock logBlock = currentInstantLogBlocks.pop();
      if (logBlock instanceof HoodieDataBlock) {
        HoodieDataBlock dataBlock = (HoodieDataBlock) logBlock;
        Pair<ClosableIterator<T>, Schema> iteratorSchemaPair = getRecordsIterator(dataBlock, Option.empty());
        if (recordIterator != null) {
          recordIterator.close();
        }
        recordIterator = iteratorSchemaPair.getLeft();
      }
    }
    if (recordIterator == null || !recordIterator.hasNext()) {
      return false;
    }
    nextRecord = bufferedRecordConverter.convert(readerContext.seal(recordIterator.next()));
    readStats.incrementNumInserts();
    return true;
  }

  @Override
  public ClosableIterator<BufferedRecord<T>> getLogRecordIterator() {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.UNMERGED;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    this.currentInstantLogBlocks.add(dataBlock);
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable index) {
    // no-op
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
    // no-op
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable index) {
    // no-op
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public void close() {
    if (recordIterator != null) {
      recordIterator.close();
    }
    super.close();
  }
}
