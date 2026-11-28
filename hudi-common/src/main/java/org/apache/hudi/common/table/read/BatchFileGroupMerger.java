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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.buffer.BatchMatcher;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * Merges the base file batches of a file slice with its buffered log records under commit-time or event-time
 * ordering. A base row with no log record stays in its batch, which is returned as a view of the rows it keeps; rows a
 * log record replaces and log records with no base row are returned as separate batches built from rows.
 *
 * @param <T> the engine row type
 * @param <B> the engine batch type
 */
class BatchFileGroupMerger<T, B> implements ClosableIterator<B> {

  static final int OUTPUT_BATCH_ROWS = 4096;

  private final BatchSupport<T, B> batchSupport;
  private final ClosableIterator<B> baseBatches;
  private BatchMatcher<T> matcher;
  private final BufferedRecordMerger<T> merger;
  private final boolean commitTimeOrdering;
  private final RecordContext<T> recordContext;
  private final HoodieSchema baseReadSchema;
  private final String[] orderingFieldNames;
  private final int numOutputColumns;
  private final int matchColumn;
  private final UnaryOperator<T> logToOutput;
  private final HoodieSchema outputSchema;
  private final ArrayDeque<B> ready = new ArrayDeque<>();
  private final List<T> pendingRows = new ArrayList<>();
  // The log records no base row took, read one output batch at a time once the base batches are done
  private Iterator<BufferedRecord<T>> leftovers;
  private boolean leftoversDone;

  /**
   * @param baseReadSchema the output schema followed by the matcher's column and, under event-time ordering, the
   *                       ordering fields the output schema lacks
   * @param logSchema      the schema of the buffered log records
   * @param outputSchema   the output schema
   */
  BatchFileGroupMerger(BatchSupport<T, B> batchSupport,
                       ClosableIterator<B> baseBatches,
                       BatchMatcher<T> matcher,
                       BufferedRecordMerger<T> merger,
                       RecordMergeMode mergeMode,
                       RecordContext<T> recordContext,
                       HoodieSchema baseReadSchema,
                       HoodieSchema logSchema,
                       HoodieSchema outputSchema,
                       List<String> orderingFieldNames) {
    this.batchSupport = batchSupport;
    this.baseBatches = baseBatches;
    this.matcher = matcher;
    this.merger = merger;
    this.commitTimeOrdering = mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING;
    this.recordContext = recordContext;
    this.baseReadSchema = baseReadSchema;
    this.orderingFieldNames = orderingFieldNames.toArray(new String[0]);
    this.numOutputColumns = outputSchema.getFields().size();
    this.matchColumn = baseReadSchema.getField(matcher.columnName()).get().pos();
    this.logToOutput = recordContext.projectRecord(logSchema, outputSchema);
    this.outputSchema = outputSchema;
  }

  @Override
  public boolean hasNext() {
    while (ready.isEmpty() && !(leftoversDone && pendingRows.isEmpty())) {
      if (leftovers == null && baseBatches.hasNext()) {
        mergeBaseBatch(baseBatches.next());
      } else if (!leftoversDone) {
        if (leftovers == null) {
          leftovers = matcher.leftovers();
        }
        while (ready.isEmpty() && leftovers.hasNext()) {
          addLogRow(leftovers.next());
        }
        leftoversDone = !leftovers.hasNext();
      } else {
        flushRows();
      }
    }
    return !ready.isEmpty();
  }

  @Override
  public B next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    // The base batch a view was taken of is only advanced once the view has been returned and consumed
    return ready.poll();
  }

  private void addLogRow(BufferedRecord<T> record) {
    if (!record.isDelete() && record.getRecord() != null) {
      pendingRows.add(recordContext.seal(outputSchema, logToOutput.apply(record.getRecord())));
      if (pendingRows.size() >= OUTPUT_BATCH_ROWS) {
        flushRows();
      }
    }
  }

  private void flushRows() {
    ready.add(batchSupport.toBatch(new ArrayList<>(pendingRows)));
    pendingRows.clear();
  }

  private void mergeBaseBatch(B batch) {
    int numRows = batchSupport.numRows(batch);
    if (numRows == 0) {
      return;
    }
    int numReadColumns = baseReadSchema.getFields().size();
    if (matcher.untouched(batchSupport, batch, matchColumn, numRows)) {
      ready.add(batchSupport.select(batch, numOutputColumns, numReadColumns, null, numRows));
      return;
    }
    int[] keep = null;
    int kept = 0;
    for (int row = 0; row < numRows; row++) {
      BufferedRecord<T> logRecord = matcher.take(batchSupport, batch, matchColumn, row);
      if (logRecord == null || keepsBaseRow(batch, row, logRecord)) {
        if (keep != null) {
          keep[kept] = row;
        }
        kept++;
      } else if (keep == null) {
        // The first row the batch drops: start tracking the rows it keeps
        keep = new int[numRows];
        for (int i = 0; i < kept; i++) {
          keep[i] = i;
        }
      }
    }
    if (kept > 0) {
      ready.add(batchSupport.select(batch, numOutputColumns, numReadColumns, keep, kept));
    }
    if (pendingRows.size() >= OUTPUT_BATCH_ROWS) {
      flushRows();
    }
  }

  /**
   * Merges a base row with its log record; returns whether the base row wins, else queues the winning log row unless
   * it is a delete.
   */
  private boolean keepsBaseRow(B batch, int row, BufferedRecord<T> logRecord) {
    BufferedRecord<T> result;
    if (commitTimeOrdering) {
      // A log record always supersedes the base row
      result = logRecord;
    } else {
      T baseRow = batchSupport.getRow(batch, row);
      Comparable orderingValue = recordContext.getOrderingValue(baseRow, baseReadSchema, orderingFieldNames);
      BufferedRecord<T> baseRecord = BufferedRecords.fromEngineRecord(baseRow, baseReadSchema, recordContext, orderingValue, null, false);
      try {
        result = merger.finalMerge(baseRecord, logRecord);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to merge a base row with its log record", e);
      }
      if (result == baseRecord) {
        return true;
      }
    }
    if (!result.isDelete() && result.getRecord() != null) {
      pendingRows.add(recordContext.seal(outputSchema, logToOutput.apply(result.getRecord())));
    }
    return false;
  }

  @Override
  public void close() {
    baseBatches.close();
    // The engine may keep a closed iterator until its task ends: drop the log records
    matcher = null;
    leftovers = null;
    leftoversDone = true;
    ready.clear();
    pendingRows.clear();
  }
}
