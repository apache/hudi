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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Abstract implementation of {@link SplitReaderFunction} that provides the per-split cursor
 * machinery shared by all reader functions.
 *
 * <p>A subclass only supplies how to create the record iterator for a split
 * ({@link #createRecordIterator(HoodieSourceSplit)}) and the {@link RowType} of the records it
 * produces ({@link #producedRowType()}); this base drives {@link #open(HoodieSourceSplit)},
 * {@link #readBatch(HoodieSourceSplit, int, java.util.function.BooleanSupplier)}, {@link #closeCurrentSplit()} and {@link #close()},
 * all on the single split-fetcher thread.
 *
 * <p>Because Flink's columnar readers (and the CDC/MOR row projections) return the same reused
 * {@link RowData} object on every {@code next()}, {@link #readBatch} copies each record before
 * buffering it - materializing raw references would make every entry in a minibatch alias the last
 * row. {@link RowDataSerializer#copy(RowData)} preserves the record's {@code RowKind}, so no
 * re-apply is needed.
 */
public abstract class AbstractSplitReaderFunction implements SplitReaderFunction<RowData> {

  protected final Configuration conf;
  protected final InternalSchemaManager internalSchemaManager;
  protected final List<ExpressionPredicates.Predicate> predicates;
  protected final boolean emitDelete;
  private transient HoodieWriteConfig writeConfig;
  private transient org.apache.hadoop.conf.Configuration hadoopConf;
  private transient RowDataSerializer copySerializer;

  // Per-split cursor state (split-fetcher thread only).
  private transient ClosableIterator<RowData> currentIterator;
  private transient long nextRecordOffset;

  public AbstractSplitReaderFunction(
      Configuration conf,
      List<ExpressionPredicates.Predicate> predicates,
      InternalSchemaManager internalSchemaManager,
      boolean emitDelete) {
    this.conf = conf;
    this.predicates = predicates;
    this.internalSchemaManager = internalSchemaManager;
    this.emitDelete = emitDelete;
  }

  /**
   * Creates the record iterator (and its underlying I/O resources) for {@code split}. Closing the
   * returned iterator must release all of those resources.
   */
  protected abstract ClosableIterator<RowData> createRecordIterator(HoodieSourceSplit split);

  /** The {@link RowType} of the records produced by {@link #createRecordIterator}. */
  protected abstract RowType producedRowType();

  @Override
  public void open(HoodieSourceSplit split) {
    this.currentIterator = createRecordIterator(split);
    try {
      // Skip the records already emitted before the last checkpoint so a recovered split resumes at
      // the right position; matches the validation the old BatchRecords#seek performed.
      long consumed = split.getConsumed();
      for (long i = 0; i < consumed; i++) {
        if (currentIterator.hasNext()) {
          currentIterator.next();
        } else {
          throw new IllegalStateException(
              String.format("Invalid starting record offset %d for split %s", consumed, split.splitId()));
        }
      }
      this.nextRecordOffset = consumed;
    } catch (RuntimeException | Error e) {
      // Close on failure so the split's I/O resources are released even if the resume-skip fails.
      closeCurrentSplit();
      throw e;
    }
  }

  @Override
  public BatchRecords<RowData> readBatch(HoodieSourceSplit split, int batchSize, BooleanSupplier wakeupSignal) {
    ValidationUtils.checkState(currentIterator != null,
        "readBatch called before open for split " + split.splitId());
    RowDataSerializer serializer = getCopySerializer();
    List<RowData> buffer = new ArrayList<>();
    try {
      // Poll wakeupSignal between records so a wakeUp() lands promptly: materialization stops early
      // and whatever is buffered so far is returned as a partial minibatch (null if nothing yet).
      while (buffer.size() < batchSize && !wakeupSignal.getAsBoolean() && currentIterator.hasNext()) {
        RowData next = currentIterator.next();
        buffer.add(serializer.copy(next));
      }
    } catch (RuntimeException | Error e) {
      // Close on failure so the split's I/O resources are released even if a mid-drain read fails.
      closeCurrentSplit();
      throw e;
    }
    if (buffer.isEmpty()) {
      return null;
    }
    long startingRecordOffset = nextRecordOffset;
    nextRecordOffset += buffer.size();
    return BatchRecords.forRecords(split.splitId(), buffer, split.getFileOffset(), startingRecordOffset);
  }

  @Override
  public void closeCurrentSplit() {
    if (currentIterator != null) {
      currentIterator.close();
      currentIterator = null;
    }
    nextRecordOffset = 0;
  }

  @Override
  public void close() throws Exception {
    closeCurrentSplit();
  }

  private RowDataSerializer getCopySerializer() {
    if (copySerializer == null) {
      copySerializer = new RowDataSerializer(producedRowType());
    }
    return copySerializer;
  }

  protected HoodieWriteConfig getWriteConfig() {
    if (writeConfig == null) {
      writeConfig = FlinkWriteClients.getHoodieClientConfig(conf);
    }
    return writeConfig;
  }

  protected org.apache.hadoop.conf.Configuration getHadoopConf() {
    if (hadoopConf == null) {
      hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    }
    return hadoopConf;
  }
}
