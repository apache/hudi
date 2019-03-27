/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.queue.BoundedInMemoryExecutor;
import com.uber.hoodie.common.util.queue.BoundedInMemoryQueueConsumer;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.io.HoodieCreateHandle;
import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new
 * files.
 */
public class CopyOnWriteLazyInsertIterable<T extends HoodieRecordPayload> extends
    LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

  protected final HoodieWriteConfig hoodieConfig;
  protected final String commitTime;
  protected final HoodieTable<T> hoodieTable;
  protected Set<String> partitionsCleaned;

  public CopyOnWriteLazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
      String commitTime, HoodieTable<T> hoodieTable) {
    super(sortedRecordItr);
    this.partitionsCleaned = new HashSet<>();
    this.hoodieConfig = config;
    this.commitTime = commitTime;
    this.hoodieTable = hoodieTable;
  }

  // Used for caching HoodieRecord along with insertValue. We need this to offload computation work to buffering thread.
  static class HoodieInsertValueGenResult<T extends HoodieRecord> {
    public T record;
    public Optional<IndexedRecord> insertValue;
    // It caches the exception seen while fetching insert value.
    public Optional<Exception> exception = Optional.empty();

    public HoodieInsertValueGenResult(T record, Schema schema) {
      this.record = record;
      try {
        this.insertValue = record.getData().getInsertValue(schema);
      } catch (Exception e) {
        this.exception = Optional.of(e);
      }
    }
  }

  /**
   * Transformer function to help transform a HoodieRecord. This transformer is used by BufferedIterator to offload some
   * expensive operations of transformation to the reader thread.
   */
  static <T extends HoodieRecordPayload> Function<HoodieRecord<T>,
      HoodieInsertValueGenResult<HoodieRecord>> getTransformFunction(Schema schema) {
    return hoodieRecord -> new HoodieInsertValueGenResult(hoodieRecord, schema);
  }

  @Override
  protected void start() {
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    BoundedInMemoryExecutor<HoodieRecord<T>,
        HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> bufferedIteratorExecutor = null;
    try {
      final Schema schema = new Schema.Parser().parse(hoodieConfig.getSchema());
      bufferedIteratorExecutor =
          new SparkBoundedInMemoryExecutor<>(hoodieConfig, inputItr,
              getInsertHandler(), getTransformFunction(schema));
      final List<WriteStatus> result = bufferedIteratorExecutor.execute();
      assert result != null && !result.isEmpty() && !bufferedIteratorExecutor.isRemaining();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (null != bufferedIteratorExecutor) {
        bufferedIteratorExecutor.shutdownNow();
      }
    }
  }

  @Override
  protected void end() {

  }

  protected CopyOnWriteInsertHandler getInsertHandler() {
    return new CopyOnWriteInsertHandler();
  }

  /**
   * Consumes stream of hoodie records from in-memory queue and
   * writes to one or more create-handles
   */
  protected class CopyOnWriteInsertHandler extends
      BoundedInMemoryQueueConsumer<HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> {

    protected final List<WriteStatus> statuses = new ArrayList<>();
    protected HoodieIOHandle handle;

    @Override
    protected void consumeOneRecord(HoodieInsertValueGenResult<HoodieRecord> payload) {
      final HoodieRecord insertPayload = payload.record;
      // clean up any partial failures
      if (!partitionsCleaned.contains(insertPayload.getPartitionPath())) {
        // This insert task could fail multiple times, but Spark will faithfully retry with
        // the same data again. Thus, before we open any files under a given partition, we
        // first delete any files in the same partitionPath written by same Spark partition
        HoodieIOHandle.cleanupTmpFilesFromCurrentCommit(hoodieConfig, commitTime, insertPayload.getPartitionPath(),
            TaskContext.getPartitionId(), hoodieTable);
        partitionsCleaned.add(insertPayload.getPartitionPath());
      }

      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle = new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, insertPayload.getPartitionPath(), UUID
            .randomUUID().toString());
      }

      if (handle.canWrite(payload.record)) {
        // write the payload, if the handle has capacity
        handle.write(insertPayload, payload.insertValue, payload.exception);
      } else {
        // handle is full.
        statuses.add(handle.close());
        // Need to handle the rejected payload & open new handle
        handle = new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, insertPayload.getPartitionPath(), UUID
            .randomUUID().toString());
        handle.write(insertPayload, payload.insertValue, payload.exception); // we should be able to write 1 payload.
      }
    }

    @Override
    protected void finish() {
      if (handle != null) {
        statuses.add(handle.close());
      }
      handle = null;
      assert statuses.size() > 0;
    }

    @Override
    protected List<WriteStatus> getResult() {
      return statuses;
    }
  }
}