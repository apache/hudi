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

package org.apache.hudi.execution;

import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Base lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new files.
 */
public abstract class AbstractLazyInsertIterable<T extends HoodieRecordPayload>
    extends LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {
  protected final HoodieWriteConfig hoodieConfig;
  protected final String instantTime;
  protected final BaseHoodieTable hoodieTable;
  protected final String idPrefix;
  protected TaskContextSupplier taskContextSupplier;
  protected WriteHandleFactory writeHandleFactory;

  public AbstractLazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
                                    String instantTime, BaseHoodieTable hoodieTable, String idPrefix,
                                    TaskContextSupplier taskContextSupplier) {
    this(sortedRecordItr, config, instantTime, hoodieTable, idPrefix, taskContextSupplier,
        new CreateHandleFactory());
  }

  public AbstractLazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
                                    String instantTime, BaseHoodieTable hoodieTable, String idPrefix,
                                    TaskContextSupplier taskContextSupplier,
                                    WriteHandleFactory writeHandleFactory) {
    super(sortedRecordItr);
    this.hoodieConfig = config;
    this.instantTime = instantTime;
    this.hoodieTable = hoodieTable;
    this.idPrefix = idPrefix;
    this.taskContextSupplier = taskContextSupplier;
    this.writeHandleFactory = writeHandleFactory;
  }

  /**
   * Used for caching HoodieRecord along with insertValue. We need this to offload computation work to buffering thread.
   *
   * @param <T>
   */
  static class HoodieInsertValueGenResult<T extends HoodieRecord> {
    public T record;
    public Option<IndexedRecord> insertValue;
    // It caches the exception seen while fetching insert value.
    public Option<Exception> exception = Option.empty();

    public HoodieInsertValueGenResult(T record, Schema schema) {
      this.record = record;
      try {
        this.insertValue = record.getData().getInsertValue(schema);
      } catch (Exception e) {
        this.exception = Option.of(e);
      }
    }
  }

  /**
   * Transformer function to help transform a HoodieRecord. This transformer is used by BufferedIterator to offload some
   * expensive operations of transformation to the reader thread.
   */
  static <T extends HoodieRecordPayload> Function<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord<T>>> getTransformFunction(
      Schema schema) {
    return hoodieRecord -> new HoodieInsertValueGenResult(hoodieRecord, schema);
  }

  @Override
  protected void start() {
  }

  @Override
  protected void end() {
  }

  protected CopyOnWriteInsertHandler getInsertHandler() {
    return new CopyOnWriteInsertHandler();
  }

  /**
   * Consumes stream of hoodie records from in-memory queue and writes to one or more create-handles.
   */
  protected class CopyOnWriteInsertHandler
      extends BoundedInMemoryQueueConsumer<HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> {

    protected final List<WriteStatus> statuses = new ArrayList<>();
    protected HoodieWriteHandle handle;

    @Override
    protected void consumeOneRecord(HoodieInsertValueGenResult<HoodieRecord> payload) {
      final HoodieRecord insertPayload = payload.record;
      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle = writeHandleFactory.create(hoodieConfig, instantTime, hoodieTable, insertPayload.getPartitionPath(),
            idPrefix, taskContextSupplier);
      }

      if (handle.canWrite(payload.record)) {
        // write the payload, if the handle has capacity
        handle.write(insertPayload, payload.insertValue, payload.exception);
      } else {
        // handle is full.
        statuses.add(handle.close());
        // Need to handle the rejected payload & open new handle
        handle = writeHandleFactory.create(hoodieConfig, instantTime, hoodieTable, insertPayload.getPartitionPath(),
            idPrefix, taskContextSupplier);
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
