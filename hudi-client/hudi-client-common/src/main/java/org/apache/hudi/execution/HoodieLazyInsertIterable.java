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

import org.apache.avro.Schema;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new files.
 */
public abstract class HoodieLazyInsertIterable<T>
    extends LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

  protected final HoodieWriteConfig hoodieConfig;
  protected final String instantTime;
  protected boolean areRecordsSorted;
  protected final HoodieTable hoodieTable;
  protected final String idPrefix;
  protected TaskContextSupplier taskContextSupplier;
  protected WriteHandleFactory writeHandleFactory;

  public HoodieLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                  boolean areRecordsSorted,
                                  HoodieWriteConfig config,
                                  String instantTime,
                                  HoodieTable hoodieTable,
                                  String idPrefix,
                                  TaskContextSupplier taskContextSupplier) {
    this(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier,
        new CreateHandleFactory<>());
  }

  public HoodieLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr, boolean areRecordsSorted,
                                  HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                                  String idPrefix, TaskContextSupplier taskContextSupplier,
                                  WriteHandleFactory writeHandleFactory) {
    super(recordItr);
    this.areRecordsSorted = areRecordsSorted;
    this.hoodieConfig = config;
    this.instantTime = instantTime;
    this.hoodieTable = hoodieTable;
    this.idPrefix = idPrefix;
    this.taskContextSupplier = taskContextSupplier;
    this.writeHandleFactory = writeHandleFactory;
  }

  // Used for caching HoodieRecord along with insertValue. We need this to offload computation work to buffering thread.
  public static class HoodieInsertValueGenResult<R extends HoodieRecord> {
    private final R record;
    public final Schema schema;
    public final Properties props;

    public HoodieInsertValueGenResult(R record, Schema schema, Properties properties) {
      this.record = record;
      this.schema = schema;
      this.props = properties;
    }

    public R getResult() {
      return record;
    }
  }

  static <T> Function<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>> getCloningTransformer(Schema schema,
                                                                                                       HoodieWriteConfig config) {
    return getCloningTransformerInternal(schema, config.getProps());
  }

  static <T> Function<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>> getCloningTransformer(Schema schema) {
    return getCloningTransformerInternal(schema, new TypedProperties());
  }

  private static <T> Function<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>> getCloningTransformerInternal(Schema schema,
                                                                                                                       TypedProperties props) {
    return record -> {
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be subsequently buffered (w/in the in-memory queue)
      HoodieRecord<T> clonedRecord = record.copy();
      return new HoodieInsertValueGenResult(clonedRecord, schema, props);
    };
  }

  @Override
  protected void start() {}

  @Override
  protected void end() {}

  protected CopyOnWriteInsertHandler getInsertHandler() {
    return new CopyOnWriteInsertHandler(hoodieConfig, instantTime, areRecordsSorted, hoodieTable, idPrefix,
        taskContextSupplier, writeHandleFactory);
  }
}
