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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.ExplicitWriteHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.List;

/**
 * Flink lazy iterable that supports explicit write handler.
 *
 * @param <T> type of the payload
 */
public class FlinkLazyInsertIterable<T> extends HoodieLazyInsertIterable<T> {

  public FlinkLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                 boolean areRecordsSorted,
                                 HoodieWriteConfig config,
                                 String instantTime,
                                 HoodieTable hoodieTable,
                                 String idPrefix,
                                 TaskContextSupplier taskContextSupplier,
                                 ExplicitWriteHandleFactory writeHandleFactory) {
    super(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier, writeHandleFactory);
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    BoundedInMemoryExecutor<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> bufferedIteratorExecutor =
        null;
    try {
      final Schema schema = new Schema.Parser().parse(hoodieConfig.getSchema());
      bufferedIteratorExecutor = new BoundedInMemoryExecutor<>(hoodieConfig.getWriteBufferLimitBytes(), new IteratorBasedQueueProducer<>(inputItr),
          Option.of(getExplicitInsertHandler()), getTransformFunction(schema, hoodieConfig));
      final List<WriteStatus> result = bufferedIteratorExecutor.execute();
      assert result != null && !result.isEmpty() && !bufferedIteratorExecutor.isRemaining();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (null != bufferedIteratorExecutor) {
        bufferedIteratorExecutor.shutdownNow();
        bufferedIteratorExecutor.awaitTermination();
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private ExplicitWriteHandler getExplicitInsertHandler() {
    HoodieWriteHandle handle = ((ExplicitWriteHandleFactory) writeHandleFactory).getWriteHandle();
    return new ExplicitWriteHandler(handle);
  }
}
