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

package org.apache.hudi.testutils;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.HoodieLazyInsertIterable;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

import java.util.List;
import java.util.function.Function;

public class HoodieExecutorTestUtils {

  public DisruptorExecutor getDisruptorExecutor(List<HoodieRecord> hoodieRecords,
                                                   BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer) {
    return new DisruptorExecutor(HoodieWriteConfig.WRITE_BUFFER_SIZE.defaultValue(), hoodieRecords.iterator(), consumer,
        getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), WaitStrategyFactory.DEFAULT_STRATEGY, getPreExecuteRunnable());
  }

  public BoundedInMemoryExecutor getBoundedInMemoryExecutor(List<HoodieRecord> hoodieRecords,
                                                            BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer) {
    return new BoundedInMemoryExecutor(Integer.parseInt(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE.defaultValue()), hoodieRecords.iterator(), consumer,
        getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), getPreExecuteRunnable());
  }

  static <T extends HoodieRecordPayload> Function<HoodieRecord<T>, HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>> getTransformFunction(
      Schema schema) {
    return hoodieRecord -> new HoodieLazyInsertIterable.HoodieInsertValueGenResult(hoodieRecord, schema, CollectionUtils.EMPTY_PROPERTIES);
  }

  private Runnable getPreExecuteRunnable() {
    final TaskContext taskContext = TaskContext.get();
    return () -> TaskContext$.MODULE$.setTaskContext(taskContext);
  }
}
