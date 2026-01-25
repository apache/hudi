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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.execution.HoodieLazyInsertIterable.getTransformerInternal;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BoundedInMemoryExecutor}.
 */
public class TestBoundedInMemoryExecutorInSpark extends BaseExecutorTestHarness {

  private final HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
      .withExecutorType(ExecutorType.BOUNDED_IN_MEMORY.name())
      .withWriteBufferLimitBytes(1024)
      .build(false);

  @Override
  protected HoodieExecutor<Integer> createExecutor(
      Iterator<HoodieRecord> records, HoodieConsumer<HoodieRecord, Integer> consumer) {
    // BoundedInMemoryExecutor with getTransformerInternal produces HoodieInsertValueGenResult
    @SuppressWarnings("rawtypes")
    HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> wrappedConsumer =
        new HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {
          private int count = 0;

          @Override
          public void consume(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> result) throws Exception {
            // Extract the HoodieRecord from the result and pass to original consumer
            consumer.consume(result.getResult());
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    @SuppressWarnings({"unchecked", "rawtypes"})
    BoundedInMemoryExecutor executor = new BoundedInMemoryExecutor(
        writeConfig.getWriteBufferLimitBytes(),
        records,
        wrappedConsumer,
        getTransformerInternal(HOODIE_SCHEMA, writeConfig),
        getPreExecuteRunnable());
    
    return executor;
  }

  @Override
  protected boolean requiresExecutorService() {
    return false;
  }

  @Override
  protected boolean supportsInterruptTest() {
    return true;
  }

  @Override
  protected boolean supportsRunningStatusCheck() {
    return true;
  }

  @Test
  public void testExecutorTermination() {
    Iterator<HoodieRecord> unboundedRecordIter = new Iterator<HoodieRecord>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public HoodieRecord next() {
        return dataGen.generateInserts(instantTime, 1).get(0);
      }
    };

    @SuppressWarnings("rawtypes")
    HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {
          @Override
          public void consume(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
          }

          @Override
          public Integer finish() {
            return 0;
          }
        };

    @SuppressWarnings({"unchecked", "rawtypes"})
    BoundedInMemoryExecutor executor =
        new BoundedInMemoryExecutor(
            writeConfig.getWriteBufferLimitBytes(), unboundedRecordIter,
            consumer, getTransformerInternal(HOODIE_SCHEMA, writeConfig),
            getPreExecuteRunnable());
    executor.shutdownNow();
    boolean terminatedGracefully = executor.awaitTermination();
    assertTrue(terminatedGracefully);
  }
}
