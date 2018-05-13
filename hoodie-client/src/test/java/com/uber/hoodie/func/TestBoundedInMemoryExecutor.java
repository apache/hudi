/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

import static com.uber.hoodie.func.CopyOnWriteLazyInsertIterable.getTransformFunction;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.queue.BoundedInMemoryQueueConsumer;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.IndexedRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

public class TestBoundedInMemoryExecutor {

  private final HoodieTestDataGenerator hoodieTestDataGenerator = new HoodieTestDataGenerator();
  private final String commitTime = HoodieActiveTimeline.createNewCommitTime();
  private SparkBoundedInMemoryExecutor<HoodieRecord,
      Tuple2<HoodieRecord, Optional<IndexedRecord>>, Integer> executor = null;

  @After
  public void afterTest() {
    if (this.executor != null) {
      this.executor.shutdownNow();
      this.executor = null;
    }
  }

  @Test
  public void testExecutor() throws Exception {

    final List<HoodieRecord> hoodieRecords = hoodieTestDataGenerator.generateInserts(commitTime, 100);

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(hoodieWriteConfig.getWriteBufferLimitBytes()).thenReturn(1024);
    BoundedInMemoryQueueConsumer<Tuple2<HoodieRecord, Optional<IndexedRecord>>, Integer> consumer =
        new BoundedInMemoryQueueConsumer<Tuple2<HoodieRecord, Optional<IndexedRecord>>, Integer>() {

          private int count = 0;

          @Override
          protected void consumeOneRecord(Tuple2<HoodieRecord, Optional<IndexedRecord>> record) {
            count++;
          }

          @Override
          protected void finish() {
          }

          @Override
          protected Integer getResult() {
            return count;
          }
        };

    executor = new SparkBoundedInMemoryExecutor(hoodieWriteConfig,
        hoodieRecords.iterator(), consumer, getTransformFunction(HoodieTestDataGenerator.avroSchema));
    int result = executor.execute();
    // It should buffer and write 100 records
    Assert.assertEquals(result, 100);
    // There should be no remaining records in the buffer
    Assert.assertFalse(executor.isRemaining());
  }
}