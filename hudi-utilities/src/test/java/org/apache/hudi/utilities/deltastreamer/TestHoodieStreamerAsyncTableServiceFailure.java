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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.async.AsyncCompactService;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.sources.TestDataSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failures of the async table services of a continuous {@link HoodieDeltaStreamer}.
 */
public class TestHoodieStreamerAsyncTableServiceFailure extends HoodieDeltaStreamerTestBase {

  @AfterEach
  public void resetFailureInjection() {
    CompactionFailingPayload.failCompaction = false;
    TestDataSource.returnEmptyBatch = false;
  }

  /**
   * When async compaction fails with an error its service cannot recover from, the streamer stops with that error as
   * the cause. It used to notice only when it next scheduled a compaction, and then without the cause.
   */
  @Test
  public void testStreamerStopsWithCauseWhenAsyncCompactionFails() throws Exception {
    String tableBasePath = basePath + "/async_compaction_failure";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, true, CompactionFailingPayload.class.getName(),
        HoodieTableType.MERGE_ON_READ.name());
    cfg.continuousMode = true;
    cfg.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), 1000));
    cfg.configs.add(String.format("%s=%d", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), 2));
    CompactionFailingPayload.failCompaction = true;

    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> sync = executor.submit(() -> {
        streamer.sync();
        return null;
      });
      ExecutionException failure = assertThrows(ExecutionException.class, () -> sync.get(5, TimeUnit.MINUTES),
          "the streamer must stop when async compaction fails");
      assertTrue(causeChainContains(failure, "Async compaction failed"), "unexpected failure: " + failure);
      assertTrue(causeChainContains(failure, CompactionFailingPayload.FAILURE_MESSAGE),
          "the compaction failure must be the cause: " + failure);
    } finally {
      streamer.shutdownGracefully();
      executor.shutdownNow();
    }
  }

  private static boolean causeChainContains(Throwable throwable, String message) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(message)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Payload whose merges fail inside async compaction, i.e. in Spark tasks of the compaction scheduler pool, while
   * {@link #failCompaction} is set.
   */
  public static class CompactionFailingPayload extends OverwriteWithLatestAvroPayload {

    static final String FAILURE_MESSAGE = "Injected async compaction failure";
    static volatile boolean failCompaction = false;

    public CompactionFailingPayload(GenericRecord record, Comparable orderingVal) {
      super(record, orderingVal);
    }

    public CompactionFailingPayload(Option<GenericRecord> record) {
      super(record);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
      TaskContext taskContext = TaskContext.get();
      if (failCompaction && taskContext != null
          && AsyncCompactService.COMPACT_POOL_NAME.equals(taskContext.getLocalProperty("spark.scheduler.pool"))) {
        throw new HoodieException(FAILURE_MESSAGE);
      }
      return super.combineAndGetUpdateValue(currentValue, schema);
    }
  }
}
