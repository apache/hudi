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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.BucketStreamWriteFunctionWrapper;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Regression test for the duplicate-fileId race of the simple Bucket Index with Flink streaming
 * writes, reported in <a href="https://github.com/apache/hudi/issues/19907">HUDI-19907</a>.
 */
public class TestBucketStreamWriteRecommitRace extends TestWriteBase {

  private static final int NUM_BUCKETS = 1;

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Override
  protected void setUp(Configuration conf) {
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, NUM_BUCKETS);
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
  }

  @Test
  public void testReusePendingFileIdAvoidsDuplicateAfterRecommitRace() throws Exception {
    // records that all fall into partition "par1" (single bucket)
    List<RowData> firstBatch = Collections.singletonList(
        TestData.insertRow(
            StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    List<RowData> secondBatch = Collections.singletonList(
        TestData.insertRow(
            StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));

    BucketStreamWriteFunctionWrapper<RowData> pipeline =
        new BucketStreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
    pipeline.openFunction();

    // Step 1: first write to the empty partition; checkpoint(1) flushes fileId-A. Hand the flush
    // event to the coordinator so it is buffered as an inflight instant, but never commit it.
    for (RowData row : firstBatch) {
      pipeline.invoke(row);
    }
    pipeline.checkpointFunction(1);
    OperatorEvent flushEvent = pipeline.getNextEvent();
    String fileIdA = fileIdOf(flushEvent);
    pipeline.getCoordinator().handleEventFromOperator(0, flushEvent);

    // Step 2: restart the write task. On restore it resends the bootstrap event carrying fileId-A,
    // which we intercept and hold. A subtask failover does not reset the coordinator, so fileId-A
    // stays buffered there.
    pipeline.subTaskFails(0, 1);
    OperatorEvent bootstrapEvent = pipeline.getNextEvent(); // fileId-A bootstrap event (held)

    // Step 3: a new record for the same bucket is processed before the coordinator recommits.
    // bootstrapIndexIfNeed() reads the empty committed view and, thanks to the fix, adopts the
    // pending fileId-A from the coordinator query instead of minting a fresh fileId.
    for (RowData row : secondBatch) {
      pipeline.invoke(row);
    }

    // Step 4: the coordinator recommits fileId-A, then checkpoint(2) flushes the second record. Its
    // flush event must reuse fileId-A rather than a freshly minted fileId-B.
    pipeline.getCoordinator().handleEventFromOperator(0, bootstrapEvent); // commits fileId-A
    pipeline.checkpointFunction(2);
    OperatorEvent flushEvent2 = pipeline.getNextEvent();
    assertThat(fileIdOf(flushEvent2))
        .as("the second record must reuse the pending fileId-A adopted from the coordinator")
        .isEqualTo(fileIdA);
    pipeline.getCoordinator().handleEventFromOperator(0, flushEvent2);
    pipeline.checkpointComplete(2);

    // Step 5: a subsequent restart bootstraps cleanly - the bucket owns exactly one fileId.
    assertDoesNotThrow(() -> {
      pipeline.subTaskFails(0, 2);
      OperatorEvent evt = pipeline.getNextEvent();
      pipeline.getCoordinator().handleEventFromOperator(0, evt);
      for (RowData row : firstBatch) {
        pipeline.invoke(row); // triggers bootstrapIndexIfNeed for par1
      }
    });

    pipeline.close();
  }

  private static String fileIdOf(OperatorEvent event) {
    return ((WriteMetadataEvent) event).getWriteStatuses().get(0).getFileId();
  }
}
