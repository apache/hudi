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
    // "Pending query wins" ordering (task-level failover): a single subtask restarts while the
    // coordinator stays alive, so fileId-A is never recommitted and stays buffered as an inflight
    // instant. The committed view is empty, so the pending query is the only source of fileId-A.
    List<RowData> firstBatch = par1Row("id1", "Danny", 23, 1);
    List<RowData> secondBatch = par1Row("id2", "Stephen", 33, 2);

    BucketStreamWriteFunctionWrapper<RowData> pipeline =
        new BucketStreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
    pipeline.openFunction();

    // Step 1: first write; checkpoint(1) flushes fileId-A. Buffer the flush event without committing.
    for (RowData row : firstBatch) {
      pipeline.invoke(row);
    }
    pipeline.checkpointFunction(1);
    OperatorEvent flushEvent = pipeline.getNextEvent();
    String fileIdA = fileIdOf(flushEvent);
    pipeline.getCoordinator().handleEventFromOperator(0, flushEvent);

    // Step 2: task-level failover (attempt 1) resends only an empty bootstrap event; fileId-A stays
    // buffered on the still-alive coordinator. We hold the event.
    pipeline.subTaskFails(0, 1);
    OperatorEvent bootstrapEvent = pipeline.getNextEvent();

    // Step 3: a new record for the same bucket adopts pending fileId-A from the query (committed view
    // is empty) instead of minting a fresh fileId.
    for (RowData row : secondBatch) {
      pipeline.invoke(row);
    }

    // Step 4: handle the empty bootstrap (no-op), then checkpoint(2) must flush the second record
    // reusing fileId-A.
    pipeline.getCoordinator().handleEventFromOperator(0, bootstrapEvent);
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

  @Test
  public void testAdoptCommittedFileIdWhenRecommitPrecedesBootstrap() throws Exception {
    // "Committed view wins" ordering (full job restart) - the realistic runtime ordering. The write
    // task resends the uncommitted fileId-A event from initializeState() before any record, so the
    // coordinator recommits and commits fileId-A (resetting its buffer) first. The later pending query
    // returns empty, so the fix must fall back to the freshly reloaded committed view that owns fileId-A.
    List<RowData> firstBatch = par1Row("id1", "Danny", 23, 1);
    List<RowData> secondBatch = par1Row("id2", "Stephen", 33, 2);

    BucketStreamWriteFunctionWrapper<RowData> pipeline =
        new BucketStreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
    pipeline.openFunction();

    // Step 1: first write; checkpoint(1) flushes fileId-A into an inflight instant.
    for (RowData row : firstBatch) {
      pipeline.invoke(row);
    }
    pipeline.checkpointFunction(1);
    OperatorEvent flushEvent = pipeline.getNextEvent();
    String fileIdA = fileIdOf(flushEvent);
    pipeline.getCoordinator().handleEventFromOperator(0, flushEvent);

    // Step 2: full job restart (attempt 0) resends the uncommitted fileId-A event; handing it to the
    // coordinator triggers a genuine recommit that commits fileId-A and resets the buffer.
    pipeline.subTaskFails(0, 0);
    OperatorEvent bootstrapEvent = pipeline.getNextEvent();
    pipeline.getCoordinator().handleEventFromOperator(0, bootstrapEvent);

    // Step 3: a new record for the same bucket gets an empty pending query but reloads fileId-A from
    // the now-committed view, so it reuses it instead of minting a duplicate.
    for (RowData row : secondBatch) {
      pipeline.invoke(row);
    }
    pipeline.checkpointFunction(2);
    OperatorEvent flushEvent2 = pipeline.getNextEvent();
    assertThat(fileIdOf(flushEvent2))
        .as("the second record must reuse the committed fileId-A after the recommit")
        .isEqualTo(fileIdA);

    pipeline.close();
  }

  private static List<RowData> par1Row(String id, String name, int age, long ts) {
    return Collections.singletonList(
        TestData.insertRow(
            StringData.fromString(id), StringData.fromString(name), age,
            TimestampData.fromEpochMillis(ts), StringData.fromString("par1")));
  }

  private static String fileIdOf(OperatorEvent event) {
    return ((WriteMetadataEvent) event).getWriteStatuses().get(0).getFileId();
  }
}
