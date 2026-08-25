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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.utils.BucketStreamWriteFunctionWrapper;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end tests for bucket writes under a tight write memory pool. */
class TestBucketStreamWriteMemoryExhaustion {

  @TempDir
  File tempFile;

  @ParameterizedTest(name = "lsmTreeLayout={0}")
  @ValueSource(booleans = {false, true})
  void testRecoveryPreservesVariableLengthValuesAndReleasesPages(boolean lsmTreeLayout) throws Exception {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("payload", DataTypes.VARCHAR(Integer.MAX_VALUE)),
        DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.VARCHAR(64), DataTypes.VARCHAR(64))),
        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
        .notNull();
    RowType rowType = (RowType) dataType.getLogicalType();

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), dataType);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    if (lsmTreeLayout) {
      conf.setString(
          HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
          HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    }
    // Leaves 1 MB for the shared RowData memory pool.
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 201.0);
    conf.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, 100);
    // Prevent batch-size based flushes so non-diverged mid-invocation flushes come from preemption.
    conf.set(FlinkOptions.WRITE_BATCH_SIZE, 1024.0);

    Map<String, String> expectedPayloads = new HashMap<>();
    Map<String, String> expectedAttributes = new HashMap<>();
    List<RowData> rows = createRows(rowType, expectedPayloads, expectedAttributes);

    TrackingBucketWriteFunctionWrapper pipeline =
        new TrackingBucketWriteFunctionWrapper(tempFile.getAbsolutePath(), conf);
    pipeline.openFunction();
    int initialFreePages = pipeline.freePages();
    try {
      boolean preemptedInactiveBucket = false;
      for (RowData row : rows) {
        int flushCountBeforeInvoke = pipeline.writeBatchCount();
        pipeline.invoke(row);
        List<FlushedBucket> invocationFlushes = pipeline.flushesFrom(flushCountBeforeInvoke);
        if (!invocationFlushes.isEmpty()
            && invocationFlushes.stream().noneMatch(flushedBucket -> flushedBucket.diverged)) {
          preemptedInactiveBucket = true;
        }
      }
      int recoveryFlushCount = pipeline.writeBatchCount();
      assertTrue(recoveryFlushCount > 0, "the tight pool should trigger at least one preemptive flush");
      assertTrue(
          preemptedInactiveBucket,
          "memory exhaustion should flush an inactive bucket before the current buffer diverges");
      pipeline.checkpointFunction(1);

      assertEquals(
          initialFreePages,
          pipeline.freePages(),
          "all pages should be returned after the checkpoint flush disposes every bucket");

      handleWriteEvents(pipeline, recoveryFlushCount + 1);
      pipeline.checkpointComplete(1);

      List<GenericRecord> actualRecords = TestData.readAllData(tempFile, rowType, 1);
      assertEquals(rows.size(), actualRecords.size(), "memory exhaustion recovery must not lose or duplicate records");
      for (GenericRecord actualRecord : actualRecords) {
        String id = actualRecord.get("uuid").toString();
        assertTrue(
            actualRecord.get("payload").toString().equals(expectedPayloads.get(id)),
            "variable-length payload should remain intact for " + id);
        Map<?, ?> actualAttributes = (Map<?, ?>) actualRecord.get("attributes");
        assertEquals(1, actualAttributes.size());
        Map.Entry<?, ?> attribute = actualAttributes.entrySet().iterator().next();
        assertEquals(
            expectedAttributes.get(id),
            attribute.getKey().toString() + "=" + attribute.getValue().toString());
      }
    } finally {
      pipeline.close();
    }
  }

  private static List<RowData> createRows(
      RowType rowType,
      Map<String, String> expectedPayloads,
      Map<String, String> expectedAttributes) {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      String id = "uuid-" + i;
      String payload = "payload-" + i + "-" + repeat((char) ('a' + i % 26), 256 * 1024);
      String attributeKey = "key-" + i;
      String attributeValue = "value-" + i;
      Map<Object, Object> attributes = new HashMap<>();
      attributes.put(StringData.fromString(attributeKey), StringData.fromString(attributeValue));

      rows.add(TestData.insertRow(
          rowType,
          StringData.fromString(id),
          StringData.fromString(payload),
          new GenericMapData(attributes),
          TimestampData.fromEpochMillis(i),
          StringData.fromString("par0")));
      expectedPayloads.put(id, payload);
      expectedAttributes.put(id, attributeKey + "=" + attributeValue);
    }
    return rows;
  }

  private static void handleWriteEvents(
      TrackingBucketWriteFunctionWrapper pipeline, int eventCount) {
    for (int i = 0; i < eventCount; i++) {
      OperatorEvent event = pipeline.getNextEvent();
      pipeline.getCoordinator().handleEventFromOperator(0, event);
    }
  }

  private static String repeat(char value, int count) {
    return String.valueOf(value).repeat(count);
  }

  private static class TrackingBucketWriteFunctionWrapper
      extends BucketStreamWriteFunctionWrapper<RowData> {

    TrackingBucketWriteFunctionWrapper(String basePath, Configuration conf) throws Exception {
      super(basePath, conf);
    }

    @Override
    protected StreamWriteFunction createWriteFunction() {
      return new TrackingBucketStreamWriteFunction(conf, rowType);
    }

    int freePages() {
      TrackingBucketStreamWriteFunction function =
          (TrackingBucketStreamWriteFunction) writeFunction;
      return function.freePages();
    }

    int writeBatchCount() {
      TrackingBucketStreamWriteFunction function =
          (TrackingBucketStreamWriteFunction) writeFunction;
      return function.writeBatchCount();
    }

    List<FlushedBucket> flushesFrom(int startIndex) {
      TrackingBucketStreamWriteFunction function =
          (TrackingBucketStreamWriteFunction) writeFunction;
      return function.flushesFrom(startIndex);
    }
  }

  private static class TrackingBucketStreamWriteFunction extends BucketStreamWriteFunction {

    private final List<FlushedBucket> flushedBuckets = new ArrayList<>();

    TrackingBucketStreamWriteFunction(Configuration conf, RowType rowType) {
      super(conf, rowType);
    }

    int freePages() {
      return memorySegmentPool.freePages();
    }

    int writeBatchCount() {
      return flushedBuckets.size();
    }

    List<FlushedBucket> flushesFrom(int startIndex) {
      return new ArrayList<>(flushedBuckets.subList(startIndex, flushedBuckets.size()));
    }

    @Override
    protected List<WriteStatus> writeRecords(String instant, RowDataBucket rowDataBucket) {
      List<WriteStatus> writeStatuses = super.writeRecords(instant, rowDataBucket);
      flushedBuckets.add(new FlushedBucket(rowDataBucket.isDiverged()));
      return writeStatuses;
    }
  }

  private static class FlushedBucket {
    private final boolean diverged;

    private FlushedBucket(boolean diverged) {
      this.diverged = diverged;
    }
  }
}
