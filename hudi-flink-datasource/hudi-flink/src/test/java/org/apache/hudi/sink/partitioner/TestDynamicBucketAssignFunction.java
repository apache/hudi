/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.partitioner.index.DummyPartitionedIndexBackend;
import org.apache.hudi.sink.partitioner.index.PartitionedIndexBackend;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.util.ViewStorageProperties;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link DynamicBucketAssignFunction}.
 */
class TestDynamicBucketAssignFunction {

  @TempDir
  File tempFile;

  @Test
  void testRoutesExistingRecordToUpdateBucket() throws Exception {
    DynamicBucketAssignFunction function = new DynamicBucketAssignFunction(new Configuration());
    PartitionedIndexBackend indexBackend = mock(PartitionedIndexBackend.class);
    BucketAssigner bucketAssigner = mock(BucketAssigner.class);
    when(indexBackend.get("partition", "key")).thenReturn("existing-file");
    when(bucketAssigner.addUpdate("partition", "existing-file"))
        .thenReturn(new BucketInfo(BucketType.UPDATE, "existing-file", "partition"));
    setField(function, "indexBackend", indexBackend);
    setField(function, "bucketAssigner", bucketAssigner);
    HoodieFlinkInternalRow record = record("key", "partition", "U");
    List<HoodieFlinkInternalRow> output = new ArrayList<>();

    function.processElement(record, null, collector(output));

    assertEquals(1, output.size());
    assertEquals("existing-file", record.getFileId());
    assertEquals("U", record.getInstantTime());
    assertEquals("U", record.getOperationType());
    verify(indexBackend, never()).update("partition", "key", "existing-file");
  }

  @Test
  void testAssignsAndCachesNewRecord() throws Exception {
    DynamicBucketAssignFunction function = new DynamicBucketAssignFunction(new Configuration());
    PartitionedIndexBackend indexBackend = mock(PartitionedIndexBackend.class);
    BucketAssigner bucketAssigner = mock(BucketAssigner.class);
    when(indexBackend.get("partition", "key")).thenReturn(null);
    when(bucketAssigner.addInsert("partition"))
        .thenReturn(new BucketInfo(BucketType.INSERT, "new-file", "partition"));
    setField(function, "indexBackend", indexBackend);
    setField(function, "bucketAssigner", bucketAssigner);
    HoodieFlinkInternalRow record = record("key", "partition", "U");
    List<HoodieFlinkInternalRow> output = new ArrayList<>();

    function.processElement(record, null, collector(output));

    assertEquals(1, output.size());
    assertEquals("new-file", record.getFileId());
    assertEquals("I", record.getInstantTime());
    assertEquals("I", record.getOperationType());
    verify(indexBackend).update("partition", "key", "new-file");
  }

  @Test
  void testCheckpointLifecycleDelegatesToBackends() throws Exception {
    DynamicBucketAssignFunction function = new DynamicBucketAssignFunction(new Configuration());
    PartitionedIndexBackend indexBackend = mock(PartitionedIndexBackend.class);
    BucketAssigner bucketAssigner = mock(BucketAssigner.class);
    Correspondent correspondent = mock(Correspondent.class);
    FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
    when(snapshotContext.getCheckpointId()).thenReturn(7L);
    setField(function, "indexBackend", indexBackend);
    setField(function, "bucketAssigner", bucketAssigner);
    function.setCorrespondent(correspondent);

    function.snapshotState(snapshotContext);
    function.notifyCheckpointComplete(7L);
    function.close();

    verify(bucketAssigner).reset();
    verify(indexBackend).onCheckpoint(7L);
    verify(bucketAssigner).reload(7L);
    verify(indexBackend).onCheckpointComplete(correspondent, 7L);
    verify(indexBackend).close();
    verify(bucketAssigner).close();
  }

  @Test
  void testInsertOverwriteUsesDummyIndexBackend() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.OPERATION, "insert_overwrite");
    DynamicBucketAssignFunction function = new DynamicBucketAssignFunction(conf);
    RuntimeContext runtimeContext = mock(RuntimeContext.class);
    when(runtimeContext.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
    function.setRuntimeContext(runtimeContext);

    function.initializeState(mock(FunctionInitializationContext.class));

    assertInstanceOf(DummyPartitionedIndexBackend.class, getField(function, "indexBackend"));
  }

  @Test
  void testOpenInitializesBucketAssignerAndTaskOwnership() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, "insert_overwrite");
    StreamerUtil.initTableIfNotExists(conf);
    ViewStorageProperties.createProperties(
        conf.get(FlinkOptions.PATH),
        FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .build(),
        conf);
    DynamicBucketAssignFunction function = new DynamicBucketAssignFunction(conf);
    function.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

    function.open(new Configuration());
    function.initializeState(mock(FunctionInitializationContext.class));

    assertInstanceOf(BucketAssigner.class, getField(function, "bucketAssigner"));
    assertInstanceOf(DummyPartitionedIndexBackend.class, getField(function, "indexBackend"));
    function.close();
  }

  private static HoodieFlinkInternalRow record(String recordKey, String partitionPath, String operationType) {
    return new HoodieFlinkInternalRow(recordKey, partitionPath, operationType, new GenericRowData(0));
  }

  private static Collector<HoodieFlinkInternalRow> collector(List<HoodieFlinkInternalRow> output) {
    return new Collector<HoodieFlinkInternalRow>() {
      @Override
      public void collect(HoodieFlinkInternalRow record) {
        output.add(record);
      }

      @Override
      public void close() {
      }
    };
  }

  private static Object getField(Object target, String fieldName) throws Exception {
    Field field = DynamicBucketAssignFunction.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = DynamicBucketAssignFunction.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
