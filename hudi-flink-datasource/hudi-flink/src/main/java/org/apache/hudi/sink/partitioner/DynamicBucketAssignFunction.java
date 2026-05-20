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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.adapter.KeyedProcessFunctionAdapter;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.partitioner.index.DummyPartitionedIndexBackend;
import org.apache.hudi.sink.partitioner.index.RecordLevelIndexBackend;
import org.apache.hudi.sink.partitioner.index.PartitionedIndexBackend;
import org.apache.hudi.sink.partitioner.profile.WriteProfile;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.util.FlinkTaskContextSupplier;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.Setter;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

/**
 * Assigns Flink streaming records to dynamic bucket file groups.
 *
 * <p>This function first checks the partition-scoped RLI backend for an existing
 * {@code recordKey -> fileGroupId} mapping. Existing keys are routed as updates to
 * the recorded file group; new keys are assigned by {@link BucketAssigner} and then
 * written back to the backend so the streaming metadata writer can persist the assignment to RLI.
 */
public class DynamicBucketAssignFunction
    extends KeyedProcessFunctionAdapter<String, HoodieFlinkInternalRow, HoodieFlinkInternalRow>
    implements CheckpointedFunction, CheckpointListener {

  private final Configuration conf;
  private final boolean isInsertOverwrite;

  private transient PartitionedIndexBackend indexBackend;
  private transient BucketAssigner bucketAssigner;

  @Setter
  protected transient Correspondent correspondent;

  private transient int maxParallelism;
  private transient int numTasks;
  private transient int taskId;

  /**
   * Creates the dynamic bucket assign function for one bucket assign operator.
   *
   * @param conf Flink write configuration
   */
  public DynamicBucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isInsertOverwrite = OptionsResolver.isInsertOverwrite(conf);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(this.conf, !OptionsResolver.isIncrementalJobGraph(conf));
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(
        HadoopFSUtils.getStorageConfWithCopy(HadoopConfigurations.getHadoopConf(this.conf)),
        new FlinkTaskContextSupplier(getRuntimeContext()));
    boolean delta = HoodieTableType.valueOf(conf.get(FlinkOptions.TABLE_TYPE)).equals(HoodieTableType.MERGE_ON_READ);
    WriteProfile writeProfile = WriteProfiles.singleton(isInsertOverwrite, delta, writeConfig, context);
    this.bucketAssigner = new BucketAssigner(
        RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
        RuntimeContextUtils.getMaxNumberOfParallelSubtasks(getRuntimeContext()),
        RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()),
        writeProfile,
        writeConfig);
    this.maxParallelism = RuntimeContextUtils.getMaxNumberOfParallelSubtasks(getRuntimeContext());
    this.numTasks = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    this.taskId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    this.indexBackend = isInsertOverwrite
        ? new DummyPartitionedIndexBackend()
        : new RecordLevelIndexBackend(conf, (partitionPath, recordKey, fileId) -> isRecordKeyOfThisTask(recordKey));
  }

  private boolean isRecordKeyOfThisTask(String recordKey) {
    return KeyGroupRangeAssignment.assignKeyToParallelOperator(recordKey, maxParallelism, numTasks) == taskId;
  }

  @Override
  public void processElement(HoodieFlinkInternalRow record, Context ctx, Collector<HoodieFlinkInternalRow> out) throws Exception {
    String partitionPath = record.getPartitionPath();
    String recordKey = record.getRecordKey();
    String fileGroupId = indexBackend.get(partitionPath, recordKey);

    BucketInfo bucketInfo;
    // `operationType` in the record is used to generate index record in the following writer operator.
    // Currently, we only emit INSERT index record and ignore DELETE index record, because we cannot be
    // certain whether data with the same key in storage will actually be deleted. It's possible that
    // data in storage is deleted, but the record level index data remains.
    // todo: support ordering value in record level index metadata payload, since the efficiency of location
    // tagging by merging lookup is intolerable in flink streaming writing scenario.
    if (fileGroupId != null) {
      bucketInfo = bucketAssigner.addUpdate(partitionPath, fileGroupId);
    } else {
      bucketInfo = bucketAssigner.addInsert(partitionPath);
      indexBackend.update(partitionPath, recordKey, bucketInfo.getFileIdPrefix());
      record.setOperationType("I");
    }

    String instantTime = bucketInfo.getBucketType() == BucketType.INSERT ? "I" : "U";
    record.setInstantTime(instantTime);
    record.setFileId(bucketInfo.getFileIdPrefix());
    out.collect(record);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    this.bucketAssigner.reset();
    this.indexBackend.onCheckpoint(context.getCheckpointId());
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    this.bucketAssigner.reload(checkpointId);
    this.indexBackend.onCheckpointComplete(this.correspondent, checkpointId);
  }

  @Override
  public void close() throws Exception {
    this.indexBackend.close();
    if (this.bucketAssigner != null) {
      this.bucketAssigner.close();
    }
  }
}
