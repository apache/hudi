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

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Bootstrap operator that loads record level index (RLI) data from metadata table.
 *
 * <p>This operator reads index data from the record_index partition of the metadata table.
 * Each subtask reads one RLI partition (bucket) based on its task index, enabling parallel loading.
 *
 * <p>The loaded index records are emitted downstream to initialize the index state in
 * {@link org.apache.hudi.sink.partitioner.BucketAssignFunction}.
 */
@Slf4j
public class RLIBootstrapOperator
    extends AbstractBootstrapOperator {

  private transient HoodieBackedTableMetadata metadataTable;
  private transient long loadedCnt;

  public RLIBootstrapOperator(Configuration conf) {
    super(conf);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    loadedCnt = 0;
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    this.metadataTable = (HoodieBackedTableMetadata) metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
    // Load RLI records
    preLoadRLIRecords();
  }

  @Override
  public void close() throws Exception {
    closeMetadataTable();
    super.close();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void preLoadRLIRecords() {
    int taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    int parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());

    log.info("Start loading RLI records from metadata table, taskId = {}, parallelism = {}", taskID, parallelism);

    SerializableFunctionUnchecked<List<FileSlice>, List<FileSlice>> fileSlicesFilter = fileSlices -> {
      List<FileSlice> filteredFileSlices = new ArrayList<>();
      for (int i = 0; i < fileSlices.size(); i++) {
        if (shouldLoadBucket(i, parallelism, taskID)) {
          filteredFileSlices.add(fileSlices.get(i));
        }
      }
      log.info("Subtask: {} will load record index records from file groups: {}, total file groups: {}.",
          taskID, filteredFileSlices.stream().map(FileSlice::getFileId).collect(Collectors.joining(",")), fileSlices.size());
      return filteredFileSlices;
    };

    // Each subtask loads buckets assigned to it
    long startTime = System.currentTimeMillis();
    HoodiePairData<String, HoodieRecordGlobalLocation> rliData = metadataTable.readRecordIndexLocations(fileSlicesFilter);
    rliData.forEach(locationPair -> emitIndexRecord(locationPair.getLeft(), locationPair.getRight()));
    long costMs = System.currentTimeMillis() - startTime;
    log.info("Finish loading RLI records, total records: {}, cost: {} ms, taskId = {}", loadedCnt, costMs, taskID);

    // Wait for other tasks to complete
    waitForBootstrapReady(taskID);

    // Cleanup resources
    closeMetadataTable();
  }

  /**
   * Determines if the given file group should be loaded by this task.
   * Uses round-robin assignment: file group i is assigned to task (i % parallelism).
   */
  private boolean shouldLoadBucket(int fileGroupIdx, int parallelism, int taskID) {
    return fileGroupIdx % parallelism == taskID;
  }

  private void emitIndexRecord(String recordKey, HoodieRecordGlobalLocation location) {
    output.collect(new StreamRecord<>(
        new HoodieFlinkInternalRow(
            recordKey,
            location.getPartitionPath(),
            location.getFileId(),
            String.valueOf(location.getInstantTime()))));
    loadedCnt += 1;
  }

  private void closeMetadataTable() {
    if (metadataTable != null) {
      try {
        metadataTable.close();
      } catch (Exception e) {
        log.warn("Failed to close metadata table", e);
      }
      metadataTable = null;
    }
  }
}
