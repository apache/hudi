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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Bootstrap operator that preload of time bounded partitioned record level index (RLI) data
 * from the metadata table.
 *
 * <p>Only data table partitions that fall within the last {@link FlinkOptions#INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS}
 * days are eagerly preloaded; the partition path of each partition is parsed as a date using
 * {@link FlinkOptions#PARTITION_FORMAT} (default {@link FlinkOptions#PARTITION_FORMAT_DAY}) to
 * determine whether it falls inside the window. Partitions outside the window, and partitions whose
 * path cannot be parsed as a date, are skipped here and are expected to be loaded on demand later.
 *
 * <p>Setting {@link FlinkOptions#INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS} to {@code 0} disables preloading
 * entirely, which is the expected fallback for non-temporal (non date-partitioned) tables.
 */
@Slf4j
public class TimeBoundedRLIBootstrapOperator
    extends AbstractBootstrapOperator {

  private transient HoodieBackedTableMetadata tableMetadata;
  private transient long loadedCnt;
  private int parallelism;
  private int taskID;
  /**
   * Functions for calculating the task partition to dispatch.
   */
  @VisibleForTesting
  Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

  public TimeBoundedRLIBootstrapOperator(Configuration conf) {
    super(conf);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    loadedCnt = 0;
    this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());

    int bootstrapDays = conf.get(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS);
    if (bootstrapDays <= 0) {
      log.info("Skip preloading partitioned RLI records because bootstrap days is configured as {}, taskId = {}",
          bootstrapDays, taskID);
      waitForBootstrapReady(taskID);
      return;
    }

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    this.tableMetadata = createTableMetadata(metaClient);

    this.parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
    preLoadPartitionedRLIRecords(metaClient.getTableConfig(), bootstrapDays);
  }

  @Override
  public void close() throws Exception {
    closeMetadataTable();
    super.close();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  @VisibleForTesting
  HoodieBackedTableMetadata createTableMetadata(HoodieTableMetaClient metaClient) {
    return new HoodieBackedTableMetadata(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
  }

  private void preLoadPartitionedRLIRecords(HoodieTableConfig tableConfig, int bootstrapDays) {
    if (!tableMetadata.enabled()) {
      if (tableConfig.isMetadataTableAvailable()) {
        throw new RuntimeException("Can not initialize the table metadata");
      }
      log.info("Skip preloading partitioned RLI records because table metadata is not initialized, taskId = {}", taskID);
      waitForBootstrapReady(taskID);
      closeMetadataTable();
      return;
    }

    if (!tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
      log.info("Skip preloading partitioned RLI records because record index is not available yet, taskId = {}", taskID);
      waitForBootstrapReady(taskID);
      closeMetadataTable();
      return;
    }

    Map<String, List<FileSlice>> partitionedFileGroups =
        tableMetadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
    List<String> partitionsInWindow = filterPartitionsInWindow(partitionedFileGroups.keySet(), bootstrapDays);

    log.info("Start preloading partitioned RLI records from metadata table for {}/{} partitions within the last {} days, "
            + "taskId = {}, parallelism = {}",
        partitionsInWindow.size(), partitionedFileGroups.size(), bootstrapDays, taskID, parallelism);

    long startTime = System.currentTimeMillis();
    for (String partitionPath : partitionsInWindow) {
      preLoadPartition(partitionPath, partitionedFileGroups.get(partitionPath), taskID);
    }
    long costMs = System.currentTimeMillis() - startTime;
    log.info("Finish preloading partitioned RLI records, total records: {}, cost: {} ms, taskId = {}", loadedCnt, costMs, taskID);

    // Wait for other tasks to complete
    waitForBootstrapReady(taskID);

    // Cleanup resources
    closeMetadataTable();
  }

  private void preLoadPartition(String partitionPath, List<FileSlice> fileSlices, int taskID) {
    List<FileSlice> filteredFileSlices = new ArrayList<>();
    for (int i = 0; i < fileSlices.size(); i++) {
      if (shouldLoadBucket(partitionPath, fileSlices.size(), i, taskID)) {
        filteredFileSlices.add(fileSlices.get(i));
      }
    }
    if (filteredFileSlices.isEmpty()) {
      return;
    }
    log.info("Subtask: {} will preload partition {} from file groups: {}, total file groups: {}.",
        taskID, partitionPath, filteredFileSlices.stream().map(FileSlice::getFileId).collect(Collectors.joining(",")),
        fileSlices.size());

    // readRecordIndexLocations() discovers the full set of RLI file slices internally and passes it to
    // the filter; the filter here ignores that argument and substitutes the file slices already scoped
    // to this data partition, mirroring RecordLevelIndexBackend#bootstrapPartition.
    SerializableFunctionUnchecked<List<FileSlice>, List<FileSlice>> fileSlicesFilter = fileSlicesToFilter -> filteredFileSlices;
    HoodiePairData<String, HoodieRecordGlobalLocation> rliData = tableMetadata.readRecordIndexLocations(fileSlicesFilter);
    rliData.forEach(locationPair -> emitIndexRecord(partitionPath, locationPair.getLeft(), locationPair.getRight()));
  }

  private void emitIndexRecord(String partitionPath, String recordKey, HoodieRecordGlobalLocation location) {
    output.collect(new StreamRecord<>(
        new HoodieFlinkInternalRow(
            recordKey,
            partitionPath,
            location.getFileId(),
            String.valueOf(location.getInstantTime()))));
    loadedCnt += 1;
  }

  /**
   * Determines if the given file group should be loaded by this task, using the same
   * partition-aware assignment as the write path (see {@link BucketIndexUtil#getPartitionIndexFunc}),
   * so that each file group is bootstrapped by the same task that owns it during writes.
   */
  @VisibleForTesting
  boolean shouldLoadBucket(String partitionPath, int fileGroupCount, int fileGroupIdx, int taskID) {
    return partitionIndexFunc.apply(fileGroupCount, partitionPath, fileGroupIdx) == taskID;
  }

  /**
   * Filters the data table partitions whose partition path can be parsed as a date within the last
   * {@code bootstrapDays} days, inclusive of today.
   */
  @VisibleForTesting
  List<String> filterPartitionsInWindow(Iterable<String> partitionPaths, int bootstrapDays) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
        conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_DAY));
    boolean hiveStylePartitioning = conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING);
    LocalDate today = conf.get(FlinkOptions.WRITE_UTC_TIMEZONE) ? LocalDate.now(ZoneOffset.UTC) : LocalDate.now();
    LocalDate cutoff = today.minusDays(bootstrapDays);

    List<String> partitionsInWindow = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      LocalDate partitionDate = StreamerUtil.parsePartitionDate(partitionPath, formatter, hiveStylePartitioning);
      if (partitionDate != null && partitionDate.isAfter(cutoff) && !partitionDate.isAfter(today)) {
        partitionsInWindow.add(partitionPath);
      }
    }
    return partitionsInWindow;
  }

  private void closeMetadataTable() {
    if (tableMetadata != null) {
      try {
        tableMetadata.close();
      } catch (Exception e) {
        log.warn("Failed to close metadata table", e);
      }
      tableMetadata = null;
    }
  }
}
