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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>All the records are tagged with HoodieRecordLocation, instead of real instant time,
 * INSERT record uses "I" and UPSERT record uses "U" as instant time. There is no need to keep
 * the "real" instant time for each record, the bucket ID (partition path & fileID) actually decides
 * where the record should write to. The "I" and "U" tags are only used for downstream to decide whether
 * the data bucket is an INSERT or an UPSERT, we should factor the tags out when the underneath writer
 * supports specifying the bucket type explicitly.
 *
 * <p>The output records should then shuffle by the bucket ID and thus do scalable write.
 *
 * @see BucketAssigner
 */
public class BucketAssignFunction<K, I, O extends HoodieRecord<?>>
    extends KeyedProcessFunction<K, I, O>
    implements CheckpointedFunction, CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(BucketAssignFunction.class);

  private HoodieFlinkEngineContext context;

  /**
   * Index cache(speed-up) state for the underneath file based(BloomFilter) indices.
   * When a record came in, we do these check:
   *
   * <ul>
   *   <li>Try to load all the records in the partition path where the record belongs to</li>
   *   <li>Checks whether the state contains the record key</li>
   *   <li>If it does, tag the record with the location</li>
   *   <li>If it does not, use the {@link BucketAssigner} to generate a new bucket ID</li>
   * </ul>
   */
  private MapState<HoodieKey, HoodieRecordLocation> indexState;

  /**
   * Bucket assigner to assign new bucket IDs or reuse existing ones.
   */
  private BucketAssigner bucketAssigner;

  private final Configuration conf;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private final boolean isChangingRecords;

  /**
   * All the partition paths when the task starts. It is used to help checking whether all the partitions
   * are loaded into the state.
   */
  private transient Set<String> initialPartitionsToLoad;

  /**
   * State to book-keep which partition is loaded into the index state {@code indexState}.
   */
  private MapState<String, Integer> partitionLoadState;

  /**
   * Whether all partitions are loaded, if it is true,
   * we can only check the state for locations.
   */
  private boolean allPartitionsLoaded = false;

  public BucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    HoodieWriteConfig writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
    this.hadoopConf = StreamerUtil.getHadoopConf();
    this.context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(this.hadoopConf),
        new FlinkTaskContextSupplier(getRuntimeContext()));
    this.bucketAssigner = BucketAssigners.create(
        HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)),
        context,
        writeConfig);

    // initialize and check the partitions load state
    loadInitialPartitions();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    this.bucketAssigner.reset();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    MapStateDescriptor<HoodieKey, HoodieRecordLocation> indexStateDesc =
        new MapStateDescriptor<>(
            "indexState",
            TypeInformation.of(HoodieKey.class),
            TypeInformation.of(HoodieRecordLocation.class));
    indexState = context.getKeyedStateStore().getMapState(indexStateDesc);
    MapStateDescriptor<String, Integer> partitionLoadStateDesc =
        new MapStateDescriptor<>("partitionLoadState", Types.STRING, Types.INT);
    partitionLoadState = context.getKeyedStateStore().getMapState(partitionLoadStateDesc);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void processElement(I value, Context ctx, Collector<O> out) throws Exception {
    // 1. put the record into the BucketAssigner;
    // 2. look up the state for location, if the record has a location, just send it out;
    // 3. if it is an INSERT, decide the location using the BucketAssigner then send it out.
    HoodieRecord<?> record = (HoodieRecord<?>) value;
    final HoodieKey hoodieKey = record.getKey();
    final BucketInfo bucketInfo;
    final HoodieRecordLocation location;
    if (!allPartitionsLoaded
        && initialPartitionsToLoad.contains(hoodieKey.getPartitionPath()) // this is an existing partition
        && !partitionLoadState.contains(hoodieKey.getPartitionPath())) {
      // If the partition records are never loaded, load the records first.
      loadRecords(hoodieKey.getPartitionPath());
    }
    // Only changing records need looking up the index for the location,
    // append only records are always recognized as INSERT.
    if (isChangingRecords && this.indexState.contains(hoodieKey)) {
      // Set up the instant time as "U" to mark the bucket as an update bucket.
      location = new HoodieRecordLocation("U", this.indexState.get(hoodieKey).getFileId());
      this.bucketAssigner.addUpdate(record.getPartitionPath(), location.getFileId());
    } else {
      bucketInfo = this.bucketAssigner.addInsert(hoodieKey.getPartitionPath());
      switch (bucketInfo.getBucketType()) {
        case INSERT:
          // This is an insert bucket, use HoodieRecordLocation instant time as "I".
          // Downstream operators can then check the instant time to know whether
          // a record belongs to an insert bucket.
          location = new HoodieRecordLocation("I", bucketInfo.getFileIdPrefix());
          break;
        case UPDATE:
          location = new HoodieRecordLocation("U", bucketInfo.getFileIdPrefix());
          break;
        default:
          throw new AssertionError();
      }
      this.indexState.put(hoodieKey, location);
    }
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();
    out.collect((O) record);
  }

  @Override
  public void notifyCheckpointComplete(long l) {
    // Refresh the table state when there are new commits.
    this.bucketAssigner.refreshTable();
    checkPartitionsLoaded();
  }

  /**
   * Load all the indices of give partition path into the backup state.
   *
   * @param partitionPath The partition path
   * @throws Exception when error occurs for state update
   */
  private void loadRecords(String partitionPath) throws Exception {
    HoodieTable<?, ?, ?, ?> hoodieTable = bucketAssigner.getTable();
    List<HoodieBaseFile> latestBaseFiles =
        HoodieIndexUtils.getLatestBaseFilesForPartition(partitionPath, hoodieTable);
    for (HoodieBaseFile baseFile : latestBaseFiles) {
      List<HoodieKey> hoodieKeys =
          ParquetUtils.fetchRecordKeyPartitionPathFromParquet(hadoopConf, new Path(baseFile.getPath()));
      hoodieKeys.forEach(hoodieKey -> {
        try {
          this.indexState.put(hoodieKey, new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId()));
        } catch (Exception e) {
          throw new HoodieIOException("Error when load record keys from file: " + baseFile);
        }
      });
    }
    // Mark the partition path as loaded.
    partitionLoadState.put(partitionPath, 0);
  }

  /**
   * Loads the existing partitions for this task.
   */
  private void loadInitialPartitions() {
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(this.context,
        this.conf.getString(FlinkOptions.PATH), false, false, false);
    final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
    final int taskID = getRuntimeContext().getIndexOfThisSubtask();
    // reference: org.apache.flink.streaming.api.datastream.KeyedStream
    this.initialPartitionsToLoad = allPartitionPaths.stream()
        .filter(partition -> KeyGroupRangeAssignment.assignKeyToParallelOperator(partition, maxParallelism, parallelism) == taskID)
        .collect(Collectors.toSet());
  }

  /**
   * Checks whether all the partitions of the table are loaded into the state,
   * set the flag {@code allPartitionsLoaded} to true if it is.
   */
  private void checkPartitionsLoaded() {
    for (String partition : this.initialPartitionsToLoad) {
      try {
        if (!this.partitionLoadState.contains(partition)) {
          return;
        }
      } catch (Exception e) {
        LOG.warn("Error when check whether all partitions are loaded, ignored", e);
        throw new HoodieException(e);
      }
    }
    this.allPartitionsLoaded = true;
  }

  @VisibleForTesting
  public boolean isAllPartitionsLoaded() {
    return this.allPartitionsLoaded;
  }

  @VisibleForTesting
  public void clearIndexState() {
    this.allPartitionsLoaded = false;
    this.indexState.clear();
    loadInitialPartitions();
  }

  @VisibleForTesting
  public boolean isKeyInState(HoodieKey hoodieKey) {
    try {
      return this.indexState.contains(hoodieKey);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }
}
