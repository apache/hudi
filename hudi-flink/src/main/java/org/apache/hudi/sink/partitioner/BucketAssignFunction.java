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

  private final boolean bootstrapIndex;

  /**
   * State to book-keep which partition is loaded into the index state {@code indexState}.
   */
  private MapState<String, Integer> partitionLoadState;

  public BucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
    this.bootstrapIndex = conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED);
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
        getRuntimeContext().getIndexOfThisSubtask(),
        getRuntimeContext().getNumberOfParallelSubtasks(),
        HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)),
        context,
        writeConfig);
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
    if (bootstrapIndex) {
      MapStateDescriptor<String, Integer> partitionLoadStateDesc =
          new MapStateDescriptor<>("partitionLoadState", Types.STRING, Types.INT);
      partitionLoadState = context.getKeyedStateStore().getMapState(partitionLoadStateDesc);
    }
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

    // The dataset may be huge, thus the processing would block for long,
    // disabled by default.
    if (bootstrapIndex && !partitionLoadState.contains(hoodieKey.getPartitionPath())) {
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
  }

  /**
   * Load all the indices of give partition path into the backup state.
   *
   * @param partitionPath The partition path
   * @throws Exception when error occurs for state update
   */
  private void loadRecords(String partitionPath) throws Exception {
    LOG.info("Start loading records under partition {} into the index state", partitionPath);
    HoodieTable<?, ?, ?, ?> hoodieTable = bucketAssigner.getTable();
    List<HoodieBaseFile> latestBaseFiles =
        HoodieIndexUtils.getLatestBaseFilesForPartition(partitionPath, hoodieTable);
    final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
    final int taskID = getRuntimeContext().getIndexOfThisSubtask();
    for (HoodieBaseFile baseFile : latestBaseFiles) {
      final List<HoodieKey> hoodieKeys;
      try {
        hoodieKeys =
            ParquetUtils.fetchRecordKeyPartitionPathFromParquet(hadoopConf, new Path(baseFile.getPath()));
      } catch (Exception e) {
        // in case there was some empty parquet file when the pipeline
        // crushes exceptionally.
        LOG.error("Error when loading record keys from file: {}", baseFile);
        continue;
      }
      hoodieKeys.forEach(hoodieKey -> {
        try {
          // Reference: org.apache.flink.streaming.api.datastream.KeyedStream,
          // the input records is shuffled by record key
          boolean shouldLoad = KeyGroupRangeAssignment.assignKeyToParallelOperator(
              hoodieKey.getRecordKey(), maxParallelism, parallelism) == taskID;
          if (shouldLoad) {
            this.indexState.put(hoodieKey, new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId()));
          }
        } catch (Exception e) {
          LOG.error("Error when putting record keys into the state from file: {}", baseFile);
        }
      });
    }
    // Mark the partition path as loaded.
    partitionLoadState.put(partitionPath, 0);
    LOG.info("Finish loading records under partition {} into the index state", partitionPath);
  }

  @VisibleForTesting
  public void clearIndexState() {
    this.indexState.clear();
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
