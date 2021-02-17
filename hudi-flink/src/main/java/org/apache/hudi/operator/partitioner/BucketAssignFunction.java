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

package org.apache.hudi.operator.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.operator.FlinkOptions;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>All the records are tagged with HoodieRecordLocation, instead of real instant time,
 * INSERT record uses "I" and UPSERT record uses "U" as instant time. There is no need to keep
 * the "real" instant time for each record, the bucket ID (partition path & fileID) actually decides
 * where the record should write to. The "I" and "U" tag is only used for downstream to decide whether
 * the data bucket is a INSERT or a UPSERT, we should factor the it out when the underneath writer
 * supports specifying the bucket type explicitly.
 *
 * <p>The output records should then shuffle by the bucket ID and thus do scalable write.
 *
 * @see BucketAssigner
 */
public class BucketAssignFunction<K, I, O extends HoodieRecord<?>>
    extends KeyedProcessFunction<K, I, O>
    implements CheckpointedFunction, CheckpointListener {

  private MapState<HoodieKey, HoodieRecordLocation> indexState;

  private BucketAssigner bucketAssigner;

  private final Configuration conf;

  private final boolean isChangingRecords;

  public BucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    HoodieWriteConfig writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(StreamerUtil.getHadoopConf()),
            new FlinkTaskContextSupplier(getRuntimeContext()));
    this.bucketAssigner = new BucketAssigner(
        context,
        writeConfig);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    // no operation
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    MapStateDescriptor<HoodieKey, HoodieRecordLocation> indexStateDesc =
        new MapStateDescriptor<>(
            "indexState",
            TypeInformation.of(HoodieKey.class),
            TypeInformation.of(HoodieRecordLocation.class));
    indexState = context.getKeyedStateStore().getMapState(indexStateDesc);
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
    this.bucketAssigner.reset();
    this.bucketAssigner.refreshTable();
  }
}
