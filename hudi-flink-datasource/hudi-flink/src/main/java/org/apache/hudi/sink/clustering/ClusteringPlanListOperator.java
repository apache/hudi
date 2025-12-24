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

package org.apache.hudi.sink.clustering;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.cluster.strategy.LsmBaseClusteringPlanStrategy;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.FsCacheCleanUtil;

import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Set;

public class ClusteringPlanListOperator extends AbstractStreamOperator<ClusteringFileEvent>
    implements OneInputStreamOperator<ClusteringPartitionEvent, ClusteringFileEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanListOperator.class);
  private final Configuration conf;
  private final RowType rowType;
  private HoodieWriteConfig hoodieConfig;
  private NonThrownExecutor executor;
  private Schema schema;
  private transient StreamRecordCollector<ClusteringFileEvent> collector;

  public ClusteringPlanListOperator(Configuration conf, RowType rowType) {
    this.conf = conf;
    this.rowType = BulkInsertWriterHelper.addMetadataFields(rowType, false);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.hoodieConfig = FlinkWriteClients.getHoodieClientConfig(conf, true);
    this.executor = NonThrownExecutor.builder(LOG).build();
    this.schema = AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(rowType));
    this.collector = new StreamRecordCollector<>(output);
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ClusteringFileEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void processElement(StreamRecord<ClusteringPartitionEvent> record) {
    executor.execute(() -> {
      HoodieFlinkTable<?> table = FlinkTables.createTable(conf, getRuntimeContext());
      LsmBaseClusteringPlanStrategy clusteringPlanStrategy = new LsmBaseClusteringPlanStrategy(table, table.getContext(), hoodieConfig, schema);
      ClusteringPartitionEvent event = record.getValue();
      String partitionPath = event.getPartitionPath();
      Pair<List<FileSlice>, Set<String>> res = clusteringPlanStrategy.getFilesEligibleForClustering(partitionPath);
      List<FileSlice> fileSlices = res.getLeft();
      fileSlices.stream().filter(fileSlice -> !fileSlice.isFileSliceEmpty()).forEach(fileSlice -> {
        String fileId = fileSlice.getFileId();
        fileSlice.getLogFiles().forEach(logfile -> {
          HoodieLSMLogFile lsmLogFile = (HoodieLSMLogFile) logfile;
          collector.collect(new ClusteringFileEvent(lsmLogFile, lsmLogFile.getFileId(), partitionPath, lsmLogFile.getFileStatus()));
        });
        // 发送一个ClusteringEndFileEvent 代表当前Slice全部发送完了
        collector.collect(new ClusteringEndFileEvent(null, fileId, partitionPath, event.getMissingAndCompletedInstants()));
      });
    }, "Get Files Eligible ForClustering");
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // no op
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    // no op
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringFileEvent>> output) {
    this.output = output;
  }

  @Override
  public void close() throws Exception {
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }
}